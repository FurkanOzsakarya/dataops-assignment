# DataOps Assignment — Store Transactions Cleaning Pipeline

Günlük gelen `dirty_store_transactions.csv` veri setini nesne depodan (RustFS)
okuyup temizleyen ve PostgreSQL `traindb` veritabanına **full load** ile yazan
uçtan uca veri mühendisliği pipeline'ı.

Kullanılan teknolojiler: **RustFS** (S3 uyumlu nesne depo), **Apache Airflow 3**,
**PySpark**, **PostgreSQL**, **git-sync** ve **GitHub Actions** (CI/CD).

---

## Mimari ve akış

```
                         ┌─────────────┐
   GitHub repo (main) ──►│  git-sync   │── repo'yu ortak volume'a çeker (CI)
        ▲                └──────┬──────┘
        │ PR (dev→main)        │ /git/repo  (dags + spark_apps)
        │                ┌─────┴──────────────┐
  GitHub Actions         ▼                    ▼
  (merge'de tetikler) ┌────────┐         ┌──────────────┐
        └────────────►│Airflow3│  SSH    │ spark_client │  spark-submit
                      │ (DAG)  ├────────►│ (PySpark)    │
                      └────────┘         └──────┬───────┘
                                                │  read s3a://dataops-bronze
                              ┌─────────┐       │  write JDBC (full load)
                              │ RustFS  │◄──────┤
                              │ (S3)    │       ▼
                              └─────────┘   ┌──────────────┐
                                            │ PostgreSQL   │
                                            │ traindb.     │
                                            │ public.      │
                                            │ clean_data_  │
                                            │ transactions │
                                            └──────────────┘
```

1. Ham veri RustFS `dataops-bronze` bucket'ına `raw/dirty_store_transactions.csv` olarak yüklenir.
2. Geliştirme `dev` branch'inde yapılır, `main`'e **Pull Request** ile merge edilir.
3. `main`'e merge → **git-sync** repo'yu (DAG + Spark kodu) ortak volume'a çeker (**CI**).
   Kod hiçbir container'a **elle kopyalanmaz**.
4. **GitHub Actions** merge'de Airflow REST API'sini çağırıp DAG'i tetikler (**CD**).
5. Airflow DAG'i **SSHOperator** ile `spark_client` container'ında `spark-submit` çalıştırır.
6. PySpark işi RustFS'ten okur, temizler, PostgreSQL'e **full load** yazar.

---

## Klasör yapısı

```
dataops-assignment/
├── docker-compose.yaml              # Tüm stack
├── .env.example                     # Ortam değişkenleri şablonu
├── dags/
│   └── store_transactions_dag.py    # Airflow DAG (SSHOperator)
├── spark_apps/
│   ├── clean_store_transactions.py  # PySpark temizleme işi
│   └── submit_clean.sh              # spark-submit sarmalayıcı (spark_client'ta çalışır)
├── scripts/
│   ├── upload_to_rustfs.py          # Veriyi bronze bucket'a yükler
│   └── requirements.txt
├── docker/
│   └── spark_client/                # Spark + SSH imajı
│       ├── Dockerfile
│       └── entrypoint.sh
├── .github/workflows/
│   └── trigger-airflow.yml          # CD: main'e merge'de DAG tetikler
└── data/
    └── sample_dirty_store_transactions.csv  # Yalnızca lokal test için örnek
```

---

## Veri temizleme kuralları (`clean_store_transactions.py`)

| Sorun | Çözüm |
|------|-------|
| `STORE_LOCATION`'da çöp karakterler (`New York(`, `New York+`, `New York"""`) | Harf/rakam/boşluk dışındaki karakterler silinir, boşluklar tek boşluğa indirgenir, trim |
| `PRODUCT_ID`'de sondaki çöp harf/sembol (`72619323C`, `87566223^`) | Sadece rakamlar tutulur |
| `MRP`, `CP`, `DISCOUNT`, `SP`'de `$` ön eki | `$` ve sayısal olmayan karakterler silinir, `double`'a cast edilir |
| `Date` metin | `yyyy-MM-dd` formatında `date` tipine çevrilir |
| Boş/eksik kritik alan (`store_id`, `product_id`) | İlgili satırlar düşürülür |
| Tekrar eden (duplicate) satırlar | `dropDuplicates()` ile kaldırılır |

Hedef tablo: `traindb.public.clean_data_transactions` (yazım modu: **overwrite = full load**).

---

## Çalıştırma adımları

### 0. Ön koşul
Docker ve Docker Compose kurulu olmalı. İnternet erişimi gerekir (imaj indirme,
PySpark `--packages` JAR indirme).

### 1. Ortam değişkenleri
```bash
cd dataops-assignment
cp .env.example .env
# .env içindeki GITSYNC_REPO değerini KENDİ GitHub repo URL'inle değiştir:
#   GITSYNC_REPO=https://github.com/<kullanıcı>/dataops-assignment.git
# Linux'ta AIRFLOW_UID değerini ayarla:  echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 2. Stack'i başlat
```bash
docker compose up -d --build
```
Servisler: `rustfs` (9000/9001), `postgres` (5432), `airflow-apiserver` (8080),
`spark_client` (ssh 2222), `git-sync`, `airflow-*`.

### 3. Veriyi bronze bucket'a yükle
```bash
pip install -r scripts/requirements.txt
S3_ENDPOINT_URL=http://localhost:9000 python scripts/upload_to_rustfs.py
```
RustFS konsolu: http://localhost:9001 (kullanıcı/şifre `.env`'deki access/secret key).

### 4. Airflow arayüzü
http://localhost:8080 (varsayılan `admin` / `admin`). `store_transactions_clean_pipeline`
DAG'ini görmelisin. Elle tetiklemek için DAG'i "Trigger" et; ya da `main`'e bir commit
merge ederek GitHub Actions üzerinden otomatik tetikle.

### 5. Sonucu doğrula
```bash
docker exec -it postgres psql -U train -d traindb \
  -c "SELECT COUNT(*) FROM public.clean_data_transactions;"
docker exec -it postgres psql -U train -d traindb \
  -c "SELECT * FROM public.clean_data_transactions LIMIT 10;"
```

---

## Git iş akışı (dev → main)

```bash
git init && git add . && git commit -m "Initial DataOps pipeline"
git branch -M main
git remote add origin https://github.com/<kullanıcı>/dataops-assignment.git
git push -u origin main

git checkout -b dev
# ... geliştirme ...
git add . && git commit -m "feature: ..." && git push -u origin dev
# GitHub'da dev -> main Pull Request aç, review/approve sonrası merge et.
```

---

## ⚠️ Senin elle yapman gereken adımlar

1. **GitHub repo URL'i**: `.env` içindeki `GITSYNC_REPO`'yu kendi repo URL'inle değiştir.
   (Repo private ise `GITSYNC_USERNAME` + `GITSYNC_PASSWORD` token'ını da gir.)
2. **GitHub Actions secret'ları** (Settings → Secrets and variables → Actions):
   - `AIRFLOW_BASE_URL` — Airflow API server'ının **public** adresi.
   - `AIRFLOW_USERNAME` (varsayılan `admin`), `AIRFLOW_PASSWORD` (varsayılan `admin`).
3. **Airflow'u internete açma**: GitHub Actions runner'ı lokal makinene erişemez.
   `main`'e merge'de otomatik tetikleme için 8080 portunu bir tünelle dışarı aç:
   ```bash
   ngrok http 8080          # veya: cloudflared tunnel --url http://localhost:8080
   ```
   Çıkan public URL'i `AIRFLOW_BASE_URL` secret'ına yaz. (Bu adım olmadan pipeline
   yine çalışır; sadece "merge'de otomatik tetikleme" devreye girmez — Airflow UI'dan
   elle tetikleyebilirsin.)
4. **Branch protection (opsiyonel ama yönergede isteniyor)**: GitHub'da `main` için
   "Require a pull request before merging" + "Require approvals" kuralını aç.

---

## Notlar
- Kod (DAG + Spark) container'lara **git-sync** ile gelir; elle kopyalama yoktur (yönerge gereği).
- PySpark işi `spark_client` içinde çalışır; Airflow yalnızca **SSHOperator** ile tetikler.
- Veri küçük olduğu için her tetiklemede **full load** (overwrite) yapılır.
