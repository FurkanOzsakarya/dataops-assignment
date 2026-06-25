#!/usr/bin/env bash
# Bir kerelik git kurulum yardımcısı.
# Kullanım:
#   1) GitHub'da BOŞ bir repo oluştur (README/lisans ekleme): dataops-assignment
#   2) Aşağıdaki REMOTE değerini kendi repo URL'inle değiştir
#   3) Bu klasörde:  bash scripts/git_bootstrap.sh
set -euo pipefail

REMOTE="https://github.com/FurkanOzsakarya/dataops-assignment.git"

git init
git add .
git commit -m "Initial DataOps pipeline (Airflow 3 + PySpark + RustFS + git-sync)"
git branch -M main
git remote add origin "${REMOTE}"
git push -u origin main

# Geliştirme branch'i
git checkout -b dev
git push -u origin dev

echo
echo ">> Sonraki adım: GitHub'da 'dev -> main' Pull Request aç,"
echo ">> review/approve sonrası merge et."
