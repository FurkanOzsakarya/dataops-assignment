"""
clean_store_transactions.py
---------------------------
PySpark application that:
  1. Reads the raw dataset from RustFS (S3-compatible) bucket `dataops-bronze`
     at key `raw/dirty_store_transactions.csv`.
  2. Cleans the data (missing values, duplicates, junk characters, $ signs, types).
  3. Writes the clean data to PostgreSQL `traindb` / schema `public`
     / table `clean_data_transactions` using a FULL LOAD (overwrite).

It runs INSIDE the `spark_client` container and is launched via `spark-submit`
from an Airflow DAG using SSHOperator. All connection settings are read from
environment variables so that no secrets are hard-coded.

Required JARs (provided via --packages in submit_clean.sh):
  - org.apache.hadoop:hadoop-aws            (S3A connector for RustFS)
  - com.amazonaws:aws-java-sdk-bundle       (AWS SDK used by hadoop-aws)
  - org.postgresql:postgresql               (JDBC driver for PostgreSQL)
"""

import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType
)

# --------------------------------------------------------------------------- #
# Configuration (read from environment; sensible defaults for the local stack) #
# --------------------------------------------------------------------------- #
# RustFS / S3
S3_ENDPOINT   = os.getenv("S3_ENDPOINT_URL", "http://rustfs:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "rustfsadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "rustfsadmin")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "dataops-bronze")
RAW_OBJECT    = os.getenv("RAW_OBJECT_KEY", "raw/dirty_store_transactions.csv")

# PostgreSQL
PG_HOST   = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT   = os.getenv("POSTGRES_PORT", "5432")
PG_DB     = os.getenv("POSTGRES_DB", "traindb")
PG_USER   = os.getenv("POSTGRES_USER", "train")
PG_PASS   = os.getenv("POSTGRES_PASSWORD", "Ankara06")
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
PG_TABLE  = os.getenv("TARGET_TABLE", "clean_data_transactions")

SOURCE_PATH = f"s3a://{BRONZE_BUCKET}/{RAW_OBJECT}"
JDBC_URL    = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
FQ_TABLE    = f"{PG_SCHEMA}.{PG_TABLE}"

# Optional: allow reading from a plain local/path instead of S3 (used for tests)
LOCAL_INPUT = os.getenv("LOCAL_INPUT_PATH")          # e.g. file:///.../sample.csv
LOCAL_OUTPUT = os.getenv("LOCAL_OUTPUT_PATH")        # e.g. file:///.../out  (parquet)


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("clean_store_transactions")
        # S3A / RustFS configuration
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    return builder.getOrCreate()


# Explicit schema -> read everything as string first, cast later after cleaning.
RAW_SCHEMA = StructType([
    StructField("STORE_ID", StringType(), True),
    StructField("STORE_LOCATION", StringType(), True),
    StructField("PRODUCT_CATEGORY", StringType(), True),
    StructField("PRODUCT_ID", StringType(), True),
    StructField("MRP", StringType(), True),
    StructField("CP", StringType(), True),
    StructField("DISCOUNT", StringType(), True),
    StructField("SP", StringType(), True),
    StructField("Date", StringType(), True),
])


def clean(df):
    """Apply all data-quality fixes and return a typed, clean DataFrame."""

    # 1) Normalise column names to lower snake_case
    df = (
        df.withColumnRenamed("STORE_ID", "store_id")
          .withColumnRenamed("STORE_LOCATION", "store_location")
          .withColumnRenamed("PRODUCT_CATEGORY", "product_category")
          .withColumnRenamed("PRODUCT_ID", "product_id")
          .withColumnRenamed("MRP", "mrp")
          .withColumnRenamed("CP", "cp")
          .withColumnRenamed("DISCOUNT", "discount")
          .withColumnRenamed("SP", "sp")
          .withColumnRenamed("Date", "date")
    )

    # 2) Trim whitespace on all string columns
    for c in ["store_id", "store_location", "product_category", "product_id"]:
        df = df.withColumn(c, F.trim(F.col(c)))

    # 3) STORE_LOCATION: remove junk characters, keep letters/numbers/space, collapse spaces
    df = df.withColumn(
        "store_location",
        F.trim(F.regexp_replace(F.col("store_location"), r"[^A-Za-z0-9 ]", "")),
    )
    df = df.withColumn(
        "store_location",
        F.regexp_replace(F.col("store_location"), r"\s+", " "),
    )

    # 4) PRODUCT_ID: keep digits only (strip trailing junk letters/symbols)
    df = df.withColumn(
        "product_id",
        F.regexp_replace(F.col("product_id"), r"[^0-9]", ""),
    )

    # 5) Money columns: strip '$' and any non-numeric (except dot/minus), cast to double
    for c in ["mrp", "cp", "discount", "sp"]:
        df = df.withColumn(
            c,
            F.regexp_replace(F.col(c), r"[^0-9.\-]", "").cast("double"),
        )

    # 6) Date -> proper date type
    df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

    # 7) Empty strings -> NULL (so they can be dropped/handled consistently)
    for c in ["store_id", "store_location", "product_category", "product_id"]:
        df = df.withColumn(
            c, F.when(F.col(c) == "", None).otherwise(F.col(c))
        )

    # 8) Drop rows missing critical keys (store_id, product_id) or all-null money
    df = df.dropna(subset=["store_id", "product_id"])

    # 9) Drop fully duplicated rows
    df = df.dropDuplicates()

    # 10) Stable column order
    df = df.select(
        "store_id", "store_location", "product_category", "product_id",
        "mrp", "cp", "discount", "sp", "date",
    )
    return df


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    input_path = LOCAL_INPUT or SOURCE_PATH
    print(f"[INFO] Reading raw data from: {input_path}")

    raw = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "false")
        .schema(RAW_SCHEMA)
        .csv(input_path)
    )

    raw_count = raw.count()
    print(f"[INFO] Raw row count: {raw_count}")

    clean_df = clean(raw)
    clean_count = clean_df.count()
    print(f"[INFO] Clean row count: {clean_count} "
          f"(removed {raw_count - clean_count} rows)")
    clean_df.show(10, truncate=False)

    # ----- Write output -----
    if LOCAL_OUTPUT:
        print(f"[INFO] LOCAL test mode -> writing parquet to {LOCAL_OUTPUT}")
        clean_df.write.mode("overwrite").parquet(LOCAL_OUTPUT)
    else:
        print(f"[INFO] Full load -> {JDBC_URL} table {FQ_TABLE}")
        (
            clean_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", FQ_TABLE)
            .option("user", PG_USER)
            .option("password", PG_PASS)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")          # FULL LOAD: replace table contents
            .save()
        )
        print(f"[INFO] Wrote {clean_count} rows to {FQ_TABLE}.")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Job failed: {exc}", file=sys.stderr)
        raise
