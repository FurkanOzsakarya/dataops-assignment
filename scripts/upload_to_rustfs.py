"""
upload_to_rustfs.py
-------------------
Downloads the dirty_store_transactions.csv dataset and uploads it to the RustFS
(S3-compatible) `dataops-bronze` bucket as `raw/dirty_store_transactions.csv`.

Creates the bucket if it does not exist. Idempotent: safe to re-run.

Usage:
    pip install boto3 requests
    python scripts/upload_to_rustfs.py

All settings can be overridden via environment variables (see .env.example).
"""

import io
import os
import sys

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError

DATASET_URL = (
    "https://raw.githubusercontent.com/erkansirin78/datasets/"
    "refs/heads/master/dirty_store_transactions.csv"
)

ENDPOINT   = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY", os.getenv("AWS_ACCESS_KEY_ID", "rustfsadmin"))
SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "rustfsadmin"))
BUCKET     = os.getenv("BRONZE_BUCKET", "dataops-bronze")
OBJECT_KEY = os.getenv("RAW_OBJECT_KEY", "raw/dirty_store_transactions.csv")
# Optional local file fallback (skip the download)
LOCAL_FILE = os.getenv("LOCAL_DATASET_PATH")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"[ok] Bucket '{bucket}' already exists.")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"[ok] Created bucket '{bucket}'.")


def load_dataset_bytes() -> bytes:
    if LOCAL_FILE:
        print(f"[info] Reading local dataset: {LOCAL_FILE}")
        with open(LOCAL_FILE, "rb") as fh:
            return fh.read()
    print(f"[info] Downloading dataset from {DATASET_URL}")
    resp = requests.get(DATASET_URL, timeout=60)
    resp.raise_for_status()
    return resp.content


def main():
    data = load_dataset_bytes()
    print(f"[info] Dataset size: {len(data)} bytes")

    s3 = get_s3_client()
    ensure_bucket(s3, BUCKET)

    s3.upload_fileobj(io.BytesIO(data), BUCKET, OBJECT_KEY)
    print(f"[done] Uploaded -> s3://{BUCKET}/{OBJECT_KEY}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
