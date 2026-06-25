#!/usr/bin/env bash
#
# submit_clean.sh
# ----------------
# Wrapper that runs inside the `spark_client` container and is invoked by the
# Airflow SSHOperator. It launches the PySpark cleaning job via spark-submit
# with all the JARs needed to talk to RustFS (S3A) and PostgreSQL (JDBC).
#
# The application code itself is delivered to this container by git-sync
# (NOT copied manually), and lives under the synced repo path.
#
set -euo pipefail

# --- Make the script self-sufficient over SSH ---------------------------------
# An SSH non-interactive session does NOT inherit the container's docker ENV
# (sshd resets the environment), so set JAVA_HOME / SPARK_HOME / PATH here.
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export SPARK_HOME="${SPARK_HOME:-/usr/local/lib/python3.11/site-packages/pyspark}"
export PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"
# Ivy/Spark need a writable home for the --packages cache and tmp dirs.
export HOME="${HOME:-/tmp}"
# -----------------------------------------------------------------------------

# Path to the synced repo inside spark_client (git-sync mounts the volume here).
APP_DIR="${APP_DIR:-/opt/dataops/repo/spark_apps}"
APP="${APP_DIR}/clean_store_transactions.py"

# Maven coordinates for the connectors. Versions match Spark 3.5 / Hadoop 3.3.
PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
org.postgresql:postgresql:42.7.3"

echo "[submit_clean] Using application: ${APP}"

exec spark-submit \
  --master "local[*]" \
  --packages "${PACKAGES}" \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT_URL:-http://rustfs:9000}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  "${APP}"
