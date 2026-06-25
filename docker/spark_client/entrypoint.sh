#!/usr/bin/env bash
set -euo pipefail

SSH_USER="${SPARK_SSH_USER:-train}"
SSH_PASS="${SPARK_SSH_PASSWORD:-Ankara06}"

# Create the SSH user on first start
if ! id "${SSH_USER}" >/dev/null 2>&1; then
    useradd -m -s /bin/bash "${SSH_USER}"
    echo "${SSH_USER}:${SSH_PASS}" | chpasswd
    # Make environment variables available to non-login SSH command sessions
    {
        echo "export JAVA_HOME=${JAVA_HOME}"
        echo "export S3_ENDPOINT_URL=${S3_ENDPOINT_URL:-http://rustfs:9000}"
        echo "export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-rustfsadmin}"
        echo "export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-rustfsadmin}"
        echo "export BRONZE_BUCKET=${BRONZE_BUCKET:-dataops-bronze}"
        echo "export RAW_OBJECT_KEY=${RAW_OBJECT_KEY:-raw/dirty_store_transactions.csv}"
        echo "export POSTGRES_HOST=${POSTGRES_HOST:-postgres}"
        echo "export POSTGRES_PORT=${POSTGRES_PORT:-5432}"
        echo "export POSTGRES_DB=${POSTGRES_DB:-traindb}"
        echo "export POSTGRES_USER=${POSTGRES_USER:-train}"
        echo "export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-Ankara06}"
        echo "export POSTGRES_SCHEMA=${POSTGRES_SCHEMA:-public}"
        echo "export TARGET_TABLE=${TARGET_TABLE:-clean_data_transactions}"
    } >> "/home/${SSH_USER}/.bashrc"
fi

# Allow password authentication
sed -i 's/#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config
sed -i 's/#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config

# Generate host keys if missing
ssh-keygen -A

echo "[spark_client] SSH ready for user '${SSH_USER}'. Starting sshd..."
exec /usr/sbin/sshd -D -e
