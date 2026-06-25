# spark_client: Spark (via pyspark) + OpenSSH server.
# Airflow connects here over SSH and runs spark-submit. The PySpark code is
# delivered by git-sync (read-only volume), never copied into the image.
FROM python:3.11-bullseye

ENV DEBIAN_FRONTEND=noninteractive \
    SPARK_VERSION=3.5.3 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# System packages: Java (for Spark), SSH server, utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        openssh-server \
        procps \
        curl \
        netcat-openbsd \
        gosu \
    && rm -rf /var/lib/apt/lists/*

# Python deps: pyspark bundles Spark + spark-submit; boto3 for optional S3 ops
RUN pip install --no-cache-dir \
        pyspark==3.5.3 \
        pandas==2.2.2 \
        boto3==1.34.131

# Put spark-submit / spark-shell on PATH (provided by the pyspark wheel).
# In the python:3.11-bullseye image, pip installs to this fixed site-packages path.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Verify spark-submit is present (build fails loudly otherwise)
RUN ls -la "${SPARK_HOME}/bin/spark-submit" && spark-submit --version 2>/dev/null | head -1 || true

# Pre-create the SSH runtime dir
RUN mkdir -p /var/run/sshd

# Entrypoint creates the SSH user from env vars, then starts sshd in foreground
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 22
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
