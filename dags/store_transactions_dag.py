"""
store_transactions_dag.py
-------------------------
Airflow 3 DAG that orchestrates the cleaning pipeline for the daily store
transactions dataset.

Flow:
  1. (optional) ensure the raw object exists in the RustFS bronze bucket.
  2. Run the PySpark cleaning job ON the `spark_client` container via SSHOperator
     (spark-submit). The Python/PySpark code is NOT shipped by Airflow — it is
     delivered to spark_client by git-sync, exactly like the DAG file itself is
     delivered to Airflow by git-sync.

This DAG is parameterised entirely through Airflow Variables / Connections so
no secrets live in the repository:
  - Connection `spark_ssh`  -> SSH access to the spark_client container
    (provided via the AIRFLOW_CONN_SPARK_SSH env var in docker-compose).

Triggering:
  - schedule is None; the run is triggered automatically by a GitHub Actions
    workflow (.github/workflows/trigger-airflow.yml) whenever code is merged
    into the `main` branch. It can also be triggered manually from the UI.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}

# Command executed inside spark_client. git-sync keeps the repo at REPO_DIR.
REPO_DIR = "/opt/dataops/repo"
RUN_CLEAN_CMD = (
    f"cd {REPO_DIR} && "
    f"APP_DIR={REPO_DIR}/spark_apps "
    f"bash {REPO_DIR}/spark_apps/submit_clean.sh"
)

with DAG(
    dag_id="store_transactions_clean_pipeline",
    description="Clean dirty_store_transactions from RustFS and full-load into PostgreSQL",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,                 # triggered by CI (GitHub Actions) or manually
    catchup=False,
    max_active_runs=1,
    tags=["dataops", "spark", "rustfs", "postgres"],
) as dag:

    run_spark_clean = SSHOperator(
        task_id="run_pyspark_clean_on_spark_client",
        ssh_conn_id="spark_ssh",
        command=RUN_CLEAN_CMD,
        cmd_timeout=1800,
        conn_timeout=60,
        get_pty=True,
    )

    run_spark_clean
