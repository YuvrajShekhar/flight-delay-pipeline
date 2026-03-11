# =============================================================================
# Flight Delay Pipeline — Monthly Ingestion DAG
# =============================================================================
# Schedule: 1st of every month at 02:00 UTC
# Purpose:  Triggers the Kafka producer which reads the monthly US DOT flight
#           CSV file and publishes each record to the Kafka topic
#           'flight-records-raw'. A MinIO consumer then persists those records
#           to the raw data lake, partitioned by year and month.
#
# Pipeline stages orchestrated by this DAG:
#   1. check_source_file    — verify the monthly CSV is present in /data
#   2. run_kafka_producer   — publish CSV records to Kafka topic
#   3. verify_minio_landing — confirm partitioned objects landed in MinIO
#   4. log_pipeline_run     — write audit record to PostgreSQL pipeline_runs
#
# Reliability:
#   - 3 automatic retries with 5-minute exponential backoff on every task
#   - Email alerts on failure (configure SMTP in Airflow connections)
#   - Idempotent: re-running the DAG for the same month overwrites the same
#     MinIO partition path, producing no duplicates
# =============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import psycopg2

# ---------------------------------------------------------------------------
# Default arguments applied to every task in this DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Environment variables injected from docker-compose .env
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flight-records-raw")
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT","http://localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "flight-data")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
DATA_LOCAL_PATH = os.getenv("DATA_LOCAL_PATH", "/opt/airflow/data")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "flight_features")
POSTGRES_USER = os.getenv("POSTGRES_USER", "spark_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")


# ---------------------------------------------------------------------------
# Task 1: Verify the monthly source CSV exists before doing anything else
# ---------------------------------------------------------------------------
def check_source_file(**context):
    """
    Confirms that flights.csv is present in the data directory.
    Raises FileNotFoundError to fail the DAG early if the file is missing,
    preventing the Kafka producer from running against a missing source.
    """
    source_path = os.path.join(DATA_LOCAL_PATH, "flights.csv")
    if not os.path.exists(source_path):
        raise FileNotFoundError(
            f"Source file not found: {source_path}. "
            "Please ensure the dataset has been downloaded and placed in the data/ directory."
        )
    file_size_mb = os.path.getsize(source_path) / (1024 * 1024)
    print(f"Source file found: {source_path} ({file_size_mb:.1f} MB)")
    return source_path


# ---------------------------------------------------------------------------
# Task 3: Verify that MinIO received the records from the Kafka consumer
# ---------------------------------------------------------------------------
def verify_minio_landing(**context):
    """
    Checks that at least one object exists in the expected MinIO partition
    for the current execution month. Uses the MinIO Python SDK.
    Raises an exception if the partition is empty, triggering a retry.
    """
    from minio import Minio

    execution_date = context["execution_date"]
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    prefix = f"raw/year={year}/month={month}/"

    client = Minio(
        MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

    objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix))
    if not objects:
        raise ValueError(
            f"No objects found in MinIO at {MINIO_BUCKET}/{prefix}. "
            "Kafka consumer may not have written records yet."
        )
    print(f"MinIO landing verified: {len(objects)} object(s) found at {prefix}")


# ---------------------------------------------------------------------------
# Task 4: Write an audit record to PostgreSQL pipeline_runs table
# ---------------------------------------------------------------------------
def log_pipeline_run(**context):
    """
    Inserts a pipeline audit record into the pipeline_runs table in PostgreSQL.
    Records the DAG name, execution date, and completion status for
    observability and governance tracking.
    """
    execution_date = str(context.get("logical_date") or context.get("execution_date"))
    dag_id = context["dag"].dag_id

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                INSERT INTO pipeline_runs
                    (run_id, dag_id, execution_date, stage, status, completed_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (dag_id, execution_date, stage)
                DO UPDATE SET status = EXCLUDED.status, completed_at = NOW()
                """,
                (f"{dag_id}_{execution_date}", dag_id, execution_date, "monthly_ingestion", "success"),
            )
    finally:
        conn.close()
    print(f"Pipeline run logged: {dag_id} | {execution_date} | monthly_ingestion | success")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="monthly_ingestion_dag",
    description="Monthly ingestion: CSV → Kafka producer → MinIO raw data lake",
    schedule_interval="0 2 1 * *",   # 02:00 UTC on the 1st of every month
    start_date=datetime(2015, 1, 1),
    catchup=False,                    # do not backfill historical months
    default_args=default_args,
    tags=["flight-delay", "ingestion", "monthly"],
    max_active_runs=1,                # prevent concurrent monthly runs overlapping
) as dag:

    # Task 1: Verify source CSV exists
    t1_check_source = PythonOperator(
        task_id="check_source_file",
        python_callable=check_source_file,
        doc_md="""
        **check_source_file**
        Verifies that flights.csv is present in the data directory before
        the Kafka producer is triggered. Fails fast if the file is missing.
        """,
    )

    # Task 2: Run the Kafka producer to publish CSV records to Kafka topic
    # The producer script is mounted into the Airflow container via docker-compose
    t2_run_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command=(
            f"python /opt/airflow/kafka/producer/producer.py "
            f"--broker {KAFKA_BROKER} "
            f"--topic {KAFKA_TOPIC} "
            f"--data-path {DATA_LOCAL_PATH}/flights.csv"
        ),
        doc_md="""
        **run_kafka_producer**
        Executes the Python Kafka producer script which reads flights.csv and
        publishes each flight record as a JSON message to the flight-records-raw
        Kafka topic. The MinIO Kafka consumer (running as a separate service)
        consumes these messages and writes them to the partitioned raw data lake.
        """,
    )

    # Task 3: Verify MinIO received the records
    t3_verify_minio = PythonOperator(
        task_id="verify_minio_landing",
        python_callable=verify_minio_landing,
        doc_md="""
        **verify_minio_landing**
        Checks that at least one object exists in the MinIO partition for the
        current execution month, confirming that the Kafka consumer successfully
        persisted the ingested records to the raw data lake.
        """,
    )

    # Task 4: Log the completed run to PostgreSQL audit table
    t4_log_run = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=log_pipeline_run,
        doc_md="""
        **log_pipeline_run**
        Writes a completion audit record to the pipeline_runs PostgreSQL table,
        recording the DAG ID, execution date, stage, and completion timestamp
        for governance and observability.
        """,
    )

    # ---------------------------------------------------------------------------
    # Task dependencies: linear pipeline
    # ---------------------------------------------------------------------------
    t1_check_source >> t2_run_producer >> t3_verify_minio >> t4_log_run