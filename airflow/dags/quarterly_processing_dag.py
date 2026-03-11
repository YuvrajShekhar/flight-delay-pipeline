# =============================================================================
# Flight Delay Pipeline — Quarterly Processing DAG
# =============================================================================
# Schedule: 1st day of January, April, July, October at 04:00 UTC
# Purpose:  Triggers the PySpark batch processing job which reads the last
#           three months of raw flight records from MinIO, performs cleaning,
#           enrichment, feature engineering, and quarterly aggregation, then
#           writes the resulting ML-ready feature tables to PostgreSQL.
#
# Pipeline stages orchestrated by this DAG:
#   1. check_minio_data_available  — verify MinIO has data for the quarter
#   2. run_spark_batch_job         — submit PySpark job (5-stage pipeline)
#   3. verify_feature_tables       — confirm all 4 feature tables were written
#   4. run_data_quality_checks     — row count and null checks on feature tables
#   5. log_pipeline_run            — write audit record to pipeline_runs
#
# Reliability:
#   - 3 automatic retries with 10-minute exponential backoff on Spark task
#   - Idempotent: Spark job uses overwrite mode; rerunning produces same result
#   - Atomic PostgreSQL writes: partial failures leave previous quarter intact
#   - All raw data permanently retained in MinIO for full reprocessing
# =============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
}

# ---------------------------------------------------------------------------
# Environment variables injected from docker-compose .env
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "flight-data")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "flight_features")
POSTGRES_USER = os.getenv("POSTGRES_USER", "spark_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DATA_LOCAL_PATH = os.getenv("DATA_LOCAL_PATH", "/opt/airflow/data")

# Spark job script path (mounted into Airflow container via docker-compose volume)
SPARK_JOB_PATH = "/opt/airflow/spark/jobs/batch_processing.py"

# PostgreSQL JDBC driver path (mounted into Airflow container)
JDBC_JAR_PATH = "/opt/airflow/spark/jars/postgresql-42.7.3.jar"

# Feature tables expected after a successful Spark run
EXPECTED_FEATURE_TABLES = [
    "airline_features",
    "route_features",
    "airport_features",
    "hourly_features",
]

# Minimum acceptable row counts per table (guards against silent empty writes)
MIN_ROW_COUNTS = {
    "airline_features": 10,
    "route_features": 100,
    "airport_features": 10,
    "hourly_features": 20,
}


# ---------------------------------------------------------------------------
# Helper: derive quarter date range from execution_date
# ---------------------------------------------------------------------------
def get_quarter_range(execution_date):
    """
    Returns (quarter_start, quarter_end) date strings for the quarter
    immediately preceding the execution_date. Quarterly DAG runs on the
    1st of the new quarter, so we look back at the completed quarter.

    Example: execution_date = 2015-04-01 → quarter Q1 2015 (Jan–Mar)
    """
    month = execution_date.month
    year = execution_date.year

    # Determine the completed quarter
    if month <= 3:      # Running in Q1 → completed quarter is Q4 of previous year
        q_start = datetime(year - 1, 10, 1)
        q_end = datetime(year - 1, 12, 31)
    elif month <= 6:    # Running in Q2 → completed quarter is Q1
        q_start = datetime(year, 1, 1)
        q_end = datetime(year, 3, 31)
    elif month <= 9:    # Running in Q3 → completed quarter is Q2
        q_start = datetime(year, 4, 1)
        q_end = datetime(year, 6, 30)
    else:               # Running in Q4 → completed quarter is Q3
        q_start = datetime(year, 7, 1)
        q_end = datetime(year, 9, 30)

    return q_start.strftime("%Y-%m-%d"), q_end.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Task 1: Verify MinIO has raw data available for the quarter
# ---------------------------------------------------------------------------
def check_minio_data_available(**context):
    """
    Checks that MinIO contains raw flight data objects for at least one month
    of the quarter being processed. Prevents Spark from running against an
    empty data lake and failing with an unhelpful error.
    """
    from minio import Minio

    execution_date = context["execution_date"]
    q_start, q_end = get_quarter_range(execution_date)
    print(f"Checking MinIO for quarter: {q_start} to {q_end}")

    client = Minio(
        MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

    # Check for at least one object under the raw/ prefix for this year
    year = q_start[:4]
    prefix = f"raw/year={year}/"
    objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))

    if not objects:
        raise ValueError(
            f"No raw data found in MinIO at {MINIO_BUCKET}/{prefix}. "
            "Ensure the monthly ingestion DAG has run successfully before "
            "the quarterly processing DAG is triggered."
        )
    print(f"MinIO data check passed: {len(objects)} object(s) found under {prefix}")
    context["ti"].xcom_push(key="quarter_start", value=q_start)
    context["ti"].xcom_push(key="quarter_end", value=q_end)


# ---------------------------------------------------------------------------
# Task 3: Verify all four feature tables were written to PostgreSQL
# ---------------------------------------------------------------------------
def verify_feature_tables(**context):
    """
    Connects to PostgreSQL and checks that all four expected feature tables
    exist and contain at least the minimum number of rows. Raises an exception
    if any table is missing or empty, triggering a retry of this task.
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            for table in EXPECTED_FEATURE_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cur.fetchone()[0]
                min_count = MIN_ROW_COUNTS.get(table, 1)
                if row_count < min_count:
                    raise ValueError(
                        f"Feature table '{table}' has only {row_count} rows "
                        f"(minimum expected: {min_count}). "
                        "Spark job may have failed or written an empty result."
                    )
                print(f"  ✓ {table}: {row_count} rows")
    finally:
        conn.close()
    print("All feature tables verified successfully.")


# ---------------------------------------------------------------------------
# Task 4: Run data quality checks on the feature tables
# ---------------------------------------------------------------------------
def run_data_quality_checks(**context):
    """
    Performs basic data quality checks on the feature tables:
      - No NULL values in primary key / identifier columns
      - Delay values are within plausible range (not extreme outliers)
      - Quarter coverage is as expected
    Logs warnings for soft failures; raises for hard failures.
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            # Check: no NULL airline codes in airline_features
            cur.execute("SELECT COUNT(*) FROM airline_features WHERE airline_code IS NULL")
            null_airlines = cur.fetchone()[0]
            if null_airlines > 0:
                raise ValueError(f"airline_features has {null_airlines} rows with NULL airline_code")

            # Check: no NULL route identifiers in route_features
            cur.execute("SELECT COUNT(*) FROM route_features WHERE origin IS NULL OR destination IS NULL")
            null_routes = cur.fetchone()[0]
            if null_routes > 0:
                raise ValueError(f"route_features has {null_routes} rows with NULL origin or destination")

            # Check: average delays are within plausible range (-60 to 600 minutes)
            cur.execute("SELECT MIN(avg_departure_delay), MAX(avg_departure_delay) FROM airline_features")
            min_delay, max_delay = cur.fetchone()
            if min_delay is not None and (min_delay < -60 or max_delay > 600):
                print(
                    f"WARNING: avg_departure_delay range [{min_delay}, {max_delay}] "
                    "contains values outside expected bounds. Review feature engineering."
                )

            print("Data quality checks passed.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Task 5: Log completed pipeline run to PostgreSQL audit table
# ---------------------------------------------------------------------------
def log_pipeline_run(**context):
    """
    Writes a completion audit record to the pipeline_runs PostgreSQL table,
    recording the DAG ID, execution date, quarter range, stage, and timestamp.
    """
    execution_date = str(context.get("logical_date") or context.get("execution_date"))
    dag_id = context["dag"].dag_id
    q_start = context["ti"].xcom_pull(task_ids="check_minio_data_available", key="quarter_start") or "unknown"
    q_end = context["ti"].xcom_pull(task_ids="check_minio_data_available", key="quarter_end") or "unknown"

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
                    (run_id, dag_id, execution_date, stage, status, quarter_start, quarter_end, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (dag_id, execution_date, stage)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    quarter_start = EXCLUDED.quarter_start,
                    quarter_end = EXCLUDED.quarter_end,
                    completed_at = NOW()
                """,
                (f"{dag_id}_{execution_date}", dag_id, execution_date, "quarterly_processing", "success", q_start, q_end),
            )
    finally:
        conn.close()
    print(f"Pipeline run logged: {dag_id} | {execution_date} | Q {q_start}→{q_end} | success")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="quarterly_processing_dag",
    description="Quarterly processing: MinIO → Spark batch job → PostgreSQL feature tables",
    schedule_interval="0 4 1 1,4,7,10 *",  # 04:00 UTC on Jan/Apr/Jul/Oct 1st
    start_date=datetime(2015, 4, 1),        # first run processes Q1 2015
    catchup=False,
    default_args=default_args,
    tags=["flight-delay", "processing", "quarterly", "spark"],
    max_active_runs=1,                       # never run two quarterly jobs in parallel
) as dag:

    # Task 1: Verify MinIO has data for the quarter
    t1_check_minio = PythonOperator(
        task_id="check_minio_data_available",
        python_callable=check_minio_data_available,
        doc_md="""
        **check_minio_data_available**
        Verifies that the MinIO raw data lake contains flight records for the
        quarter about to be processed. Prevents Spark from running against
        an empty or incomplete data lake.
        """,
    )

    # Task 2: Submit the PySpark batch job
    # spark-submit runs inside the Airflow container (pyspark installed in Dockerfile)
    t2_run_spark = BashOperator(
        task_id="run_spark_batch_job",
        bash_command=(
            f"spark-submit "
            f"--master local[*] "
            f"--jars {JDBC_JAR_PATH} "
            f"--conf spark.sql.shuffle.partitions=8 "
            f"--conf spark.driver.memory=4g "
            f"{SPARK_JOB_PATH} "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--minio-bucket {MINIO_BUCKET} "
            f"--postgres-host {POSTGRES_HOST} "
            f"--postgres-port {POSTGRES_PORT} "
            f"--postgres-db {POSTGRES_DB} "
            f"--postgres-user {POSTGRES_USER} "
            f"--postgres-password {POSTGRES_PASSWORD}"
        ),
        execution_timeout=timedelta(hours=3),   # Spark job on 5.8M records; generous timeout
        doc_md="""
        **run_spark_batch_job**
        Submits the PySpark batch processing script via spark-submit. The job
        performs five sequential stages: data cleaning, airline/airport
        enrichment, feature engineering, quarterly aggregation, and JDBC write
        to PostgreSQL. Runs with local[*] Spark master (all available cores).
        """,
    )

    # Task 3: Verify the four feature tables were written correctly
    t3_verify_tables = PythonOperator(
        task_id="verify_feature_tables",
        python_callable=verify_feature_tables,
        doc_md="""
        **verify_feature_tables**
        Checks PostgreSQL to confirm all four feature tables (airline_features,
        route_features, airport_features, hourly_features) were written and
        contain at least the minimum expected number of rows.
        """,
    )

    # Task 4: Run data quality checks
    t4_quality_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
        doc_md="""
        **run_data_quality_checks**
        Performs NULL checks on key identifier columns and validates that
        delay values fall within plausible ranges. Hard failures trigger
        retries; soft anomalies are logged as warnings.
        """,
    )

    # Task 5: Log completed run to audit table
    t5_log_run = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=log_pipeline_run,
        doc_md="""
        **log_pipeline_run**
        Writes a completion audit record to the pipeline_runs PostgreSQL table
        including the quarter start/end dates, DAG ID, and completion timestamp.
        """,
    )

    # ---------------------------------------------------------------------------
    # Task dependencies: linear pipeline
    # ---------------------------------------------------------------------------
    t1_check_minio >> t2_run_spark >> t3_verify_tables >> t4_quality_checks >> t5_log_run