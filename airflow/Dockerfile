# =============================================================================
# Flight Delay Pipeline — Custom Airflow Image
# Extends apache/airflow:2.7.0 with PySpark and psycopg2-binary so that
# DAGs can submit Spark jobs and write results to PostgreSQL directly.
# =============================================================================

FROM apache/airflow:2.7.0

# Switch to root to install system-level Java dependency (required by PySpark)
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark can locate the JVM
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to the airflow user for pip installs (security best practice)
USER airflow

# Install Python dependencies needed by DAGs:
#   pyspark        — submits Spark batch jobs from within Airflow operators
#   psycopg2-binary — connects to PostgreSQL for health checks and auditing
#   minio          — interacts with MinIO object storage from DAG tasks
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary==2.9.9 \
    minio==7.2.5