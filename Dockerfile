# =============================================================
# Custom Airflow Image
# Extends apache/airflow:2.7.0 with:
#   - pyspark (to submit Spark jobs from DAGs)
#   - psycopg2-binary (to connect to PostgreSQL from DAGs)
#   - minio (to interact with MinIO from DAGs)
# =============================================================

FROM apache/airflow:2.7.0

USER root

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python packages needed by the pipeline DAGs
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary==2.9.9 \
    minio==7.2.0 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.0
