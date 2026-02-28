# Flight Delay Prediction - Batch Processing Data Pipeline

**IU International University of Applied Sciences**  
Module: Data Engineering (DLMDSEDE02) | Task 1 | Phase 2  

**Author:** Yuvraj Shekhar

---

## Project Overview

This repository is the full Infrastructure as Code (IaC) implementation of a batch-processing data pipeline for a flight delay prediction ML application. The system ingests ~5.8 million US domestic flight records, stores them in a partitioned data lake, performs quarterly feature engineering via Apache Spark, and delivers ML-ready feature tables to a PostgreSQL feature store.

The entire infrastructure is defined as code and reproduced on any machine with Docker installed using a single `docker-compose up` command.

---

## Architecture

```
US DOT / Kaggle CSV
        │
        ▼  monthly
  Apache Kafka          ← Ingestion layer
        │
        ▼  raw records
      MinIO              ← Data lake (S3-compatible object storage)
        │
        ▼  quarterly
  Apache Spark          ← Batch processing & feature engineering
        │
        ▼  ML features
   PostgreSQL           ← Feature store / serving layer
        │
        ▼  quarterly read
  ML Application        ← (out of scope)

  Apache Airflow        ← Orchestration & scheduling (all stages)
```

---

## Tech Stack

| Service | Docker Image | Role |
|---|---|---|
| Apache Kafka | `confluentinc/cp-kafka:7.5.0` | Data ingestion message broker |
| Zookeeper | `confluentinc/cp-zookeeper:7.5.0` | Kafka cluster coordination |
| MinIO | `minio/minio:latest` | S3-compatible raw data lake |
| Apache Spark | `bitnami/spark:3.5` | Batch processing & feature engineering |
| PostgreSQL | `postgres:15` | ML feature store |
| Apache Airflow | `apache/airflow:2.7.0` (custom) | Pipeline orchestration & scheduling |

---

## Repository Structure

```
flight-delay-pipeline/
│
├── docker-compose.yml               # Core IaC — all 6 microservices
├── .env.example                     # Env variable template (copy to .env)
├── .gitignore
├── README.md
│
├── kafka/
│   └── producer/
│       ├── producer.py              # CSV → Kafka topic publisher
│       └── requirements.txt
│
├── minio/
│   └── init_buckets.py             # Creates MinIO buckets on first run
│
├── spark/
│   └── jobs/
│       └── batch_processing.py     # PySpark: clean → enrich → engineer → aggregate
│
├── postgres/
│   └── init/
│       └── 01_schema.sql           # Feature table schema + user access control
│
├── airflow/
│   ├── Dockerfile                  # Custom image: airflow + pyspark + psycopg2
│   ├── dags/
│   │   ├── monthly_ingestion_dag.py
│   │   └── quarterly_processing_dag.py
│   ├── plugins/
│   └── logs/
│
├── docs/
│   └── architecture_diagram.png    # Phase 1 architecture diagram
│
└── data/                           # Dataset files — gitignored (download from Kaggle)
    └── .gitkeep
```

---

## Prerequisites

- Docker Desktop (or Docker Engine + Compose) installed
- Recommended: **16 GB RAM, 4 CPU cores**
- Dataset downloaded from Kaggle (see below)

---

## Dataset Setup

Download from Kaggle: [2015 Flight Delays and Cancellations](https://www.kaggle.com/datasets/usdot/flight-delays)

Place files in the `data/` directory:
```
data/
├── flights.csv      # ~5.8 million rows
├── airlines.csv     # Airline code lookup
└── airports.csv     # Airport metadata lookup
```

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/<your-username>/flight-delay-pipeline.git
cd flight-delay-pipeline

# 2. Configure credentials
cp .env.example .env
# Edit .env with your chosen passwords

# 3. Start all services
docker-compose up -d

# 4. Verify all containers are running
docker-compose ps

# 5. Access UIs
# Airflow:  http://localhost:8080
# MinIO:    http://localhost:9001
```

---

## Security

- All credentials in `.env` — never committed to Git (`.gitignore` enforced)
- MinIO buckets private; key authentication required
- PostgreSQL: separate `spark_user` (write) and `ml_readonly` (read-only) accounts
- All services on private Docker bridge network; no internal ports exposed externally except Airflow UI (8080)

---

## References

- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly Media.
- Marz, N., & Warren, J. (2015). *Big Data*. Manning Publications.
- Kreps, J. (2014). *Event Data, Stream Processing, and Data Integration*. O'Reilly Media.
- Zaharia, M., et al. (2016). Apache Spark. *Communications of the ACM, 59*(11), 56–65.
