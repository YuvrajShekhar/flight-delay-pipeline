"""
=============================================================
PySpark Batch Processing Job — Flight Delay Pipeline
IU International University of Applied Sciences
Module: Data Engineering (DLMDSEDE02) | Task 1 | Phase 2
Author: Yuvraj Shekhar

Quarterly batch job that reads raw flight CSV data from MinIO,
performs the following sequential stages:
  1. Data Cleaning       — nulls, types, cancelled flights
  2. Data Enrichment     — join airlines + airports lookup tables
  3. Feature Engineering — delay features, route stats, time features
  4. Aggregation         — quarterly ML-ready summary tables
  5. Load                — write feature tables to PostgreSQL

Run manually:
  spark-submit --master spark://spark-master:7077 \
    spark/jobs/batch_processing.py

Or triggered by Airflow quarterly_processing_dag.
=============================================================
"""

import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, TimestampType
)

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT",      "http://localhost:9000")
MINIO_ACCESS_KEY    = os.getenv("MINIO_ROOT_USER",      "minioadmin")
MINIO_SECRET_KEY    = os.getenv("MINIO_ROOT_PASSWORD",  "yuvi@123")
MINIO_BUCKET        = os.getenv("MINIO_BUCKET",         "flight-data")

POSTGRES_HOST       = os.getenv("POSTGRES_HOST",        "postgres")
POSTGRES_PORT       = os.getenv("POSTGRES_PORT",        "5432")
POSTGRES_DB         = os.getenv("POSTGRES_DB",          "flight_features")
POSTGRES_USER       = os.getenv("POSTGRES_USER",        "spark_user")
POSTGRES_PASSWORD   = os.getenv("POSTGRES_PASSWORD",    "changeme_spark_password")

# DATA_PATH           = os.getenv("DATA_LOCAL_PATH", "/opt/airflow/data")
DATA_PATH = "/root/Yuvraj_Projects/Project_Data_Enginnering/flight-delay-pipeline/data/"
FLIGHTS_CSV         = os.path.join(DATA_PATH, "flights.csv")
AIRLINES_CSV        = os.path.join(DATA_PATH, "airlines.csv")
AIRPORTS_CSV        = os.path.join(DATA_PATH, "airports.csv")

POSTGRES_JDBC_URL   = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
PROCESSED_PATH      = f"s3a://{MINIO_BUCKET}/processed/year=2015"
QUARTER             = os.getenv("QUARTER", "Q1")


# ── Spark Session ─────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    """
    Create SparkSession configured with:
    - S3A connector for MinIO access
    - PostgreSQL JDBC driver
    """
    log.info("Creating Spark session...")

    spark = (
        SparkSession.builder
        .appName("FlightDelayPipeline-BatchProcessing")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.adaptive.enabled",                  "true")
        .config("spark.sql.shuffle.partitions",                "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created. Version: %s", spark.version)
    return spark


# ── Stage 1: Data Cleaning ────────────────────────────────────
def clean_data(spark: SparkSession) -> DataFrame:
    """
    Load raw flights.csv and perform:
    - Cast columns to correct types
    - Filter out cancelled flights
    - Drop rows with null arrival/departure delay
    - Standardise column names to snake_case
    """
    log.info("Stage 1: Loading and cleaning raw flight data...")

    df = spark.read.csv(FLIGHTS_CSV, header=True, inferSchema=False)

    log.info("Raw records loaded: %d", df.count())

    # Rename all columns to lowercase snake_case
    df = df.toDF(*[c.lower() for c in df.columns])

    # Cast numeric columns
    numeric_cols = [
        "departure_delay", "arrival_delay", "air_time",
        "distance", "taxi_out", "taxi_in", "elapsed_time",
        "scheduled_time", "air_system_delay", "security_delay",
        "airline_delay", "late_aircraft_delay", "weather_delay"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(FloatType()))

    df = df.withColumn("year",  F.col("year").cast(IntegerType()))
    df = df.withColumn("month", F.col("month").cast(IntegerType()))
    df = df.withColumn("day",   F.col("day").cast(IntegerType()))
    df = df.withColumn("day_of_week", F.col("day_of_week").cast(IntegerType()))

    # Filter out cancelled flights
    df = df.filter(F.col("cancelled") == "0")

    # Drop rows missing key delay columns
    df = df.dropna(subset=["arrival_delay", "departure_delay"])

    # Add is_delayed flag (delay > 15 min is industry standard threshold)
    df = df.withColumn("is_delayed", (F.col("arrival_delay") > 15).cast(IntegerType()))

    # Add quarter column
    df = df.withColumn(
        "quarter",
        F.when(F.col("month").isin(1, 2, 3),  F.lit("Q1"))
         .when(F.col("month").isin(4, 5, 6),  F.lit("Q2"))
         .when(F.col("month").isin(7, 8, 9),  F.lit("Q3"))
         .otherwise(F.lit("Q4"))
    )

    # Add route key (origin-destination pair)
    df = df.withColumn(
        "route",
        F.concat(F.col("origin_airport"), F.lit("-"), F.col("destination_airport"))
    )

    clean_count = df.count()
    log.info("Stage 1 complete. Clean records: %d", clean_count)
    return df


# ── Stage 2: Data Enrichment ──────────────────────────────────
def enrich_data(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Join flights with airlines and airports lookup tables to add:
    - Full airline name
    - Origin airport name, city, state, latitude, longitude
    - Destination airport name, city, state
    """
    log.info("Stage 2: Enriching with airlines and airports data...")

    # Load lookup tables
    airlines_df = spark.read.csv(AIRLINES_CSV, header=True, inferSchema=False)
    airports_df = spark.read.csv(AIRPORTS_CSV, header=True, inferSchema=False)

    airlines_df = airlines_df.toDF(*[c.lower() for c in airlines_df.columns])
    airports_df = airports_df.toDF(*[c.lower() for c in airports_df.columns])

    # Join airline names
    df = df.join(
        airlines_df.select(
            F.col("iata_code").alias("airline"),
            F.col("airline").alias("airline_name")
        ),
        on="airline",
        how="left"
    )

    # Join origin airport metadata
    df = df.join(
        airports_df.select(
            F.col("iata_code").alias("origin_airport"),
            F.col("airport").alias("origin_airport_name"),
            F.col("city").alias("origin_city"),
            F.col("state").alias("origin_state"),
            F.col("latitude").cast(FloatType()).alias("origin_lat"),
            F.col("longitude").cast(FloatType()).alias("origin_lon"),
        ),
        on="origin_airport",
        how="left"
    )

    # Join destination airport metadata
    df = df.join(
        airports_df.select(
            F.col("iata_code").alias("destination_airport"),
            F.col("airport").alias("destination_airport_name"),
            F.col("city").alias("destination_city"),
            F.col("state").alias("destination_state"),
        ),
        on="destination_airport",
        how="left"
    )

    log.info("Stage 2 complete. Enriched columns added.")
    return df


# ── Stage 3: Feature Engineering ─────────────────────────────
def engineer_features(df: DataFrame) -> DataFrame:
    """
    Compute ML-ready features:
    - Scheduled departure hour (time-of-day feature)
    - Delay cause flags
    - Total delay cause sum
    """
    log.info("Stage 3: Engineering features...")

    # Extract departure hour from scheduled_departure (format: HHMM)
    df = df.withColumn(
        "scheduled_departure_hour",
        (F.col("scheduled_departure").cast(IntegerType()) / 100).cast(IntegerType())
    )

    # Binary delay cause flags
    df = df.withColumn("has_weather_delay",
        (F.coalesce(F.col("weather_delay"), F.lit(0)) > 0).cast(IntegerType()))
    df = df.withColumn("has_carrier_delay",
        (F.coalesce(F.col("airline_delay"), F.lit(0)) > 0).cast(IntegerType()))
    df = df.withColumn("has_nas_delay",
        (F.coalesce(F.col("air_system_delay"), F.lit(0)) > 0).cast(IntegerType()))
    df = df.withColumn("has_late_aircraft_delay",
        (F.coalesce(F.col("late_aircraft_delay"), F.lit(0)) > 0).cast(IntegerType()))

    # Total delay minutes across all causes
    df = df.withColumn(
        "total_delay_causes_min",
        F.coalesce(F.col("weather_delay"),       F.lit(0)) +
        F.coalesce(F.col("airline_delay"),        F.lit(0)) +
        F.coalesce(F.col("air_system_delay"),     F.lit(0)) +
        F.coalesce(F.col("late_aircraft_delay"),  F.lit(0)) +
        F.coalesce(F.col("security_delay"),       F.lit(0))
    )

    log.info("Stage 3 complete. Feature columns added.")
    return df


# ── Stage 4: Aggregation ──────────────────────────────────────
def aggregate_features(df: DataFrame) -> dict:
    """
    Produce quarterly ML-ready aggregated feature tables:
    - airline_features:  per-airline delay statistics
    - route_features:    per-route delay statistics
    - airport_features:  per-airport on-time performance
    - hourly_features:   delay patterns by hour of day
    """
    log.info("Stage 4: Aggregating quarterly feature tables...")

    # ── Table 1: Airline features ──────────────────────────
    airline_features = df.groupBy("airline", "airline_name", "quarter").agg(
        F.count("*").alias("total_flights"),
        F.sum("is_delayed").alias("delayed_flights"),
        F.round(F.mean("arrival_delay"), 2).alias("avg_arrival_delay_min"),
        F.round(F.mean("departure_delay"), 2).alias("avg_departure_delay_min"),
        F.round(F.stddev("arrival_delay"), 2).alias("std_arrival_delay"),
        F.round(F.sum("is_delayed") / F.count("*") * 100, 2).alias("delay_rate_pct"),
        F.round(F.mean("weather_delay"), 2).alias("avg_weather_delay_min"),
        F.round(F.mean("airline_delay"), 2).alias("avg_carrier_delay_min"),
        F.round(F.mean("air_system_delay"), 2).alias("avg_nas_delay_min"),
    ).withColumn("processed_at", F.lit(datetime.utcnow().isoformat()))

    # ── Table 2: Route features ────────────────────────────
    route_features = df.groupBy(
        "route", "origin_airport", "destination_airport",
        "origin_city", "destination_city", "quarter"
    ).agg(
        F.count("*").alias("total_flights"),
        F.sum("is_delayed").alias("delayed_flights"),
        F.round(F.mean("arrival_delay"), 2).alias("avg_arrival_delay_min"),
        F.round(F.mean("departure_delay"), 2).alias("avg_departure_delay_min"),
        F.round(F.sum("is_delayed") / F.count("*") * 100, 2).alias("delay_rate_pct"),
        F.round(F.mean("distance"), 2).alias("avg_distance_miles"),
        F.round(F.mean("air_time"), 2).alias("avg_air_time_min"),
    ).withColumn("processed_at", F.lit(datetime.utcnow().isoformat()))

    # ── Table 3: Airport features ──────────────────────────
    airport_features = df.groupBy(
        "origin_airport", "origin_airport_name",
        "origin_city", "origin_state", "quarter"
    ).agg(
        F.count("*").alias("total_departures"),
        F.sum("is_delayed").alias("delayed_departures"),
        F.round(F.mean("departure_delay"), 2).alias("avg_departure_delay_min"),
        F.round(F.sum("is_delayed") / F.count("*") * 100, 2).alias("departure_delay_rate_pct"),
        F.round(F.mean("taxi_out"), 2).alias("avg_taxi_out_min"),
    ).withColumn("processed_at", F.lit(datetime.utcnow().isoformat()))

    # ── Table 4: Hourly features ───────────────────────────
    hourly_features = df.groupBy(
        "scheduled_departure_hour", "day_of_week", "quarter"
    ).agg(
        F.count("*").alias("total_flights"),
        F.sum("is_delayed").alias("delayed_flights"),
        F.round(F.mean("arrival_delay"), 2).alias("avg_arrival_delay_min"),
        F.round(F.sum("is_delayed") / F.count("*") * 100, 2).alias("delay_rate_pct"),
    ).withColumn("processed_at", F.lit(datetime.utcnow().isoformat()))

    log.info("Stage 4 complete. 4 feature tables aggregated.")

    return {
        "airline_features":  airline_features,
        "route_features":    route_features,
        "airport_features":  airport_features,
        "hourly_features":   hourly_features,
    }


# ── Stage 5: Load to PostgreSQL ───────────────────────────────
def load_to_postgres(tables: dict) -> None:
    """
    Write all aggregated feature tables to PostgreSQL.
    Uses overwrite mode so each quarterly run refreshes the tables.
    """
    log.info("Stage 5: Loading feature tables to PostgreSQL...")

    jdbc_props = {
        "user":     POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver":   "org.postgresql.Driver",
    }

    for table_name, df in tables.items():
        log.info("Writing table: %s ...", table_name)
        df.write.jdbc(
            url=POSTGRES_JDBC_URL,
            table=table_name,
            mode="overwrite",
            properties=jdbc_props,
        )
        log.info("Table '%s' written successfully.", table_name)

    log.info("Stage 5 complete. All feature tables loaded to PostgreSQL.")


# ── Main ──────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("Flight Delay Pipeline — PySpark Batch Job")
    log.info("Quarter  : %s", QUARTER)
    log.info("Source   : %s", FLIGHTS_CSV)
    log.info("Target   : %s/%s", POSTGRES_HOST, POSTGRES_DB)
    log.info("=" * 60)

    start = datetime.utcnow()
    spark = create_spark_session()

    try:
        # Stage 1 — Clean
        clean_df = clean_data(spark)

        # Stage 2 — Enrich
        enriched_df = enrich_data(spark, clean_df)

        # Stage 3 — Feature Engineering
        featured_df = engineer_features(enriched_df)

        # Stage 4 — Aggregation
        tables = aggregate_features(featured_df)

        # Stage 5 — Load to PostgreSQL
        load_to_postgres(tables)

    finally:
        spark.stop()
        log.info("Spark session stopped.")

    elapsed = (datetime.utcnow() - start).seconds
    log.info("=" * 60)
    log.info("Batch job complete. Total time: %ds", elapsed)
    log.info("=" * 60)


if __name__ == "__main__":
    main()