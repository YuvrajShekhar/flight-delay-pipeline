"""
producer.py
===========
Flight Delay Pipeline — Kafka Producer
IU International University of Applied Sciences
Module: Data Engineering (DLMDSEDE02) | Task 1 | Phase 2
Author: Yuvraj Shekhar

Reads the US DOT 2015 flights.csv and publishes each record as a
JSON message to the Kafka topic: flight-records-raw.

Usage (standalone):
    python producer.py

Usage (via Airflow DAG):
    Called by monthly_ingestion_dag.py as a BashOperator
"""

import csv
import json
import logging
import os
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from dotenv import load_dotenv

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Load environment variables ────────────────────────────────
load_dotenv()

# KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",  "flight-records-raw")
# DATA_PATH     = os.getenv("DATA_LOCAL_PATH", "/opt/airflow/data")
DATA_PATH = "/root/Yuvraj_Projects/Project_Data_Enginnering/flight-delay-pipeline/data/"
FLIGHTS_FILE  = os.path.join(DATA_PATH, "flights.csv")

# How many records to log progress on
LOG_INTERVAL  = 100_000
# Delay between batches (seconds) — prevents overwhelming the broker
BATCH_SIZE    = 10_000
BATCH_DELAY   = 0.5


def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """
    Creates a KafkaProducer with retry logic.
    Retries up to `retries` times with `delay` seconds between attempts.
    This handles cases where Kafka is still starting up when the producer runs.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                # Serialize each message value as UTF-8 encoded JSON
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Serialize the key as UTF-8 string
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                # Wait for all replicas to acknowledge (reliability)
                acks="all",
                # Retry failed sends up to 3 times
                retries=3,
                # Batch messages for efficiency (16KB batches)
                batch_size=16384,
                # Wait up to 10ms to fill a batch before sending
                linger_ms=10,
                # Compress messages to reduce network load
                compression_type="gzip",
            )
            logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka not available yet (attempt {attempt}/{retries}). "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)

    raise RuntimeError(
        f"Could not connect to Kafka broker at {KAFKA_BROKER} "
        f"after {retries} attempts."
    )


def delivery_report(err, msg):
    """Callback fired when a message is acknowledged or fails."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")


def publish_flights(producer: KafkaProducer) -> dict:
    """
    Reads flights.csv row by row and publishes each record to Kafka.
    Returns a summary dict with counts for logging/monitoring.
    """
    if not os.path.exists(FLIGHTS_FILE):
        raise FileNotFoundError(
            f"Dataset not found at {FLIGHTS_FILE}. "
            "Please place flights.csv in the data/ directory."
        )

    stats = {
        "total_rows": 0,
        "published":  0,
        "skipped":    0,
        "start_time": datetime.utcnow().isoformat(),
    }

    logger.info(f"Starting ingestion from: {FLIGHTS_FILE}")
    logger.info(f"Publishing to topic: {KAFKA_TOPIC}")

    with open(FLIGHTS_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            stats["total_rows"] += 1

            # Skip rows with missing critical fields
            if not row.get("FLIGHT_NUMBER") or not row.get("DEPARTURE_TIME"):
                stats["skipped"] += 1
                continue

            # Build the message payload — keep all 31 columns
            message = {
                # Identity fields
                "year":               row.get("YEAR"),
                "month":              row.get("MONTH"),
                "day":                row.get("DAY"),
                "day_of_week":        row.get("DAY_OF_WEEK"),
                # Airline & flight
                "airline":            row.get("AIRLINE"),
                "flight_number":      row.get("FLIGHT_NUMBER"),
                "tail_number":        row.get("TAIL_NUMBER"),
                # Route
                "origin_airport":     row.get("ORIGIN_AIRPORT"),
                "destination_airport":row.get("DESTINATION_AIRPORT"),
                # Schedule
                "scheduled_departure":row.get("SCHEDULED_DEPARTURE"),
                "departure_time":     row.get("DEPARTURE_TIME"),
                "departure_delay":    row.get("DEPARTURE_DELAY"),
                "taxi_out":           row.get("TAXI_OUT"),
                "wheels_off":         row.get("WHEELS_OFF"),
                "scheduled_time":     row.get("SCHEDULED_TIME"),
                "elapsed_time":       row.get("ELAPSED_TIME"),
                "air_time":           row.get("AIR_TIME"),
                "distance":           row.get("DISTANCE"),
                "wheels_on":          row.get("WHEELS_ON"),
                "taxi_in":            row.get("TAXI_IN"),
                "scheduled_arrival":  row.get("SCHEDULED_ARRIVAL"),
                "arrival_time":       row.get("ARRIVAL_TIME"),
                "arrival_delay":      row.get("ARRIVAL_DELAY"),
                # Status
                "diverted":           row.get("DIVERTED"),
                "cancelled":          row.get("CANCELLED"),
                "cancellation_reason":row.get("CANCELLATION_REASON"),
                # Delay causes
                "air_system_delay":   row.get("AIR_SYSTEM_DELAY"),
                "security_delay":     row.get("SECURITY_DELAY"),
                "airline_delay":      row.get("AIRLINE_DELAY"),
                "late_aircraft_delay":row.get("LATE_AIRCRAFT_DELAY"),
                "weather_delay":      row.get("WEATHER_DELAY"),
                # Metadata
                "ingested_at":        datetime.utcnow().isoformat(),
            }

            # Use airline+flight_number+date as the message key
            # This ensures records for the same flight go to the same partition
            key = f"{row.get('AIRLINE','X')}-{row.get('FLIGHT_NUMBER','0')}-{row.get('YEAR','2015')}{row.get('MONTH','01')}{row.get('DAY','01')}"

            producer.send(
                KAFKA_TOPIC,
                key=key,
                value=message,
            )
            stats["published"] += 1

            # Log progress every LOG_INTERVAL records
            if stats["published"] % LOG_INTERVAL == 0:
                logger.info(
                    f"Progress: {stats['published']:,} records published "
                    f"({stats['skipped']:,} skipped)..."
                )

            # Flush in batches to avoid memory buildup with 5.8M records
            if stats["published"] % BATCH_SIZE == 0:
                producer.flush()
                time.sleep(BATCH_DELAY)

    # Final flush to ensure all buffered messages are sent
    producer.flush()

    stats["end_time"] = datetime.utcnow().isoformat()
    return stats


def main():
    logger.info("=" * 60)
    logger.info("Flight Delay Pipeline — Kafka Producer")
    logger.info("=" * 60)

    producer = create_producer()

    try:
        stats = publish_flights(producer)
        logger.info("=" * 60)
        logger.info("Ingestion complete!")
        logger.info(f"  Total rows read : {stats['total_rows']:,}")
        logger.info(f"  Published       : {stats['published']:,}")
        logger.info(f"  Skipped         : {stats['skipped']:,}")
        logger.info(f"  Started at      : {stats['start_time']}")
        logger.info(f"  Finished at     : {stats['end_time']}")
        logger.info("=" * 60)
    except FileNotFoundError as e:
        logger.error(str(e))
        raise
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    main()