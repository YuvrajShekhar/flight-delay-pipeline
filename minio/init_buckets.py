"""
=============================================================
MinIO Bucket Initialisation — Flight Delay Pipeline
IU International University of Applied Sciences
Module: Data Engineering (DLMDSEDE02) | Task 1 | Phase 2
Author: Yuvraj Shekhar

Creates the flight-data bucket with partitioned folder
structure on first run. Safe to re-run — skips if bucket
already exists.

Run manually:
  python minio/init_buckets.py

Or triggered automatically on pipeline first run.
=============================================================
"""

import json
import logging
import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

# ── Load environment variables ────────────────────────────────
load_dotenv()

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "").replace("https://", "")
MINIO_ROOT_USER    = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD= os.getenv("MINIO_ROOT_PASSWORD", "yuvi@123")
MINIO_BUCKET       = os.getenv("MINIO_BUCKET", "flight-data")

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Partition structure to initialise ─────────────────────────
# Creates placeholder objects to establish the folder hierarchy
# raw/  — incoming CSV records from Kafka consumer
# processed/ — Parquet files written by Spark after processing
PARTITIONS = [
    "raw/year=2015/month=01/.keep",
    "raw/year=2015/month=02/.keep",
    "raw/year=2015/month=03/.keep",
    "raw/year=2015/month=04/.keep",
    "raw/year=2015/month=05/.keep",
    "raw/year=2015/month=06/.keep",
    "raw/year=2015/month=07/.keep",
    "raw/year=2015/month=08/.keep",
    "raw/year=2015/month=09/.keep",
    "raw/year=2015/month=10/.keep",
    "raw/year=2015/month=11/.keep",
    "raw/year=2015/month=12/.keep",
    "processed/year=2015/quarter=Q1/.keep",
    "processed/year=2015/quarter=Q2/.keep",
    "processed/year=2015/quarter=Q3/.keep",
    "processed/year=2015/quarter=Q4/.keep",
]


def create_client() -> Minio:
    """Create and return a MinIO client."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )
    log.info("MinIO client created for endpoint: %s", MINIO_ENDPOINT)
    return client


def create_bucket(client: Minio, bucket: str) -> bool:
    """
    Create the bucket if it doesn't exist.
    Returns True if created, False if it already existed.
    """
    if client.bucket_exists(bucket):
        log.info("Bucket '%s' already exists — skipping creation.", bucket)
        return False

    client.make_bucket(bucket)
    log.info("Bucket '%s' created successfully.", bucket)
    return True


def create_partition_structure(client: Minio, bucket: str) -> int:
    """
    Create placeholder objects to establish the partitioned
    folder hierarchy in MinIO. Returns count of objects created.
    """
    created = 0
    placeholder = b""  # empty content — just establishes the path

    for path in PARTITIONS:
        try:
            # Check if placeholder already exists
            try:
                client.stat_object(bucket, path)
                log.debug("Partition already exists: %s", path)
                continue
            except S3Error:
                pass  # doesn't exist yet — create it

            import io
            client.put_object(
                bucket,
                path,
                data=io.BytesIO(placeholder),
                length=0,
            )
            log.info("Created partition: %s/%s", bucket, path)
            created += 1

        except S3Error as e:
            log.error("Failed to create partition %s: %s", path, e)

    return created


def write_metadata(client: Minio, bucket: str) -> None:
    """
    Write a metadata JSON object to the bucket root
    recording when it was initialised and by what pipeline.
    """
    import io

    metadata = {
        "pipeline":      "flight-delay-prediction",
        "module":        "DLMDSEDE02",
        "author":        "Yuvraj Shekhar",
        "bucket":        bucket,
        "initialised_at": datetime.utcnow().isoformat(),
        "structure": {
            "raw":       "Monthly CSV records from Kafka consumer",
            "processed": "Quarterly Parquet feature tables from Spark",
        },
    }

    content = json.dumps(metadata, indent=2).encode("utf-8")

    client.put_object(
        bucket,
        "_metadata.json",
        data=io.BytesIO(content),
        length=len(content),
        content_type="application/json",
    )
    log.info("Metadata written to %s/_metadata.json", bucket)


def main():
    log.info("=" * 60)
    log.info("Flight Delay Pipeline — MinIO Initialisation")
    log.info("Endpoint : %s", MINIO_ENDPOINT)
    log.info("Bucket   : %s", MINIO_BUCKET)
    log.info("=" * 60)

    client = create_client()

    # 1. Create bucket
    create_bucket(client, MINIO_BUCKET)

    # 2. Create partitioned folder structure
    created = create_partition_structure(client, MINIO_BUCKET)
    log.info("Partition structure: %d new paths created.", created)

    # 3. Write metadata
    write_metadata(client, MINIO_BUCKET)

    log.info("=" * 60)
    log.info("MinIO initialisation complete.")
    log.info("Bucket '%s' is ready for ingestion.", MINIO_BUCKET)
    log.info("=" * 60)


if __name__ == "__main__":
    main()