"""Part 1 - Ingest: download the TLC files and land them, unchanged, in S3 raw/.

Run:  python -m pipeline.ingest            (uses TRIP_YEAR / TRIP_MONTH from .env)
      python -m pipeline.ingest --year 2025 --month 1

Why it is safe to re-run (idempotent):
  * Every file always goes to the same, deterministic S3 key
    (raw/yellow_trips/year=2025/month=01/yellow_tripdata_2025-01.parquet), so a
    re-run can only overwrite, never add a second copy.
  * We store the file's SHA-256 in the S3 object metadata. If the object already
    exists with the same hash we skip the upload entirely.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import tempfile

import boto3
import requests
from botocore.exceptions import ClientError

from pipeline.config import TRIPS_URL_TEMPLATE, ZONES_URL, Config

log = logging.getLogger("ingest")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download(url: str, dest: str) -> str:
    log.info("Downloading %s", url)
    headers = {"User-Agent": "nyc-taxi-iceberg-pipeline/1.0"}
    with requests.get(url, stream=True, timeout=120, headers=headers) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    log.info("Downloaded %.1f MB", os.path.getsize(dest) / 1e6)
    return dest


def existing_sha256(s3, bucket: str, key: str) -> str | None:
    """Return the sha256 stored on the S3 object, or None if the object doesn't exist."""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return head.get("Metadata", {}).get("sha256")


def upload_if_changed(s3, local_path: str, bucket: str, key: str, source_url: str) -> bool:
    """Upload only if the object is missing or its content changed. Returns True if uploaded."""
    digest = sha256_file(local_path)
    if existing_sha256(s3, bucket, key) == digest:
        log.info("SKIP  s3://%s/%s (already present, same sha256)", bucket, key)
        return False
    s3.upload_file(
        local_path,
        bucket,
        key,
        ExtraArgs={
            "Metadata": {"sha256": digest, "source_url": source_url},
            "ServerSideEncryption": "AES256",
        },
    )
    log.info("UPLOAD s3://%s/%s", bucket, key)
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Land NYC TLC files in S3 raw/")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    args = parser.parse_args()
    if args.year:
        os.environ["TRIP_YEAR"] = str(args.year)
    if args.month:
        os.environ["TRIP_MONTH"] = str(args.month)

    cfg = Config()
    s3 = boto3.Session(region_name=cfg.region).client("s3")
    trips_url = TRIPS_URL_TEMPLATE.format(year=cfg.year, mm=cfg.mm)

    with tempfile.TemporaryDirectory() as tmp:
        files = [
            (trips_url, os.path.join(tmp, cfg.trips_file_name), cfg.trips_raw_key),
            (ZONES_URL, os.path.join(tmp, "taxi_zone_lookup.csv"), cfg.zones_raw_key),
        ]
        for url, local, key in files:
            download(url, local)
            upload_if_changed(s3, local, cfg.bucket, key, url)

    log.info("Ingest finished. Raw files are under s3://%s/raw/", cfg.bucket)


if __name__ == "__main__":
    main()
