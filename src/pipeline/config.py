"""All settings come from environment variables (see .env.example).

No credentials live in this file or anywhere in the repo. AWS credentials come
from your AWS profile (AWS_PROFILE) via the standard AWS credential chain.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing required environment variable: {name} (see .env.example)")
    return value


@dataclass(frozen=True)
class Config:
    bucket: str = field(default_factory=lambda: _env("S3_BUCKET"))
    region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1"))
    year: int = field(default_factory=lambda: int(_env("TRIP_YEAR", "2025")))
    month: int = field(default_factory=lambda: int(_env("TRIP_MONTH", "1")))

    # Catalog: "polaris" (Iceberg REST, preferred) or "glue" (AWS Glue Data Catalog fallback)
    catalog_type: str = field(default_factory=lambda: _env("CATALOG_TYPE", "polaris").lower())
    namespace: str = field(default_factory=lambda: _env("ICEBERG_NAMESPACE", "nyc_taxi"))
    polaris_uri: str = field(default_factory=lambda: _env("POLARIS_URI", "http://localhost:8181/api/catalog"))
    polaris_catalog: str = field(default_factory=lambda: _env("POLARIS_CATALOG", "taxi_catalog"))

    # Spark
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[*]"))
    driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "4g"))
    s3_scheme: str = field(default_factory=lambda: _env("S3_SCHEME", "s3a"))  # use "s3" inside AWS Glue

    # Spark catalog alias used in every table name: lake.<namespace>.<table>
    spark_catalog: str = "lake"

    # ---- S3 layout -------------------------------------------------------
    @property
    def mm(self) -> str:
        return f"{self.month:02d}"

    @property
    def trips_file_name(self) -> str:
        return f"yellow_tripdata_{self.year}-{self.mm}.parquet"

    @property
    def trips_raw_key(self) -> str:
        return f"raw/yellow_trips/year={self.year}/month={self.mm}/{self.trips_file_name}"

    @property
    def zones_raw_key(self) -> str:
        return "raw/taxi_zones/taxi_zone_lookup.csv"

    @property
    def warehouse_uri(self) -> str:
        return f"s3://{self.bucket}/warehouse"

    def spark_path(self, key: str) -> str:
        return f"{self.s3_scheme}://{self.bucket}/{key}"

    # ---- Table names -----------------------------------------------------
    def table(self, name: str) -> str:
        return f"{self.spark_catalog}.{self.namespace}.{name}"

    @property
    def trips_clean(self) -> str:
        return self.table("trips_clean")

    @property
    def trips_rejected(self) -> str:
        return self.table("trips_rejected")

    @property
    def daily_zone_metrics(self) -> str:
        return self.table("daily_zone_metrics")


TRIPS_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{mm}.parquet"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
