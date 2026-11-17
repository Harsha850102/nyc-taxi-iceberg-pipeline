"""Part 2 + Part 3.1 + Part 5 (quarantine) - raw S3 files -> Iceberg tables.

Run:  python -m pipeline.transform_job

Flow:
  raw parquet + zone CSV (S3)
    -> standardize column names/types
    -> trip_id = sha256(all columns)
    -> validation rules -> reject_reason
    -> valid rows:    enrich (minutes, mph, tip %, zone names) -> MERGE INTO trips_clean
    -> rejected rows: MERGE INTO trips_rejected (quarantine, with reject_reason)

MERGE INTO ... WHEN NOT MATCHED THEN INSERT means a row that is already in the
table is ignored, so running this job twice never creates duplicates.
"""
from __future__ import annotations

import logging

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline import transforms as T
from pipeline.config import Config
from pipeline.spark_session import build_spark, ensure_namespace, table_exists

log = logging.getLogger("transform")


def read_raw(spark: SparkSession, cfg: Config) -> tuple[DataFrame, DataFrame, str]:
    trips_path = cfg.spark_path(cfg.trips_raw_key)
    zones_path = cfg.spark_path(cfg.zones_raw_key)
    trips = spark.read.parquet(trips_path)
    zones = spark.read.option("header", True).option("inferSchema", True).csv(zones_path)
    return trips, zones, trips_path


def align_to_table(spark: SparkSession, df: DataFrame, table: str) -> DataFrame:
    """Make the source frame have exactly the target table's columns.

    Needed after schema evolution: once is_airport_trip exists on the table,
    new loads must provide it too (we compute it rather than leave it NULL).
    """
    target_cols = spark.table(table).schema
    for f in target_cols:
        if f.name not in df.columns:
            if f.name == "is_airport_trip":
                df = df.withColumn(f.name, T.airport_trip_expr())
            else:
                df = df.withColumn(f.name, F.lit(None).cast(f.dataType))
    return df.select([f.name for f in target_cols])


def create_if_missing(spark: SparkSession, df: DataFrame, table: str, partition_col: str) -> None:
    if table_exists(spark, table):
        return
    log.info("Creating Iceberg table %s partitioned by %s", table, partition_col)
    (
        df.limit(0)
        .writeTo(table)
        .using("iceberg")
        .partitionedBy(F.col(partition_col))
        .tableProperty("format-version", "2")
        .create()
    )


def merge_new_rows(spark: SparkSession, df: DataFrame, table: str, on: str) -> None:
    """Idempotent load: insert only rows whose key is not already in the table."""
    view = f"src_{table.split('.')[-1]}"
    align_to_table(spark, df, table).createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING {view} AS s
        ON {on}
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def run(cfg: Config) -> None:
    spark = build_spark("trips_clean_load", cfg)
    ensure_namespace(spark, cfg)

    raw_trips, raw_zones, source_file = read_raw(spark, cfg)

    # 1) standardize + key
    std = T.standardize_columns(raw_trips)
    original_cols = std.columns
    keyed = T.add_trip_id(std, original_cols)

    # 2) validate
    flagged = T.assign_reject_reason(keyed, cfg.year, cfg.month)
    valid, rejected = T.split_valid_rejected(flagged)

    load_ts = F.current_timestamp()
    zones = T.prepare_zones(raw_zones)

    # 3) enrich the valid rows
    clean = (
        T.enrich(valid, zones)
        .withColumn("source_file", F.lit(source_file))
        .withColumn("loaded_at", load_ts)
    )
    rejected = rejected.withColumn("source_file", F.lit(source_file)).withColumn("rejected_at", load_ts)

    # Cache so the counts below and the MERGEs don't re-read S3 several times.
    clean = clean.persist(StorageLevel.DISK_ONLY)
    rejected = rejected.persist(StorageLevel.DISK_ONLY)
    raw_count = raw_trips.count()
    clean_count = clean.count()
    rejected_count = rejected.count()
    log.info("raw=%s clean=%s rejected=%s (raw == clean + rejected: %s)",
             raw_count, clean_count, rejected_count, raw_count == clean_count + rejected_count)
    for r in rejected.groupBy("reject_reason").count().orderBy(F.desc("count")).collect():
        log.info("  rejected %-28s %s", r.reject_reason, r["count"])

    # 4) write
    create_if_missing(spark, clean, cfg.trips_clean, "pickup_date")
    create_if_missing(spark, rejected, cfg.trips_rejected, "reject_reason")

    merge_new_rows(spark, clean, cfg.trips_clean,
                   on="t.trip_id = s.trip_id AND t.pickup_date = s.pickup_date")
    merge_new_rows(spark, rejected, cfg.trips_rejected,
                   on="t.reject_id = s.reject_id")

    total = spark.table(cfg.trips_clean).count()
    log.info("%s now has %s rows (re-running this job should NOT change this number)", cfg.trips_clean, total)
    spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(Config())


if __name__ == "__main__":
    main()
