"""Part 3.4 - build daily_zone_metrics from trips_clean.

Run:  python -m pipeline.metrics_job

Grain: one row per (pickup_date, pickup zone).
Columns: trips, total_revenue, avg_tip_pct, avg_trip_minutes.

Idempotent: we use overwritePartitions() (dynamic partition overwrite). Every
pickup_date partition we compute replaces the old one, so re-running gives the
same result instead of doubling the numbers.
"""
from __future__ import annotations

import logging

from pyspark.sql import functions as F

from pipeline import transforms as T
from pipeline.config import Config
from pipeline.spark_session import build_spark, table_exists

log = logging.getLogger("metrics")


def run(cfg: Config) -> None:
    spark = build_spark("daily_zone_metrics", cfg)
    trips = spark.table(cfg.trips_clean)
    metrics = T.build_daily_zone_metrics(trips).withColumn("computed_at", F.current_timestamp())

    if not table_exists(spark, cfg.daily_zone_metrics):
        log.info("Creating %s", cfg.daily_zone_metrics)
        (metrics.limit(0).writeTo(cfg.daily_zone_metrics).using("iceberg")
         .partitionedBy(F.col("pickup_date")).tableProperty("format-version", "2").create())

    metrics.writeTo(cfg.daily_zone_metrics).overwritePartitions()

    summary = spark.sql(
        f"SELECT count(*) AS rows, sum(trips) AS trips, round(sum(total_revenue), 2) AS revenue "
        f"FROM {cfg.daily_zone_metrics}").first()
    log.info("%s: %s rows, %s trips, $%s revenue", cfg.daily_zone_metrics, summary.rows, summary.trips, summary.revenue)
    spark.sql(f"SELECT * FROM {cfg.daily_zone_metrics} ORDER BY total_revenue DESC LIMIT 10").show(truncate=False)
    spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(Config())


if __name__ == "__main__":
    main()
