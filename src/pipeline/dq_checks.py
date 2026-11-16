"""Part 5 - automated data quality checks on the Iceberg tables.

Run:  python -m pipeline.dq_checks

Each check returns PASS/FAIL. Severity:
  * error -> the job exits with code 1 if it fails (blocks downstream use)
  * warn  -> reported, but real-world taxi data is allowed to be a bit messy

Results are printed and saved to results/dq_report.json.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import asdict, dataclass

from pyspark.sql import SparkSession

from pipeline import transforms as T
from pipeline.config import Config
from pipeline.spark_session import build_spark

log = logging.getLogger("dq")
REPORT_PATH = os.path.join("results", "dq_report.json")


@dataclass
class CheckResult:
    name: str
    severity: str
    passed: bool
    detail: str


def scalar(spark: SparkSession, sql: str):
    return spark.sql(sql).first()[0]


def run_checks(spark: SparkSession, cfg: Config) -> list[CheckResult]:
    clean, rej, met = cfg.trips_clean, cfg.trips_rejected, cfg.daily_zone_metrics
    start, end = T.month_bounds(cfg.year, cfg.month)
    source_file = cfg.spark_path(cfg.trips_raw_key)
    results: list[CheckResult] = []

    def check(name: str, severity: str, bad_count: int, what: str):
        results.append(CheckResult(name, severity, bad_count == 0, f"{bad_count:,} {what}"))

    # 1. Non-null keys
    check("not_null_keys", "error",
          scalar(spark, f"SELECT count(*) FROM {clean} WHERE trip_id IS NULL "
                        f"OR pickup_datetime IS NULL OR dropoff_datetime IS NULL OR pickup_date IS NULL"),
          "rows with NULL trip_id / pickup / dropoff / pickup_date")

    # 2. No duplicates
    check("unique_trip_id", "error",
          scalar(spark, f"SELECT count(*) - count(DISTINCT trip_id) FROM {clean}"),
          "duplicate trip_id values")

    # 3. Valid value ranges guaranteed by the cleaning rules
    check("fare_amount_positive", "error",
          scalar(spark, f"SELECT count(*) FROM {clean} WHERE fare_amount <= 0"), "rows with fare_amount <= 0")
    check("dropoff_not_before_pickup", "error",
          scalar(spark, f"SELECT count(*) FROM {clean} WHERE dropoff_datetime < pickup_datetime"),
          "rows with dropoff before pickup")
    check("pickup_within_month", "error",
          scalar(spark, f"SELECT count(*) FROM {clean} WHERE source_file = '{source_file}' "
                        f"AND (pickup_date < DATE'{start}' OR pickup_date >= DATE'{end}')"),
          f"rows with pickup outside [{start}, {end})")

    # 4. Quarantine rows always explain themselves
    allowed = ", ".join(f"'{r}'" for r in T.REJECT_REASONS)
    check("reject_reason_valid", "error",
          scalar(spark, f"SELECT count(*) FROM {rej} WHERE reject_reason IS NULL "
                        f"OR reject_reason NOT IN ({allowed})"),
          "rejected rows with a missing/unknown reject_reason")

    # 5. Row-count reconciliation: raw = clean + rejected
    raw_n = spark.read.parquet(source_file).count()
    clean_n = scalar(spark, f"SELECT count(*) FROM {clean} WHERE source_file = '{source_file}'")
    rej_n = scalar(spark, f"SELECT count(*) FROM {rej} WHERE source_file = '{source_file}'")
    results.append(CheckResult(
        "reconciliation_raw_eq_clean_plus_rejected", "error", raw_n == clean_n + rej_n,
        f"raw={raw_n:,} clean={clean_n:,} rejected={rej_n:,} diff={raw_n - clean_n - rej_n:,}"))

    # 6. Metrics table agrees with trips_clean
    m_trips = scalar(spark, f"SELECT coalesce(sum(trips), 0) FROM {met}")
    c_trips = scalar(spark, f"SELECT count(*) FROM {clean}")
    results.append(CheckResult("metrics_trips_match_clean", "error", m_trips == c_trips,
                               f"sum(trips) in metrics={m_trips:,} vs rows in trips_clean={c_trips:,}"))
    m_rev = scalar(spark, f"SELECT coalesce(sum(total_revenue), 0) FROM {met}")
    c_rev = scalar(spark, f"SELECT coalesce(sum(total_amount), 0) FROM {clean}")
    diff = abs(float(m_rev) - float(c_rev))
    # Each metrics row is rounded to cents, so allow up to 1 cent of rounding per row.
    tolerance = 0.01 * scalar(spark, f"SELECT count(*) FROM {met}")
    results.append(CheckResult("metrics_revenue_match_clean", "error", diff <= tolerance,
                               f"metrics={float(m_rev):,.2f} clean={float(c_rev):,.2f} diff={diff:,.2f}"))

    # 7. Zone lookup coverage (warn): unknown LocationIDs should be rare
    no_zone = scalar(spark, f"SELECT count(*) FROM {clean} WHERE pickup_zone IS NULL OR dropoff_zone IS NULL")
    total = max(c_trips, 1)
    results.append(CheckResult("zone_lookup_coverage", "warn", no_zone / total < 0.01,
                               f"{no_zone:,} rows ({no_zone / total:.3%}) without a zone name (threshold 1%)"))

    # 8. Plausibility (warn): real TLC data has some outliers; we report, not block
    check("plausible_speed", "warn",
          scalar(spark, f"SELECT count(*) FROM {clean} WHERE avg_mph > 100"), "rows with avg_mph > 100")
    check("plausible_trip_length", "warn",
          scalar(spark, f"SELECT count(*) FROM {clean} WHERE trip_minutes > 24 * 60"), "rows longer than 24 hours")
    check("non_negative_amounts", "warn",
          scalar(spark, f"SELECT count(*) FROM {clean} WHERE tip_amount < 0 OR total_amount < 0"),
          "rows with negative tip or total")
    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = Config()
    spark = build_spark("dq_checks", cfg)
    results = run_checks(spark, cfg)
    spark.stop()

    print(f"\n{'CHECK':<45} {'SEVERITY':<9} {'RESULT':<7} DETAIL")
    for r in results:
        print(f"{r.name:<45} {r.severity:<9} {'PASS' if r.passed else 'FAIL':<7} {r.detail}")

    os.makedirs("results", exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump([asdict(r) for r in results], f, indent=2)
    log.info("Saved %s", REPORT_PATH)

    if any(not r.passed and r.severity == "error" for r in results):
        log.error("At least one ERROR-level data quality check failed")
        sys.exit(1)
    log.info("All error-level data quality checks passed")


if __name__ == "__main__":
    main()
