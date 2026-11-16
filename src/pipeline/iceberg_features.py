"""Part 3.2 + 3.3 - schema evolution and time travel on trips_clean.

Run:  python -m pipeline.iceberg_features

What it shows (and saves to results/iceberg_features_report.txt):
  1. The snapshot history before any change.
  2. Schema evolution: ALTER TABLE ... ADD COLUMNS (is_airport_trip boolean).
     Proof that no data was rewritten: same data files, same snapshot count.
     Existing rows simply read the new column as NULL.
  3. Backfill: an UPDATE fills is_airport_trip -> this creates a NEW snapshot.
  4. Time travel: count rows (and airport rows) AS OF the snapshot before the
     change vs the current snapshot.

Safe to re-run: if the column already exists the ALTER and backfill are skipped and
only the time-travel report is printed.
"""
from __future__ import annotations

import logging
import os

from pipeline import transforms as T
from pipeline.config import Config
from pipeline.spark_session import build_spark

log = logging.getLogger("iceberg_features")
REPORT_PATH = os.path.join("results", "iceberg_features_report.txt")


class Report:
    def __init__(self):
        self.lines: list[str] = []

    def add(self, text: str = "") -> None:
        print(text)
        self.lines.append(text)

    def save(self) -> None:
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w") as f:
            f.write("\n".join(self.lines) + "\n")


def df_to_text(df, n: int = 50) -> str:
    return df._jdf.showString(n, 0, False)


def run(cfg: Config) -> None:
    spark = build_spark("iceberg_features", cfg)
    t = cfg.trips_clean
    rpt = Report()

    rpt.add(f"=== Snapshot history of {t} BEFORE changes ===")
    rpt.add(df_to_text(spark.sql(
        f"SELECT committed_at, snapshot_id, parent_id, operation FROM {t}.snapshots ORDER BY committed_at")))

    columns = [f.name for f in spark.table(t).schema]
    if "is_airport_trip" not in columns:
        before_snapshot = spark.sql(
            f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
        files_before = {r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.files").collect()}
        snaps_before = spark.sql(f"SELECT count(*) FROM {t}.snapshots").first()[0]

        # ---- 3.2 Schema evolution: metadata-only change -------------------
        rpt.add("=== Schema evolution: ADD COLUMN is_airport_trip ===")
        spark.sql(f"ALTER TABLE {t} ADD COLUMNS (is_airport_trip boolean "
                  f"COMMENT 'Pickup or dropoff at Newark, JFK or LaGuardia')")

        files_after = {r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.files").collect()}
        snaps_after = spark.sql(f"SELECT count(*) FROM {t}.snapshots").first()[0]
        rpt.add(f"Data files before ALTER: {len(files_before)}, after: {len(files_after)}, "
                f"identical: {files_before == files_after}")
        rpt.add(f"Snapshots before ALTER: {snaps_before}, after: {snaps_after} "
                f"(ALTER only writes a new metadata.json, no data rewrite)")
        rpt.add(df_to_text(spark.sql(
            f"SELECT count(*) AS rows, count(is_airport_trip) AS non_null_is_airport_trip FROM {t}")))

        # ---- Backfill: this is the 'change' we time-travel across ----------
        rpt.add("=== Backfill is_airport_trip (UPDATE creates a new snapshot) ===")
        ids = ", ".join(str(i) for i in T.AIRPORT_LOCATION_IDS)
        spark.sql(f"UPDATE {t} SET is_airport_trip = "
                  f"(pu_location_id IN ({ids}) OR do_location_id IN ({ids}))")
    else:
        rpt.add("is_airport_trip already exists - skipping ALTER and backfill (re-run).")
        # Snapshot just before the backfill = parent of the latest 'overwrite' snapshot.
        before_snapshot = spark.sql(
            f"SELECT parent_id FROM {t}.snapshots WHERE operation = 'overwrite' "
            f"ORDER BY committed_at DESC LIMIT 1").first()[0]

    current_snapshot = spark.sql(
        f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]

    # ---- 3.3 Time travel -------------------------------------------------
    rpt.add("=== Time travel: before vs after the change ===")
    before = spark.sql(f"SELECT count(*) AS c FROM {t} VERSION AS OF {before_snapshot}").first().c
    after = spark.sql(f"SELECT count(*) AS c FROM {t} VERSION AS OF {current_snapshot}").first().c
    airport_after = spark.sql(
        f"SELECT count(*) FROM {t} VERSION AS OF {current_snapshot} WHERE is_airport_trip").first()[0]
    rpt.add(f"Snapshot BEFORE change: {before_snapshot}  rows = {before:,}  (is_airport_trip column did not exist)")
    rpt.add(f"Snapshot AFTER  change: {current_snapshot}  rows = {after:,}  is_airport_trip = true: {airport_after:,}")
    rpt.add(f"Row count unchanged by the change: {before == after}")
    rpt.add("")
    rpt.add("=== Row count at every snapshot ===")
    for s in spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {t}.snapshots "
                       f"ORDER BY committed_at").collect():
        c = spark.sql(f"SELECT count(*) FROM {t} VERSION AS OF {s.snapshot_id}").first()[0]
        rpt.add(f"{s.committed_at}  {s.snapshot_id}  {s.operation:<10} rows = {c:,}")

    rpt.add("")
    rpt.add("Example time-travel SQL:")
    rpt.add(f"  SELECT count(*) FROM {t} VERSION AS OF {before_snapshot};")
    rpt.add(f"  SELECT count(*) FROM {t} TIMESTAMP AS OF '<committed_at of a snapshot>';")
    rpt.save()
    log.info("Report saved to %s", REPORT_PATH)
    spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(Config())


if __name__ == "__main__":
    main()
