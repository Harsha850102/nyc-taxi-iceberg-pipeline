# Interview prep: the 30-minute follow-up call

The call has three parts: **walk through the design**, **explain your choices**, **make one small change live**.
The brief says you must be able to explain every line. Use this page to rehearse. (You can delete it before submitting
or keep it; it's your call.)

---

## 1. The 60-second walkthrough (say this out loud)

> "The city team wants taxi demand and revenue by neighborhood. I built a three-layer lakehouse.
> **Raw:** a Python script downloads the January 2025 yellow-taxi Parquet file and the zone lookup and lands them unchanged
> in a private S3 bucket, using a least-privilege IAM user. It's idempotent: same S3 key, and it skips the upload when the SHA-256 matches.
> **Clean:** a PySpark job validates every row against five rules, sends failures to a quarantine table with a reject reason,
> enriches the good rows with trip minutes, speed, tip percentage and zone names, and MERGEs them into an Iceberg table
> partitioned by pickup date and registered in Apache Polaris. Because the MERGE key is a hash of the row, re-running never duplicates.
> **Metrics:** a second job aggregates daily metrics per pickup zone.
> I showed schema evolution by adding is_airport_trip as a metadata-only change, and time travel by querying the snapshot before the backfill.
> Snowflake reads the same Iceberg files in place through a read-only external volume, so there's no copy of the data.
> Finally, 13 data quality checks, including raw = clean + rejected, plus 24 pytest unit tests."

---

## 2. Concepts you must be able to explain

**Apache Iceberg** is an open *table format*: a set of rules for storing a table as Parquet data files plus metadata files, so
many engines (Spark, Snowflake, Trino…) can read and write the same table safely.
Layers, from the top: **catalog** (name → current metadata file) → **metadata.json** (schema, partition spec, list of snapshots)
→ **manifest list** (one per snapshot) → **manifests** (lists of data files + column stats) → **data files** (Parquet).

**Snapshot** = the state of the table after a commit. Every write creates a new snapshot; old ones stay until expired. That's what makes **time travel** (`VERSION AS OF <snapshot_id>` / `TIMESTAMP AS OF`) and rollback possible.

**Atomic commit** = the catalog swaps the pointer from the old metadata.json to the new one in one step. Readers see the whole commit or none of it.

**Catalog (Polaris)** = the service that stores "table name → current metadata file" and does the atomic pointer swap.
Polaris implements the **Iceberg REST catalog API**, so any engine can talk to it. It also manages access (principals, roles, grants).

**MERGE INTO** = upsert. `WHEN NOT MATCHED THEN INSERT` adds only rows whose key isn't already there, so a re-run is a no-op.

**Partitioning** by `pickup_date` → each day's files are in their own folder; a query filtered on a date reads only those files (partition pruning).
Iceberg also supports *hidden partitioning* (`days(pickup_datetime)`), where users don't need to know the partition column. I used an explicit
`pickup_date` column because it's easy to read and it's also the grain of the metrics table.

**Schema evolution** = Iceberg tracks columns by **ID**, not by name or position, so adding a column only writes a new metadata.json.
No data file is rewritten; old files simply return NULL for the new column. My report proves it: same file list before and after.

**External volume (Snowflake)** = a Snowflake object holding an IAM role Snowflake assumes to read your bucket.
**Catalog integration** = tells Snowflake where the Iceberg metadata comes from. I used `OBJECT_STORE` (read metadata.json straight from S3)
because Snowflake's cloud can't reach Polaris on my laptop.

**Idempotent** = running the same job twice gives the same result as running it once. Ingest: deterministic key + hash check.
Transform: MERGE on a deterministic key. Metrics: overwrite the partitions you recompute.

**Least privilege** = every identity gets only what it needs: the pipeline user can only touch `raw/*` and `warehouse/*` in one bucket,
the Polaris user can only assume one role, Snowflake's role is read-only and needs an external ID.

---

## 3. File-by-file (what each important piece does)

| File | Key lines to explain |
|---|---|
| `config.py` | Reads env vars; builds S3 keys like `raw/yellow_trips/year=2025/month=01/...` (Hive-style partition folders) |
| `ingest.py` | `existing_sha256()` → `head_object` reads stored metadata; `upload_if_changed()` skips on same hash; `ServerSideEncryption` |
| `spark_session.py` | `spark.jars.packages` pulls Iceberg + AWS jars; `IcebergSparkSessionExtensions` enables MERGE/ALTER/`.snapshots`; catalog `lake` is `type=rest` (Polaris) or `GlueCatalog`; `S3FileIO` writes Iceberg files |
| `transforms.py` | `standardize_columns` (snake_case + types) → `add_trip_id` (SHA-256 of all columns) → `assign_reject_reason` (`F.when` chain, first failure wins; `row_number` over `trip_id` finds duplicates) → `split_valid_rejected` → `add_derived_metrics` (`timestampdiff`, divide-by-zero guards) → `join_zones` (broadcast left join, twice) → `build_daily_zone_metrics` |
| `transform_job.py` | `cache()` before counting; `create_if_missing` (partitioned, format v2); `merge_new_rows` (temp view + MERGE); `align_to_table` fills new columns after schema evolution |
| `iceberg_features.py` | `ALTER TABLE ADD COLUMNS`, `.files` and `.snapshots` metadata tables, `UPDATE` backfill, `VERSION AS OF` |
| `metrics_job.py` | `groupBy` + `agg`; `overwritePartitions()` = dynamic partition overwrite (idempotent) |
| `dq_checks.py` | error vs warn severity; reconciliation filtered by `source_file`; exit code 1 blocks downstream |
| `snowflake_metadata.py` | `metadata_log_entries` gives the newest metadata.json; turns it into a path relative to the external volume |

**Why `F.broadcast(zones)`?** The lookup has ~265 rows. Broadcasting sends it to every executor, so the 3.5M-row trips table isn't shuffled.

**Why `left` join?** An unknown LocationID shouldn't make a valid, paid trip disappear. I keep it and monitor coverage with a DQ check.

**Why `cache()`?** I count raw, clean and rejected, then MERGE. Without cache, Spark would re-read S3 and recompute everything for each action.

**Why `timestamp_ntz`?** TLC timestamps are NYC local wall-clock time with no zone. Storing them without a time zone means
`HOUR()` in Snowflake returns the real local hour, not a shifted one.

---

## 4. Likely questions with short answers

* **Why hash all columns for trip_id?** There's no natural key in TLC data. A content hash is deterministic, so the same row always gets the same id → MERGE can recognise it on re-runs, and exact duplicates collide on purpose.
* **What if two genuinely different trips have identical values?** They'd be indistinguishable in the source anyway; the brief asks to remove exact duplicates.
* **Why the order of reject rules?** Each rejected row needs exactly one reason so that raw = clean + rejected holds. The most basic problems (missing timestamps) come first.
* **Why not filter the month on dropoff too?** A trip that starts in January belongs to January's demand. It's documented as an assumption.
* **Why insert-only MERGE?** Cheapest idempotent load: re-runs write nothing. Trade-off: logic changes don't update old rows (documented limitation). Fix: `WHEN MATCHED THEN UPDATE SET *` or a full refresh.
* **Why does UPDATE create a new snapshot but ALTER doesn't?** ALTER changes only the schema in metadata.json. UPDATE changes rows, so it rewrites the affected data files (copy-on-write) and commits a snapshot.
* **Copy-on-write vs merge-on-read?** CoW rewrites files on update (slower writes, fastest reads). MoR writes small delete files (fast writes, reads merge them). For a nightly batch with many reads, CoW is a good default.
* **How would you schedule this?** Airflow/MWAA or Step Functions: ingest → transform → metrics → dq (fail stops the DAG) → Snowflake refresh. Glue jobs or EMR Serverless instead of local Spark.
* **How would you scale to years of data?** Loop months / partition the ingest, run on Glue or EMR, compact small files (`rewrite_data_files`), expire old snapshots, consider hidden partitioning by `days()` or `months()`.
* **How does Snowflake see new data?** `ALTER ICEBERG TABLE … REFRESH '<new metadata.json>'`. With Open Catalog or Glue integration you can set `AUTO_REFRESH = TRUE`.
* **Where are the secrets?** Only in `.env` (git-ignored) and `~/.aws/credentials`. The Streamlit app reads CSVs, so it has no credentials at all.
* **What would you add with more time?** Orchestration, Great Expectations / Soda for DQ, table maintenance, a Postgres-backed Polaris, CI that runs a small end-to-end test with MinIO, cost tags.

---

## 5. "Make one small change live": practise these

Practise each one in under 5 minutes. For every change: edit → `make test` → re-run the affected job → show the result.

### A. Add a new reject rule: negative trip distance
`transforms.py`:
```python
REASON_NEGATIVE_DISTANCE = "negative_trip_distance"
REJECT_REASONS = [..., REASON_OUTSIDE_MONTH, REASON_NEGATIVE_DISTANCE, REASON_DUPLICATE]
# in assign_reject_reason, before the duplicate rule:
.when(F.col("trip_distance") < 0, F.lit(REASON_NEGATIVE_DISTANCE))
```
`tests/test_transforms.py`:
```python
def test_negative_distance_rejected(spark):
    _, rejected = run_validation(spark, {**GOOD, "trip_distance": -1.0})
    assert reasons(rejected) == [T.REASON_NEGATIVE_DISTANCE]
```
Point to make: existing clean rows aren't updated by the insert-only MERGE, so for a rule change you'd fully reload
(`DROP TABLE lake.nyc_taxi.trips_clean` + `trips_rejected`, then `make transform`).

### B. Add a column to the metrics table, e.g. average distance
`build_daily_zone_metrics`: add `F.round(F.avg("trip_distance"), 2).alias("avg_trip_miles")`.
Then on the existing table: `ALTER TABLE lake.nyc_taxi.daily_zone_metrics ADD COLUMNS (avg_trip_miles double)` and `make metrics`
(or drop the metrics table; it's fully derived).

### C. Load another month
```bash
make ingest transform metrics dq TRIP_MONTH=2   # command-line variables override .env
```
Rows for February are added to the same tables (new partitions). The reconciliation check is per `source_file`, so it still holds.

### D. New Snowflake query: busiest day of the week
```sql
SELECT DAYNAME("pickup_date") AS day_of_week, SUM("trips") AS trips
FROM daily_zone_metrics GROUP BY 1 ORDER BY trips DESC;
```

### E. Time travel in one line
```sql
SELECT snapshot_id, committed_at, operation FROM lake.nyc_taxi.trips_clean.snapshots;
SELECT count(*) FROM lake.nyc_taxi.trips_clean VERSION AS OF <snapshot_id>;
```
To get an interactive Spark SQL shell with the same config:
```bash
PYTHONPATH=src python -i -c "from pipeline.config import Config; from pipeline.spark_session import build_spark; spark = build_spark('shell', Config())"
>>> spark.sql("SELECT ...").show()
```

### F. Partition evolution
```sql
ALTER TABLE lake.nyc_taxi.trips_clean ADD PARTITION FIELD hour(pickup_datetime);
```
Only new data uses the new layout; old files keep the old one, and queries still work. That's Iceberg partition evolution.
