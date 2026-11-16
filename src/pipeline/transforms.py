"""Pure PySpark transformation functions.

Everything in this module takes a DataFrame and returns a DataFrame. Nothing here
talks to S3, Iceberg or a catalog, which is what makes it easy to unit-test with
pytest on a local SparkSession (see tests/test_transforms.py).
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Raw TLC column names -> our snake_case names. Anything not listed is lower-cased.
COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}

# Target types for the raw columns, so every month lands with the same schema.
RAW_TYPES = {
    "vendor_id": "int",
    "pickup_datetime": "timestamp_ntz",
    "dropoff_datetime": "timestamp_ntz",
    "passenger_count": "int",
    "trip_distance": "double",
    "ratecode_id": "int",
    "store_and_fwd_flag": "string",
    "pu_location_id": "int",
    "do_location_id": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "cbd_congestion_fee": "double",
}

# Reject reasons, in the order they are evaluated. A row gets the FIRST reason it fails.
REASON_NULL_TIMESTAMP = "null_pickup_or_dropoff"
REASON_DROPOFF_BEFORE_PICKUP = "dropoff_before_pickup"
REASON_NON_POSITIVE_FARE = "fare_not_positive"
REASON_OUTSIDE_MONTH = "pickup_outside_month"
REASON_DUPLICATE = "duplicate"
REJECT_REASONS = [
    REASON_NULL_TIMESTAMP,
    REASON_DROPOFF_BEFORE_PICKUP,
    REASON_NON_POSITIVE_FARE,
    REASON_OUTSIDE_MONTH,
    REASON_DUPLICATE,
]

# Taxi zone LocationIDs for the three airports (from taxi_zone_lookup.csv).
AIRPORT_LOCATION_IDS = (1, 132, 138)  # 1 = Newark Airport, 132 = JFK Airport, 138 = LaGuardia Airport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def month_bounds(year: int, month: int) -> tuple[date, date]:
    """Return [start, end) dates for a month, e.g. (2025-01-01, 2025-02-01)."""
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


# ---------------------------------------------------------------------------
# Step 1: standardize
# ---------------------------------------------------------------------------

def standardize_columns(df: DataFrame) -> DataFrame:
    """Rename raw TLC columns to snake_case and cast them to stable types."""
    for col_name in df.columns:
        new_name = COLUMN_RENAMES.get(col_name, col_name.lower())
        if new_name != col_name:
            df = df.withColumnRenamed(col_name, new_name)
    for col_name, col_type in RAW_TYPES.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
    return df


def add_trip_id(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Add trip_id = SHA-256 of every original column.

    The TLC data has no primary key, so we build a deterministic one. Two rows get
    the same trip_id only if they are exact duplicates. This id is the MERGE key
    that makes re-runs idempotent.
    """
    cols = columns or df.columns
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]
    return df.withColumn("trip_id", F.sha2(F.concat_ws("||", *parts), 256))


# ---------------------------------------------------------------------------
# Step 2: validate
# ---------------------------------------------------------------------------

def assign_reject_reason(df: DataFrame, year: int, month: int) -> DataFrame:
    """Add reject_reason (NULL means the row is valid) and dup_rank.

    Requires trip_id. Exact duplicates share a trip_id, so the 2nd, 3rd, ... copy of a
    valid row is tagged 'duplicate' and the first copy is kept.
    """
    start, end = month_bounds(year, month)
    pickup, dropoff, fare = F.col("pickup_datetime"), F.col("dropoff_datetime"), F.col("fare_amount")

    # Identical rows are interchangeable, so ordering inside the window does not matter.
    dup_window = Window.partitionBy("trip_id").orderBy(F.lit(1))
    df = df.withColumn("dup_rank", F.row_number().over(dup_window))

    reason = (
        F.when(pickup.isNull() | dropoff.isNull(), F.lit(REASON_NULL_TIMESTAMP))
        .when(dropoff < pickup, F.lit(REASON_DROPOFF_BEFORE_PICKUP))
        .when(fare.isNull() | (fare <= 0), F.lit(REASON_NON_POSITIVE_FARE))
        .when(
            (F.to_date(pickup) < F.lit(start)) | (F.to_date(pickup) >= F.lit(end)),
            F.lit(REASON_OUTSIDE_MONTH),
        )
        .when(F.col("dup_rank") > 1, F.lit(REASON_DUPLICATE))
    )
    return df.withColumn("reject_reason", reason)


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a frame that has reject_reason into (valid, rejected).

    rejected gets a reject_id that stays unique even for duplicate copies
    (trip_id + copy number), so the quarantine table can also be MERGEd idempotently.
    """
    valid = df.filter(F.col("reject_reason").isNull()).drop("reject_reason", "dup_rank")
    rejected = (
        df.filter(F.col("reject_reason").isNotNull())
        .withColumn("reject_id", F.sha2(F.concat_ws("-", "trip_id", F.col("dup_rank").cast("string")), 256))
        .drop("dup_rank")
    )
    return valid, rejected


# ---------------------------------------------------------------------------
# Step 3: enrich
# ---------------------------------------------------------------------------

def prepare_zones(zones_raw: DataFrame) -> DataFrame:
    """Clean the zone lookup CSV into (location_id, borough, zone, service_zone)."""
    return zones_raw.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )


def add_derived_metrics(df: DataFrame) -> DataFrame:
    """Add pickup_date, trip_minutes, avg_mph and tip_pct."""
    seconds = F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)")
    minutes = F.round(seconds / 60.0, 2)
    return (
        df.withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("trip_minutes", minutes)
        # Guard against divide-by-zero: a 0-minute trip has no meaningful speed.
        .withColumn(
            "avg_mph",
            F.when(seconds > 0, F.round(F.col("trip_distance") / (seconds / 3600.0), 2)),
        )
        # Tip as a percentage of the fare (fare is always > 0 after validation).
        .withColumn(
            "tip_pct",
            F.when(F.col("fare_amount") > 0, F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)),
        )
    )


def join_zones(df: DataFrame, zones: DataFrame) -> DataFrame:
    """Left-join the zone lookup twice: once for pickup, once for dropoff.

    Left join so a trip with an unknown LocationID is kept (zone columns are NULL)
    instead of silently disappearing.
    """
    pu = F.broadcast(zones).select(
        F.col("location_id").alias("_pu_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
    )
    do = F.broadcast(zones).select(
        F.col("location_id").alias("_do_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    )
    return (
        df.join(pu, df["pu_location_id"] == pu["_pu_id"], "left")
        .join(do, df["do_location_id"] == do["_do_id"], "left")
        .drop("_pu_id", "_do_id")
    )


def enrich(df: DataFrame, zones: DataFrame) -> DataFrame:
    return join_zones(add_derived_metrics(df), zones)


def airport_trip_expr() -> Column:
    """True if the trip starts or ends at Newark, JFK or LaGuardia."""
    ids = list(AIRPORT_LOCATION_IDS)
    return F.col("pu_location_id").isin(ids) | F.col("do_location_id").isin(ids)


# ---------------------------------------------------------------------------
# Step 4: aggregate
# ---------------------------------------------------------------------------

def build_daily_zone_metrics(trips: DataFrame) -> DataFrame:
    """Daily metrics by pickup date and pickup zone.

    total_revenue = SUM(total_amount), i.e. everything the passenger paid
    (fare + surcharges + tolls + tip). See README assumptions.
    """
    return (
        trips.groupBy("pickup_date", "pu_location_id", "pickup_borough", "pickup_zone")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
        )
    )
