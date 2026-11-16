"""Unit tests for pipeline.transforms (run with: pytest -v)."""
from datetime import date, datetime

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from pipeline import transforms as T

# Raw-looking rows, using the original TLC column names.
GOOD = dict(
    VendorID=1,
    tpep_pickup_datetime=datetime(2025, 1, 10, 8, 0, 0),
    tpep_dropoff_datetime=datetime(2025, 1, 10, 8, 30, 0),
    passenger_count=1,
    trip_distance=10.0,
    RatecodeID=1,
    store_and_fwd_flag="N",
    PULocationID=132,  # JFK Airport
    DOLocationID=236,  # Upper East Side North
    payment_type=1,
    fare_amount=40.0,
    extra=0.0,
    mta_tax=0.5,
    tip_amount=10.0,
    tolls_amount=0.0,
    improvement_surcharge=1.0,
    total_amount=51.5,
    congestion_surcharge=0.0,
    Airport_fee=1.75,
)


def _spark_type(name, value):
    if name.endswith("_datetime"):
        return TimestampNTZType()  # same as the real TLC Parquet files (no time zone)
    if isinstance(value, str):
        return StringType()
    if isinstance(value, float):
        return DoubleType()
    return LongType()


RAW_SCHEMA = StructType([StructField(k, _spark_type(k, v), True) for k, v in GOOD.items()])


def raw_df(spark, *rows):
    # Explicit schema: keeps naive datetimes as-is and allows None values.
    return spark.createDataFrame([tuple(r[k] for k in GOOD) for r in rows], RAW_SCHEMA)


def zones_df(spark):
    return spark.createDataFrame(
        [
            Row(LocationID=132, Borough="Queens", Zone="JFK Airport", service_zone="Airports"),
            Row(LocationID=236, Borough="Manhattan", Zone="Upper East Side North", service_zone="Yellow Zone"),
        ]
    )


def run_validation(spark, *rows):
    df = T.add_trip_id(T.standardize_columns(raw_df(spark, *rows)))
    df = T.assign_reject_reason(df, 2025, 1)
    return T.split_valid_rejected(df)


def reasons(rejected):
    return sorted(r.reject_reason for r in rejected.collect())


# ---------------------------------------------------------------- standardize

def test_standardize_renames_and_casts(spark):
    df = T.standardize_columns(raw_df(spark, GOOD))
    assert "pickup_datetime" in df.columns
    assert "pu_location_id" in df.columns
    assert "airport_fee" in df.columns
    assert "VendorID" not in df.columns
    assert dict(df.dtypes)["pickup_datetime"] == "timestamp_ntz"


def test_trip_id_is_deterministic(spark):
    a = T.add_trip_id(T.standardize_columns(raw_df(spark, GOOD))).first().trip_id
    b = T.add_trip_id(T.standardize_columns(raw_df(spark, GOOD))).first().trip_id
    assert a == b and len(a) == 64


# ---------------------------------------------------------------- validation rules

def test_valid_row_passes(spark):
    valid, rejected = run_validation(spark, GOOD)
    assert valid.count() == 1
    assert rejected.count() == 0


@pytest.mark.parametrize("field", ["tpep_pickup_datetime", "tpep_dropoff_datetime"])
def test_null_timestamps_rejected(spark, field):
    valid, rejected = run_validation(spark, {**GOOD, field: None})
    assert valid.count() == 0
    assert reasons(rejected) == [T.REASON_NULL_TIMESTAMP]


def test_dropoff_before_pickup_rejected(spark):
    bad = {**GOOD, "tpep_dropoff_datetime": datetime(2025, 1, 10, 7, 0, 0)}
    _, rejected = run_validation(spark, bad)
    assert reasons(rejected) == [T.REASON_DROPOFF_BEFORE_PICKUP]


@pytest.mark.parametrize("fare", [0.0, -5.0, None])
def test_non_positive_fare_rejected(spark, fare):
    _, rejected = run_validation(spark, {**GOOD, "fare_amount": fare})
    assert reasons(rejected) == [T.REASON_NON_POSITIVE_FARE]


@pytest.mark.parametrize(
    "pickup",
    [datetime(2024, 12, 31, 23, 59, 59), datetime(2025, 2, 1, 0, 0, 0), datetime(2009, 1, 1, 0, 0, 0)],
)
def test_pickup_outside_month_rejected(spark, pickup):
    bad = {**GOOD, "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": pickup.replace(year=pickup.year + 20)}
    _, rejected = run_validation(spark, bad)
    assert reasons(rejected) == [T.REASON_OUTSIDE_MONTH]


def test_month_edges_are_kept(spark):
    first = {**GOOD, "tpep_pickup_datetime": datetime(2025, 1, 1, 0, 0, 0)}
    last = {**GOOD, "tpep_pickup_datetime": datetime(2025, 1, 31, 23, 59, 59),
            "tpep_dropoff_datetime": datetime(2025, 2, 1, 0, 20, 0)}
    valid, _ = run_validation(spark, first, last)
    assert valid.count() == 2


def test_exact_duplicates_keep_one(spark):
    valid, rejected = run_validation(spark, GOOD, GOOD, GOOD)
    assert valid.count() == 1
    assert reasons(rejected) == [T.REASON_DUPLICATE, T.REASON_DUPLICATE]
    # Each rejected copy still has its own unique reject_id.
    assert rejected.select("reject_id").distinct().count() == 2


def test_first_failing_rule_wins(spark):
    # Null pickup AND zero fare -> the first rule (null timestamp) is reported.
    bad = {**GOOD, "tpep_pickup_datetime": None, "fare_amount": 0.0}
    _, rejected = run_validation(spark, bad)
    assert reasons(rejected) == [T.REASON_NULL_TIMESTAMP]


def test_reconciliation_raw_equals_clean_plus_rejected(spark):
    rows = [
        GOOD,
        GOOD,  # duplicate
        {**GOOD, "fare_amount": 0.0},
        {**GOOD, "tpep_pickup_datetime": None},
        {**GOOD, "trip_distance": 3.3},  # different row -> valid
    ]
    valid, rejected = run_validation(spark, *rows)
    assert valid.count() + rejected.count() == len(rows)
    assert valid.count() == 2


# ---------------------------------------------------------------- enrichment

def test_derived_metrics(spark):
    valid, _ = run_validation(spark, GOOD)
    out = T.enrich(valid, T.prepare_zones(zones_df(spark))).first()
    assert out.pickup_date == date(2025, 1, 10)
    assert out.trip_minutes == 30.0
    assert out.avg_mph == 20.0          # 10 miles in 0.5 hours
    assert out.tip_pct == 25.0          # 10 / 40
    assert out.pickup_zone == "JFK Airport"
    assert out.pickup_borough == "Queens"
    assert out.dropoff_zone == "Upper East Side North"


def test_zero_minute_trip_has_null_speed(spark):
    same_time = {**GOOD, "tpep_dropoff_datetime": GOOD["tpep_pickup_datetime"]}
    valid, _ = run_validation(spark, same_time)
    out = T.add_derived_metrics(valid).first()
    assert out.trip_minutes == 0.0
    assert out.avg_mph is None


def test_unknown_zone_is_kept_with_null_name(spark):
    valid, _ = run_validation(spark, {**GOOD, "PULocationID": 999})
    out = T.enrich(valid, T.prepare_zones(zones_df(spark)))
    assert out.count() == 1
    assert out.first().pickup_zone is None


def test_airport_flag(spark):
    rows = [GOOD, {**GOOD, "PULocationID": 236, "DOLocationID": 236}]
    valid, _ = run_validation(spark, *rows)
    flags = sorted(r.f for r in valid.select(T.airport_trip_expr().alias("f")).collect())
    assert flags == [False, True]


# ---------------------------------------------------------------- metrics

def test_daily_zone_metrics(spark):
    rows = [GOOD, {**GOOD, "tip_amount": 0.0, "total_amount": 41.5, "trip_distance": 5.0}]
    valid, _ = run_validation(spark, *rows)
    trips = T.enrich(valid, T.prepare_zones(zones_df(spark)))
    m = T.build_daily_zone_metrics(trips).collect()
    assert len(m) == 1
    r = m[0]
    assert r.trips == 2
    assert r.total_revenue == 93.0
    assert r.avg_tip_pct == 12.5        # (25 + 0) / 2
    assert r.avg_trip_minutes == 30.0
    assert r.pickup_zone == "JFK Airport"


def test_month_bounds_december():
    assert T.month_bounds(2025, 12) == (date(2025, 12, 1), date(2026, 1, 1))
