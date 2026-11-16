-- =============================================================================
-- Part 4 - Analytics queries on the Iceberg tables, run in Snowflake.
-- Export each result (Snowsight: Download -> CSV) to results/ with the file
-- name shown above each query. Those CSVs feed the Streamlit app.
--
-- Note on "double quotes": the Iceberg column names are lower-case. Snowflake
-- upper-cases unquoted identifiers, so we quote them to match exactly.
-- =============================================================================
USE WAREHOUSE TAXI_WH;
USE SCHEMA TAXI_DB.ANALYTICS;

-- -----------------------------------------------------------------------------
-- Q1. Which 10 pickup zones earn the most revenue?      -> results/q1_top10_revenue_zones.csv
-- Revenue = SUM(total_amount) (what passengers paid incl. surcharges, tolls, tips).
-- Uses the pre-aggregated metrics table, so it scans ~8k rows instead of ~3M.
-- -----------------------------------------------------------------------------
SELECT
    "pickup_zone"                                  AS pickup_zone,
    "pickup_borough"                               AS pickup_borough,
    SUM("trips")                                   AS trips,
    ROUND(SUM("total_revenue"), 2)                 AS total_revenue,
    ROUND(SUM("total_revenue") / SUM("trips"), 2)  AS revenue_per_trip
FROM daily_zone_metrics
GROUP BY 1, 2
ORDER BY total_revenue DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Q2. How does trip demand vary by hour of day?         -> results/q2_demand_by_hour.csv
-- Timestamps are NYC local time (TLC records local wall-clock time).
-- avg_trips_per_day lets you compare hours fairly across the month.
-- -----------------------------------------------------------------------------
SELECT
    HOUR("pickup_datetime")                                    AS pickup_hour,
    COUNT(*)                                                   AS trips,
    ROUND(COUNT(*) / COUNT(DISTINCT "pickup_date"), 0)         AS avg_trips_per_day,
    ROUND(100 * RATIO_TO_REPORT(COUNT(*)) OVER (), 2)          AS pct_of_all_trips,
    ROUND(AVG("trip_minutes"), 1)                              AS avg_trip_minutes
FROM trips_clean
GROUP BY 1
ORDER BY 1;

-- -----------------------------------------------------------------------------
-- Q3. What is the average tip percentage by payment type? -> results/q3_tip_pct_by_payment_type.csv
-- Codes from the TLC data dictionary. Cash tips are usually not recorded, so
-- cash shows ~0% - that is a property of the data, not a bug.
-- -----------------------------------------------------------------------------
SELECT
    "payment_type"                    AS payment_type,
    CASE "payment_type"
        WHEN 0 THEN 'Flex Fare'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
    END                               AS payment_type_name,
    COUNT(*)                          AS trips,
    ROUND(AVG("tip_pct"), 2)          AS avg_tip_pct,
    ROUND(MEDIAN("tip_pct"), 2)       AS median_tip_pct
FROM trips_clean
GROUP BY 1, 2
ORDER BY trips DESC;
