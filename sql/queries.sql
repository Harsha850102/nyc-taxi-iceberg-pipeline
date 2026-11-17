-- =============================================================================
-- Part 4 - Analytics queries on the Iceberg tables, run in Snowflake.
-- Fully qualified names, so they run in any worksheet without USE statements.
-- Export each result (Download -> CSV) to results/ with the file name shown.
-- =============================================================================

-- Q1. Which 10 pickup zones earn the most revenue?   -> results/q1_top10_revenue_zones.csv
-- Revenue = SUM(total_amount): fare + surcharges + tolls + tip.
SELECT
    pickup_zone,
    pickup_borough,
    SUM(trips)                                                AS trips,
    ROUND(SUM(total_revenue), 2)                              AS total_revenue,
    ROUND(SUM(total_revenue) / SUM(trips), 2)                 AS revenue_per_trip,
    ROUND(100 * SUM(total_revenue) / SUM(SUM(total_revenue)) OVER (), 2) AS pct_of_total_revenue
FROM TAXI_DB.ANALYTICS.daily_zone_metrics
GROUP BY pickup_zone, pickup_borough
ORDER BY total_revenue DESC
LIMIT 10;

-- Q2. How does trip demand vary by hour of day?      -> results/q2_demand_by_hour.csv
-- Timestamps are NYC local time.
SELECT
    HOUR(pickup_datetime)                                     AS pickup_hour,
    LPAD(HOUR(pickup_datetime)::STRING, 2, '0') || ':00'      AS hour_label,
    COUNT(*)                                                  AS trips,
    ROUND(COUNT(*) / COUNT(DISTINCT pickup_date), 0)          AS avg_trips_per_day,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)          AS pct_of_all_trips,
    ROUND(AVG(trip_minutes), 1)                               AS avg_trip_minutes,
    ROUND(AVG(total_amount), 2)                               AS avg_revenue_per_trip
FROM TAXI_DB.ANALYTICS.trips_clean
GROUP BY pickup_hour, hour_label
ORDER BY pickup_hour;

-- Q3. What is the average tip percentage by payment type? -> results/q3_tip_pct_by_payment_type.csv
-- Cash tips are usually not recorded by TLC, so cash shows ~0%.
SELECT
    payment_type,
    CASE payment_type
        WHEN 0 THEN 'Flex Fare'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
    END                                                       AS payment_type_name,
    COUNT(*)                                                  AS trips,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)          AS pct_of_trips,
    ROUND(AVG(tip_pct), 2)                                    AS avg_tip_pct,
    ROUND(MEDIAN(tip_pct), 2)                                 AS median_tip_pct,
    ROUND(100 * AVG(IFF(tip_amount > 0, 1, 0)), 1)            AS pct_trips_with_tip
FROM TAXI_DB.ANALYTICS.trips_clean
GROUP BY payment_type, payment_type_name
ORDER BY trips DESC;
