-- NYC TLC Trip Data Analytics Queries with nw_taxi namespace
-- Advanced analytics for NYC Taxi and Limousine Commission data
-- All queries use the nw_taxi schema for proper data isolation

-- ============================================
-- 1. DEMAND ANALYSIS & PATTERNS
-- ============================================

-- Hourly demand patterns by day of week
SELECT 
    pickup_hour,
    CASE pickup_day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    trip_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_pct,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) as median_total
FROM nw_taxi.valid_trips
GROUP BY pickup_hour, pickup_day_of_week, trip_type
ORDER BY pickup_day_of_week, pickup_hour, trip_type;

-- Peak hours identification with surge potential
WITH hourly_stats AS (
    SELECT 
        pickup_hour,
        pickup_day_of_week,
        COUNT(*) as trip_count,
        AVG(fare_amount) as avg_fare,
        AVG(total_amount) as avg_total,
        STDDEV(total_amount) as stddev_total
    FROM nw_taxi.valid_trips
    WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY pickup_hour, pickup_day_of_week
),
overall_avg AS (
    SELECT AVG(trip_count) as avg_hourly_trips
    FROM hourly_stats
)
SELECT 
    h.pickup_hour,
    CASE h.pickup_day_of_week
        WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    h.trip_count,
    h.avg_fare,
    h.avg_total,
    ROUND((h.trip_count::NUMERIC / o.avg_hourly_trips - 1) * 100, 1) as demand_vs_avg_pct,
    CASE 
        WHEN h.trip_count > o.avg_hourly_trips * 1.5 THEN 'High Demand'
        WHEN h.trip_count > o.avg_hourly_trips * 1.2 THEN 'Medium Demand'
        ELSE 'Normal Demand'
    END as demand_level
FROM hourly_stats h
CROSS JOIN overall_avg o
ORDER BY h.trip_count DESC;

-- Monthly trends analysis
SELECT 
    pickup_year,
    pickup_month,
    trip_type,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare,
    AVG(tip_percentage) as avg_tip_pct,
    COUNT(DISTINCT pickup_location_id) as unique_pickup_zones,
    LAG(COUNT(*)) OVER (PARTITION BY trip_type ORDER BY pickup_year, pickup_month) as prev_month_trips,
    ROUND(
        (COUNT(*)::NUMERIC - LAG(COUNT(*)) OVER (PARTITION BY trip_type ORDER BY pickup_year, pickup_month)) 
        / NULLIF(LAG(COUNT(*)) OVER (PARTITION BY trip_type ORDER BY pickup_year, pickup_month), 0) * 100, 
        2
    ) as month_over_month_growth
FROM nw_taxi.valid_trips
GROUP BY pickup_year, pickup_month, trip_type
ORDER BY pickup_year, pickup_month, trip_type;

-- ============================================
-- 2. GEOGRAPHIC ANALYSIS
-- ============================================

-- Top pickup zones by volume and revenue
SELECT 
    pz.borough,
    pz.zone as pickup_zone,
    COUNT(*) as trip_count,
    SUM(t.total_amount) as total_revenue,
    AVG(t.fare_amount) as avg_fare,
    AVG(t.trip_distance) as avg_distance,
    AVG(t.tip_percentage) as avg_tip_pct,
    AVG(t.trip_duration_minutes) as avg_duration_min,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as market_share_pct
FROM nw_taxi.valid_trips t
JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
WHERE t.pickup_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY pz.borough, pz.zone
ORDER BY trip_count DESC
LIMIT 25;

-- Borough-to-borough flow analysis
SELECT 
    pickup_borough,
    dropoff_borough,
    trip_count,
    avg_fare,
    avg_distance,
    avg_duration,
    total_revenue,
    ROUND(trip_count::NUMERIC / SUM(trip_count) OVER () * 100, 2) as flow_percentage
FROM (
    SELECT 
        pz.borough as pickup_borough,
        dz.borough as dropoff_borough,
        COUNT(*) as trip_count,
        AVG(t.fare_amount) as avg_fare,
        AVG(t.trip_distance) as avg_distance,
        AVG(t.trip_duration_minutes) as avg_duration,
        SUM(t.total_amount) as total_revenue
    FROM nw_taxi.valid_trips t
    JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
    JOIN nw_taxi.taxi_zones dz ON t.dropoff_location_id = dz.location_id
    WHERE t.pickup_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY pz.borough, dz.borough
) borough_flows
ORDER BY trip_count DESC;

-- Airport trips analysis
WITH airport_zones AS (
    SELECT location_id, zone
    FROM nw_taxi.taxi_zones 
    WHERE zone ILIKE '%Airport%' 
       OR zone ILIKE '%JFK%' 
       OR zone ILIKE '%LaGuardia%' 
       OR zone ILIKE '%LGA%'
       OR zone ILIKE '%Newark%'
       OR zone ILIKE '%EWR%'
)
SELECT 
    az.zone as airport,
    CASE 
        WHEN t.pickup_location_id = az.location_id THEN 'From Airport'
        WHEN t.dropoff_location_id = az.location_id THEN 'To Airport'
    END as direction,
    t.trip_type,
    COUNT(*) as trip_count,
    AVG(t.fare_amount) as avg_fare,
    AVG(t.total_amount) as avg_total,
    AVG(t.trip_distance) as avg_distance,
    AVG(t.trip_duration_minutes) as avg_duration,
    AVG(t.tip_percentage) as avg_tip_pct
FROM nw_taxi.valid_trips t
JOIN airport_zones az ON (t.pickup_location_id = az.location_id OR t.dropoff_location_id = az.location_id)
WHERE t.pickup_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY az.zone, direction, t.trip_type
ORDER BY trip_count DESC;

-- ============================================
-- 3. PAYMENT AND PRICING ANALYSIS
-- ============================================

-- Payment method analysis with tipping patterns
SELECT 
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
        ELSE 'Other'
    END as payment_method,
    trip_type,
    COUNT(*) as trip_count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY trip_type) * 100, 2) as pct_of_type,
    SUM(total_amount) as total_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    AVG(tip_percentage) as avg_tip_pct,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tip_percentage) as median_tip_pct,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tip_percentage) as p95_tip_pct
FROM nw_taxi.valid_trips
WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY payment_type, trip_type
ORDER BY trip_type, trip_count DESC;

-- Fare analysis by distance bands
WITH distance_bands AS (
    SELECT 
        *,
        CASE 
            WHEN trip_distance <= 1 THEN '0-1 miles'
            WHEN trip_distance <= 2 THEN '1-2 miles'
            WHEN trip_distance <= 5 THEN '2-5 miles'
            WHEN trip_distance <= 10 THEN '5-10 miles'
            WHEN trip_distance <= 20 THEN '10-20 miles'
            ELSE '20+ miles'
        END as distance_band
    FROM nw_taxi.valid_trips
    WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    distance_band,
    trip_type,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare,
    AVG(fare_per_mile) as avg_fare_per_mile,
    AVG(total_amount) as avg_total,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(trip_speed_mph) as avg_speed,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fare_per_mile) as median_fare_per_mile
FROM distance_bands
GROUP BY distance_band, trip_type
ORDER BY trip_type, 
         CASE distance_band
             WHEN '0-1 miles' THEN 1
             WHEN '1-2 miles' THEN 2
             WHEN '2-5 miles' THEN 3
             WHEN '5-10 miles' THEN 4
             WHEN '10-20 miles' THEN 5
             ELSE 6
         END;

-- Rate code analysis
SELECT 
    CASE rate_code_id
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Unknown'
    END as rate_type,
    trip_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(total_amount) as avg_total,
    AVG(trip_distance) as avg_distance,
    AVG(trip_duration_minutes) as avg_duration
FROM nw_taxi.valid_trips
WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY rate_code_id, trip_type
ORDER BY trip_count DESC;

-- ============================================
-- 4. TIME-BASED PERFORMANCE ANALYSIS
-- ============================================

-- Daily performance metrics
SELECT 
    pickup_date,
    trip_type,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_pct,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(trip_speed_mph) as avg_speed,
    COUNT(DISTINCT pickup_location_id) as unique_pickup_zones,
    COUNT(DISTINCT dropoff_location_id) as unique_dropoff_zones
FROM nw_taxi.valid_trips
WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY pickup_date, trip_type
ORDER BY pickup_date DESC, trip_type;

-- Weekend vs weekday comparison
SELECT 
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as period_type,
    trip_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_pct,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(trip_speed_mph) as avg_speed,
    SUM(total_amount) as total_revenue
FROM nw_taxi.valid_trips
WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY is_weekend, trip_type
ORDER BY trip_type, is_weekend;

-- Rush hour analysis (7-9 AM and 5-7 PM)
WITH rush_hour_classification AS (
    SELECT 
        *,
        CASE 
            WHEN pickup_hour BETWEEN 7 AND 9 THEN 'Morning Rush'
            WHEN pickup_hour BETWEEN 17 AND 19 THEN 'Evening Rush'
            WHEN pickup_hour BETWEEN 10 AND 16 THEN 'Daytime'
            WHEN pickup_hour BETWEEN 20 AND 23 THEN 'Evening'
            ELSE 'Late Night/Early Morning'
        END as time_period
    FROM nw_taxi.valid_trips
    WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
      AND NOT is_weekend
)
SELECT 
    time_period,
    trip_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(trip_speed_mph) as avg_speed,
    AVG(tip_percentage) as avg_tip_pct,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY trip_type) * 100, 2) as pct_of_total
FROM rush_hour_classification
GROUP BY time_period, trip_type
ORDER BY trip_type, 
         CASE time_period
             WHEN 'Morning Rush' THEN 1
             WHEN 'Daytime' THEN 2
             WHEN 'Evening Rush' THEN 3
             WHEN 'Evening' THEN 4
             ELSE 5
         END;

-- ============================================
-- 5. EFFICIENCY AND PERFORMANCE METRICS
-- ============================================

-- Speed analysis by hour and borough
SELECT 
    pickup_hour,
    pz.borough,
    trip_type,
    COUNT(*) as trip_count,
    AVG(trip_speed_mph) as avg_speed,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_speed_mph) as median_speed,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_speed_mph) as q1_speed,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_speed_mph) as q3_speed,
    AVG(trip_duration_minutes) as avg_duration
FROM nw_taxi.valid_trips t
JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
WHERE t.pickup_date >= CURRENT_DATE - INTERVAL '30 days'
  AND t.trip_speed_mph IS NOT NULL
  AND t.trip_speed_mph BETWEEN 1 AND 60  -- Filter unrealistic speeds
GROUP BY pickup_hour, pz.borough, trip_type
HAVING COUNT(*) >= 100  -- Minimum sample size
ORDER BY pickup_hour, pz.borough, trip_type;

-- Trip efficiency analysis
WITH efficiency_metrics AS (
    SELECT 
        pickup_location_id,
        dropoff_location_id,
        COUNT(*) as trip_count,
        AVG(trip_distance) as avg_distance,
        AVG(trip_duration_minutes) as avg_duration,
        AVG(trip_speed_mph) as avg_speed,
        AVG(fare_per_mile) as avg_fare_per_mile,
        STDDEV(trip_duration_minutes) as duration_stddev
    FROM nw_taxi.valid_trips
    WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
      AND trip_speed_mph IS NOT NULL
      AND trip_speed_mph BETWEEN 1 AND 60
    GROUP BY pickup_location_id, dropoff_location_id
    HAVING COUNT(*) >= 20
)
SELECT 
    pz.zone as pickup_zone,
    dz.zone as dropoff_zone,
    pz.borough as pickup_borough,
    dz.borough as dropoff_borough,
    em.trip_count,
    ROUND(em.avg_distance, 2) as avg_distance,
    ROUND(em.avg_duration, 1) as avg_duration_min,
    ROUND(em.avg_speed, 1) as avg_speed_mph,
    ROUND(em.avg_fare_per_mile, 2) as avg_fare_per_mile,
    ROUND(em.duration_stddev, 1) as duration_variability
FROM efficiency_metrics em
JOIN nw_taxi.taxi_zones pz ON em.pickup_location_id = pz.location_id
JOIN nw_taxi.taxi_zones dz ON em.dropoff_location_id = dz.location_id
ORDER BY em.trip_count DESC
LIMIT 50;

-- ============================================
-- 6. ANOMALY DETECTION
-- ============================================

-- Unusual fare patterns
WITH fare_stats AS (
    SELECT 
        AVG(fare_per_mile) as avg_fare_per_mile,
        STDDEV(fare_per_mile) as stddev_fare_per_mile,
        AVG(total_amount) as avg_total,
        STDDEV(total_amount) as stddev_total
    FROM nw_taxi.valid_trips
    WHERE pickup_date >= CURRENT_DATE - INTERVAL '7 days'
      AND fare_per_mile IS NOT NULL
      AND fare_per_mile > 0
)
SELECT 
    t.pickup_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.trip_distance,
    t.fare_amount,
    t.total_amount,
    t.fare_per_mile,
    t.trip_duration_minutes,
    t.payment_type,
    ROUND((t.fare_per_mile - fs.avg_fare_per_mile) / NULLIF(fs.stddev_fare_per_mile, 0), 2) as fare_per_mile_zscore,
    ROUND((t.total_amount - fs.avg_total) / NULLIF(fs.stddev_total, 0), 2) as total_amount_zscore,
    CASE 
        WHEN ABS((t.fare_per_mile - fs.avg_fare_per_mile) / NULLIF(fs.stddev_fare_per_mile, 0)) > 3 THEN 'High Fare Anomaly'
        WHEN ABS((t.total_amount - fs.avg_total) / NULLIF(fs.stddev_total, 0)) > 3 THEN 'High Total Anomaly'
        WHEN t.trip_duration_minutes < 2 AND t.trip_distance > 1 THEN 'Suspiciously Fast'
        WHEN t.trip_speed_mph > 80 THEN 'Unrealistic Speed'
        ELSE 'Review Required'
    END as anomaly_type
FROM nw_taxi.valid_trips t
CROSS JOIN fare_stats fs
WHERE t.pickup_date >= CURRENT_DATE - INTERVAL '7 days'
  AND (
      ABS((t.fare_per_mile - fs.avg_fare_per_mile) / NULLIF(fs.stddev_fare_per_mile, 0)) > 3
      OR ABS((t.total_amount - fs.avg_total) / NULLIF(fs.stddev_total, 0)) > 3
      OR (t.trip_duration_minutes < 2 AND t.trip_distance > 1)
      OR t.trip_speed_mph > 80
  )
ORDER BY t.pickup_datetime DESC
LIMIT 100;

-- ============================================
-- 7. BUSINESS INTELLIGENCE SUMMARY QUERIES
-- ============================================

-- Executive dashboard summary
WITH daily_summary AS (
    SELECT 
        pickup_date,
        COUNT(*) as total_trips,
        SUM(total_amount) as total_revenue,
        AVG(fare_amount) as avg_fare,
        COUNT(DISTINCT pickup_location_id) as unique_zones_served
    FROM nw_taxi.valid_trips
    WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY pickup_date
),
growth_calc AS (
    SELECT 
        *,
        LAG(total_trips) OVER (ORDER BY pickup_date) as prev_day_trips,
        LAG(total_revenue) OVER (ORDER BY pickup_date) as prev_day_revenue
    FROM daily_summary
)
SELECT 
    'Last 30 Days Summary' as metric_group,
    SUM(total_trips) as total_trips,
    SUM(total_revenue) as total_revenue,
    AVG(avg_fare) as avg_fare,
    AVG(unique_zones_served) as avg_zones_per_day,
    MAX(total_trips) as peak_day_trips,
    MIN(total_trips) as min_day_trips,
    AVG(total_trips) as avg_daily_trips,
    ROUND(AVG((total_trips - prev_day_trips)::NUMERIC / NULLIF(prev_day_trips, 0) * 100), 2) as avg_daily_growth_pct
FROM growth_calc;

-- Market share by trip type
SELECT 
    trip_type,
    COUNT(*) as trip_count,
    SUM(total_amount) as total_revenue,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as trip_market_share_pct,
    ROUND(SUM(total_amount)::NUMERIC / SUM(SUM(total_amount)) OVER () * 100, 2) as revenue_market_share_pct,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_pct
FROM nw_taxi.valid_trips
WHERE pickup_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY trip_type
ORDER BY trip_count DESC;