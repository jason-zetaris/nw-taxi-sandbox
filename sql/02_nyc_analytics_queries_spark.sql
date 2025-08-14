-- NYC TLC Trip Data Analytics Queries - Spark SQL Compatible Version
-- Advanced analytics for NYC Taxi and Limousine Commission data
-- All queries use the nw_taxi schema and are optimized for Apache Spark SQL

-- Set database context
USE nw_taxi;

-- ============================================
-- 1. DEMAND ANALYSIS & PATTERNS
-- ============================================

-- Hourly demand patterns by day of week
SELECT 
    pickup_hour,
    CASE pickup_day_of_week
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END as day_name,
    trip_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_pct,
    PERCENTILE_APPROX(total_amount, 0.5) as median_total
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
        STDDEV_POP(total_amount) as stddev_total
    FROM nw_taxi.valid_trips
    WHERE pickup_date >= date_sub(current_date(), 30)
    GROUP BY pickup_hour, pickup_day_of_week
),
overall_avg AS (
    SELECT AVG(trip_count) as avg_hourly_trips
    FROM hourly_stats
)
SELECT 
    h.pickup_hour,
    CASE h.pickup_day_of_week
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END as day_name,
    h.trip_count,
    h.avg_fare,
    h.avg_total,
    h.stddev_total,
    o.avg_hourly_trips,
    (h.trip_count / o.avg_hourly_trips) as surge_multiplier,
    CASE 
        WHEN h.trip_count > o.avg_hourly_trips * 1.5 THEN 'High Demand'
        WHEN h.trip_count > o.avg_hourly_trips * 1.2 THEN 'Moderate Demand' 
        ELSE 'Normal Demand'
    END as demand_level
FROM hourly_stats h
CROSS JOIN overall_avg o
ORDER BY h.pickup_day_of_week, h.pickup_hour;

-- ============================================
-- 2. GEOGRAPHIC ANALYSIS & PATTERNS
-- ============================================

-- Most popular pickup zones by trip volume
SELECT 
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    COUNT(*) as trip_count,
    AVG(t.fare_amount) as avg_fare,
    AVG(t.trip_distance) as avg_distance,
    SUM(t.total_amount) as total_revenue,
    AVG(t.tip_percentage) as avg_tip_pct,
    COUNT(DISTINCT t.pickup_date) as active_days
FROM nw_taxi.valid_trips t
JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
WHERE t.pickup_date >= date_sub(current_date(), 30)
GROUP BY pz.borough, pz.zone
HAVING COUNT(*) >= 100
ORDER BY trip_count DESC
LIMIT 25;

-- Borough to borough flow analysis
SELECT 
    pickup_borough,
    dropoff_borough,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(trip_duration_minutes) as avg_duration,
    SUM(total_amount) as total_revenue,
    AVG(tip_percentage) as avg_tip_pct
FROM (
    SELECT 
        t.*,
        pz.borough as pickup_borough,
        dz.borough as dropoff_borough
    FROM nw_taxi.valid_trips t
    JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
    JOIN nw_taxi.taxi_zones dz ON t.dropoff_location_id = dz.location_id
    WHERE t.pickup_date >= date_sub(current_date(), 30)
) trips_with_boroughs
GROUP BY pickup_borough, dropoff_borough
HAVING COUNT(*) >= 50
ORDER BY trip_count DESC;

-- Airport trips analysis
WITH airport_zones AS (
    SELECT location_id, zone, borough
    FROM nw_taxi.taxi_zones 
    WHERE zone LIKE '%Airport%' 
       OR zone LIKE '%JFK%' 
       OR zone LIKE '%LaGuardia%' 
       OR zone LIKE '%LGA%'
       OR zone LIKE '%Newark%'
)
SELECT 
    CASE 
        WHEN ap.location_id = t.pickup_location_id THEN 'From Airport'
        WHEN ap.location_id = t.dropoff_location_id THEN 'To Airport'
        ELSE 'Inter-Airport'
    END as trip_category,
    ap.zone as airport_name,
    ap.borough,
    t.trip_type,
    COUNT(*) as trip_count,
    AVG(t.fare_amount) as avg_fare,
    AVG(t.total_amount) as avg_total,
    AVG(t.trip_distance) as avg_distance,
    AVG(t.trip_duration_minutes) as avg_duration,
    AVG(t.tip_percentage) as avg_tip_pct
FROM nw_taxi.valid_trips t
JOIN airport_zones ap ON (t.pickup_location_id = ap.location_id OR t.dropoff_location_id = ap.location_id)
WHERE t.pickup_date >= date_sub(current_date(), 30)
GROUP BY trip_category, ap.zone, ap.borough, t.trip_type
ORDER BY trip_count DESC;

-- ============================================
-- 3. PAYMENT & PRICING ANALYSIS
-- ============================================

-- Payment method impact on tips
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
    AVG(tip_amount) as avg_tip_amount,
    AVG(tip_percentage) as avg_tip_pct,
    PERCENTILE_APPROX(tip_percentage, 0.5) as median_tip_pct,
    PERCENTILE_APPROX(tip_percentage, 0.95) as p95_tip_pct
FROM nw_taxi.valid_trips
WHERE pickup_date >= date_sub(current_date(), 30)
GROUP BY payment_type, trip_type
ORDER BY avg_tip_pct DESC;

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
    WHERE pickup_date >= date_sub(current_date(), 30)
)
SELECT 
    distance_band,
    trip_type,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare,
    AVG(fare_per_mile) as avg_fare_per_mile,
    PERCENTILE_APPROX(fare_per_mile, 0.5) as median_fare_per_mile,
    AVG(total_amount) as avg_total,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(trip_speed_mph) as avg_speed
FROM distance_bands
GROUP BY distance_band, trip_type
ORDER BY 
    trip_type,
    CASE distance_band
        WHEN '0-1 miles' THEN 1
        WHEN '1-2 miles' THEN 2
        WHEN '2-5 miles' THEN 3
        WHEN '5-10 miles' THEN 4
        WHEN '10-20 miles' THEN 5
        ELSE 6
    END;

-- ============================================
-- 4. TIME-BASED ANALYSIS
-- ============================================

-- Rush hour vs non-rush hour comparison
SELECT 
    CASE 
        WHEN pickup_hour BETWEEN 7 AND 9 AND NOT is_weekend THEN 'Morning Rush'
        WHEN pickup_hour BETWEEN 17 AND 19 AND NOT is_weekend THEN 'Evening Rush'
        WHEN pickup_hour BETWEEN 10 AND 16 AND NOT is_weekend THEN 'Daytime'
        WHEN pickup_hour BETWEEN 20 AND 23 THEN 'Evening'
        WHEN is_weekend THEN 'Weekend'
        ELSE 'Late Night/Early Morning'
    END as time_period,
    trip_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(trip_speed_mph) as avg_speed,
    AVG(tip_percentage) as avg_tip_pct
FROM nw_taxi.valid_trips
WHERE pickup_date >= date_sub(current_date(), 30)
GROUP BY time_period, trip_type
ORDER BY trip_count DESC;

-- Monthly trends analysis
SELECT 
    pickup_year,
    pickup_month,
    trip_type,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_pct,
    AVG(trip_speed_mph) as avg_speed
FROM nw_taxi.valid_trips
WHERE pickup_date >= date_sub(current_date(), 90)
GROUP BY pickup_year, pickup_month, trip_type
ORDER BY pickup_year, pickup_month, trip_type;

-- Day of week patterns
SELECT 
    CASE pickup_day_of_week
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END as day_name,
    is_weekend,
    trip_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    SUM(total_amount) as total_revenue,
    AVG(tip_percentage) as avg_tip_pct
FROM nw_taxi.valid_trips
WHERE pickup_date >= date_sub(current_date(), 30)
GROUP BY pickup_day_of_week, is_weekend, trip_type
ORDER BY pickup_day_of_week, trip_type;

-- ============================================
-- 5. PERFORMANCE & EFFICIENCY ANALYSIS
-- ============================================

-- Trip speed analysis by borough and time
WITH trip_speeds AS (
    SELECT 
        t.*,
        pz.borough as pickup_borough
    FROM nw_taxi.valid_trips t
    JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
    WHERE t.pickup_date >= date_sub(current_date(), 30)
      AND t.trip_speed_mph BETWEEN 1 AND 60  -- Filter outliers
)
SELECT 
    pickup_hour,
    pickup_borough,
    trip_type,
    COUNT(*) as trip_count,
    AVG(trip_speed_mph) as avg_speed,
    PERCENTILE_APPROX(trip_speed_mph, 0.5) as median_speed,
    PERCENTILE_APPROX(trip_speed_mph, 0.25) as q1_speed,
    PERCENTILE_APPROX(trip_speed_mph, 0.75) as q3_speed,
    AVG(trip_duration_minutes) as avg_duration
FROM trip_speeds
GROUP BY pickup_hour, pickup_borough, trip_type
HAVING COUNT(*) >= 100
ORDER BY pickup_borough, pickup_hour, trip_type;

-- Driver efficiency metrics
WITH efficiency_metrics AS (
    SELECT 
        pickup_date,
        pickup_hour,
        trip_type,
        COUNT(*) as trips_per_hour,
        SUM(total_amount) as revenue_per_hour,
        AVG(trip_duration_minutes) as avg_duration,
        SUM(trip_duration_minutes) as total_active_minutes,
        STDDEV_POP(trip_duration_minutes) as duration_stddev
    FROM nw_taxi.valid_trips
    WHERE pickup_date >= date_sub(current_date(), 30)
    GROUP BY pickup_date, pickup_hour, trip_type
)
SELECT 
    trip_type,
    AVG(trips_per_hour) as avg_trips_per_hour,
    AVG(revenue_per_hour) as avg_revenue_per_hour,
    AVG(avg_duration) as avg_trip_duration,
    AVG(total_active_minutes) as avg_active_minutes_per_hour,
    AVG(duration_stddev) as avg_duration_variability
FROM efficiency_metrics
GROUP BY trip_type
ORDER BY avg_revenue_per_hour DESC;

-- ============================================
-- 6. ANOMALY DETECTION & DATA QUALITY
-- ============================================

-- Unusual fare patterns detection
WITH fare_stats AS (
    SELECT 
        trip_type,
        AVG(fare_per_mile) as avg_fare_per_mile,
        STDDEV_POP(fare_per_mile) as stddev_fare_per_mile,
        AVG(total_amount) as avg_total,
        STDDEV_POP(total_amount) as stddev_total
    FROM nw_taxi.valid_trips
    WHERE pickup_date >= date_sub(current_date(), 7)
      AND fare_per_mile > 0
    GROUP BY trip_type
)
SELECT 
    t.trip_id,
    t.pickup_datetime,
    t.trip_type,
    t.fare_amount,
    t.trip_distance,
    t.fare_per_mile,
    t.total_amount,
    fs.avg_fare_per_mile,
    fs.stddev_fare_per_mile,
    ABS(t.fare_per_mile - fs.avg_fare_per_mile) / fs.stddev_fare_per_mile as fare_z_score,
    CASE 
        WHEN ABS(t.fare_per_mile - fs.avg_fare_per_mile) / fs.stddev_fare_per_mile > 3 THEN 'Anomaly'
        WHEN ABS(t.fare_per_mile - fs.avg_fare_per_mile) / fs.stddev_fare_per_mile > 2 THEN 'Outlier'
        ELSE 'Normal'
    END as fare_classification
FROM nw_taxi.valid_trips t
JOIN fare_stats fs ON t.trip_type = fs.trip_type
WHERE t.pickup_date >= date_sub(current_date(), 7)
  AND ABS(t.fare_per_mile - fs.avg_fare_per_mile) / fs.stddev_fare_per_mile > 2
ORDER BY fare_z_score DESC;

-- ============================================
-- 7. BUSINESS INTELLIGENCE QUERIES
-- ============================================

-- Revenue summary by borough and trip type
SELECT 
    pz.borough,
    t.trip_type,
    COUNT(*) as total_trips,
    SUM(t.total_amount) as total_revenue,
    AVG(t.total_amount) as avg_revenue_per_trip,
    SUM(t.tip_amount) as total_tips,
    AVG(t.tip_percentage) as avg_tip_percentage,
    COUNT(DISTINCT t.pickup_date) as active_days
FROM nw_taxi.valid_trips t
JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
WHERE t.pickup_date >= date_sub(current_date(), 30)
GROUP BY pz.borough, t.trip_type
ORDER BY total_revenue DESC;

-- Peak demand forecasting data
SELECT 
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    CASE pickup_day_of_week
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END as day_name,
    is_weekend,
    trip_type,
    COUNT(*) as trip_count,
    SUM(total_amount) as total_revenue,
    AVG(fare_amount) as avg_fare
FROM nw_taxi.valid_trips
WHERE pickup_date >= date_sub(current_date(), 30)
GROUP BY pickup_date, pickup_hour, pickup_day_of_week, is_weekend, trip_type
ORDER BY pickup_date, pickup_hour, trip_type;

-- Market share analysis
SELECT 
    trip_type,
    COUNT(*) as trip_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as market_share_trips,
    SUM(total_amount) as total_revenue,
    SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER () as market_share_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_percentage
FROM nw_taxi.valid_trips
WHERE pickup_date >= date_sub(current_date(), 30)
GROUP BY trip_type
ORDER BY trip_count DESC;

-- ============================================
-- 8. OPTIMIZATION QUERIES FOR SPARK
-- ============================================

-- Cache frequently used tables for better performance
-- Run these commands to optimize query performance:

-- CACHE TABLE nw_taxi.valid_trips;
-- CACHE TABLE nw_taxi.trips_with_zones;

-- Optimize Delta tables (if using Delta Lake)
-- OPTIMIZE nw_taxi.taxi_trips;
-- OPTIMIZE nw_taxi.taxi_zones;

-- Update table statistics for better query planning
-- ANALYZE TABLE nw_taxi.taxi_trips COMPUTE STATISTICS;
-- ANALYZE TABLE nw_taxi.taxi_zones COMPUTE STATISTICS;

-- Vacuum old files (Delta Lake feature)
-- VACUUM nw_taxi.taxi_trips RETAIN 168 HOURS;  -- Keep 7 days of history