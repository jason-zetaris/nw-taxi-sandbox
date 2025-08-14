-- Taxi Service Analytics Queries
-- Comprehensive SQL queries for data analysis and business intelligence

-- ============================================
-- 1. DEMAND FORECASTING & SURGE PRICING
-- ============================================

-- Hourly demand pattern by zone
WITH hourly_demand AS (
    SELECT 
        DATE_TRUNC('hour', pickup_datetime) as hour,
        EXTRACT(HOUR FROM pickup_datetime) as hour_of_day,
        EXTRACT(DOW FROM pickup_datetime) as day_of_week,
        pickup_zone_id,
        z.zone_name,
        z.area_type,
        COUNT(*) as trip_count,
        AVG(total_fare) as avg_fare,
        AVG(surge_multiplier) as avg_surge,
        AVG(wait_time) as avg_wait_time
    FROM trips t
    JOIN zones z ON t.pickup_zone_id = z.zone_id
    WHERE trip_status = 'Completed'
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT 
    hour_of_day,
    CASE day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    zone_name,
    area_type,
    AVG(trip_count) as avg_trips,
    AVG(avg_fare) as avg_fare,
    AVG(avg_surge) as avg_surge_multiplier,
    AVG(avg_wait_time) as avg_wait_seconds
FROM hourly_demand
GROUP BY hour_of_day, day_of_week, zone_name, area_type
ORDER BY day_of_week, hour_of_day, avg_trips DESC;

-- Surge pricing opportunities analysis
WITH demand_supply AS (
    SELECT 
        DATE_TRUNC('hour', pickup_datetime) as hour,
        pickup_zone_id,
        COUNT(DISTINCT trip_id) as demand,
        COUNT(DISTINCT driver_id) as supply,
        AVG(wait_time) as avg_wait,
        AVG(total_fare) as avg_fare
    FROM trips
    WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1, 2
)
SELECT 
    hour,
    z.zone_name,
    demand,
    supply,
    ROUND(demand::NUMERIC / NULLIF(supply, 0), 2) as demand_supply_ratio,
    avg_wait,
    avg_fare,
    CASE 
        WHEN demand::NUMERIC / NULLIF(supply, 0) > 3 THEN 'High Surge Recommended'
        WHEN demand::NUMERIC / NULLIF(supply, 0) > 2 THEN 'Moderate Surge Recommended'
        WHEN demand::NUMERIC / NULLIF(supply, 0) > 1.5 THEN 'Low Surge Recommended'
        ELSE 'No Surge Needed'
    END as surge_recommendation
FROM demand_supply ds
JOIN zones z ON ds.pickup_zone_id = z.zone_id
WHERE demand > 10
ORDER BY hour DESC, demand_supply_ratio DESC;

-- Weather impact on demand
SELECT 
    w.weather_condition,
    COUNT(t.trip_id) as total_trips,
    AVG(t.total_fare) as avg_fare,
    AVG(t.surge_multiplier) as avg_surge,
    AVG(t.wait_time) as avg_wait_time,
    STDDEV(t.total_fare) as fare_stddev
FROM trips t
JOIN weather_data w ON 
    t.pickup_zone_id = w.zone_id 
    AND DATE_TRUNC('hour', t.pickup_datetime) = DATE_TRUNC('hour', w.datetime)
WHERE t.trip_status = 'Completed'
GROUP BY w.weather_condition
ORDER BY total_trips DESC;

-- ============================================
-- 2. DRIVER PERFORMANCE & OPTIMIZATION
-- ============================================

-- Top performing drivers
WITH driver_metrics AS (
    SELECT 
        d.driver_id,
        d.first_name || ' ' || d.last_name as driver_name,
        d.rating as driver_rating,
        COUNT(t.trip_id) as total_trips,
        SUM(t.total_fare) as total_revenue,
        AVG(t.total_fare) as avg_fare_per_trip,
        AVG(t.trip_distance) as avg_distance,
        AVG(t.customer_rating) as avg_customer_rating,
        SUM(t.tips) as total_tips,
        AVG(t.tips) as avg_tips,
        AVG(t.route_efficiency) as avg_route_efficiency
    FROM drivers d
    LEFT JOIN trips t ON d.driver_id = t.driver_id
    WHERE t.trip_status = 'Completed'
        AND t.pickup_datetime >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY d.driver_id, driver_name, d.rating
)
SELECT 
    driver_name,
    driver_rating,
    total_trips,
    ROUND(total_revenue, 2) as total_revenue,
    ROUND(avg_fare_per_trip, 2) as avg_fare,
    ROUND(avg_distance, 2) as avg_distance_miles,
    ROUND(avg_customer_rating, 2) as avg_customer_rating,
    ROUND(total_tips, 2) as total_tips,
    ROUND(avg_tips, 2) as avg_tips,
    ROUND(avg_route_efficiency, 2) as route_efficiency,
    ROUND(total_revenue / NULLIF(total_trips, 0), 2) as revenue_per_trip
FROM driver_metrics
ORDER BY total_revenue DESC
LIMIT 20;

-- Driver utilization analysis
WITH driver_shifts AS (
    SELECT 
        driver_id,
        DATE(pickup_datetime) as work_date,
        MIN(pickup_datetime) as first_trip,
        MAX(dropoff_datetime) as last_trip,
        COUNT(*) as trips_count,
        SUM(trip_duration) as total_driving_time,
        SUM(total_fare) as daily_earnings
    FROM trips
    WHERE trip_status = 'Completed'
    GROUP BY driver_id, work_date
)
SELECT 
    d.driver_id,
    d.first_name || ' ' || d.last_name as driver_name,
    COUNT(DISTINCT ds.work_date) as days_worked,
    AVG(ds.trips_count) as avg_trips_per_day,
    AVG(EXTRACT(EPOCH FROM (ds.last_trip - ds.first_trip))/3600) as avg_hours_per_day,
    AVG(ds.total_driving_time/3600.0) as avg_driving_hours,
    AVG(ds.daily_earnings) as avg_daily_earnings,
    SUM(ds.daily_earnings) as total_earnings,
    AVG(ds.total_driving_time::NUMERIC / NULLIF(EXTRACT(EPOCH FROM (ds.last_trip - ds.first_trip)), 0)) as utilization_rate
FROM drivers d
JOIN driver_shifts ds ON d.driver_id = ds.driver_id
WHERE ds.work_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.driver_id, driver_name
ORDER BY avg_daily_earnings DESC;

-- Driver churn prediction indicators
SELECT 
    d.driver_id,
    d.first_name || ' ' || d.last_name as driver_name,
    d.status,
    d.rating,
    DATE_PART('day', CURRENT_DATE - MAX(t.pickup_datetime)) as days_since_last_trip,
    COUNT(t.trip_id) as trips_last_30_days,
    AVG(t.total_fare) as avg_fare_last_30_days,
    AVG(t.customer_rating) as avg_customer_rating,
    CASE 
        WHEN DATE_PART('day', CURRENT_DATE - MAX(t.pickup_datetime)) > 14 THEN 'High Risk'
        WHEN DATE_PART('day', CURRENT_DATE - MAX(t.pickup_datetime)) > 7 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as churn_risk
FROM drivers d
LEFT JOIN trips t ON d.driver_id = t.driver_id 
    AND t.pickup_datetime >= CURRENT_DATE - INTERVAL '30 days'
WHERE d.status = 'Active'
GROUP BY d.driver_id, d.first_name, d.last_name, d.status, d.rating
HAVING COUNT(t.trip_id) > 0
ORDER BY days_since_last_trip DESC;

-- ============================================
-- 3. ROUTE OPTIMIZATION & TRAFFIC ANALYSIS
-- ============================================

-- Most profitable routes
WITH route_analysis AS (
    SELECT 
        pz.zone_name as pickup_zone,
        dz.zone_name as dropoff_zone,
        COUNT(*) as trip_count,
        AVG(total_fare) as avg_fare,
        AVG(trip_distance) as avg_distance,
        AVG(trip_duration/60.0) as avg_duration_minutes,
        AVG(total_fare / NULLIF(trip_distance, 0)) as fare_per_mile,
        AVG(route_efficiency) as avg_efficiency
    FROM trips t
    JOIN zones pz ON t.pickup_zone_id = pz.zone_id
    JOIN zones dz ON t.dropoff_zone_id = dz.zone_id
    WHERE t.trip_status = 'Completed'
    GROUP BY pz.zone_name, dz.zone_name
    HAVING COUNT(*) > 50
)
SELECT 
    pickup_zone,
    dropoff_zone,
    trip_count,
    ROUND(avg_fare, 2) as avg_fare,
    ROUND(avg_distance, 2) as avg_distance_miles,
    ROUND(avg_duration_minutes, 2) as avg_duration_min,
    ROUND(fare_per_mile, 2) as fare_per_mile,
    ROUND(avg_efficiency, 2) as route_efficiency
FROM route_analysis
ORDER BY avg_fare DESC, trip_count DESC
LIMIT 25;

-- Traffic pattern analysis by hour
SELECT 
    EXTRACT(HOUR FROM pickup_datetime) as hour,
    AVG(trip_distance / NULLIF(trip_duration/3600.0, 0)) as avg_speed_mph,
    AVG(trip_duration/60.0) as avg_trip_duration_min,
    AVG(route_efficiency) as avg_route_efficiency,
    COUNT(*) as total_trips
FROM trips
WHERE trip_status = 'Completed'
    AND trip_duration > 0
    AND trip_distance > 0
GROUP BY hour
ORDER BY hour;

-- ============================================
-- 4. CUSTOMER SEGMENTATION & BEHAVIOR
-- ============================================

-- Customer lifetime value analysis
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name as customer_name,
        c.customer_type,
        c.registration_date,
        COUNT(t.trip_id) as total_trips,
        SUM(t.total_fare) as lifetime_value,
        AVG(t.total_fare) as avg_fare,
        SUM(t.tips) as total_tips,
        AVG(t.driver_rating) as avg_rating_given,
        DATE_PART('day', CURRENT_DATE - c.registration_date) as customer_age_days,
        DATE_PART('day', CURRENT_DATE - MAX(t.pickup_datetime)) as days_since_last_trip
    FROM customers c
    LEFT JOIN trips t ON c.customer_id = t.customer_id
    WHERE t.trip_status = 'Completed'
    GROUP BY c.customer_id, customer_name, c.customer_type, c.registration_date
)
SELECT 
    customer_type,
    COUNT(*) as customer_count,
    AVG(total_trips) as avg_trips_per_customer,
    AVG(lifetime_value) as avg_lifetime_value,
    AVG(avg_fare) as avg_fare_per_trip,
    AVG(total_tips) as avg_total_tips,
    AVG(lifetime_value / NULLIF(customer_age_days, 0) * 30) as avg_monthly_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_value) as median_lifetime_value
FROM customer_metrics
GROUP BY customer_type
ORDER BY avg_lifetime_value DESC;

-- Customer churn analysis
WITH customer_activity AS (
    SELECT 
        c.customer_id,
        c.customer_type,
        c.is_active,
        MAX(t.pickup_datetime) as last_trip_date,
        COUNT(t.trip_id) as trips_last_90_days,
        SUM(t.total_fare) as revenue_last_90_days
    FROM customers c
    LEFT JOIN trips t ON c.customer_id = t.customer_id 
        AND t.pickup_datetime >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY c.customer_id, c.customer_type, c.is_active
)
SELECT 
    customer_type,
    COUNT(CASE WHEN DATE_PART('day', CURRENT_DATE - last_trip_date) > 30 THEN 1 END) as churned_customers,
    COUNT(CASE WHEN DATE_PART('day', CURRENT_DATE - last_trip_date) <= 30 THEN 1 END) as active_customers,
    COUNT(*) as total_customers,
    ROUND(100.0 * COUNT(CASE WHEN DATE_PART('day', CURRENT_DATE - last_trip_date) > 30 THEN 1 END) / NULLIF(COUNT(*), 0), 2) as churn_rate,
    AVG(trips_last_90_days) as avg_trips_per_customer,
    AVG(revenue_last_90_days) as avg_revenue_per_customer
FROM customer_activity
GROUP BY customer_type;

-- Trip pattern analysis
SELECT 
    c.customer_type,
    EXTRACT(HOUR FROM t.pickup_datetime) as hour,
    CASE EXTRACT(DOW FROM t.pickup_datetime)
        WHEN 0 THEN 'Weekend'
        WHEN 6 THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    COUNT(*) as trip_count,
    AVG(t.total_fare) as avg_fare,
    AVG(t.trip_distance) as avg_distance,
    AVG(t.passenger_count) as avg_passengers
FROM trips t
JOIN customers c ON t.customer_id = c.customer_id
WHERE t.trip_status = 'Completed'
GROUP BY c.customer_type, hour, day_type
ORDER BY c.customer_type, trip_count DESC;

-- ============================================
-- 5. REVENUE & FINANCIAL ANALYTICS
-- ============================================

-- Daily revenue trends
SELECT 
    DATE(pickup_datetime) as trip_date,
    COUNT(*) as total_trips,
    SUM(total_fare) as total_revenue,
    SUM(base_fare) as base_revenue,
    SUM(distance_fare) as distance_revenue,
    SUM(time_fare) as time_revenue,
    SUM(tips) as total_tips,
    SUM(tolls) as total_tolls,
    AVG(surge_multiplier) as avg_surge,
    SUM(CASE WHEN surge_multiplier > 1 THEN (total_fare - tips - tolls) * (surge_multiplier - 1) / surge_multiplier ELSE 0 END) as surge_revenue
FROM trips
WHERE trip_status = 'Completed'
    AND pickup_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY trip_date
ORDER BY trip_date DESC;

-- Revenue by service type
SELECT 
    service_type,
    COUNT(*) as trip_count,
    SUM(total_fare) as total_revenue,
    AVG(total_fare) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(trip_duration/60.0) as avg_duration_min,
    SUM(tips) as total_tips,
    AVG(tips) as avg_tips,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as market_share_pct
FROM trips
WHERE trip_status = 'Completed'
GROUP BY service_type
ORDER BY total_revenue DESC;

-- Zone profitability analysis
WITH zone_metrics AS (
    SELECT 
        z.zone_id,
        z.zone_name,
        z.area_type,
        COUNT(t.trip_id) as total_trips,
        SUM(t.total_fare) as total_revenue,
        AVG(t.total_fare) as avg_fare,
        AVG(t.trip_distance) as avg_distance,
        AVG(t.wait_time) as avg_wait_time,
        COUNT(DISTINCT t.driver_id) as unique_drivers,
        COUNT(DISTINCT t.customer_id) as unique_customers
    FROM zones z
    LEFT JOIN trips t ON z.zone_id = t.pickup_zone_id
    WHERE t.trip_status = 'Completed'
    GROUP BY z.zone_id, z.zone_name, z.area_type
)
SELECT 
    zone_name,
    area_type,
    total_trips,
    ROUND(total_revenue, 2) as total_revenue,
    ROUND(avg_fare, 2) as avg_fare,
    ROUND(avg_distance, 2) as avg_distance,
    ROUND(avg_wait_time/60.0, 2) as avg_wait_min,
    unique_drivers,
    unique_customers,
    ROUND(total_revenue / NULLIF(unique_drivers, 0), 2) as revenue_per_driver,
    ROUND(total_revenue / NULLIF(unique_customers, 0), 2) as revenue_per_customer
FROM zone_metrics
ORDER BY total_revenue DESC;

-- ============================================
-- 6. ANOMALY DETECTION QUERIES
-- ============================================

-- Unusual trip patterns (potential fraud)
WITH trip_stats AS (
    SELECT 
        AVG(total_fare) as avg_fare,
        STDDEV(total_fare) as stddev_fare,
        AVG(trip_distance) as avg_distance,
        STDDEV(trip_distance) as stddev_distance,
        AVG(trip_duration) as avg_duration,
        STDDEV(trip_duration) as stddev_duration
    FROM trips
    WHERE trip_status = 'Completed'
)
SELECT 
    t.trip_id,
    t.driver_id,
    t.customer_id,
    t.pickup_datetime,
    t.total_fare,
    t.trip_distance,
    t.trip_duration/60.0 as duration_minutes,
    ROUND((t.total_fare - ts.avg_fare) / NULLIF(ts.stddev_fare, 0), 2) as fare_zscore,
    ROUND((t.trip_distance - ts.avg_distance) / NULLIF(ts.stddev_distance, 0), 2) as distance_zscore,
    CASE 
        WHEN ABS((t.total_fare - ts.avg_fare) / NULLIF(ts.stddev_fare, 0)) > 3 THEN 'High Risk - Unusual Fare'
        WHEN ABS((t.trip_distance - ts.avg_distance) / NULLIF(ts.stddev_distance, 0)) > 3 THEN 'High Risk - Unusual Distance'
        WHEN t.trip_distance > 0 AND t.trip_duration < 60 THEN 'High Risk - Impossibly Fast'
        WHEN t.total_fare / NULLIF(t.trip_distance, 0) > 20 THEN 'Medium Risk - High Fare per Mile'
        ELSE 'Normal'
    END as anomaly_flag
FROM trips t
CROSS JOIN trip_stats ts
WHERE t.trip_status = 'Completed'
    AND (
        ABS((t.total_fare - ts.avg_fare) / NULLIF(ts.stddev_fare, 0)) > 3
        OR ABS((t.trip_distance - ts.avg_distance) / NULLIF(ts.stddev_distance, 0)) > 3
        OR (t.trip_distance > 0 AND t.trip_duration < 60)
        OR t.total_fare / NULLIF(t.trip_distance, 0) > 20
    )
ORDER BY t.pickup_datetime DESC
LIMIT 100;

-- ============================================
-- 7. REAL-TIME MONITORING QUERIES
-- ============================================

-- Current active trips and drivers
SELECT 
    COUNT(CASE WHEN trip_status = 'InProgress' THEN 1 END) as active_trips,
    COUNT(DISTINCT CASE WHEN trip_status = 'InProgress' THEN driver_id END) as active_drivers,
    COUNT(CASE WHEN trip_status = 'Requested' THEN 1 END) as pending_requests,
    AVG(CASE WHEN trip_status = 'InProgress' THEN 
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - pickup_datetime))/60 
    END) as avg_trip_time_minutes
FROM trips
WHERE pickup_datetime >= CURRENT_TIMESTAMP - INTERVAL '4 hours';

-- Real-time zone demand heatmap
SELECT 
    z.zone_name,
    z.latitude,
    z.longitude,
    COUNT(CASE WHEN t.pickup_datetime >= CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN 1 END) as trips_last_hour,
    COUNT(CASE WHEN t.pickup_datetime >= CURRENT_TIMESTAMP - INTERVAL '15 minutes' THEN 1 END) as trips_last_15_min,
    AVG(CASE WHEN t.pickup_datetime >= CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN t.wait_time END) as avg_wait_time,
    COUNT(DISTINCT CASE WHEN t.pickup_datetime >= CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN t.driver_id END) as available_drivers
FROM zones z
LEFT JOIN trips t ON z.zone_id = t.pickup_zone_id
GROUP BY z.zone_id, z.zone_name, z.latitude, z.longitude
ORDER BY trips_last_hour DESC;