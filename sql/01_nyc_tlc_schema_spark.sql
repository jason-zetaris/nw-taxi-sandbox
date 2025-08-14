-- NYC TLC Trip Record Data Schema - Spark SQL Compatible Version
-- Based on official NYC Taxi and Limousine Commission data format
-- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
-- Optimized for Apache Spark SQL execution

-- Create namespace/database if it doesn't exist
CREATE DATABASE IF NOT EXISTS nw_taxi;
USE nw_taxi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS nw_taxi.taxi_trips;
DROP TABLE IF EXISTS nw_taxi.taxi_zones;
DROP TABLE IF EXISTS nw_taxi.trip_summary_daily;
DROP TABLE IF EXISTS nw_taxi.trip_summary_hourly;

-- Taxi Zone Lookup Table
-- Based on official NYC TLC zone lookup data
CREATE TABLE nw_taxi.taxi_zones (
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING,
    created_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Main trips table combining Yellow and Green taxi data
CREATE TABLE nw_taxi.taxi_trips (
    trip_id BIGINT,
    
    -- Trip identification
    trip_type STRING, -- 'yellow' or 'green'
    vendor_id INT,
    
    -- Pickup information
    pickup_datetime TIMESTAMP,
    pickup_location_id INT,
    
    -- Dropoff information
    dropoff_datetime TIMESTAMP,
    dropoff_location_id INT,
    
    -- Trip details
    passenger_count INT,
    trip_distance DOUBLE,
    rate_code_id INT,
    store_and_fwd_flag STRING,
    
    -- Pricing breakdown
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    ehail_fee DOUBLE,
    total_amount DOUBLE,
    
    -- Payment information
    payment_type INT,
    
    -- Time-based fields for analysis (computed at insert time)
    pickup_hour INT,
    pickup_day_of_week INT,
    pickup_month INT,
    pickup_year INT,
    pickup_date DATE,
    is_weekend BOOLEAN,
    
    -- Derived fields for analysis (computed at insert time)
    trip_duration_minutes DOUBLE,
    trip_speed_mph DOUBLE,
    fare_per_mile DOUBLE,
    tip_percentage DOUBLE,
    
    -- Data quality flags
    is_valid_trip BOOLEAN,
    
    created_at TIMESTAMP
) USING DELTA
PARTITIONED BY (pickup_year, pickup_month)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Daily trip summary table for faster aggregations
CREATE TABLE nw_taxi.trip_summary_daily (
    summary_date DATE,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_trip_distance DOUBLE,
    avg_fare_amount DOUBLE,
    avg_tip_amount DOUBLE,
    avg_trip_duration DOUBLE,
    unique_pickup_zones BIGINT,
    unique_dropoff_zones BIGINT,
    yellow_trips BIGINT,
    green_trips BIGINT,
    cash_trips BIGINT,
    card_trips BIGINT,
    updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Hourly trip summary table for time-based analysis
CREATE TABLE nw_taxi.trip_summary_hourly (
    summary_date DATE,
    summary_hour INT,
    trip_type STRING,
    pickup_borough STRING,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_trip_distance DOUBLE,
    avg_fare_amount DOUBLE,
    avg_tip_percentage DOUBLE,
    avg_speed_mph DOUBLE
) USING DELTA
PARTITIONED BY (summary_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Create views for analysis (Spark SQL compatible)

-- Valid trips only view
CREATE OR REPLACE VIEW nw_taxi.valid_trips AS
SELECT * FROM nw_taxi.taxi_trips 
WHERE is_valid_trip = TRUE;

-- Daily trip statistics view
CREATE OR REPLACE VIEW nw_taxi.daily_trip_stats AS
SELECT 
    pickup_date,
    trip_type,
    COUNT(*) as trip_count,
    SUM(total_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    AVG(trip_duration_minutes) as avg_duration_min,
    AVG(tip_percentage) as avg_tip_pct,
    COUNT(DISTINCT pickup_location_id) as unique_pickup_zones,
    COUNT(DISTINCT dropoff_location_id) as unique_dropoff_zones
FROM nw_taxi.valid_trips
GROUP BY pickup_date, trip_type;

-- Hourly demand pattern view
CREATE OR REPLACE VIEW nw_taxi.hourly_demand AS
SELECT 
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
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_pct
FROM nw_taxi.valid_trips
GROUP BY pickup_hour, pickup_day_of_week, is_weekend, trip_type;

-- Zone popularity view
CREATE OR REPLACE VIEW nw_taxi.zone_popularity AS
SELECT 
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    COUNT(*) as pickup_count,
    AVG(t.fare_amount) as avg_fare,
    AVG(t.trip_distance) as avg_distance,
    AVG(t.tip_percentage) as avg_tip_pct
FROM nw_taxi.valid_trips t
JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
GROUP BY pz.borough, pz.zone;

-- Payment type breakdown view
CREATE OR REPLACE VIEW nw_taxi.payment_analysis AS
SELECT 
    payment_type,
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
    SUM(total_amount) as total_revenue,
    AVG(tip_amount) as avg_tip,
    AVG(tip_percentage) as avg_tip_pct
FROM nw_taxi.valid_trips
GROUP BY payment_type, trip_type;

-- Trip distance distribution view
CREATE OR REPLACE VIEW nw_taxi.distance_distribution AS
SELECT 
    CASE 
        WHEN trip_distance <= 1 THEN '0-1 miles'
        WHEN trip_distance <= 2 THEN '1-2 miles'
        WHEN trip_distance <= 5 THEN '2-5 miles'
        WHEN trip_distance <= 10 THEN '5-10 miles'
        WHEN trip_distance <= 20 THEN '10-20 miles'
        ELSE '20+ miles'
    END as distance_range,
    trip_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(trip_speed_mph) as avg_speed
FROM nw_taxi.valid_trips
WHERE trip_distance > 0
GROUP BY 
    CASE 
        WHEN trip_distance <= 1 THEN '0-1 miles'
        WHEN trip_distance <= 2 THEN '1-2 miles'
        WHEN trip_distance <= 5 THEN '2-5 miles'
        WHEN trip_distance <= 10 THEN '5-10 miles'
        WHEN trip_distance <= 20 THEN '10-20 miles'
        ELSE '20+ miles'
    END, 
    trip_type;

-- Trips with zone information view (for easier geographic analysis)
CREATE OR REPLACE VIEW nw_taxi.trips_with_zones AS
SELECT 
    t.*,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    pz.service_zone as pickup_service_zone,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,
    dz.service_zone as dropoff_service_zone
FROM nw_taxi.valid_trips t
LEFT JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
LEFT JOIN nw_taxi.taxi_zones dz ON t.dropoff_location_id = dz.location_id;

-- Monthly statistics table (replacement for materialized view)
CREATE TABLE IF NOT EXISTS nw_taxi.monthly_statistics (
    pickup_year INT,
    pickup_month INT,
    trip_type STRING,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_fare DOUBLE,
    avg_distance DOUBLE,
    avg_tip_pct DOUBLE,
    avg_duration DOUBLE,
    unique_pickup_zones BIGINT,
    unique_dropoff_zones BIGINT,
    updated_at TIMESTAMP
) USING DELTA
PARTITIONED BY (pickup_year)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Create common table for trips that are used in analytics (replacement for valid_trips)
-- This creates an alias to the view for better compatibility
CREATE OR REPLACE VIEW trips AS 
SELECT * FROM nw_taxi.valid_trips;

-- Create zone reference table for easier access
CREATE OR REPLACE VIEW zones AS 
SELECT * FROM nw_taxi.taxi_zones;

-- Create trips with zones for geographic analysis
CREATE OR REPLACE VIEW trips_with_zones AS 
SELECT * FROM nw_taxi.trips_with_zones;

-- Data insertion utility function (Spark SQL compatible)
-- Note: Functions in Spark SQL are limited, so we provide SQL templates instead

-- Template for inserting trip data with computed fields
-- Use this as a template for ETL processes:
/*
INSERT INTO nw_taxi.taxi_trips 
SELECT 
    -- Auto-generate trip_id using row_number() or monotonically_increasing_id()
    row_number() OVER (ORDER BY pickup_datetime) as trip_id,
    
    -- Raw trip data
    trip_type,
    vendor_id,
    pickup_datetime,
    pickup_location_id,
    dropoff_datetime,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    ehail_fee,
    total_amount,
    payment_type,
    
    -- Computed time fields
    hour(pickup_datetime) as pickup_hour,
    dayofweek(pickup_datetime) as pickup_day_of_week,
    month(pickup_datetime) as pickup_month,
    year(pickup_datetime) as pickup_year,
    date(pickup_datetime) as pickup_date,
    dayofweek(pickup_datetime) IN (1, 7) as is_weekend,
    
    -- Computed trip metrics
    (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60.0 as trip_duration_minutes,
    CASE 
        WHEN (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) > 0 
        THEN (trip_distance * 3600.0) / (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime))
        ELSE NULL
    END as trip_speed_mph,
    CASE 
        WHEN trip_distance > 0 
        THEN fare_amount / trip_distance
        ELSE NULL
    END as fare_per_mile,
    CASE 
        WHEN fare_amount > 0 
        THEN (tip_amount / fare_amount) * 100
        ELSE 0
    END as tip_percentage,
    
    -- Data quality flag
    (pickup_datetime < dropoff_datetime
        AND trip_distance > 0
        AND trip_distance < 100
        AND fare_amount > 0
        AND total_amount > 0
        AND passenger_count > 0
        AND passenger_count <= 6
        AND pickup_location_id IS NOT NULL
        AND dropoff_location_id IS NOT NULL) as is_valid_trip,
    
    current_timestamp() as created_at
    
FROM raw_trip_data;
*/

-- Template for updating monthly statistics
-- Run this periodically to update the monthly stats table:
/*
INSERT OVERWRITE nw_taxi.monthly_statistics
SELECT 
    pickup_year,
    pickup_month,
    trip_type,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_pct,
    AVG(trip_duration_minutes) as avg_duration,
    COUNT(DISTINCT pickup_location_id) as unique_pickup_zones,
    COUNT(DISTINCT dropoff_location_id) as unique_dropoff_zones,
    current_timestamp() as updated_at
FROM nw_taxi.valid_trips
GROUP BY pickup_year, pickup_month, trip_type
ORDER BY pickup_year, pickup_month, trip_type;
*/

-- Optimization commands for Spark
-- Run these after data loading for better performance:
/*
-- Optimize tables using Delta Lake features
OPTIMIZE nw_taxi.taxi_trips;
OPTIMIZE nw_taxi.taxi_zones;
OPTIMIZE nw_taxi.trip_summary_daily;
OPTIMIZE nw_taxi.trip_summary_hourly;

-- Analyze tables for better query planning
ANALYZE TABLE nw_taxi.taxi_trips COMPUTE STATISTICS;
ANALYZE TABLE nw_taxi.taxi_zones COMPUTE STATISTICS;

-- Cache frequently used tables in memory
CACHE TABLE nw_taxi.valid_trips;
CACHE TABLE nw_taxi.trips_with_zones;
*/