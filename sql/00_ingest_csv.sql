-- =====================================================
-- NYC TLC Data Ingestion - Lightning SQL Compatible
-- =====================================================
-- Load NYC Taxi and Limousine Commission data from CSV files
-- This script handles both Yellow and Green taxi trip records
-- Optimized for Lightning SQL execution

-- Set database context
USE nw_taxi;

-- =====================================================
-- 1. LOAD TAXI ZONES LOOKUP DATA
-- =====================================================

-- Load taxi zones from CSV (official NYC TLC zone lookup)
-- Download: https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv
CREATE OR REPLACE TEMPORARY VIEW taxi_zones_csv
USING CSV
OPTIONS (
    path '/path/to/data/taxi_zone_lookup.csv',
    header 'true',
    inferSchema 'true'
);

-- Insert taxi zones with proper formatting
INSERT INTO nw_taxi.taxi_zones 
SELECT 
    LocationID                AS location_id,
    Borough                   AS borough,
    Zone                      AS zone,
    service_zone              AS service_zone,
    current_timestamp()       AS created_at
FROM taxi_zones_csv;

-- =====================================================
-- 2. LOAD YELLOW TAXI TRIP DATA
-- =====================================================

-- Create temporary view for Yellow taxi CSV data
-- Format: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
CREATE OR REPLACE TEMPORARY VIEW yellow_taxi_raw
USING CSV
OPTIONS (
    path '/path/to/data/yellow_tripdata_*.csv',
    header 'true',
    inferSchema 'true',
    timestampFormat 'yyyy-MM-dd HH:mm:ss'
);

-- Insert Yellow taxi trips with computed fields
INSERT INTO nw_taxi.taxi_trips 
SELECT 
    -- Primary key generation
    monotonically_increasing_id()                                           AS trip_id,
    
    -- Trip identification
    'yellow'                                                                AS trip_type,
    VendorID                                                                AS vendor_id,
    
    -- Pickup information
    tpep_pickup_datetime                                                    AS pickup_datetime,
    PULocationID                                                            AS pickup_location_id,
    
    -- Dropoff information  
    tpep_dropoff_datetime                                                   AS dropoff_datetime,
    DOLocationID                                                            AS dropoff_location_id,
    
    -- Trip details
    passenger_count,
    trip_distance,
    RatecodeID                                                              AS rate_code_id,
    store_and_fwd_flag,
    
    -- Pricing breakdown
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    0.0                                                                     AS ehail_fee,
    total_amount,
    
    -- Payment information
    payment_type,
    
    -- Computed time fields
    hour(tpep_pickup_datetime)                                              AS pickup_hour,
    dayofweek(tpep_pickup_datetime)                                         AS pickup_day_of_week,
    month(tpep_pickup_datetime)                                             AS pickup_month,
    year(tpep_pickup_datetime)                                              AS pickup_year,
    date(tpep_pickup_datetime)                                              AS pickup_date,
    dayofweek(tpep_pickup_datetime) IN (1, 7)                               AS is_weekend,
    
    -- Computed trip metrics
    (unix_timestamp(tpep_dropoff_datetime) - 
     unix_timestamp(tpep_pickup_datetime)) / 60.0                          AS trip_duration_minutes,
     
    CASE 
        WHEN (unix_timestamp(tpep_dropoff_datetime) - 
              unix_timestamp(tpep_pickup_datetime)) > 0 
        THEN (trip_distance * 3600.0) / 
             (unix_timestamp(tpep_dropoff_datetime) - 
              unix_timestamp(tpep_pickup_datetime))
        ELSE NULL
    END                                                                     AS trip_speed_mph,
    
    CASE 
        WHEN trip_distance > 0 
        THEN fare_amount / trip_distance
        ELSE NULL
    END                                                                     AS fare_per_mile,
    
    CASE 
        WHEN fare_amount > 0 
        THEN (tip_amount / fare_amount) * 100
        ELSE 0
    END                                                                     AS tip_percentage,
    
    -- Data quality validation
    (tpep_pickup_datetime < tpep_dropoff_datetime
        AND trip_distance > 0
        AND trip_distance < 100
        AND fare_amount > 0
        AND total_amount > 0
        AND passenger_count > 0
        AND passenger_count <= 6
        AND PULocationID IS NOT NULL
        AND DOLocationID IS NOT NULL)                                       AS is_valid_trip,
    
    current_timestamp()                                                     AS created_at
    
FROM yellow_taxi_raw
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL;

-- =====================================================
-- 3. LOAD GREEN TAXI TRIP DATA
-- =====================================================

-- Create temporary view for Green taxi CSV data
-- Format: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
CREATE OR REPLACE TEMPORARY VIEW green_taxi_raw
USING CSV
OPTIONS (
    path '/path/to/data/green_tripdata_*.csv',
    header 'true',
    inferSchema 'true',
    timestampFormat 'yyyy-MM-dd HH:mm:ss'
);

-- Insert Green taxi trips with computed fields
INSERT INTO nw_taxi.taxi_trips 
SELECT 
    -- Primary key generation with offset
    monotonically_increasing_id() + 1000000000                              AS trip_id,
    
    -- Trip identification
    'green'                                                                 AS trip_type,
    VendorID                                                                AS vendor_id,
    
    -- Pickup information
    lpep_pickup_datetime                                                    AS pickup_datetime,
    PULocationID                                                            AS pickup_location_id,
    
    -- Dropoff information
    lpep_dropoff_datetime                                                   AS dropoff_datetime,
    DOLocationID                                                            AS dropoff_location_id,
    
    -- Trip details
    passenger_count,
    trip_distance,
    RatecodeID                                                              AS rate_code_id,
    store_and_fwd_flag,
    
    -- Pricing breakdown
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    0.0                                                                     AS airport_fee,
    ehail_fee,
    total_amount,
    
    -- Payment information
    payment_type,
    
    -- Computed time fields
    hour(lpep_pickup_datetime)                                              AS pickup_hour,
    dayofweek(lpep_pickup_datetime)                                         AS pickup_day_of_week,
    month(lpep_pickup_datetime)                                             AS pickup_month,
    year(lpep_pickup_datetime)                                              AS pickup_year,
    date(lpep_pickup_datetime)                                              AS pickup_date,
    dayofweek(lpep_pickup_datetime) IN (1, 7)                               AS is_weekend,
    
    -- Computed trip metrics
    (unix_timestamp(lpep_dropoff_datetime) - 
     unix_timestamp(lpep_pickup_datetime)) / 60.0                          AS trip_duration_minutes,
     
    CASE 
        WHEN (unix_timestamp(lpep_dropoff_datetime) - 
              unix_timestamp(lpep_pickup_datetime)) > 0 
        THEN (trip_distance * 3600.0) / 
             (unix_timestamp(lpep_dropoff_datetime) - 
              unix_timestamp(lpep_pickup_datetime))
        ELSE NULL
    END                                                                     AS trip_speed_mph,
    
    CASE 
        WHEN trip_distance > 0 
        THEN fare_amount / trip_distance
        ELSE NULL
    END                                                                     AS fare_per_mile,
    
    CASE 
        WHEN fare_amount > 0 
        THEN (tip_amount / fare_amount) * 100
        ELSE 0
    END                                                                     AS tip_percentage,
    
    -- Data quality validation
    (lpep_pickup_datetime < lpep_dropoff_datetime
        AND trip_distance > 0
        AND trip_distance < 100
        AND fare_amount > 0
        AND total_amount > 0
        AND passenger_count > 0
        AND passenger_count <= 6
        AND PULocationID IS NOT NULL
        AND DOLocationID IS NOT NULL)                                       AS is_valid_trip,
    
    current_timestamp()                                                     AS created_at
    
FROM green_taxi_raw
WHERE lpep_pickup_datetime IS NOT NULL
  AND lpep_dropoff_datetime IS NOT NULL;

-- =====================================================
-- 4. DATA VALIDATION AND SUMMARY
-- =====================================================

-- Validate data ingestion results
SELECT 
    'Data Ingestion Summary'                                            AS metric,
    COUNT(*)                                                            AS total_records,
    SUM(CASE WHEN trip_type = 'yellow' THEN 1 ELSE 0 END)              AS yellow_trips,
    SUM(CASE WHEN trip_type = 'green' THEN 1 ELSE 0 END)               AS green_trips,
    SUM(CASE WHEN is_valid_trip = true THEN 1 ELSE 0 END)              AS valid_trips,
    SUM(CASE WHEN is_valid_trip = false THEN 1 ELSE 0 END)             AS invalid_trips,
    ROUND(
        SUM(CASE WHEN is_valid_trip = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                                   AS valid_percentage
FROM nw_taxi.taxi_trips;

-- Check date range of ingested data
SELECT 
    'Date Range Coverage'                                               AS metric,
    trip_type,
    MIN(pickup_date)                                                    AS earliest_date,
    MAX(pickup_date)                                                    AS latest_date,
    COUNT(DISTINCT pickup_date)                                         AS unique_dates
FROM nw_taxi.taxi_trips
GROUP BY trip_type
ORDER BY trip_type;

-- Validate zone references
SELECT 
    'Zone Reference Validation'                                         AS metric,
    SUM(
        CASE WHEN pickup_location_id NOT IN 
            (SELECT location_id FROM nw_taxi.taxi_zones) 
        THEN 1 ELSE 0 END
    )                                                                   AS invalid_pickup_zones,
    SUM(
        CASE WHEN dropoff_location_id NOT IN 
            (SELECT location_id FROM nw_taxi.taxi_zones) 
        THEN 1 ELSE 0 END
    )                                                                   AS invalid_dropoff_zones
FROM nw_taxi.taxi_trips;

-- =====================================================
-- 5. POST-INGESTION OPTIMIZATION
-- =====================================================

-- Update table statistics for optimal query planning
ANALYZE TABLE nw_taxi.taxi_trips COMPUTE STATISTICS;
ANALYZE TABLE nw_taxi.taxi_zones COMPUTE STATISTICS;

-- Optimize Delta Lake tables for performance
OPTIMIZE nw_taxi.taxi_trips;
OPTIMIZE nw_taxi.taxi_zones;

-- Cache frequently accessed views in memory
CACHE TABLE nw_taxi.valid_trips;
CACHE TABLE nw_taxi.trips_with_zones;

-- =====================================================
-- 6. LIGHTNING SQL USAGE INSTRUCTIONS
-- =====================================================

/*
=====================================================
LIGHTNING SQL USAGE INSTRUCTIONS
=====================================================

1. DATA PREPARATION:
   - Download NYC TLC Zone Lookup:
     https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv
   
   - Download NYC TLC Trip Data:
     https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
     (Both Yellow and Green taxi datasets)

2. FILE PATH CONFIGURATION:
   - Update '/path/to/data/' with your actual data directory
   - Ensure CSV file patterns match your downloaded files:
     * taxi_zone_lookup.csv
     * yellow_tripdata_*.csv  
     * green_tripdata_*.csv

3. EXECUTION SEQUENCE:
   ① Run: 01_nyc_tlc_schema_spark.sql (create tables)
   ② Run: 00_ingest_csv.sql (this file - load data)
   ③ Run: 02_nyc_analytics_queries_spark.sql (analytics)

4. LIGHTNING SQL EXECUTION:
   
   # Using Lightning SQL CLI
   lightning-sql -f 00_ingest_csv.sql
   
   # Using Spark SQL with Lightning optimization
   spark-sql \
     --master local[*] \
     --conf spark.sql.adaptive.enabled=true \
     --conf spark.sql.adaptive.coalescePartitions.enabled=true \
     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
     -f 00_ingest_csv.sql

5. PERFORMANCE OPTIMIZATION:
   - Batch processing: Load data in monthly chunks for large datasets
   - Memory settings: Increase executor memory for large CSV files
   - Partitioning: Data is auto-partitioned by pickup_year/pickup_month
   - Caching: Frequently used views are automatically cached

6. MONITORING & VALIDATION:
   - Check Spark UI for ingestion progress
   - Review data quality validation output
   - Monitor invalid record counts and percentages

7. ALTERNATIVE DATA SOURCES:
   - Testing: scripts/download_nyc_data.py (auto-download samples)
   - Development: scripts/generate_sample_data.py (synthetic data)

8. TROUBLESHOOTING:
   - Ensure sufficient memory allocation
   - Verify CSV file formats match NYC TLC specifications
   - Check that all required columns are present in source files

*/