-- NYC TLC Trip Record Data Schema with nw_taxi namespace
-- Based on official NYC Taxi and Limousine Commission data format
-- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

-- Create namespace/schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS nw_taxi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS nw_taxi.taxi_trips CASCADE;
DROP TABLE IF EXISTS nw_taxi.taxi_zones CASCADE;
DROP TABLE IF EXISTS nw_taxi.trip_summary_daily CASCADE;
DROP TABLE IF EXISTS nw_taxi.trip_summary_hourly CASCADE;

-- Taxi Zone Lookup Table
-- Based on official NYC TLC zone lookup data
CREATE TABLE nw_taxi.taxi_zones (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(100) NOT NULL,
    service_zone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Main trips table combining Yellow and Green taxi data
CREATE TABLE nw_taxi.taxi_trips (
    trip_id SERIAL PRIMARY KEY,
    
    -- Trip identification
    trip_type VARCHAR(10) NOT NULL, -- 'yellow' or 'green'
    vendor_id INTEGER,
    
    -- Pickup information
    pickup_datetime TIMESTAMP NOT NULL,
    pickup_location_id INTEGER,
    
    -- Dropoff information
    dropoff_datetime TIMESTAMP NOT NULL,
    dropoff_location_id INTEGER,
    
    -- Trip details
    passenger_count INTEGER,
    trip_distance DECIMAL(10, 2),
    rate_code_id INTEGER,
    store_and_fwd_flag CHAR(1),
    
    -- Pricing breakdown
    fare_amount DECIMAL(10, 2),
    extra DECIMAL(10, 2),
    mta_tax DECIMAL(10, 2),
    tip_amount DECIMAL(10, 2),
    tolls_amount DECIMAL(10, 2),
    improvement_surcharge DECIMAL(10, 2),
    congestion_surcharge DECIMAL(10, 2),
    airport_fee DECIMAL(10, 2),
    ehail_fee DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    
    -- Payment information
    payment_type INTEGER,
    
    -- Derived fields for analysis
    trip_duration_minutes INTEGER GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60
    ) STORED,
    
    trip_speed_mph DECIMAL(5, 2) GENERATED ALWAYS AS (
        CASE 
            WHEN EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) > 0 
            THEN (trip_distance * 3600.0) / EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))
            ELSE NULL
        END
    ) STORED,
    
    fare_per_mile DECIMAL(10, 2) GENERATED ALWAYS AS (
        CASE 
            WHEN trip_distance > 0 
            THEN fare_amount / trip_distance
            ELSE NULL
        END
    ) STORED,
    
    tip_percentage DECIMAL(5, 2) GENERATED ALWAYS AS (
        CASE 
            WHEN fare_amount > 0 
            THEN (tip_amount / fare_amount) * 100
            ELSE 0
        END
    ) STORED,
    
    -- Time-based fields for analysis
    pickup_hour INTEGER GENERATED ALWAYS AS (EXTRACT(HOUR FROM pickup_datetime)) STORED,
    pickup_day_of_week INTEGER GENERATED ALWAYS AS (EXTRACT(DOW FROM pickup_datetime)) STORED,
    pickup_month INTEGER GENERATED ALWAYS AS (EXTRACT(MONTH FROM pickup_datetime)) STORED,
    pickup_year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM pickup_datetime)) STORED,
    pickup_date DATE GENERATED ALWAYS AS (pickup_datetime::DATE) STORED,
    
    is_weekend BOOLEAN GENERATED ALWAYS AS (
        EXTRACT(DOW FROM pickup_datetime) IN (0, 6)
    ) STORED,
    
    -- Data quality flags
    is_valid_trip BOOLEAN GENERATED ALWAYS AS (
        pickup_datetime < dropoff_datetime
        AND trip_distance > 0
        AND trip_distance < 100
        AND fare_amount > 0
        AND total_amount > 0
        AND passenger_count > 0
        AND passenger_count <= 6
        AND pickup_location_id IS NOT NULL
        AND dropoff_location_id IS NOT NULL
    ) STORED,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    CONSTRAINT fk_pickup_zone FOREIGN KEY (pickup_location_id) REFERENCES nw_taxi.taxi_zones(location_id),
    CONSTRAINT fk_dropoff_zone FOREIGN KEY (dropoff_location_id) REFERENCES nw_taxi.taxi_zones(location_id)
);

-- Daily trip summary table for faster aggregations
CREATE TABLE nw_taxi.trip_summary_daily (
    summary_date DATE PRIMARY KEY,
    total_trips INTEGER,
    total_revenue DECIMAL(15, 2),
    avg_trip_distance DECIMAL(10, 2),
    avg_fare_amount DECIMAL(10, 2),
    avg_tip_amount DECIMAL(10, 2),
    avg_trip_duration DECIMAL(10, 2),
    unique_pickup_zones INTEGER,
    unique_dropoff_zones INTEGER,
    yellow_trips INTEGER,
    green_trips INTEGER,
    cash_trips INTEGER,
    card_trips INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Hourly trip summary table for time-based analysis
CREATE TABLE nw_taxi.trip_summary_hourly (
    summary_date DATE,
    summary_hour INTEGER,
    trip_type VARCHAR(10),
    pickup_borough VARCHAR(50),
    total_trips INTEGER,
    total_revenue DECIMAL(12, 2),
    avg_trip_distance DECIMAL(10, 2),
    avg_fare_amount DECIMAL(10, 2),
    avg_tip_percentage DECIMAL(5, 2),
    avg_speed_mph DECIMAL(5, 2),
    PRIMARY KEY (summary_date, summary_hour, trip_type, pickup_borough)
);

-- Create indexes for better query performance
CREATE INDEX idx_taxi_trips_pickup_datetime ON nw_taxi.taxi_trips(pickup_datetime);
CREATE INDEX idx_taxi_trips_dropoff_datetime ON nw_taxi.taxi_trips(dropoff_datetime);
CREATE INDEX idx_taxi_trips_pickup_location ON nw_taxi.taxi_trips(pickup_location_id);
CREATE INDEX idx_taxi_trips_dropoff_location ON nw_taxi.taxi_trips(dropoff_location_id);
CREATE INDEX idx_taxi_trips_trip_type ON nw_taxi.taxi_trips(trip_type);
CREATE INDEX idx_taxi_trips_pickup_date ON nw_taxi.taxi_trips(pickup_date);
CREATE INDEX idx_taxi_trips_pickup_hour ON nw_taxi.taxi_trips(pickup_hour);
CREATE INDEX idx_taxi_trips_pickup_dow ON nw_taxi.taxi_trips(pickup_day_of_week);
CREATE INDEX idx_taxi_trips_payment_type ON nw_taxi.taxi_trips(payment_type);
CREATE INDEX idx_taxi_trips_valid ON nw_taxi.taxi_trips(is_valid_trip);
CREATE INDEX idx_taxi_trips_distance ON nw_taxi.taxi_trips(trip_distance);
CREATE INDEX idx_taxi_trips_fare ON nw_taxi.taxi_trips(fare_amount);

-- Composite indexes for common queries
CREATE INDEX idx_taxi_trips_datetime_type ON nw_taxi.taxi_trips(pickup_datetime, trip_type);
CREATE INDEX idx_taxi_trips_date_hour ON nw_taxi.taxi_trips(pickup_date, pickup_hour);
CREATE INDEX idx_taxi_trips_location_datetime ON nw_taxi.taxi_trips(pickup_location_id, pickup_datetime);

-- Create useful views for analysis

-- Valid trips only view
CREATE VIEW valid_trips AS
SELECT * FROM nw_taxi.taxi_trips 
WHERE is_valid_trip = TRUE;

-- Daily trip statistics view
CREATE VIEW daily_trip_stats AS
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
FROM valid_trips
GROUP BY pickup_date, trip_type;

-- Hourly demand pattern view
CREATE VIEW hourly_demand AS
SELECT 
    pickup_hour,
    pickup_day_of_week,
    CASE pickup_day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    is_weekend,
    trip_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(tip_percentage) as avg_tip_pct
FROM valid_trips
GROUP BY pickup_hour, pickup_day_of_week, is_weekend, trip_type;

-- Zone popularity view
CREATE VIEW zone_popularity AS
SELECT 
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    COUNT(*) as pickup_count,
    AVG(t.fare_amount) as avg_fare,
    AVG(t.trip_distance) as avg_distance,
    AVG(t.tip_percentage) as avg_tip_pct
FROM valid_trips t
JOIN nw_taxi.taxi_zones pz ON t.pickup_location_id = pz.location_id
GROUP BY pz.borough, pz.zone;

-- Payment type breakdown view
CREATE VIEW payment_analysis AS
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
FROM valid_trips
GROUP BY payment_type, trip_type;

-- Trip distance distribution view
CREATE VIEW distance_distribution AS
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
FROM valid_trips
WHERE trip_distance > 0
GROUP BY distance_range, trip_type;

-- Functions for data analysis

-- Function to calculate trip summary for a date range
CREATE OR REPLACE FUNCTION get_trip_summary(
    start_date DATE,
    end_date DATE,
    trip_type_filter VARCHAR DEFAULT NULL
)
RETURNS TABLE (
    total_trips BIGINT,
    total_revenue NUMERIC,
    avg_fare NUMERIC,
    avg_distance NUMERIC,
    avg_duration NUMERIC,
    unique_zones BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::BIGINT,
        SUM(t.total_amount)::NUMERIC,
        AVG(t.fare_amount)::NUMERIC,
        AVG(t.trip_distance)::NUMERIC,
        AVG(t.trip_duration_minutes)::NUMERIC,
        COUNT(DISTINCT t.pickup_location_id)::BIGINT
    FROM valid_trips t
    WHERE t.pickup_date BETWEEN start_date AND end_date
    AND (trip_type_filter IS NULL OR t.trip_type = trip_type_filter);
END;
$$ LANGUAGE plpgsql;

-- Function to get top pickup zones
CREATE OR REPLACE FUNCTION get_top_pickup_zones(
    limit_count INTEGER DEFAULT 10,
    start_date DATE DEFAULT NULL,
    end_date DATE DEFAULT NULL
)
RETURNS TABLE (
    zone_name VARCHAR,
    borough VARCHAR,
    trip_count BIGINT,
    avg_fare NUMERIC,
    total_revenue NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        z.zone,
        z.borough,
        COUNT(*)::BIGINT,
        AVG(t.fare_amount)::NUMERIC,
        SUM(t.total_amount)::NUMERIC
    FROM valid_trips t
    JOIN nw_taxi.taxi_zones z ON t.pickup_location_id = z.location_id
    WHERE (start_date IS NULL OR t.pickup_date >= start_date)
    AND (end_date IS NULL OR t.pickup_date <= end_date)
    GROUP BY z.zone, z.borough
    ORDER BY COUNT(*) DESC
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- Create materialized view for faster monthly statistics
CREATE MATERIALIZED VIEW monthly_statistics AS
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
    COUNT(DISTINCT dropoff_location_id) as unique_dropoff_zones
FROM valid_trips
GROUP BY pickup_year, pickup_month, trip_type
ORDER BY pickup_year, pickup_month, trip_type;

-- Create index on materialized view
CREATE INDEX idx_monthly_stats_year_month ON monthly_statistics(pickup_year, pickup_month);

-- Create trigger to update daily summary when new trips are inserted
CREATE OR REPLACE FUNCTION update_daily_summary()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO nw_taxi.trip_summary_daily (
        summary_date,
        total_trips,
        total_revenue,
        avg_trip_distance,
        avg_fare_amount,
        avg_tip_amount,
        avg_trip_duration,
        unique_pickup_zones,
        unique_dropoff_zones,
        yellow_trips,
        green_trips,
        cash_trips,
        card_trips,
        updated_at
    )
    SELECT 
        NEW.pickup_date,
        COUNT(*),
        SUM(total_amount),
        AVG(trip_distance),
        AVG(fare_amount),
        AVG(tip_amount),
        AVG(trip_duration_minutes),
        COUNT(DISTINCT pickup_location_id),
        COUNT(DISTINCT dropoff_location_id),
        COUNT(*) FILTER (WHERE trip_type = 'yellow'),
        COUNT(*) FILTER (WHERE trip_type = 'green'),
        COUNT(*) FILTER (WHERE payment_type = 2),
        COUNT(*) FILTER (WHERE payment_type = 1),
        CURRENT_TIMESTAMP
    FROM valid_trips
    WHERE pickup_date = NEW.pickup_date
    ON CONFLICT (summary_date) DO UPDATE SET
        total_trips = EXCLUDED.total_trips,
        total_revenue = EXCLUDED.total_revenue,
        avg_trip_distance = EXCLUDED.avg_trip_distance,
        avg_fare_amount = EXCLUDED.avg_fare_amount,
        avg_tip_amount = EXCLUDED.avg_tip_amount,
        avg_trip_duration = EXCLUDED.avg_trip_duration,
        unique_pickup_zones = EXCLUDED.unique_pickup_zones,
        unique_dropoff_zones = EXCLUDED.unique_dropoff_zones,
        yellow_trips = EXCLUDED.yellow_trips,
        green_trips = EXCLUDED.green_trips,
        cash_trips = EXCLUDED.cash_trips,
        card_trips = EXCLUDED.card_trips,
        updated_at = CURRENT_TIMESTAMP;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Note: The trigger is commented out as it might slow down bulk inserts
-- CREATE TRIGGER trigger_update_daily_summary
--     AFTER INSERT ON nw_taxi.taxi_trips
--     FOR EACH ROW
--     EXECUTE FUNCTION update_daily_summary();