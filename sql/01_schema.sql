-- Taxi Service Database Schema
-- Comprehensive schema for taxi analytics and data science demos

-- Drop existing tables if they exist
DROP TABLE IF EXISTS trip_payments CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS surge_pricing CASCADE;
DROP TABLE IF EXISTS driver_shifts CASCADE;
DROP TABLE IF EXISTS drivers CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS vehicles CASCADE;
DROP TABLE IF EXISTS zones CASCADE;
DROP TABLE IF EXISTS weather_data CASCADE;

-- Zones/Locations table
CREATE TABLE zones (
    zone_id SERIAL PRIMARY KEY,
    zone_name VARCHAR(100) NOT NULL,
    borough VARCHAR(50),
    area_type VARCHAR(50), -- 'Airport', 'Downtown', 'Suburban', 'Industrial'
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    avg_demand_score DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Vehicles table
CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    license_plate VARCHAR(20) UNIQUE NOT NULL,
    make VARCHAR(50),
    model VARCHAR(50),
    year INTEGER,
    vehicle_type VARCHAR(30), -- 'Standard', 'Premium', 'XL', 'Electric'
    fuel_type VARCHAR(20), -- 'Gas', 'Hybrid', 'Electric'
    seating_capacity INTEGER,
    registration_date DATE,
    last_inspection_date DATE,
    status VARCHAR(20) DEFAULT 'Active', -- 'Active', 'Maintenance', 'Retired'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Drivers table
CREATE TABLE drivers (
    driver_id SERIAL PRIMARY KEY,
    driver_license VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    hire_date DATE NOT NULL,
    birth_date DATE,
    rating DECIMAL(3, 2) CHECK (rating >= 0 AND rating <= 5),
    total_trips INTEGER DEFAULT 0,
    total_earnings DECIMAL(12, 2) DEFAULT 0,
    status VARCHAR(20) DEFAULT 'Active', -- 'Active', 'Inactive', 'Suspended', 'Terminated'
    home_zone_id INTEGER REFERENCES zones(zone_id),
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    registration_date DATE NOT NULL,
    customer_type VARCHAR(20) DEFAULT 'Regular', -- 'Regular', 'Premium', 'Corporate'
    rating DECIMAL(3, 2) CHECK (rating >= 0 AND rating <= 5),
    total_trips INTEGER DEFAULT 0,
    total_spent DECIMAL(12, 2) DEFAULT 0,
    preferred_payment VARCHAR(20), -- 'Card', 'Cash', 'Digital'
    home_zone_id INTEGER REFERENCES zones(zone_id),
    work_zone_id INTEGER REFERENCES zones(zone_id),
    is_active BOOLEAN DEFAULT TRUE,
    last_trip_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Driver shifts table
CREATE TABLE driver_shifts (
    shift_id SERIAL PRIMARY KEY,
    driver_id INTEGER REFERENCES drivers(driver_id),
    shift_start TIMESTAMP NOT NULL,
    shift_end TIMESTAMP,
    start_zone_id INTEGER REFERENCES zones(zone_id),
    end_zone_id INTEGER REFERENCES zones(zone_id),
    total_trips INTEGER DEFAULT 0,
    total_distance DECIMAL(10, 2) DEFAULT 0,
    total_earnings DECIMAL(10, 2) DEFAULT 0,
    fuel_cost DECIMAL(10, 2) DEFAULT 0,
    status VARCHAR(20) DEFAULT 'Active', -- 'Active', 'Completed', 'Cancelled'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Surge pricing table
CREATE TABLE surge_pricing (
    surge_id SERIAL PRIMARY KEY,
    zone_id INTEGER REFERENCES zones(zone_id),
    surge_multiplier DECIMAL(3, 2) NOT NULL CHECK (surge_multiplier >= 1),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    reason VARCHAR(100), -- 'High Demand', 'Special Event', 'Weather', 'Rush Hour'
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Main trips table
CREATE TABLE trips (
    trip_id SERIAL PRIMARY KEY,
    driver_id INTEGER REFERENCES drivers(driver_id),
    customer_id INTEGER REFERENCES customers(customer_id),
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    shift_id INTEGER REFERENCES driver_shifts(shift_id),
    
    -- Pickup information
    pickup_datetime TIMESTAMP NOT NULL,
    pickup_zone_id INTEGER REFERENCES zones(zone_id),
    pickup_latitude DECIMAL(10, 8),
    pickup_longitude DECIMAL(11, 8),
    pickup_address TEXT,
    
    -- Dropoff information
    dropoff_datetime TIMESTAMP,
    dropoff_zone_id INTEGER REFERENCES zones(zone_id),
    dropoff_latitude DECIMAL(10, 8),
    dropoff_longitude DECIMAL(11, 8),
    dropoff_address TEXT,
    
    -- Trip details
    trip_distance DECIMAL(10, 2), -- in miles
    trip_duration INTEGER, -- in seconds
    passenger_count INTEGER DEFAULT 1,
    
    -- Pricing
    base_fare DECIMAL(10, 2),
    surge_multiplier DECIMAL(3, 2) DEFAULT 1.0,
    distance_fare DECIMAL(10, 2),
    time_fare DECIMAL(10, 2),
    tips DECIMAL(10, 2) DEFAULT 0,
    tolls DECIMAL(10, 2) DEFAULT 0,
    misc_fees DECIMAL(10, 2) DEFAULT 0,
    total_fare DECIMAL(10, 2),
    
    -- Status and ratings
    trip_status VARCHAR(20) DEFAULT 'Requested', -- 'Requested', 'Accepted', 'InProgress', 'Completed', 'Cancelled'
    payment_status VARCHAR(20) DEFAULT 'Pending', -- 'Pending', 'Paid', 'Failed', 'Refunded'
    driver_rating INTEGER CHECK (driver_rating >= 1 AND driver_rating <= 5),
    customer_rating INTEGER CHECK (customer_rating >= 1 AND customer_rating <= 5),
    
    -- Additional metrics
    wait_time INTEGER, -- seconds between request and pickup
    route_efficiency DECIMAL(5, 2), -- actual distance / straight-line distance
    
    -- Metadata
    booking_type VARCHAR(20) DEFAULT 'OnDemand', -- 'OnDemand', 'Scheduled', 'Corporate'
    service_type VARCHAR(20) DEFAULT 'Standard', -- 'Standard', 'Premium', 'Pool', 'XL'
    device_type VARCHAR(20), -- 'iOS', 'Android', 'Web'
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trip payments table
CREATE TABLE trip_payments (
    payment_id SERIAL PRIMARY KEY,
    trip_id INTEGER REFERENCES trips(trip_id),
    payment_method VARCHAR(20) NOT NULL, -- 'Card', 'Cash', 'Digital', 'Corporate'
    payment_amount DECIMAL(10, 2) NOT NULL,
    payment_datetime TIMESTAMP NOT NULL,
    transaction_id VARCHAR(100),
    payment_status VARCHAR(20) DEFAULT 'Pending', -- 'Pending', 'Completed', 'Failed', 'Refunded'
    refund_amount DECIMAL(10, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Weather data table (for analysis)
CREATE TABLE weather_data (
    weather_id SERIAL PRIMARY KEY,
    zone_id INTEGER REFERENCES zones(zone_id),
    datetime TIMESTAMP NOT NULL,
    temperature DECIMAL(5, 2), -- in Fahrenheit
    precipitation DECIMAL(5, 2), -- in inches
    wind_speed DECIMAL(5, 2), -- in mph
    visibility DECIMAL(5, 2), -- in miles
    weather_condition VARCHAR(50), -- 'Clear', 'Rain', 'Snow', 'Fog', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_trips_pickup_datetime ON trips(pickup_datetime);
CREATE INDEX idx_trips_dropoff_datetime ON trips(dropoff_datetime);
CREATE INDEX idx_trips_driver_id ON trips(driver_id);
CREATE INDEX idx_trips_customer_id ON trips(customer_id);
CREATE INDEX idx_trips_pickup_zone ON trips(pickup_zone_id);
CREATE INDEX idx_trips_dropoff_zone ON trips(dropoff_zone_id);
CREATE INDEX idx_trips_status ON trips(trip_status);
CREATE INDEX idx_drivers_status ON drivers(status);
CREATE INDEX idx_customers_active ON customers(is_active);
CREATE INDEX idx_surge_active ON surge_pricing(active, zone_id);
CREATE INDEX idx_weather_datetime ON weather_data(datetime, zone_id);

-- Create views for common analytics queries
CREATE VIEW active_drivers AS
SELECT * FROM drivers WHERE status = 'Active';

CREATE VIEW completed_trips AS
SELECT * FROM trips WHERE trip_status = 'Completed';

CREATE VIEW hourly_demand AS
SELECT 
    DATE_TRUNC('hour', pickup_datetime) as hour,
    pickup_zone_id,
    COUNT(*) as trip_count,
    AVG(total_fare) as avg_fare,
    AVG(trip_distance) as avg_distance
FROM trips
WHERE trip_status = 'Completed'
GROUP BY DATE_TRUNC('hour', pickup_datetime), pickup_zone_id;