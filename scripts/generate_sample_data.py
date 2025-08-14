#!/usr/bin/env python3
"""
Generate realistic sample data for taxi service database
"""

import random
import datetime
from datetime import timedelta
import json
import csv
import os

# Configuration
NUM_ZONES = 50
NUM_DRIVERS = 500
NUM_CUSTOMERS = 5000
NUM_VEHICLES = 450
NUM_TRIPS = 100000
NUM_WEATHER_RECORDS = 10000

# NYC-like zone names
ZONE_NAMES = [
    "Manhattan - Midtown", "Manhattan - Upper East", "Manhattan - Upper West", "Manhattan - Downtown",
    "Manhattan - Financial District", "Manhattan - Chelsea", "Manhattan - Tribeca", "Manhattan - SoHo",
    "Brooklyn - Downtown", "Brooklyn - Park Slope", "Brooklyn - Williamsburg", "Brooklyn - DUMBO",
    "Brooklyn - Brooklyn Heights", "Brooklyn - Bushwick", "Brooklyn - Bed-Stuy", "Brooklyn - Crown Heights",
    "Queens - LIC", "Queens - Astoria", "Queens - Flushing", "Queens - Jackson Heights",
    "Queens - Forest Hills", "Queens - Jamaica", "Bronx - South", "Bronx - Fordham",
    "Bronx - Riverdale", "Staten Island - North", "Staten Island - South",
    "JFK Airport", "LGA Airport", "Newark Airport", "Times Square", "Central Park",
    "Grand Central", "Penn Station", "Wall Street", "Broadway District", "Columbia University",
    "NYU Area", "Yankee Stadium", "Citi Field", "Madison Square Garden", "Barclays Center",
    "Brooklyn Bridge", "Hudson Yards", "High Line", "Battery Park", "Ellis Island",
    "Roosevelt Island", "Governors Island", "Randalls Island"
]

AREA_TYPES = ["Downtown", "Suburban", "Airport", "Industrial", "Entertainment", "Educational", "Residential"]
BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]

FIRST_NAMES = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", 
               "Thomas", "Mark", "Donald", "Paul", "Steven", "Andrew", "Kenneth", "Joshua",
               "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica",
               "Sarah", "Karen", "Nancy", "Betty", "Helen", "Sandra", "Donna", "Carol"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson"]

VEHICLE_MAKES = ["Toyota", "Honda", "Ford", "Chevrolet", "Nissan", "Tesla", "BMW", "Mercedes"]
VEHICLE_MODELS = {
    "Toyota": ["Camry", "Prius", "Corolla", "Highlander"],
    "Honda": ["Accord", "Civic", "CR-V", "Pilot"],
    "Ford": ["Fusion", "Escape", "Explorer", "F-150"],
    "Chevrolet": ["Malibu", "Impala", "Suburban", "Tahoe"],
    "Nissan": ["Altima", "Sentra", "Rogue", "Pathfinder"],
    "Tesla": ["Model 3", "Model Y", "Model S", "Model X"],
    "BMW": ["3 Series", "5 Series", "X3", "X5"],
    "Mercedes": ["C-Class", "E-Class", "GLC", "GLE"]
}

WEATHER_CONDITIONS = ["Clear", "Cloudy", "Rain", "Heavy Rain", "Snow", "Fog", "Windy"]

def generate_zones():
    """Generate zones data"""
    zones = []
    for i in range(NUM_ZONES):
        zone_name = ZONE_NAMES[i] if i < len(ZONE_NAMES) else f"Zone {i+1}"
        zones.append({
            'zone_id': i + 1,
            'zone_name': zone_name,
            'borough': random.choice(BOROUGHS),
            'area_type': random.choice(AREA_TYPES),
            'latitude': round(40.7128 + random.uniform(-0.1, 0.1), 8),
            'longitude': round(-74.0060 + random.uniform(-0.1, 0.1), 8),
            'avg_demand_score': round(random.uniform(1, 10), 2)
        })
    return zones

def generate_vehicles():
    """Generate vehicles data"""
    vehicles = []
    for i in range(NUM_VEHICLES):
        make = random.choice(VEHICLE_MAKES)
        model = random.choice(VEHICLE_MODELS[make])
        year = random.randint(2018, 2024)
        
        vehicle_type = random.choices(
            ["Standard", "Premium", "XL", "Electric"],
            weights=[60, 20, 15, 5]
        )[0]
        
        fuel_type = "Electric" if vehicle_type == "Electric" or make == "Tesla" else random.choice(["Gas", "Hybrid"])
        
        vehicles.append({
            'vehicle_id': i + 1,
            'license_plate': f"{random.choice(['ABC', 'XYZ', 'DEF', 'GHI'])}-{random.randint(1000, 9999)}",
            'make': make,
            'model': model,
            'year': year,
            'vehicle_type': vehicle_type,
            'fuel_type': fuel_type,
            'seating_capacity': 4 if vehicle_type == "Standard" else (6 if vehicle_type == "XL" else 4),
            'registration_date': (datetime.date.today() - timedelta(days=random.randint(100, 1000))).isoformat(),
            'last_inspection_date': (datetime.date.today() - timedelta(days=random.randint(1, 180))).isoformat(),
            'status': random.choices(["Active", "Maintenance"], weights=[95, 5])[0]
        })
    return vehicles

def generate_drivers(num_zones, num_vehicles):
    """Generate drivers data"""
    drivers = []
    used_vehicles = set()
    
    for i in range(NUM_DRIVERS):
        # Ensure each driver gets a unique vehicle
        vehicle_id = None
        if len(used_vehicles) < num_vehicles:
            while vehicle_id is None or vehicle_id in used_vehicles:
                vehicle_id = random.randint(1, num_vehicles)
            used_vehicles.add(vehicle_id)
        
        hire_date = datetime.date.today() - timedelta(days=random.randint(30, 2000))
        
        drivers.append({
            'driver_id': i + 1,
            'driver_license': f"DL{random.randint(100000, 999999)}",
            'first_name': random.choice(FIRST_NAMES),
            'last_name': random.choice(LAST_NAMES),
            'email': f"driver{i+1}@taxiservice.com",
            'phone': f"+1{random.randint(2000000000, 9999999999)}",
            'hire_date': hire_date.isoformat(),
            'birth_date': (hire_date - timedelta(days=random.randint(7300, 18250))).isoformat(),
            'rating': round(random.uniform(3.5, 5.0), 2),
            'total_trips': random.randint(0, 5000),
            'total_earnings': round(random.uniform(0, 150000), 2),
            'status': random.choices(["Active", "Inactive", "Suspended"], weights=[85, 10, 5])[0],
            'home_zone_id': random.randint(1, num_zones),
            'vehicle_id': vehicle_id
        })
    return drivers

def generate_customers(num_zones):
    """Generate customers data"""
    customers = []
    for i in range(NUM_CUSTOMERS):
        registration_date = datetime.date.today() - timedelta(days=random.randint(1, 1500))
        
        customers.append({
            'customer_id': i + 1,
            'first_name': random.choice(FIRST_NAMES),
            'last_name': random.choice(LAST_NAMES),
            'email': f"customer{i+1}@email.com",
            'phone': f"+1{random.randint(2000000000, 9999999999)}",
            'registration_date': registration_date.isoformat(),
            'customer_type': random.choices(["Regular", "Premium", "Corporate"], weights=[70, 20, 10])[0],
            'rating': round(random.uniform(3.0, 5.0), 2),
            'total_trips': random.randint(0, 500),
            'total_spent': round(random.uniform(0, 10000), 2),
            'preferred_payment': random.choice(["Card", "Cash", "Digital"]),
            'home_zone_id': random.randint(1, num_zones),
            'work_zone_id': random.randint(1, num_zones),
            'is_active': random.choices([True, False], weights=[90, 10])[0],
            'last_trip_date': (datetime.date.today() - timedelta(days=random.randint(0, 90))).isoformat()
        })
    return customers

def generate_trips(num_drivers, num_customers, num_vehicles, num_zones):
    """Generate trips data"""
    trips = []
    start_date = datetime.datetime.now() - timedelta(days=90)
    
    for i in range(NUM_TRIPS):
        pickup_datetime = start_date + timedelta(
            days=random.uniform(0, 90),
            hours=random.uniform(0, 24),
            minutes=random.uniform(0, 60)
        )
        
        # Trip duration between 5 minutes and 2 hours
        trip_duration = random.randint(300, 7200)
        dropoff_datetime = pickup_datetime + timedelta(seconds=trip_duration)
        
        # Distance calculation (rough approximation)
        trip_distance = round(trip_duration / 60 * random.uniform(0.3, 0.8), 2)
        
        # Pricing calculation
        base_fare = 3.50
        distance_fare = trip_distance * 2.50
        time_fare = (trip_duration / 60) * 0.50
        
        # Surge pricing (more likely during peak hours)
        hour = pickup_datetime.hour
        surge_multiplier = 1.0
        if hour in [7, 8, 9, 17, 18, 19]:  # Rush hours
            if random.random() < 0.3:
                surge_multiplier = round(random.uniform(1.2, 2.5), 2)
        elif hour in [22, 23, 0, 1, 2]:  # Late night
            if random.random() < 0.2:
                surge_multiplier = round(random.uniform(1.1, 1.8), 2)
        
        tips = round(random.uniform(0, 10), 2) if random.random() < 0.7 else 0
        tolls = round(random.uniform(0, 15), 2) if random.random() < 0.1 else 0
        misc_fees = round(random.uniform(0, 5), 2) if random.random() < 0.05 else 0
        
        total_fare = round((base_fare + distance_fare + time_fare) * surge_multiplier + tips + tolls + misc_fees, 2)
        
        trips.append({
            'trip_id': i + 1,
            'driver_id': random.randint(1, num_drivers),
            'customer_id': random.randint(1, num_customers),
            'vehicle_id': random.randint(1, num_vehicles),
            'pickup_datetime': pickup_datetime.isoformat(),
            'pickup_zone_id': random.randint(1, num_zones),
            'pickup_latitude': round(40.7128 + random.uniform(-0.1, 0.1), 8),
            'pickup_longitude': round(-74.0060 + random.uniform(-0.1, 0.1), 8),
            'dropoff_datetime': dropoff_datetime.isoformat(),
            'dropoff_zone_id': random.randint(1, num_zones),
            'dropoff_latitude': round(40.7128 + random.uniform(-0.1, 0.1), 8),
            'dropoff_longitude': round(-74.0060 + random.uniform(-0.1, 0.1), 8),
            'trip_distance': trip_distance,
            'trip_duration': trip_duration,
            'passenger_count': random.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0],
            'base_fare': base_fare,
            'surge_multiplier': surge_multiplier,
            'distance_fare': distance_fare,
            'time_fare': time_fare,
            'tips': tips,
            'tolls': tolls,
            'misc_fees': misc_fees,
            'total_fare': total_fare,
            'trip_status': random.choices(["Completed", "Cancelled"], weights=[95, 5])[0],
            'payment_status': random.choices(["Paid", "Pending"], weights=[98, 2])[0],
            'driver_rating': random.choices([5, 4, 3, 2, 1], weights=[60, 25, 10, 3, 2])[0],
            'customer_rating': random.choices([5, 4, 3, 2, 1], weights=[65, 20, 10, 3, 2])[0],
            'wait_time': random.randint(60, 600),
            'route_efficiency': round(random.uniform(1.1, 1.5), 2),
            'booking_type': random.choices(["OnDemand", "Scheduled"], weights=[90, 10])[0],
            'service_type': random.choices(["Standard", "Premium", "Pool", "XL"], weights=[60, 20, 15, 5])[0],
            'device_type': random.choices(["iOS", "Android", "Web"], weights=[45, 45, 10])[0]
        })
    
    return trips

def generate_weather_data(num_zones):
    """Generate weather data"""
    weather_data = []
    start_date = datetime.datetime.now() - timedelta(days=90)
    
    for i in range(NUM_WEATHER_RECORDS):
        record_datetime = start_date + timedelta(
            days=random.uniform(0, 90),
            hours=random.uniform(0, 24)
        )
        
        # Seasonal temperature variation
        month = record_datetime.month
        if month in [12, 1, 2]:  # Winter
            base_temp = 35
        elif month in [3, 4, 5]:  # Spring
            base_temp = 55
        elif month in [6, 7, 8]:  # Summer
            base_temp = 75
        else:  # Fall
            base_temp = 55
        
        temperature = round(base_temp + random.uniform(-15, 15), 2)
        
        weather_condition = random.choices(
            WEATHER_CONDITIONS,
            weights=[40, 30, 15, 5, 3, 5, 2]
        )[0]
        
        precipitation = 0
        if weather_condition in ["Rain", "Heavy Rain", "Snow"]:
            precipitation = round(random.uniform(0.1, 2.0), 2)
        
        weather_data.append({
            'weather_id': i + 1,
            'zone_id': random.randint(1, num_zones),
            'datetime': record_datetime.isoformat(),
            'temperature': temperature,
            'precipitation': precipitation,
            'wind_speed': round(random.uniform(0, 30), 2),
            'visibility': round(random.uniform(1, 10), 2),
            'weather_condition': weather_condition
        })
    
    return weather_data

def save_to_csv(data, filename):
    """Save data to CSV file"""
    if not data:
        return
    
    filepath = os.path.join('data', filename)
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} records to {filepath}")

def save_to_json(data, filename):
    """Save data to JSON file"""
    filepath = os.path.join('data', filename)
    with open(filepath, 'w') as jsonfile:
        json.dump(data, jsonfile, indent=2, default=str)
    print(f"Saved {len(data)} records to {filepath}")

def main():
    """Main function to generate all sample data"""
    print("Generating sample data for taxi service database...")
    
    # Generate data
    print("Generating zones...")
    zones = generate_zones()
    
    print("Generating vehicles...")
    vehicles = generate_vehicles()
    
    print("Generating drivers...")
    drivers = generate_drivers(NUM_ZONES, NUM_VEHICLES)
    
    print("Generating customers...")
    customers = generate_customers(NUM_ZONES)
    
    print("Generating trips...")
    trips = generate_trips(NUM_DRIVERS, NUM_CUSTOMERS, NUM_VEHICLES, NUM_ZONES)
    
    print("Generating weather data...")
    weather_data = generate_weather_data(NUM_ZONES)
    
    # Save to CSV
    print("\nSaving data to CSV files...")
    save_to_csv(zones, 'zones.csv')
    save_to_csv(vehicles, 'vehicles.csv')
    save_to_csv(drivers, 'drivers.csv')
    save_to_csv(customers, 'customers.csv')
    save_to_csv(trips, 'trips.csv')
    save_to_csv(weather_data, 'weather_data.csv')
    
    # Also save to JSON for convenience
    print("\nSaving data to JSON files...")
    save_to_json(zones, 'zones.json')
    save_to_json(vehicles, 'vehicles.json')
    save_to_json(drivers, 'drivers.json')
    save_to_json(customers, 'customers.json')
    save_to_json(trips, 'trips.json')
    save_to_json(weather_data, 'weather_data.json')
    
    print("\nSample data generation complete!")
    print(f"Generated:")
    print(f"  - {NUM_ZONES} zones")
    print(f"  - {NUM_VEHICLES} vehicles")
    print(f"  - {NUM_DRIVERS} drivers")
    print(f"  - {NUM_CUSTOMERS} customers")
    print(f"  - {NUM_TRIPS} trips")
    print(f"  - {NUM_WEATHER_RECORDS} weather records")

if __name__ == "__main__":
    main()