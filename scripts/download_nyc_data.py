#!/usr/bin/env python3
"""
Download and process NYC TLC Trip Record Data
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""

import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import zipfile
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Data URLs - NYC TLC publishes data monthly
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Available data types
DATA_TYPES = {
    'yellow': 'yellow_tripdata',    # Yellow taxi
    'green': 'green_tripdata',      # Green taxi (Boro)
    'fhv': 'fhv_tripdata',         # For-hire vehicles
    'fhvhv': 'fhvhv_tripdata'      # High volume for-hire vehicles (Uber/Lyft)
}

# Zone lookup data
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

def download_file(url, local_path):
    """Download a file from URL to local path"""
    try:
        logger.info(f"Downloading {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Downloaded to {local_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        return False

def download_zone_lookup():
    """Download taxi zone lookup data"""
    local_path = "../data/taxi_zone_lookup.csv"
    return download_file(ZONE_LOOKUP_URL, local_path)

def download_monthly_data(data_type, year, month):
    """Download monthly trip data for a specific type"""
    if data_type not in DATA_TYPES:
        raise ValueError(f"Invalid data type. Choose from: {list(DATA_TYPES.keys())}")
    
    # Format: yellow_tripdata_2024-01.parquet
    filename = f"{DATA_TYPES[data_type]}_{year}-{month:02d}.parquet"
    url = BASE_URL + filename
    local_path = f"../data/raw/{filename}"
    
    return download_file(url, local_path)

def get_available_months(start_year=2023, end_year=None):
    """Get list of available months for download"""
    if end_year is None:
        end_year = datetime.now().year
    
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Don't try to download future months
            if datetime(year, month, 1) <= datetime.now().replace(day=1):
                months.append((year, month))
    return months

def process_yellow_taxi_data(file_path):
    """Process yellow taxi data with standard column mapping"""
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded {len(df):,} records from {file_path}")
        
        # Standardize column names (they sometimes change)
        column_mapping = {
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'PULocationID': 'pickup_location_id',
            'DOLocationID': 'dropoff_location_id',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'improvement_surcharge': 'improvement_surcharge',
            'total_amount': 'total_amount',
            'payment_type': 'payment_type',
            'RatecodeID': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'VendorID': 'vendor_id',
            'congestion_surcharge': 'congestion_surcharge',
            'airport_fee': 'airport_fee'
        }
        
        # Rename columns that exist
        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_columns)
        
        # Add data type identifier
        df['trip_type'] = 'yellow'
        
        # Data quality filters
        initial_count = len(df)
        
        # Remove invalid trips
        df = df[
            (df['pickup_datetime'] < df['dropoff_datetime']) &
            (df['trip_distance'] > 0) &
            (df['trip_distance'] < 100) &  # Remove extremely long trips
            (df['fare_amount'] > 0) &
            (df['total_amount'] > 0) &
            (df['passenger_count'] > 0) &
            (df['passenger_count'] <= 6) &  # Reasonable passenger count
            (df['pickup_location_id'].notna()) &
            (df['dropoff_location_id'].notna())
        ]
        
        filtered_count = len(df)
        logger.info(f"Filtered data: {initial_count:,} -> {filtered_count:,} records ({100*filtered_count/initial_count:.1f}% retained)")
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return None

def process_green_taxi_data(file_path):
    """Process green taxi data with standard column mapping"""
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded {len(df):,} records from {file_path}")
        
        # Green taxi has slightly different column names
        column_mapping = {
            'lpep_pickup_datetime': 'pickup_datetime',
            'lpep_dropoff_datetime': 'dropoff_datetime',
            'PULocationID': 'pickup_location_id',
            'DOLocationID': 'dropoff_location_id',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'improvement_surcharge': 'improvement_surcharge',
            'total_amount': 'total_amount',
            'payment_type': 'payment_type',
            'RatecodeID': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'VendorID': 'vendor_id',
            'congestion_surcharge': 'congestion_surcharge',
            'ehail_fee': 'ehail_fee'
        }
        
        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_columns)
        
        df['trip_type'] = 'green'
        
        # Apply same filters as yellow taxi
        initial_count = len(df)
        df = df[
            (df['pickup_datetime'] < df['dropoff_datetime']) &
            (df['trip_distance'] > 0) &
            (df['trip_distance'] < 100) &
            (df['fare_amount'] > 0) &
            (df['total_amount'] > 0) &
            (df['passenger_count'] > 0) &
            (df['passenger_count'] <= 6) &
            (df['pickup_location_id'].notna()) &
            (df['dropoff_location_id'].notna())
        ]
        
        filtered_count = len(df)
        logger.info(f"Filtered data: {initial_count:,} -> {filtered_count:,} records ({100*filtered_count/initial_count:.1f}% retained)")
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return None

def combine_and_save_data(data_frames, output_path):
    """Combine multiple dataframes and save"""
    if not data_frames:
        logger.warning("No data frames to combine")
        return
    
    # Find common columns across all dataframes
    common_columns = set(data_frames[0].columns)
    for df in data_frames[1:]:
        common_columns = common_columns.intersection(set(df.columns))
    
    common_columns = list(common_columns)
    logger.info(f"Common columns across datasets: {len(common_columns)}")
    
    # Keep only common columns and combine
    combined_df = pd.concat([df[common_columns] for df in data_frames], ignore_index=True)
    
    # Sort by pickup time
    combined_df = combined_df.sort_values('pickup_datetime')
    
    logger.info(f"Combined dataset: {len(combined_df):,} records")
    
    # Save as parquet for efficiency
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined_df.to_parquet(output_path, index=False)
    logger.info(f"Saved combined data to {output_path}")
    
    # Also save a CSV sample for quick inspection
    sample_path = output_path.replace('.parquet', '_sample.csv')
    combined_df.head(10000).to_csv(sample_path, index=False)
    logger.info(f"Saved sample data to {sample_path}")
    
    return combined_df

def main():
    """Main function to download and process NYC taxi data"""
    logger.info("Starting NYC TLC data download and processing")
    
    # Create directories
    os.makedirs("../data/raw", exist_ok=True)
    os.makedirs("../data/processed", exist_ok=True)
    
    # Download zone lookup data
    logger.info("Downloading zone lookup data...")
    download_zone_lookup()
    
    # Download recent months of data (last 3 months for demo)
    end_date = datetime.now().replace(day=1)
    start_date = end_date - timedelta(days=90)
    
    data_frames = []
    
    # Download yellow taxi data
    logger.info("Downloading yellow taxi data...")
    for year in range(start_date.year, end_date.year + 1):
        start_month = start_date.month if year == start_date.year else 1
        end_month = end_date.month if year == end_date.year else 12
        
        for month in range(start_month, end_month + 1):
            if download_monthly_data('yellow', year, month):
                file_path = f"../data/raw/yellow_tripdata_{year}-{month:02d}.parquet"
                df = process_yellow_taxi_data(file_path)
                if df is not None:
                    data_frames.append(df)
    
    # Download green taxi data
    logger.info("Downloading green taxi data...")
    for year in range(start_date.year, end_date.year + 1):
        start_month = start_date.month if year == start_date.year else 1
        end_month = end_date.month if year == end_date.year else 12
        
        for month in range(start_month, end_month + 1):
            if download_monthly_data('green', year, month):
                file_path = f"../data/raw/green_tripdata_{year}-{month:02d}.parquet"
                df = process_green_taxi_data(file_path)
                if df is not None:
                    data_frames.append(df)
    
    # Combine and save processed data
    if data_frames:
        logger.info("Combining datasets...")
        combined_df = combine_and_save_data(data_frames, "../data/processed/nyc_taxi_trips.parquet")
        
        # Generate summary statistics
        logger.info("Generating summary statistics...")
        print("\n" + "="*60)
        print("NYC TAXI DATA SUMMARY")
        print("="*60)
        print(f"Total trips: {len(combined_df):,}")
        print(f"Date range: {combined_df['pickup_datetime'].min()} to {combined_df['pickup_datetime'].max()}")
        print(f"Trip types: {combined_df['trip_type'].value_counts().to_dict()}")
        print(f"Average trip distance: {combined_df['trip_distance'].mean():.2f} miles")
        print(f"Average fare: ${combined_df['fare_amount'].mean():.2f}")
        print(f"Average total amount: ${combined_df['total_amount'].mean():.2f}")
        print(f"Unique pickup locations: {combined_df['pickup_location_id'].nunique()}")
        print(f"Unique dropoff locations: {combined_df['dropoff_location_id'].nunique()}")
        
    else:
        logger.error("No data was successfully downloaded and processed")

if __name__ == "__main__":
    main()