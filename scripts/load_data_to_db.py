#!/usr/bin/env python3
"""
Load NYC TLC data into PostgreSQL database
Handles both zone lookup data and trip data
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import logging
from typing import Optional
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NYCTaxiDataLoader:
    def __init__(self, db_url: str):
        """Initialize with database connection string"""
        self.db_url = db_url
        self.engine = create_engine(db_url)
    
    def load_zone_lookup(self, csv_path: str = "../data/taxi_zone_lookup.csv"):
        """Load taxi zone lookup data"""
        try:
            logger.info(f"Loading zone lookup data from {csv_path}")
            
            if not os.path.exists(csv_path):
                logger.error(f"Zone lookup file not found: {csv_path}")
                return False
            
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} zones")
            
            # Rename columns to match schema
            column_mapping = {
                'LocationID': 'location_id',
                'Borough': 'borough',
                'Zone': 'zone',
                'service_zone': 'service_zone'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Clean data
            df['borough'] = df['borough'].fillna('Unknown')
            df['service_zone'] = df['service_zone'].fillna('Unknown')
            
            # Load to database
            df.to_sql('taxi_zones', self.engine, if_exists='append', index=False, method='multi')
            logger.info(f"Successfully loaded {len(df)} zones to database")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading zone data: {e}")
            return False
    
    def load_trip_data(self, parquet_path: str, batch_size: int = 50000):
        """Load trip data in batches"""
        try:
            logger.info(f"Loading trip data from {parquet_path}")
            
            if not os.path.exists(parquet_path):
                logger.error(f"Trip data file not found: {parquet_path}")
                return False
            
            # Read parquet file
            df = pd.read_parquet(parquet_path)
            total_rows = len(df)
            logger.info(f"Loaded {total_rows:,} trips from parquet file")
            
            # Process data
            df = self.process_trip_data(df)
            processed_rows = len(df)
            logger.info(f"After processing: {processed_rows:,} valid trips ({100*processed_rows/total_rows:.1f}% retention)")
            
            # Load in batches
            batches = (processed_rows // batch_size) + 1
            logger.info(f"Loading data in {batches} batches of {batch_size:,} rows each")
            
            for i in range(0, processed_rows, batch_size):
                batch_df = df.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                logger.info(f"Loading batch {batch_num}/{batches} ({len(batch_df):,} rows)")
                
                # Load batch to database
                batch_df.to_sql('taxi_trips', self.engine, if_exists='append', index=False, method='multi')
                
                if batch_num % 10 == 0:
                    logger.info(f"Completed {batch_num}/{batches} batches")
            
            logger.info(f"Successfully loaded {processed_rows:,} trips to database")
            return True
            
        except Exception as e:
            logger.error(f"Error loading trip data: {e}")
            return False
    
    def process_trip_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and clean trip data"""
        logger.info("Processing trip data...")
        
        initial_count = len(df)
        
        # Ensure required columns exist
        required_columns = [
            'pickup_datetime', 'dropoff_datetime', 'pickup_location_id', 
            'dropoff_location_id', 'trip_distance', 'fare_amount', 'total_amount'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()
        
        # Convert datetime columns
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
        
        # Data quality filters
        df = df[
            (df['pickup_datetime'] < df['dropoff_datetime']) &
            (df['trip_distance'] > 0) &
            (df['trip_distance'] < 100) &
            (df['fare_amount'] > 0) &
            (df['total_amount'] > 0) &
            (df['pickup_location_id'].notna()) &
            (df['dropoff_location_id'].notna()) &
            (df['pickup_location_id'] > 0) &
            (df['dropoff_location_id'] > 0)
        ]
        
        # Handle passenger count
        if 'passenger_count' in df.columns:
            df['passenger_count'] = df['passenger_count'].fillna(1)
            df = df[(df['passenger_count'] > 0) & (df['passenger_count'] <= 6)]
        else:
            df['passenger_count'] = 1
        
        # Fill missing numeric columns with 0
        numeric_columns = [
            'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
            'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'ehail_fee'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0)
            else:
                df[col] = 0
        
        # Handle payment type
        if 'payment_type' not in df.columns:
            df['payment_type'] = 1  # Default to credit card
        else:
            df['payment_type'] = df['payment_type'].fillna(1)
        
        # Handle rate code
        if 'rate_code_id' not in df.columns:
            df['rate_code_id'] = 1  # Default to standard rate
        else:
            df['rate_code_id'] = df['rate_code_id'].fillna(1)
        
        # Handle vendor ID
        if 'vendor_id' not in df.columns:
            df['vendor_id'] = 1
        else:
            df['vendor_id'] = df['vendor_id'].fillna(1)
        
        # Handle store and forward flag
        if 'store_and_fwd_flag' not in df.columns:
            df['store_and_fwd_flag'] = 'N'
        else:
            df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N')
        
        # Ensure trip_type exists
        if 'trip_type' not in df.columns:
            df['trip_type'] = 'yellow'  # Default assumption
        
        final_count = len(df)
        logger.info(f"Data processing complete: {initial_count:,} -> {final_count:,} rows ({100*final_count/initial_count:.1f}% retained)")
        
        return df
    
    def verify_data_load(self):
        """Verify data was loaded correctly"""
        try:
            with self.engine.connect() as conn:
                # Check zones
                zone_count = pd.read_sql("SELECT COUNT(*) as count FROM taxi_zones", conn).iloc[0]['count']
                logger.info(f"Zones in database: {zone_count:,}")
                
                # Check trips
                trip_count = pd.read_sql("SELECT COUNT(*) as count FROM taxi_trips", conn).iloc[0]['count']
                logger.info(f"Trips in database: {trip_count:,}")
                
                # Check valid trips
                valid_count = pd.read_sql("SELECT COUNT(*) as count FROM taxi_trips WHERE is_valid_trip = true", conn).iloc[0]['count']
                logger.info(f"Valid trips: {valid_count:,} ({100*valid_count/trip_count:.1f}%)")
                
                # Check date range
                date_range = pd.read_sql("""
                    SELECT 
                        MIN(pickup_datetime) as min_date,
                        MAX(pickup_datetime) as max_date
                    FROM taxi_trips
                """, conn)
                
                logger.info(f"Date range: {date_range.iloc[0]['min_date']} to {date_range.iloc[0]['max_date']}")
                
                # Check trip types
                trip_types = pd.read_sql("""
                    SELECT trip_type, COUNT(*) as count 
                    FROM taxi_trips 
                    GROUP BY trip_type
                """, conn)
                
                logger.info("Trip types:")
                for _, row in trip_types.iterrows():
                    logger.info(f"  {row['trip_type']}: {row['count']:,}")
                
                return True
                
        except Exception as e:
            logger.error(f"Error verifying data: {e}")
            return False
    
    def refresh_materialized_views(self):
        """Refresh materialized views after data load"""
        try:
            logger.info("Refreshing materialized views...")
            with self.engine.connect() as conn:
                conn.execute("REFRESH MATERIALIZED VIEW monthly_statistics")
            logger.info("Materialized views refreshed successfully")
            return True
        except Exception as e:
            logger.error(f"Error refreshing materialized views: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Load NYC TLC data into PostgreSQL database')
    parser.add_argument('--db-url', required=True, help='PostgreSQL connection string')
    parser.add_argument('--zone-file', default='../data/taxi_zone_lookup.csv', help='Path to zone lookup CSV')
    parser.add_argument('--trip-file', default='../data/processed/nyc_taxi_trips.parquet', help='Path to trip data parquet')
    parser.add_argument('--batch-size', type=int, default=50000, help='Batch size for loading trips')
    parser.add_argument('--skip-zones', action='store_true', help='Skip loading zone data')
    parser.add_argument('--skip-trips', action='store_true', help='Skip loading trip data')
    
    args = parser.parse_args()
    
    logger.info("Starting NYC TLC data loading process")
    logger.info(f"Database URL: {args.db_url.split('@')[0]}@***")  # Hide password
    
    # Initialize loader
    loader = NYCTaxiDataLoader(args.db_url)
    
    success = True
    
    # Load zone data
    if not args.skip_zones:
        if not loader.load_zone_lookup(args.zone_file):
            success = False
    
    # Load trip data
    if not args.skip_trips:
        if not loader.load_trip_data(args.trip_file, args.batch_size):
            success = False
    
    # Verify data
    if success:
        if loader.verify_data_load():
            loader.refresh_materialized_views()
            logger.info("Data loading completed successfully!")
        else:
            logger.error("Data verification failed!")
            success = False
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())