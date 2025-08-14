#!/usr/bin/env python3
"""
PySpark setup and configuration for NYC Taxi Analytics
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os

def create_spark_session(app_name="NYC_Taxi_Analytics", memory="4g", cores="*"):
    """
    Create and configure Spark session for NYC taxi analytics
    
    Args:
        app_name: Name of the Spark application
        memory: Driver memory allocation (e.g., "4g", "8g")
        cores: Number of cores to use ("*" for all available)
    
    Returns:
        SparkSession object
    """
    
    # Spark configuration
    conf = SparkConf()
    conf.setAppName(app_name)
    conf.set("spark.driver.memory", memory)
    conf.set("spark.driver.maxResultSize", "2g")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    # Enable Hive support for better SQL compatibility
    conf.set("spark.sql.catalogImplementation", "hive")
    
    # Configure for large datasets
    conf.set("spark.sql.shuffle.partitions", "200")
    conf.set("spark.default.parallelism", "100")
    
    # Create Spark session
    spark = SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark Session Created: {app_name}")
    print(f"📊 Spark Version: {spark.version}")
    print(f"🔧 Driver Memory: {memory}")
    print(f"⚙️ Available Cores: {spark.sparkContext.defaultParallelism}")
    
    return spark

def configure_spark_for_taxi_data(spark):
    """
    Configure Spark session specifically for NYC taxi data processing
    
    Args:
        spark: SparkSession object
    """
    
    # Register custom functions for taxi analytics
    spark.udf.register("calculate_tip_percentage", lambda tip, fare: (tip / fare * 100) if fare > 0 else 0.0)
    spark.udf.register("calculate_speed", lambda distance, duration: (distance / (duration / 3600)) if duration > 0 else 0.0)
    spark.udf.register("get_hour", lambda timestamp: timestamp.hour if timestamp else None)
    spark.udf.register("get_day_of_week", lambda timestamp: timestamp.weekday() if timestamp else None)
    
    # Set catalog database for taxi analytics
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS taxi_analytics")
        spark.sql("USE taxi_analytics")
        print("📁 Created/Using database: taxi_analytics")
    except Exception as e:
        print(f"⚠️ Database setup warning: {e}")

def get_recommended_partitions(file_size_gb):
    """
    Get recommended partition count based on file size
    
    Args:
        file_size_gb: Approximate file size in GB
    
    Returns:
        Recommended partition count
    """
    if file_size_gb < 1:
        return 50
    elif file_size_gb < 5:
        return 100
    elif file_size_gb < 10:
        return 200
    else:
        return 400

def optimize_spark_for_data_size(spark, data_size_gb):
    """
    Optimize Spark configuration based on data size
    
    Args:
        spark: SparkSession object
        data_size_gb: Approximate data size in GB
    """
    partitions = get_recommended_partitions(data_size_gb)
    
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    print(f"🔧 Optimized for {data_size_gb}GB data with {partitions} partitions")

def create_temp_views(spark, trips_df=None, zones_df=None):
    """
    Create temporary views for SQL analytics
    
    Args:
        spark: SparkSession object
        trips_df: Trips DataFrame (optional)
        zones_df: Zones DataFrame (optional)
    """
    if trips_df is not None:
        trips_df.createOrReplaceTempView("trips")
        print("📊 Created temp view: trips")
    
    if zones_df is not None:
        zones_df.createOrReplaceTempView("zones")
        print("📍 Created temp view: zones")

def print_spark_ui_info(spark):
    """
    Print Spark UI access information
    
    Args:
        spark: SparkSession object
    """
    try:
        ui_url = spark.sparkContext.uiWebUrl
        print(f"\n🌐 Spark UI available at: {ui_url}")
        print("📈 Monitor job progress, stages, and executors in the Spark UI")
    except:
        print("🌐 Spark UI: Check http://localhost:4040 (default)")

def stop_spark_session(spark):
    """
    Properly stop Spark session
    
    Args:
        spark: SparkSession object
    """
    if spark:
        spark.stop()
        print("🛑 Spark session stopped")

# Helper functions for common Spark operations

def read_parquet_optimized(spark, file_path, num_partitions=None):
    """
    Read parquet file with optimizations
    
    Args:
        spark: SparkSession object
        file_path: Path to parquet file
        num_partitions: Number of partitions (optional)
    
    Returns:
        DataFrame
    """
    df = spark.read.parquet(file_path)
    
    if num_partitions:
        df = df.repartition(num_partitions)
    
    return df

def write_parquet_optimized(df, output_path, mode="overwrite", partition_by=None):
    """
    Write DataFrame to parquet with optimizations
    
    Args:
        df: DataFrame to write
        output_path: Output path
        mode: Write mode ("overwrite", "append", etc.)
        partition_by: Column(s) to partition by (optional)
    """
    writer = df.write.mode(mode)
    
    if partition_by:
        if isinstance(partition_by, str):
            partition_by = [partition_by]
        writer = writer.partitionBy(*partition_by)
    
    writer.parquet(output_path)

def cache_dataframe(df, storage_level="MEMORY_AND_DISK"):
    """
    Cache DataFrame with specified storage level
    
    Args:
        df: DataFrame to cache
        storage_level: Spark storage level
    
    Returns:
        Cached DataFrame
    """
    from pyspark import StorageLevel
    
    storage_levels = {
        "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
        "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
        "DISK_ONLY": StorageLevel.DISK_ONLY,
        "MEMORY_ONLY_SER": StorageLevel.MEMORY_ONLY_SER,
        "MEMORY_AND_DISK_SER": StorageLevel.MEMORY_AND_DISK_SER
    }
    
    level = storage_levels.get(storage_level, StorageLevel.MEMORY_AND_DISK)
    return df.persist(level)

# Example usage
if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session("NYC_Taxi_Test", memory="4g")
    
    # Configure for taxi data
    configure_spark_for_taxi_data(spark)
    
    # Print UI info
    print_spark_ui_info(spark)
    
    # Test basic functionality
    test_df = spark.range(100).toDF("number")
    print(f"✅ Test DataFrame created with {test_df.count()} rows")
    
    # Stop session
    stop_spark_session(spark)