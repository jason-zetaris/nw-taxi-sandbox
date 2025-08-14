# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a comprehensive **big data analytics project** using **Apache Spark (PySpark)** and **real NYC Taxi and Limousine Commission (TLC) Trip Record Data**. It demonstrates advanced distributed data processing, scalable analytics, and enterprise-grade business intelligence techniques on millions of actual taxi trips from New York City.

## Common Development Commands

```bash
# Download and process with PySpark (distributed processing)
python scripts/spark_data_processor.py

# Start Jupyter with PySpark capabilities
jupyter notebook

# Run distributed analytics notebook
# Open: notebooks/01_nyc_taxi_spark_eda.ipynb

# Custom Spark session for development
python scripts/spark_setup.py

# Traditional database setup (optional)
createdb nyc_taxi_analytics
psql -d nyc_taxi_analytics -f sql/01_nyc_tlc_schema.sql
python scripts/load_data_to_db.py --db-url "postgresql://user:password@localhost/nyc_taxi_analytics"
```

## High-Level Architecture

### Big Data Layer
- **Apache Spark**: Distributed processing engine for horizontal scaling
- **PySpark**: Python API for Spark development and analytics
- **Spark SQL**: Large-scale SQL analytics with catalyst optimizer
- **Data Volume**: Scales from MBs to TBs - unlimited by design
- **Storage Format**: Optimized parquet with columnar compression

### Analytics Layer
- **Distributed Computing**: Parallel processing across multiple cores/nodes
- **Spark SQL**: Complex analytics queries optimized by Catalyst
- **Spark DataFrame API**: Type-safe transformations and aggregations
- **MLlib Integration**: Machine learning at scale with built-in algorithms
- **Streaming Ready**: Framework for real-time analytics

### Key Components
1. **scripts/spark_setup.py**: Optimized Spark session configuration and utilities
2. **scripts/spark_data_processor.py**: Distributed ETL pipeline using PySpark
3. **notebooks/01_nyc_taxi_spark_eda.ipynb**: Comprehensive big data analysis
4. **sql/**: Traditional SQL scripts for database analytics (optional)
5. **data/processed/**: Partitioned parquet files for optimal query performance

## Key Use Cases

1. **Scalable Temporal Analysis**: Process years of historical data for demand patterns
2. **Distributed Geographic Intelligence**: Borough flows across massive datasets
3. **Real-time Payment Analytics**: Streaming analysis of payment and tip patterns
4. **High-Performance BI**: Sub-second queries on 100M+ trip datasets
5. **Machine Learning Pipeline**: MLlib models for demand forecasting and pricing
6. **Anomaly Detection at Scale**: Distributed fraud detection algorithms
7. **Streaming Analytics**: Real-time dashboards and live operational insights

## Big Data Architecture Patterns

### **Spark DataFrames and SQL**
- Distributed DataFrames for type-safe transformations
- Spark SQL with Catalyst optimizer for complex analytics
- Window functions for time-series analysis at scale
- Broadcast joins for efficient lookup operations

### **Performance Optimization**
- In-memory caching of hot datasets (`.cache()`, `.persist()`)
- Partitioning strategies for optimal query performance
- Columnar parquet storage with compression
- Adaptive query execution (AQE) for runtime optimization

### **Scalability Patterns**
- Horizontal scaling from single machine to clusters
- Dynamic resource allocation and executor management
- Fault tolerance with automatic job recovery
- Integration with cloud data platforms (EMR, Databricks, Dataproc)

## Important Spark Patterns

- **Lazy Evaluation**: Transformations are optimized before execution
- **Immutable DataFrames**: Functional programming paradigm for data processing
- **Catalyst Optimizer**: Automatic SQL query optimization and code generation
- **Columnar Processing**: Vectorized operations for numerical computations
- **Broadcast Variables**: Efficient distribution of lookup tables to all nodes
- **Accumulators**: Distributed counters for metrics and debugging
- **Checkpointing**: Lineage truncation for long-running jobs