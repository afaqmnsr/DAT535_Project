# REWDP Energy Dataset - Final Project

**From Raw Watts to Clean Data: Energy Analytics with Medallion Architecture**

This project implements a complete data pipeline using Apache Spark and the Medallion Architecture (Bronze-Silver-Gold) to process and analyze residential energy consumption data from Pakistan.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Dataset Information](#dataset-information)
3. [Project Structure](#project-structure)
4. [Quick Start Guide](#quick-start-guide)
6. [Part 1: Data Ingestion](#part-1-data-ingestion)
7. [Part 2: Data Cleaning](#part-2-data-cleaning)
8. [Part 3: Data Serving](#part-3-data-serving)
9. [Architecture](#architecture)
10. [Design Decisions](#design-decisions)

---

## Project Overview

### Use Case: Pakistan's Energy Crisis

Pakistan faces significant energy challenges with high demand and supply shortages. This project addresses the need to understand residential consumption patterns across multiple cities to:

- Analyze energy consumption across 60 households
- Identify peak demand periods
- Enable predictive analytics for demand forecasting
- Help utilities plan capacity
- Support efficiency programs
- Improve grid stability

### Business Value

- **Capacity Planning**: Help utilities understand demand patterns
- **Efficiency Programs**: Identify high-consumption households for targeted programs
- **Grid Stability**: Predict peak demand to prevent blackouts
- **Cost Optimization**: Enable time-based pricing strategies

---

## Dataset Information

### REWDP Dataset (Residential Energy and Weather Data Pakistan)

- **Source**: LUMS Energy Institute
- **Scale**: 60 households across 6 cities
- **Total Records**: 35.7 million
- **Cities**: Lahore, Karachi, Multan, Peshawar, Islamabad, Skardu

### Data Structure

**Energy Data** (`rewdp_dataset/`):
- CSV files per city/household
- Columns:
  - `datetime` - timestamp
  - `Usage (kW)` - total consumption
  - `TVL_AC (kW)` - AC/TV usage
  - `Kitchen (kW)` - kitchen appliance usage

**Weather Data** (`weather_dataset/`):
- CSV files per city
- Columns: Temperature, Humidity, Solar Radiation, Wind Speed

### Why This Dataset?

- Real-world, unstructured data
- Large-scale (35M+ records) - perfect for Spark processing
- Multiple data sources (energy + weather)
- Represents actual energy consumption patterns

---

## Project Structure

```
Final_Project/
├── Part1_Data_Ingestion/
│   ├── ingestion_script.py
│   └── raw_data/
│       ├── rewdp_bronze.jsonl
│       └── ingestion_metadata.json
├── Part2_Data_Cleaning/
│   └── part2_cleaning_mapreduce.ipynb
├── Part3_Data_Serving/
│   ├── part3_serving_analytics.ipynb
│   └── gold_data/
├── rewdp_dataset/          # Raw energy data (ignored by git)
├── weather_dataset/         # Raw weather data (ignored by git)
└── README.md               # This file
```

---

## Quick Start Guide

### Prerequisites

- Python 3.x
- Apache Spark (configured and running)
- Jupyter Notebook
- Required Python packages (pyspark, pandas, etc.)

### Step 1: Run Part 1 - Data Ingestion

```bash
cd Part1_Data_Ingestion
# Update SOURCE_DIR path if needed (default: ../rewdp_dataset)
python ingestion_script.py
```

**Output**: `Part1_Data_Ingestion/raw_data/rewdp_bronze.jsonl`

**What it does**:
- Reads 69 CSV files from directory structure
- Adds metadata to each record (timestamp, source, city)
- Saves as JSON Lines format (Bronze layer)
- Creates ingestion metadata file

### Step 2: Run Part 2 - Data Cleaning (MapReduce)

1. Open `Part2_Data_Cleaning/part2_cleaning_mapreduce.ipynb`
2. Update path: `bronze_data_path = "../Part1_Data_Ingestion/raw_data/"`
3. Run all cells

**Output**: `silver_data/` directory (Silver layer)

**Key Requirements**:
- RDD/MapReduce ONLY (no SQL, no DataFrames except final inspection)
- Comprehensive data quality validation
- Performance profiling and tuning

### Step 3: Run Part 3 - Analytics (SQL/MLlib)

1. Open `Part3_Data_Serving/part3_serving_analytics.ipynb`
2. Update path: `silver_data_path = "../Part2_Data_Cleaning/silver_data/"`
3. Run all cells

**Output**: `gold_data/` directory with 8 analytics-ready datasets:
- Daily/hourly/monthly consumption metrics and trends
- Weather-energy correlations
- Peak demand analysis
- Household clusters (4 segments)
- Consumption predictions from ML model
- 6 visualization PNG files

**Key Requirements**:
- SQL and MLlib (high-level APIs)
- Business use case implementation
- Multiple algorithms (Linear Regression, K-Means)
- Performance profiling and tuning

### Troubleshooting

- **Check file paths**: Ensure paths match the directory structure
- **Test with subset**: Test with 1-2 CSV files first
- **Spark UI**: Check http://localhost:4040 for job monitoring
- **Column names**: Verify column names match (datetime, Usage (kW), etc.)

---

## Project Overview

This project implements a complete Medallion Architecture pipeline with three distinct layers:

1. **Bronze Layer (Part 1)**: Raw data ingestion with metadata tracking
2. **Silver Layer (Part 2)**: Data cleaning using pure MapReduce/RDD operations
3. **Gold Layer (Part 3)**: Analytics and machine learning using SQL and MLlib

### Key Features

- **MapReduce Implementation**: Pure RDD operations for data cleaning in Part 2
- **Performance Optimization**: Comprehensive profiling and tuning across all parts
- **Medallion Architecture**: Clear separation of Bronze, Silver, and Gold layers
- **Machine Learning**: Multiple algorithms (Linear Regression, K-Means) for analytics
- **Production-Ready Outputs**: Multiple formats (Parquet, CSV, JSON Lines) for different use cases

---

## Part 1: Data Ingestion

### Implementation

The data ingestion process reads raw CSV files from the REWDP dataset and converts them into a Bronze layer format. The implementation includes:

- **Use Case**: REWDP (Residential Energy and Weather Data Pakistan) dataset
- **Cloud Setup**: VM setup configured for processing
- **Ingestion Method**: Python script using CSV reading and JSON Lines output
- **Output**: `rewdp_bronze.jsonl` with full metadata tracking

### Features

- Clean, well-structured ingestion script
- Proper metadata tracking (`_ingestion_metadata`) for data lineage
- Handles multiple CSV files from nested directory structure
- Outputs in JSON Lines format for efficient Spark processing

### Ingestion Method: JSON Lines

**Why JSON Lines?**
- Preserves raw data exactly as received
- Easy to process line-by-line in Spark
- Supports schema evolution
- Maintains data lineage with metadata

**Implementation**:
1. Read 69 CSV files from directory structure
2. Add metadata to each record (timestamp, source, city)
3. Save as JSON Lines (one JSON per line)

**Results**:
- **Input**: 69 CSV files from nested directory structure (6 cities × multiple households)
- **Output**: 35,734,531 records in `rewdp_bronze.jsonl`
- **Metadata**: Full lineage tracking with ingestion timestamp, source, city, and source file

---

## Part 2: Data Cleaning

### Implementation

The data cleaning process implements a pure MapReduce pipeline using RDD operations exclusively. The implementation includes:

- **Cleaning Steps**: Parsing, validation, deduplication, quality checks
- **MapReduce Implementation**: Uses RDD operations (map, reduceByKey, filter, etc.)
- **No SQL/DataFrames**: Only uses DataFrame at the end for inspection
- **Performance Profiling**: Comprehensive profiling with execution times and throughput
- **Optimization**: Documented tuning strategies and performance improvements
- **Output**: Silver layer data saved to `silver_data/` directory

### Features

- Pure MapReduce implementation using RDD operations
- Comprehensive data quality validation
- Performance profiling with execution times and throughput metrics
- Documented optimization strategies
- Robust error handling and validation

### Processing Summary

- **Input**: 35,734,531 records (Bronze)
- **Valid**: 33,332,692 records (93.3%)
- **Invalid**: 2,401,839 records (6.7%)
- **Duplicates removed**: 5,346,326 records (16.0% of valid records)
- **Output**: 27,986,366 cleaned records (Silver)

### Data Quality Metrics

- **Completeness Score**: 99.8%
- **Validity Score**: 93.3%
- **Uniqueness**: 100%
- **Overall Quality Score**: High

### Cleaning Pipeline (8 Steps)

1. **Step 1: Load Raw Data from Bronze Layer**: Load JSON Lines files with 64 partitions
2. **Step 2: Data Parsing and Validation (Map Phase)**: Parse JSON, normalize datetime, validate ranges, categorize errors
3. **Step 3: Extract Cleaned Data**: Filter valid records and convert to lightweight tuples
4. **Step 4: Deduplication (MapReduce Reduce Phase)**: Remove duplicates using `reduceByKey` with composite keys
5. **Step 5: Data Quality Validation (MapReduce Aggregations)**: Completeness, range validation, statistical summaries, distribution analysis
6. **Step 6: Silver Layer Output**: Save cleaned data as JSON Lines and DataFrame for inspection
7. **Step 7: Performance Profiling and Analysis**: Measure execution times, throughput, partition analysis
8. **Step 8: Optimization Strategies and Tuning Recommendations**: Document optimization strategies based on profiling

### Key MapReduce Operations

**1. Parsing (Map)**
```python
parsed_rdd = raw_rdd.map(parse_and_validate)
```

**2. Deduplication (Reduce)**
```python
keyed_rdd = clean_rdd.map(lambda rec: (key, rec))
deduplicated = keyed_rdd.reduceByKey(lambda a, b: a)
```

**3. Quality Checks (Map + Reduce)**
```python
nulls = rdd.map(lambda rec: 1 if field_is_null else 0).reduce(add)
```

### Challenges & Solutions

**Challenge 1: Large Dataset**
- **Problem**: 35.7M records causing memory pressure
- **Solution**: 
  - Used `MEMORY_AND_DISK` storage level
  - Repartitioned to 64 partitions
  - Processed in stages (parse → filter → deduplicate)

**Challenge 2: Data Quality Issues**
- **Problem**: Missing values, "NA" strings, invalid ranges
- **Solution**:
  - Robust parsing with error handling
  - Categorized errors (invalid, parse_error, skip)
  - Saved rejected records for analysis

**Challenge 3: Deduplication at Scale**
- **Problem**: Finding duplicates across 33M records efficiently
- **Solution**:
  - Used `reduceByKey` (distributed reduce)
  - Created composite keys (household + datetime)
  - Cached intermediate results

**Challenge 4: Performance Optimization**
- **Problem**: Slow processing times
- **Solution**:
  - KryoSerializer for faster serialization
  - Strategic caching (persist frequently-used RDDs)
  - Partition tuning (64 partitions for parallelism)

### Performance Metrics

Based on profiling results:
- **Data Loading**: 15.6 seconds (2.3M records/second)
- **Parsing & Validation**: 8.8 seconds (4.1M records/second)
- **Filtering Valid Records**: 10.4 seconds (3.2M records/second)
- **Deduplication**: Variable time depending on data distribution
- **Total Processing Time**: Approximately 2-3 minutes for 35M+ records
- **Optimization**: KryoSerializer, 64 partitions, MEMORY_AND_DISK storage level


---

## Part 3: Data Serving

### Implementation

The data serving layer implements analytics and machine learning using SQL and MLlib. The implementation includes:

- **Business Use Case**: Energy Consumption Optimization
- **Algorithms**: Linear Regression and K-Means Clustering implemented using MLlib
- **SQL Queries**: Multiple SQL queries for aggregations and analytics
- **Performance Profiling**: Comprehensive profiling with execution times
- **Optimization**: Documented tuning strategies
- **Output**: Gold layer outputs saved to `gold_data/` in multiple formats

### Features

- Clear business use case addressing specific energy consumption questions
- Multiple algorithms implemented (Linear Regression, K-Means)
- Extensive use of SQL for analytics and aggregations
- Performance profiling with detailed metrics
- Visualizations for trends and patterns
- Multiple output formats (Parquet, CSV, PNG) for different use cases

### Business Use Case: Energy Consumption Optimization

**Key Business Questions**:
1. How does weather (temperature, humidity, solar radiation) correlate with energy consumption?
2. Can we predict energy consumption based on weather conditions?
3. What are peak consumption periods and how do they relate to weather?
4. How can we cluster households by consumption patterns for targeted efficiency programs?

**Analytics Pipeline (7 Steps)**:
1. **Step 1: Load Data from Silver Layer and Weather Dataset**: Load cleaned energy data and weather CSV files
2. **Step 2: Weather-Energy Data Integration**: Join energy consumption with weather data by city and datetime
3. **Step 3: Business Metrics with SQL**: Daily consumption, hourly patterns, peak demand analysis using SQL queries
4. **Step 4: Advanced Analytics with DataFrame API**: Time-series analysis, rolling averages, monthly trends
5. **Step 5: Machine Learning with MLlib**: 
   - Linear Regression for consumption prediction (RMSE, R² metrics)
   - K-Means Clustering for household segmentation (4 clusters)
6. **Step 6: Data Visualizations**: Generate plots for trends, correlations, and patterns
7. **Step 7: Prepare Data for Serving (Gold Layer)**: Save 8 analytics-ready datasets in multiple formats

### Algorithm Selection

**Linear Regression**: Selected for consumption prediction due to its interpretability and effectiveness with continuous target variables. The relationship between weather features and energy consumption is well-suited for linear modeling.

**K-Means Clustering**: Chosen for household segmentation to identify groups with similar consumption patterns. This enables targeted efficiency programs and demand forecasting.

### Model Evaluation

- RMSE and R² metrics calculated for regression model
- Cluster analysis performed for household segmentation
- Feature importance analysis included

### Gold Layer Outputs (8 Datasets)

The following analytics-ready datasets are saved to `gold_data/`:

1. **`daily_consumption_metrics/`** - Daily energy consumption by city (CSV)
2. **`weather_energy_correlations/`** - Weather-consumption correlations (CSV)
3. **`peak_demand_analysis/`** - Peak demand by temperature ranges (CSV)
4. **`monthly_trends/`** - Monthly consumption trends with growth rates (CSV)
5. **`household_summary/`** - Household-level consumption statistics (Parquet)
6. **`household_clusters/`** - Household clusters for targeting (Parquet)
7. **`consumption_predictions/`** - ML model predictions for forecasting (Parquet)
8. **`hourly_patterns/`** - Hourly consumption patterns by city (CSV)

**Visualizations** (PNG format):
- `daily_consumption_trends.png`
- `hourly_patterns.png`
- `household_clusters.png`
- `ml_model_performance.png`
- `peak_demand_by_temp.png`
- `weather_correlations.png`

**Output Formats**:
- **Parquet**: For efficient storage and querying (household data, predictions, clusters)
- **CSV**: For easy analysis and visualization (metrics, trends, correlations)
- **PNG**: Visualizations for reports and presentations

---

## Architecture

### Medallion Architecture Implementation

This project implements the Medallion Architecture pattern with three distinct layers:

- **Bronze Layer (Part 1)**: Raw data ingestion with full metadata tracking
- **Silver Layer (Part 2)**: Cleaned, validated, and deduplicated data using MapReduce
- **Gold Layer (Part 3)**: Analytics-ready, aggregated, and ML-enhanced data

### Requirements Implementation

**Part 1 - Data Ingestion**:
- Use case identified and documented
- Cloud setup configured
- Ingestion method: Python script with JSON Lines output
- Raw data ingested with metadata

**Part 2 - Data Cleaning**:
- Cleaning steps identified and implemented
- Pure MapReduce implementation using RDD operations
- No SQL/DataFrames (except final inspection)
- Comprehensive profiling and tuning

**Part 3 - Data Serving**:
- Business use case clearly defined with 4 key questions
- 7-step analytics pipeline implemented
- Algorithms: Linear Regression (consumption prediction), K-Means (4 clusters for household segmentation)
- SQL and MLlib used extensively for analytics
- 8 analytics-ready datasets generated in multiple formats
- Performance profiling and optimization documented
- 6 visualization files created for reporting

---

## Design Decisions

### Part 1: JSON Lines Format

JSON Lines was chosen for the Bronze layer because it:
- Preserves raw data exactly as received
- Supports schema evolution
- Enables efficient line-by-line processing in Spark
- Maintains data lineage with metadata

### Part 2: MapReduce Implementation

The decision to use pure MapReduce/RDD operations (instead of DataFrames) was made to:
- Meet project requirements for low-level Spark operations
- Provide direct control over transformations
- Demonstrate understanding of distributed processing patterns
- Enable fine-grained optimization

**Key Optimizations**:
- Repartitioned to 64 partitions for optimal parallelism
- Used KryoSerializer for faster serialization
- Implemented strategic caching for frequently-used RDDs

### Part 3: Algorithm Selection

**Linear Regression**: Chosen for consumption prediction due to interpretability and effectiveness with continuous variables.

**K-Means Clustering**: Selected for household segmentation to identify consumption patterns.

---

## Key Implementation Details

### Part 1: Data Ingestion
- **Script**: `ingestion_script.py` with `ingest_all_csv_files()` function
- **Processing**: Recursively finds all CSV files in `rewdp_dataset/` directory structure
- **Metadata Added**: City name, source filename, ingestion timestamp, dataset information
- **Output Format**: JSON Lines (one JSON object per line) for efficient Spark processing
- **Files Generated**: `rewdp_bronze.jsonl` and `ingestion_metadata.json`

### Part 2: Data Cleaning
- **Notebook**: `part2_cleaning_mapreduce.ipynb`
- **Key Functions**: 
  - `parse_and_validate()`: JSON parsing with datetime normalization and validation
  - `extract_key_tuple()`: Composite key generation for deduplication
  - `check_field_completeness()`: MapReduce-based completeness analysis
  - `count_out_of_range()`: Range validation using MapReduce
- **Storage Level**: `MEMORY_AND_DISK` for handling large datasets
- **Partitioning**: 64 partitions for optimal parallelism
- **Serialization**: KryoSerializer configured for faster serialization
- **Output**: JSON Lines format in `silver_data/` directory

### Part 3: Data Serving
- **Notebook**: `part3_serving_analytics.ipynb`
- **Spark Configuration**: 
  - Adaptive execution enabled
  - 200 shuffle partitions
  - 2GB executor and driver memory
- **ML Pipeline**:
  - Feature engineering: VectorAssembler, StandardScaler, StringIndexer
  - Train/test split: 80/20
  - Linear Regression: L2 regularization (regParam=0.01)
  - K-Means: 4 clusters for household segmentation
- **SQL Operations**: Window functions, aggregations, joins, time-series analysis
- **Output**: 8 datasets in Parquet/CSV formats + 6 visualization PNG files

## Conclusion

This project implements a complete Medallion Architecture pipeline for processing and analyzing residential energy consumption data. The implementation includes:

- **Bronze Layer**: Raw data ingestion with metadata tracking
- **Silver Layer**: Data cleaning using pure MapReduce operations
- **Gold Layer**: Analytics and machine learning using SQL and MLlib

The pipeline successfully processes 35.7 million records through three layers:
- **Bronze Layer**: 35,734,531 raw records ingested with metadata
- **Silver Layer**: 27,986,366 cleaned records (99.8% data quality)
- **Gold Layer**: 8 analytics-ready datasets with ML predictions and visualizations

The implementation demonstrates effective use of Spark's distributed processing capabilities, achieving high throughput (3-4M records/second) and comprehensive data quality validation.

---

## Additional Resources

- Spark UI: http://localhost:4040
- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- Medallion Architecture: https://www.databricks.com/glossary/medallion-architecture

---

**Last Updated**: November 2025

