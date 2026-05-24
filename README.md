# Food Delivery Data Engineering Project

## Overview

This project is an end-to-end AWS data engineering pipeline built using Amazon S3, AWS Glue, PySpark, and Athena.

The pipeline follows a Bronze → Silver → Gold architecture:

- Bronze Layer: Raw CSV files stored in Amazon S3
- Silver Layer: Cleaned and transformed Parquet data
- Gold Layer: Business-focused aggregated tables for analytics

The objective of the project is to process food delivery logistics data and generate insights related to delivery performance, traffic conditions, weather impact, vehicle efficiency, driver performance, and operational trends.

---

## Technologies Used

- Python
- PySpark
- Amazon S3
- AWS Glue
- AWS Glue Crawlers
- AWS Glue Data Catalog
- Amazon Athena
- SQL
- Parquet

---

## Architecture

```text
Raw CSV Files
      │
      ▼
Bronze Layer (S3)
      │
      ▼
AWS Glue + PySpark
      │
      ▼
Silver Layer (Parquet)
      │
      ▼
Glue Catalog + Crawlers
      │
      ▼
Amazon Athena
      │
      ▼
Gold Analytics Tables
```

---

## Bronze Layer

The Bronze layer stores raw delivery data in CSV format without modification.

### Source Data

Food Delivery Logistics historical dataset containing:

- Delivery partner information
- Restaurant locations
- Delivery locations
- Order timestamps
- Traffic conditions
- Weather conditions
- Vehicle information
- Delivery duration

---

## Silver Layer

The Silver layer performs data cleaning and transformation using AWS Glue and PySpark.

### Transformations Performed

- Applied explicit schema
- Removed duplicate records
- Removed records with critical missing values
- Trimmed leading and trailing spaces
- Standardized weather condition values
- Converted delivery duration into numeric format
- Created order timestamp
- Created pickup timestamp
- Converted CSV data to Parquet format
- Partitioned data by year and month

### Benefits

- Improved data quality
- Reduced storage requirements
- Faster query performance
- Optimized Athena costs

---

## Gold Layer

The Gold layer contains business-ready datasets created for analytics and reporting.

### gold_delivery_kpis

Provides overall operational KPIs:

- Total orders
- Average delivery time
- Average driver rating
- Total drivers

### gold_city_performance

Analyzes delivery performance by city.

Metrics:

- Total orders
- Average delivery time
- Average driver rating
- Total drivers

### gold_traffic_impact

Measures the impact of traffic density on delivery efficiency.

Metrics:

- Total orders
- Average delivery time

### gold_weather_impact

Measures the impact of weather conditions on delivery performance.

Metrics:

- Total orders
- Average delivery time

### gold_vehicle_performance

Compares delivery efficiency across vehicle types.

Metrics:

- Total orders
- Average delivery time

### gold_driver_performance

Provides driver-level performance metrics.

Metrics:

- Total orders
- Average delivery time
- Average driver rating

### gold_daily_delivery_trends

Tracks daily operational trends.

Metrics:

- Total orders
- Average delivery time
- Average driver rating

---

## Business Questions Answered

This project helps answer questions such as:

- Which city has the highest delivery volume?
- How does traffic affect delivery time?
- How do weather conditions impact delivery performance?
- Which vehicle type performs most efficiently?
- Which drivers perform best?
- How do delivery metrics change over time?
- What are the overall operational KPIs?

---

##----What I Learned-------------------

Through this project I gained hands-on experience with:

- Building ETL pipelines using AWS Glue and PySpark
- Implementing Bronze, Silver, and Gold architecture
- Data cleaning and transformation techniques
- Working with Parquet files
- Partitioning datasets for performance optimization
- Managing metadata with AWS Glue Catalog
- Querying large datasets using Amazon Athena
- Designing business-focused analytical datasets

---

Srikanth Mallireddy

Aspiring Data Engineer with hands-on experience in AWS, Python, SQL, PySpark, ETL Pipelines, Data Lakes, and Analytics Engineering.
