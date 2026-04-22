# Stock Data Pipeline (Databricks + PySpark)

Built an end-to-end data pipeline to process stock and ETF data using Databricks and PySpark.

---

## Architecture

Bronze → Silver → Gold

---

## Bronze Layer (Ingestion)

- Read raw CSV files from multiple folders (stocks / etfs)
- Extracted ticker and asset type from file path
- Standardized column names (snake_case)
- Converted data types using `try_cast`

---

## Silver Layer (Cleaning + Enrichment)

- Removed invalid rows (missing date or all price fields null)
- Handled missing values (volume)
- Joined stock data with metadata (company details)

---

## Gold Layer (Analytics)

Created time-based metrics using window functions:

- Daily Return  
- 7-day Moving Average  
- 7-day Volatility  

---

## Tech Stack

- Databricks  
- PySpark  
- Delta Lake  

---

## Key Learnings

- Handling multi-file ingestion in Spark  
- Working with metadata and file paths  
- Data cleaning and null handling  
- Using window functions for time-series analysis  
- Building layered data architecture  

---

## Dataset

- Historical stock and ETF data (CSV files)
- Metadata file containing company information

---

## Author

Utkarsh Tanwar