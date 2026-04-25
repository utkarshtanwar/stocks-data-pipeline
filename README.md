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

## Data Validation

Data quality checks were performed at each layer:

**Bronze Layer**
- Row count validation
- Date range checks (min/max)
- Detection of invalid values

**Silver Layer**
- Removed rows where all OHLC values were null
- Ensured no null dates
- Verified row count difference with Bronze

**Gold Layer**
- Validated min/max date range
- Checked derived columns (daily_return, ma_7, volatility)

---

## SQL Validation

SQL queries were used for validation and sanity checks such as:
- Row counts
- Null value checks
- Date range validation

All queries are available in the `sql/` folder.

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

- Historical stock and ETF data (CSV files) - https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset 
- Metadata file containing company information
- 28 million+ rows stocks dataset

---

## Author

Utkarsh Tanwar
