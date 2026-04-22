# ================================
# STEP 1: Read data
# ================================
# read all csv files

df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", False) \
    .option("recursiveFileLookup", "true") \
    .load("/Volumes/databricks_stocks/default/stocks/")

# read metadata separately
meta_df = spark.read.csv(
    "/Volumes/databricks_stocks/default/stocks/Databricks/symbols_valid_meta.csv",
    header=True,
    inferSchema=True
)


# ================================
# STEP 2: Add file info
# ================================
# extract file path

from pyspark.sql.functions import col, regexp_extract, expr

df = df.withColumn("file_path", col("_metadata.file_path"))


# ================================
# STEP 3: Extract columns
# ================================
# ticker from file name

df = df.withColumn(
    "ticker",
    regexp_extract(col("file_path"), r"([^/]+)\.csv", 1)
)

# asset type from folder

df = df.withColumn(
    "asset_type",
    regexp_extract(col("file_path"), r"Databricks/(.*?)/", 1)
)


# ================================
# STEP 4: Clean column names
# ================================
# snake case

df = df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

meta_df = meta_df.toDF(*[c.lower().replace(" ", "_") for c in meta_df.columns])


# ================================
# STEP 5: Fix data types
# ================================
# string → numeric

df = (
    df.withColumn("open", expr("try_cast(open as double)"))
      .withColumn("high", expr("try_cast(high as double)"))
      .withColumn("low", expr("try_cast(low as double)"))
      .withColumn("close", expr("try_cast(close as double)"))
      .withColumn("adj_close", expr("try_cast(adj_close as double)"))
      .withColumn("volume", expr("cast(try_cast(volume as double) as bigint)"))
)


# ================================
# STEP 6: Quick checks
# ================================
print("rows:", df.count())

df.select("ticker").distinct().show(10)
df.groupBy("asset_type").count().show()
df.groupBy("ticker").count().orderBy("count", ascending=False).show(10)

df.show(10)


# ================================
# STEP 7: Save bronze tables
# ================================

# save metadata
meta_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("databricks_stocks.default.symbols_meta_bronze_layer")

# save stocks
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("databricks_stocks.default.stocks_bronze_layer")