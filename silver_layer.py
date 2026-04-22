# ================================
# STEP 1: Read bronze tables
# ================================

stocks_df = spark.read.table("databricks_stocks.default.stocks_bronze_layer")
meta_df   = spark.read.table("databricks_stocks.default.symbols_meta_bronze_layer")


# ================================
# STEP 2: Clean stock data
# ================================
# remove bad rows

from pyspark.sql.functions import col

df_clean = stocks_df.filter(
    col("date").isNotNull() &
    ~(
        col("open").isNull() &
        col("high").isNull() &
        col("low").isNull() &
        col("close").isNull()
    )
)

# fill safe nulls
df_clean = df_clean.fillna({
    "volume": 0
})


# ================================
# STEP 3: Prepare metadata
# ================================
# clean column names

meta_df = meta_df.toDF(*[c.strip().lower().replace(" ", "_") for c in meta_df.columns])

# rename for join
meta_df = meta_df.withColumnRenamed("symbol", "ticker")


# ================================
# STEP 4: Join data
# ================================
# add company info

df_silver = df_clean.join(
    meta_df,
    on="ticker",
    how="left"
)


# ================================
# STEP 5: Quick check
# ================================

print("rows:", df_silver.count())

df_silver.select("ticker", "security_name", "listing_exchange").show(10)


# ================================
# STEP 6: Save silver table
# ================================

df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("databricks_stocks.default.stocks_silver_layer")