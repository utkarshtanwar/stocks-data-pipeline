# ================================
# STEP 1: Read silver table
# ================================

df = spark.read.table("databricks_stocks.default.stocks_silver_layer")


# ================================
# STEP 2: Window setup
# ================================

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, expr, avg, stddev

window_spec = Window.partitionBy("ticker").orderBy("date")


# ================================
# STEP 3: Daily return
# ================================

df = df.withColumn(
    "prev_close",
    lag("close").over(window_spec)
)

df = df.withColumn(
    "daily_return",
    expr("try_divide(close - prev_close, prev_close)")
)


# ================================
# STEP 4: Moving average (7)
# ================================

window_ma = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)

df = df.withColumn(
    "ma_7",
    avg("close").over(window_ma)
)


# ================================
# STEP 5: Volatility (7)
# ================================

window_vol = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)

df = df.withColumn(
    "volatility_7",
    stddev("daily_return").over(window_vol)
)


# ================================
# STEP 6: Final columns
# ================================

df_gold = df.select(
    "ticker",
    "date",
    "close",
    "daily_return",
    "ma_7",
    "volatility_7",
    "security_name",
    "listing_exchange"
)


# ================================
# STEP 7: Save gold table
# ================================

df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("databricks_stocks.default.stocks_gold_layer")