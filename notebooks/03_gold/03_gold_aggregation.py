# Databricks notebook source
# Databricks Notebook: Gold Layer - Aggregation & Business Metrics
# Section 3.2: Medallion Architecture - Gold Aggregation
# Revenue by region/day, top products, customer lifetime value with window functions

from pyspark.sql.functions import sum, count, avg, max, min, row_number, dense_rank, col, datediff
from pyspark.sql.types import LongType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import re

def sanitize_secret(secret_value):
    if not secret_value: return ""
    return re.sub(r'[\s\x00-\x1F\x7F-\x9F]', '', secret_value)

# ============================================================================
# CONFIGURATION
# ============================================================================
ADLS_ACCOUNT_NAME = sanitize_secret(dbutils.secrets.get("databricks-secrets", "adls-account-name"))
CONTAINER_NAME = "rawdata"
ROOT_PATH = f"abfss://{CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"

SILVER_PATH = f"{ROOT_PATH}/silver/orders"
GOLD_BASE_PATH = f"{ROOT_PATH}/gold"

DAILY_REVENUE_PATH = f"{GOLD_BASE_PATH}/daily_revenue"
TOP_PRODUCTS_PATH = f"{GOLD_BASE_PATH}/top_products"
CUSTOMER_LTV_PATH = f"{GOLD_BASE_PATH}/customer_ltv"
REGIONAL_PERF_PATH = f"{GOLD_BASE_PATH}/regional_performance"

# Widgets to reset state if needed
dbutils.widgets.text("reset_data", "false", "Reset Data (true/false)")

if dbutils.widgets.get("reset_data").lower() == "true":
    print(f"🗑️  RESETTING GOLD LAYER: {GOLD_BASE_PATH}")
    dbutils.fs.rm(GOLD_BASE_PATH, True)

# ============================================================================
# READ FROM SILVER LAYER
# ============================================================================
silver_df = spark.read.format("delta").load(SILVER_PATH)

print(f"Reading {silver_df.count()} records from Silver layer")
silver_df.printSchema()

# ============================================================================
# AGGREGATION 1: DAILY REVENUE BY REGION
# ============================================================================
daily_revenue = silver_df.groupBy("region", "event_date").agg(
    sum("total_price").alias("revenue"),
    count("order_id").alias("order_count"),
    sum("quantity").alias("total_units_sold"),
    avg("total_price").alias("avg_order_value"),
    max("total_price").alias("max_order_value"),
    min("total_price").alias("min_order_value")
).orderBy("event_date", "region")

daily_revenue.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(DAILY_REVENUE_PATH)

print("Daily revenue aggregation completed")
display(daily_revenue.limit(20))

# ============================================================================
# AGGREGATION 2: TOP PRODUCTS
# ============================================================================
# Fixed window logic: use 'revenue' (aggregated sum) instead of raw 'total_price'
product_window = Window.partitionBy("event_date").orderBy(col("revenue").desc())

top_products = silver_df.groupBy("product_id", "product_name", "event_date").agg(
    sum("total_price").alias("revenue"),
    sum("quantity").alias("units_sold"),
    count("order_id").alias("order_count"),
    avg("unit_price").alias("avg_unit_price")
)

top_products_with_rank = top_products.withColumn(
    "daily_rank", dense_rank().over(product_window)
).filter(col("daily_rank") <= 10)

top_products_with_rank.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(TOP_PRODUCTS_PATH)

print("Top products aggregation completed")
display(top_products_with_rank.limit(20))

# ============================================================================
# AGGREGATION 3: CUSTOMER LIFETIME VALUE (CLV)
# ============================================================================
# Window for 90-day rolling CLV
clv_window = Window.partitionBy("customer_id").orderBy("event_date").rowsBetween(-90, 0)

customer_ltv = silver_df.groupBy("customer_id", "region").agg(
    sum("total_price").alias("lifetime_value"),
    count("order_id").alias("total_orders"),
    sum("quantity").alias("total_units_purchased"),
    avg("total_price").alias("avg_order_value"),
    min("event_date").alias("first_order_date"),
    max("event_date").alias("last_order_date")
).withColumn(
    "days_as_customer",
    datediff(col("last_order_date"), col("first_order_date"))
).withColumn(
    "orders_per_month",
    col("total_orders") / (col("days_as_customer") / 30.0 + 1)
).orderBy(col("lifetime_value").desc())

customer_ltv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(CUSTOMER_LTV_PATH)

print("Customer LTV aggregation completed")
display(customer_ltv.limit(20))

# ============================================================================
# AGGREGATION 4: REGIONAL PERFORMANCE
# ============================================================================
regional_perf = silver_df.groupBy("region").agg(
    sum("total_price").alias("total_revenue"),
    count("order_id").alias("total_orders"),
    count("customer_id").alias("unique_customers"),
    sum("quantity").alias("total_units_sold"),
    avg("total_price").alias("avg_order_value"),
    max("total_price").alias("max_single_order")
).withColumn(
    "avg_orders_per_customer",
    col("total_orders") / col("unique_customers")
).orderBy(col("total_revenue").desc())

regional_perf.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(REGIONAL_PERF_PATH)

print("Regional performance aggregation completed")
display(regional_perf)

# ============================================================================
# VERIFY GOLD LAYER
# ============================================================================
print("\n=== Gold Layer Tables ===")
gold_paths = {
    "daily_revenue": DAILY_REVENUE_PATH,
    "top_products": TOP_PRODUCTS_PATH,
    "customer_ltv": CUSTOMER_LTV_PATH,
    "regional_performance": REGIONAL_PERF_PATH
}
for name, path in gold_paths.items():
    count = spark.read.format("delta").load(path).count()
    print(f"{name}: {count} records")
