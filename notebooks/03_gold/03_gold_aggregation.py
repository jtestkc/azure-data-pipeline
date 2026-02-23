# Databricks Notebook: Gold Layer - Aggregation & Business Metrics
# Section 3.2: Medallion Architecture - Gold Aggregation
# Revenue by region/day, top products, customer lifetime value with window functions

from pyspark.sql.functions import sum, count, avg, max, min, row_number, dense_rank
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# ============================================================================
# CONFIGURATION
# ============================================================================
CATALOG_NAME = "sales_catalog"
SILVER_TABLE = f"{CATALOG_NAME}.silver.orders"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.gold")

# ============================================================================
# READ FROM SILVER LAYER
# ============================================================================
silver_df = spark.read.format("delta").table(SILVER_TABLE)

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
    .saveAsTable(f"{CATALOG_NAME}.gold.daily_revenue")

print("Daily revenue aggregation completed")
display(daily_revenue.limit(20))

# ============================================================================
# AGGREGATION 2: TOP PRODUCTS
# ============================================================================
product_window = Window.partitionBy("event_date").orderBy(sum("total_price").desc())

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
    .saveAsTable(f"{CATALOG_NAME}.gold.top_products")

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
    col("last_order_date") - col("first_order_date")
).withColumn(
    "orders_per_month",
    col("total_orders") / (col("days_as_customer") / 30.0 + 1)
).orderBy(col("lifetime_value").desc())

customer_ltv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG_NAME}.gold.customer_ltv")

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
    .saveAsTable(f"{CATALOG_NAME}.gold.regional_performance")

print("Regional performance aggregation completed")
display(regional_perf)

# ============================================================================
# VERIFY GOLD LAYER
# ============================================================================
print("\n=== Gold Layer Tables ===")
gold_tables = ["daily_revenue", "top_products", "customer_ltv", "regional_performance"]
for table in gold_tables:
    count = spark.read.format("delta").table(f"{CATALOG_NAME}.gold.{table}").count()
    print(f"{table}: {count} records")
