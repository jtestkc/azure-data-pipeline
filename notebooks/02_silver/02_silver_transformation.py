# Databricks notebook source
# Databricks Notebook: Silver Layer - Data Transformation
# Section 3.1: Medallion Architecture - Silver Transformation
# Null handling, deduplication, type casting

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lag, lead, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType
import delta
import re

def sanitize_secret(secret_value):
    if not secret_value: return ""
    return re.sub(r'[\s\x00-\x1F\x7F-\x9F]', '', secret_value)

# ============================================================================
# CONFIGURATION
# ============================================================================
ADLS_ACCOUNT_NAME = sanitize_secret(dbutils.secrets.get("kv-secrets", "adls-account-name"))
CONTAINER_NAME = "rawdata"
ROOT_PATH = f"abfss://{CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"

BRONZE_PATH = f"{ROOT_PATH}/bronze/orders"
SILVER_PATH = f"{ROOT_PATH}/silver/orders"

# ============================================================================
# READ FROM BRONZE LAYER
# ============================================================================
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

print("Bronze layer schema:")
bronze_df.printSchema()

# ============================================================================
# NULL HANDLING
# ============================================================================
silver_df = bronze_df.withColumn(
    "product_name",
    when(col("product_name").isNull(), "Unknown Product").otherwise(col("product_name"))
).withColumn(
    "region",
    when(col("region").isNull(), "Unknown Region").otherwise(col("region"))
).withColumn(
    "quantity",
    when(col("quantity").isNull(), 0).otherwise(col("quantity"))
).withColumn(
    "unit_price",
    when(col("unit_price").isNull(), 0.0).otherwise(col("unit_price"))
).withColumn(
    "total_price",
    when(col("total_price").isNull(), 0.0).otherwise(col("total_price"))
)

# Fill nulls in critical fields with defaults
silver_df = silver_df.fillna({
    "order_id": "UNKNOWN",
    "customer_id": "UNKNOWN",
    "product_id": "UNKNOWN"
})

# ============================================================================
# TYPE CASTING
# ============================================================================
silver_df = silver_df \
    .withColumn("quantity", col("quantity").cast(IntegerType())) \
    .withColumn("unit_price", col("unit_price").cast(DoubleType())) \
    .withColumn("total_price", col("total_price").cast(DoubleType()))

# ============================================================================
# DEDUPLICATION - Remove duplicate orders based on order_id
# ============================================================================
window_spec = Window.partitionBy("order_id").orderBy(col("ingestion_timestamp").desc())
deduped_df = silver_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"Records before deduplication: {bronze_df.count()}")
print(f"Records after deduplication: {deduped_df.count()}")

# ============================================================================
# BUSINESS RULES VALIDATION
# ============================================================================
# Validate price consistency
deduped_df = deduped_df.withColumn(
    "calculated_total",
    col("quantity") * col("unit_price")
).withColumn(
    "price_valid",
    when(col("calculated_total") == col("total_price"), True).otherwise(False)
)

# Flag invalid records
invalid_records = deduped_df.filter(col("price_valid") == False).count()
print(f"Invalid price records: {invalid_records}")

# Use calculated_total where there's a mismatch
final_df = deduped_df.withColumn(
    "total_price",
    when(col("price_valid") == False, col("calculated_total")).otherwise(col("total_price"))
).drop("calculated_total", "price_valid")

# ============================================================================
# WRITE TO SILVER LAYER - DELTA TABLE WITH MERGE
# ============================================================================
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    delta_table = DeltaTable.forPath(spark, SILVER_PATH)
    
    delta_table.alias("silver").merge(
        final_df.alias("bronze"),
        "bronze.order_id = silver.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print(f"Merged {final_df.count()} records into Silver layer")
else:
    final_df.write.format("delta") \
        .mode("overwrite") \
        .save(SILVER_PATH)
    
    print(f"Created Silver Delta Log with {final_df.count()} records")

# ============================================================================
# VERIFY SILVER LAYER
# ============================================================================
silver_count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"Total records in Silver layer: {silver_count}")

# Show sample data
display(spark.read.format("delta").load(SILVER_PATH).limit(10))
