# Databricks notebook source
# Databricks Notebook: Bronze Layer - Batch Ingestion from ADLS
# Reads order data from JSONL files in ADLS Gen2

from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType

# Hardcoded for testing (should use secrets in production)
ADLS_ACCOUNT_NAME = dbutils.secrets.get("databricks-secrets", "adls-account-name")
ADLS_STORAGE_KEY = dbutils.secrets.get("databricks-secrets", "adls-storage-key")

# Configure Spark to access ADLS Gen2
spark.conf.set(f"fs.azure.account.key.{ADLS_ACCOUNT_NAME}.dfs.core.windows.net", ADLS_STORAGE_KEY)

# Enable schema evolution for Delta
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

CONTAINER_NAME = "rawdata"
ROOT_PATH = f"abfss://{CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"

# Check if bronze data already exists
dbutils.widgets.text("skip_bronze", "true", "Skip Bronze if data exists (true/false)")
skip_bronze = dbutils.widgets.get("skip_bronze").lower() == "true"

bronze_path = f"{ROOT_PATH}/bronze/orders"
if skip_bronze:
    try:
        bronze_check = spark.read.format("delta").load(bronze_path)
        count = bronze_check.count()
        if count > 0:
            print(f"Bronze data already exists with {count} records. Skipping ingestion.")
            dbutils.notebook.exit("SKIP_BRONZE_SUCCESS")
    except Exception as e:
        print(f"No existing bronze data found or error checking: {e}")
        print("Proceeding with ingestion...")

# Define schema
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("order_timestamp", StringType(), True),
])

print("Reading orders from JSONL files...")
input_path = f"{ROOT_PATH}/orders_batch_*.jsonl"

bronze_df = (
    spark.read
    .schema(order_schema)
    .json(input_path)
)

# Add metadata columns
bronze_df = (
    bronze_df
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
    .withColumn("event_date", col("order_timestamp").cast("date"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print(f"Bronze schema: {bronze_df.schema.json()}")
print(f"Total records: {bronze_df.count()}")

# Output path
output_path = f"{ROOT_PATH}/bronze/orders"

# Write to Delta Lake
print(f"Writing to Delta Lake: {output_path}")
bronze_df.write.mode("overwrite").format("delta").save(output_path)

print("Bronze layer complete!")
