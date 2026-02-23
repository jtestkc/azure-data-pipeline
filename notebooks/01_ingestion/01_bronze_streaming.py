# Databricks Notebook: Bronze Layer - Streaming Ingestion from Event Hubs
# Section 2: Data Ingestion (Streaming)
# Ingests real-time order events from Event Hubs into Delta Lake (Bronze)

from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
import json

# ============================================================================
# CONFIGURATION
# ============================================================================
EVENTHUB_CONNECTION_STRING = dbutils.secrets.get("kv-secrets", "eventhub-connection-string")
EVENTHUB_NAME = "orders"
CONSUMER_GROUP = "$Default"

ADLS_ACCOUNT_NAME = dbutils.secrets.get("kv-secrets", "adls-account-name")
CONTAINER_NAME = "rawdata"
MOUNT_POINT = "/mnt/datalake"

CATALOG_NAME = "sales_catalog"
SCHEMA_NAME = "bronze"
TABLE_NAME = "orders"

# ============================================================================
# MOUNT ADLS GEN2
# ============================================================================
def mount_adls():
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": dbutils.secrets.get("kv-secrets", "sp-client-id"),
        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("kv-secrets", "sp-client-secret"),
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get('kv-secrets', 'sp-tenant-id')}/oauth2/token"
    }
    
    dbutils.fs.mount(
        source=f"abfss://{CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/",
        mount_point=MOUNT_POINT,
        extra_configs=configs
    )

try:
    mount_adls()
    print(f"ADLS mounted successfully at {MOUNT_POINT}")
except Exception as e:
    print(f"ADLS already mounted or error: {e}")

# ============================================================================
# DEFINE SCHEMA FOR ORDER EVENTS
# ============================================================================
order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_price", DoubleType(), False),
    StructField("region", StringType(), True),
    StructField("order_timestamp", StringType(), True)
])

# ============================================================================
# CREATE STREAMING DATAFRAME FROM EVENT HUBS
# ============================================================================
eh_conf = {
    "eventhubs.connectionString": EVENTHUB_CONNECTION_STRING,
    "eventhubs.consumerGroup": CONSUMER_GROUP,
    "startingPosition": "earliest"
}

streaming_df = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
)

# Parse JSON payload
parsed_df = streaming_df.withColumn("body", col("body").cast("string")) \
    .withColumn("data", from_json(col("body"), order_schema)) \
    .select("data.*", "enqueuedTime", "sequenceNumber")

# Add derived columns
bronze_df = parsed_df \
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
    .withColumn("event_date", col("order_timestamp").cast("date")) \
    .withColumn("ingestion_timestamp", current_timestamp())

# ============================================================================
# WRITE TO DELTA LAKE (BRONZE LAYER) - 2 MINUTE TRIGGER, PARTITION BY EVENT_DATE
# ============================================================================
checkpoint_path = f"{MOUNT_POINT}/checkpoints/bronze_orders"
output_path = f"{MOUNT_POINT}/bronze/orders"

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="2 minutes")
    .partitionBy("event_date")
    .start(output_path)
)

print(f"Streaming query started. Writing to: {output_path}")
print(f"Checkpoint location: {checkpoint_path}")
print(f"Trigger interval: 2 minutes")
print(f"Partitioning by: event_date")

# Wait for query to start
query.awaitTermination()
