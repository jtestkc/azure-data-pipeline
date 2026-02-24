# Databricks notebook source
# Databricks Notebook: Bronze Layer - Streaming Ingestion from Event Hubs via Kafka
# Ingests real-time order events from Event Hubs into Delta Lake (Bronze)

from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
import re

# ============================================================================
# CONFIGURATION - Get secrets from Key Vault
# ============================================================================

# Get Event Hubs connection string from Key Vault
EVENTHUB_CONNECTION_STRING = dbutils.secrets.get("kv-secrets", "eventhub-connection-string")
EVENTHUB_NAME = "orders"

# Get Azure Storage credentials
ADLS_ACCOUNT_NAME = dbutils.secrets.get("kv-secrets", "adls-account-name")
ADLS_STORAGE_KEY = dbutils.secrets.get("kv-secrets", "adls-storage-key")

# Configure Spark to access ADLS Gen2
spark.conf.set(f"fs.azure.account.key.{ADLS_ACCOUNT_NAME}.dfs.core.windows.net", ADLS_STORAGE_KEY)

# ============================================================================
# EXTRACT KAFKA CONFIGURATION FROM CONNECTION STRING
# ============================================================================

# Extract namespace FQDN from connection string
# Format: Endpoint=sb://<namespace>.servicebus.windows.net/;...
namespace_match = re.search(r'Endpoint=sb://([^/;\s]+)', EVENTHUB_CONNECTION_STRING)

if not namespace_match:
    raise ValueError("FATAL: Could not parse Event Hubs namespace from connection string")

EVENTHUB_NAMESPACE = namespace_match.group(1)

# Correct the bootstrap server calculation (avoid double domain bug)
if ".servicebus.windows.net" in EVENTHUB_NAMESPACE:
    KAFKA_BOOTSTRAP_SERVERS = f"{EVENTHUB_NAMESPACE}:9093"
else:
    KAFKA_BOOTSTRAP_SERVERS = f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"

# ============================================================================
# ✅ CRITICAL: BUILD KAFKA-CONFIGURATION
# Use the connection string as is (Namespace level)
kafka_password = EVENTHUB_CONNECTION_STRING

# JAAS config must use kafkashaded prefix (NOT the old azure-eventhubs-spark library)
KAFKA_JAAS_CONFIG = (
    f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
    f"required username=\"$ConnectionString\" "
    f"password=\"{kafka_password}\";"
)

# Kafka configuration options
kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": KAFKA_JAAS_CONFIG,
    "subscribe": EVENTHUB_NAME,
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "1000",
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000"
}

print("=" * 70)
print("KAFKA CONFIGURATION")
print("=" * 70)
print(f"Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Topic: {EVENTHUB_NAME}")
print(f"Security: SASL_SSL + PLAIN")
print("=" * 70)

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
# CREATE STREAMING DATAFRAME FROM EVENT HUBS VIA KAFKA
# ============================================================================

print("\n📡 Connecting to Event Hubs via Kafka protocol...")

streaming_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

print(f"✅ Connected! Kafka schema: {streaming_df.schema.json()}")

# Parse JSON payload from Kafka message value
parsed_df = streaming_df.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*")

# Add metadata columns
bronze_df = (
    parsed_df
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
    .withColumn("event_date", col("order_timestamp").cast("date"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

print(f"✅ Parsed! Bronze schema: {bronze_df.schema.json()}")

# ============================================================================
# WRITE TO DELTA LAKE BRONZE LAYER
# ============================================================================

CONTAINER_NAME = "rawdata"
ROOT_PATH = f"abfss://{CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"

checkpoint_path = f"{ROOT_PATH}/_checkpoints/bronze_orders"
output_path = f"{ROOT_PATH}/bronze/orders"

# Widget to reset checkpoint if needed
dbutils.widgets.text("reset_checkpoint", "false", "Reset Checkpoint (true/false)")
if dbutils.widgets.get("reset_checkpoint").lower() == "true":
    print(f"🗑️  RESETTING CHECKPOINT: {checkpoint_path}")
    dbutils.fs.rm(checkpoint_path, True)

# Start streaming query (optimized for batch completion)
query = (
    bronze_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("path", output_path)
    .trigger(availableNow=True)
    .start()
)

print("=" * 70)
print("✅ BRONZE STREAMING PIPELINE STARTED!")
print("=" * 70)
print(f"📍 Output Path: {output_path}")
print(f"📍 Checkpoint: {checkpoint_path}")
print(f"📡 Event Hub: {EVENTHUB_NAMESPACE}/{EVENTHUB_NAME}")
print(f"⏱️  Trigger: Every 2 minutes")
print(f"📊 Partition: event_date")
print("=" * 70)

# Wait for query to terminate (or stop manually)
query.awaitTermination()
