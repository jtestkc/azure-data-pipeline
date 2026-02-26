# Databricks notebook source
# Databricks Notebook: JDBC Enrichment - Azure SQL Database Join
# Section 4: Batch Join & Enrichment
# JDBC connection to Azure SQL Database with broadcast join

from pyspark.sql.functions import broadcast, col, concat, lit
from pyspark.sql import SparkSession
import re

def sanitize_secret(secret_value):
    return secret_value.strip() if secret_value else ""

# ============================================================================
# CONFIGURATION
# ============================================================================
ADLS_ACCOUNT_NAME = sanitize_secret(dbutils.secrets.get("kv-secrets", "adls-account-name"))
ADLS_STORAGE_KEY = sanitize_secret(dbutils.secrets.get("kv-secrets", "adls-storage-key"))

# Configure Spark to access ADLS Gen2
spark.conf.set(f"fs.azure.account.key.{ADLS_ACCOUNT_NAME}.dfs.core.windows.net", ADLS_STORAGE_KEY)

CONTAINER_NAME = "rawdata"
ROOT_PATH = f"abfss://{CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"

SILVER_PATH = f"{ROOT_PATH}/silver/orders"
ENRICHED_ORDERS_PATH = f"{ROOT_PATH}/enriched/orders"
ENRICHMENT_METRICS_PATH = f"{ROOT_PATH}/enriched/enrichment_metrics"

# Widgets to reset state if needed
dbutils.widgets.text("reset_data", "false", "Reset Data (true/false)")

if dbutils.widgets.get("reset_data").lower() == "true":
    print(f"🗑️  RESETTING ENRICHED LAYER: {ENRICHED_ORDERS_PATH}")
    dbutils.fs.rm(ENRICHED_ORDERS_PATH, True)
    dbutils.fs.rm(ENRICHMENT_METRICS_PATH, True)

JDBC_URL = sanitize_secret(dbutils.secrets.get("kv-secrets", "sql-jdbc-url"))
JDBC_USER = sanitize_secret(dbutils.secrets.get("kv-secrets", "sql-username"))
JDBC_PASSWORD = sanitize_secret(dbutils.secrets.get("kv-secrets", "sql-password"))

SQL_TABLE = "customers"

# ============================================================================
# READ FROM SILVER LAYER
# ============================================================================
silver_orders = spark.read.format("delta").load(SILVER_PATH)

print(f"Orders in Silver layer: {silver_orders.count()}")
silver_orders.printSchema()

# ============================================================================
# READ CUSTOMER DIMENSION FROM AZURE SQL DATABASE
# ============================================================================
customer_properties = {
    "url": JDBC_URL,
    "dbtable": SQL_TABLE,
    "user": JDBC_USER,
    "password": JDBC_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "numPartitions": "4",
    "fetchSize": "10000"
}

customers_df = spark.read.format("jdbc").options(**customer_properties).load()

print(f"Customers in Azure SQL: {customers_df.count()}")
customers_df.printSchema()

# ============================================================================
# BROADCAST JOIN - Enrich orders with customer data
# Broadcast the smaller dimension table (customers) to avoid shuffle
# ============================================================================
enriched_orders = silver_orders.join(
    broadcast(customers_df),
    silver_orders.customer_id == customers_df.customer_id,
    "left"
)

# Select relevant columns and rename for clarity
enriched_orders = enriched_orders.select(
    silver_orders.order_id,
    silver_orders.customer_id,
    customers_df.customer_name,
    customers_df.email,
    customers_df.loyalty_tier,
    customers_df.registration_date,
    customers_df.country,
    customers_df.city,
    silver_orders.product_id,
    silver_orders.product_name,
    silver_orders.quantity,
    silver_orders.unit_price,
    silver_orders.total_price,
    silver_orders.region,
    silver_orders.order_timestamp,
    silver_orders.event_date,
    silver_orders.ingestion_timestamp
)

# Fill nulls for customers without matches
enriched_orders = enriched_orders.fillna({
    "customer_name": "Unknown",
    "email": "unknown@unknown.com",
    "loyalty_tier": "Standard",
    "country": "Unknown",
    "city": "Unknown"
})

# ============================================================================
# WRITE TO ENRICHED LAYER
# ============================================================================
enriched_orders.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(ENRICHED_ORDERS_PATH)

print("Enrichment completed")
print(f"Total enriched records: {enriched_orders.count()}")

# ============================================================================
# ANALYZE ENRICHMENT QUALITY
# ============================================================================
matched = enriched_orders.filter(col("customer_name") != "Unknown").count()
unmatched = enriched_orders.filter(col("customer_name") == "Unknown").count()
total = enriched_orders.count()

print(f"\n=== Enrichment Quality ===")
print(f"Total orders: {total}")
if total > 0:
    print(f"Matched with customer: {matched} ({matched/total*100:.1f}%)")
    print(f"Unmatched: {unmatched} ({unmatched/total*100:.1f}%)")
else:
    print("Matched with customer: 0 (0.0%)")
    print("Unmatched: 0 (0.0%)")

# Show sample enriched data
display(enriched_orders.limit(10))

# ============================================================================
# SAVE ENRICHMENT METRICS
# ============================================================================
match_rate = (matched/total*100) if total > 0 else 0.0

enrichment_metrics = spark.createDataFrame([
    ("total_orders", float(total)),
    ("matched_customers", float(matched)),
    ("unmatched_customers", float(unmatched)),
    ("match_rate", float(match_rate))
], ["metric_name", "metric_value"])

enrichment_metrics.write.format("delta") \
    .mode("overwrite") \
    .save(ENRICHMENT_METRICS_PATH)
