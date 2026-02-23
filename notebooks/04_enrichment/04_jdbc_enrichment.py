# Databricks Notebook: JDBC Enrichment - Azure SQL Database Join
# Section 4: Batch Join & Enrichment
# JDBC connection to Azure SQL Database with broadcast join

from pyspark.sql.functions import broadcast, col, concat, lit
from pyspark.sql import SparkSession

# ============================================================================
# CONFIGURATION
# ============================================================================
CATALOG_NAME = "sales_catalog"

JDBC_URL = dbutils.secrets.get("kv-secrets", "sql-jdbc-url")
JDBC_USER = dbutils.secrets.get("kv-secrets", "sql-username")
JDBC_PASSWORD = dbutils.secrets.get("kv-secrets", "sql-password")

SQL_TABLE = "customers"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.enriched")

# ============================================================================
# READ FROM SILVER LAYER
# ============================================================================
silver_orders = spark.read.format("delta").table(f"{CATALOG_NAME}.silver.orders")

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
    .saveAsTable(f"{CATALOG_NAME}.enriched.orders")

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
print(f"Matched with customer: {matched} ({matched/total*100:.1f}%)")
print(f"Unmatched: {unmatched} ({unmatched/total*100:.1f}%)")

# Show sample enriched data
display(enriched_orders.limit(10))

# ============================================================================
# SAVE ENRICHMENT METRICS
# ============================================================================
enrichment_metrics = spark.createDataFrame([
    ("total_orders", total),
    ("matched_customers", matched),
    ("unmatched_customers", unmatched),
    ("match_rate", matched/total*100)
], ["metric_name", "metric_value"])

enrichment_metrics.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG_NAME}.enriched.enrichment_metrics")
