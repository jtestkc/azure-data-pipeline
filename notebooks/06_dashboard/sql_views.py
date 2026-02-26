# Databricks notebook source
# SQL Views for Dashboard - Creates Delta views for Databricks SQL

from pyspark.sql import SparkSession

# ============================================================================
# CONFIGURATION
# ============================================================================
ADLS_ACCOUNT_NAME = dbutils.secrets.get("databricks-secrets", "adls-account-name")
CONTAINER_NAME = "rawdata"
ROOT_PATH = f"abfss://{CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
GOLD_BASE_PATH = f"{ROOT_PATH}/gold"

# ============================================================================
# CREATE TEMP VIEWS FOR SQL DASHBOARD
# ============================================================================
print("=" * 70)
print("Creating SQL Views for Dashboard")
print("=" * 70)

# Register Regional Performance as temp view
regional_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/regional_performance")
regional_df.createOrReplaceTempView("sales_regional_performance")
print(f"✅ Created view: sales_regional_performance ({regional_df.count()} rows)")

# Register Daily Revenue as temp view
daily_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/daily_revenue")
daily_df.createOrReplaceTempView("sales_daily_revenue")
print(f"✅ Created view: sales_daily_revenue ({daily_df.count()} rows)")

# Register Top Products as temp view
top_products_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/top_products")
top_products_df.createOrReplaceTempView("sales_top_products")
print(f"✅ Created view: sales_top_products ({top_products_df.count()} rows)")

# Register Customer LTV as temp view
customer_ltv_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/customer_ltv")
customer_ltv_df.createOrReplaceTempView("sales_customer_ltv")
print(f"✅ Created view: sales_customer_ltv ({customer_ltv_df.count()} rows)")

print("\n" + "=" * 70)
print("SQL Views Created Successfully!")
print("=" * 70)
print("\nYou can now create SQL Dashboard queries using:")
print("  SELECT * FROM sales_regional_performance")
print("  SELECT * FROM sales_daily_revenue")
print("  SELECT * FROM sales_top_products")
print("  SELECT * FROM sales_customer_ltv")
print("\nGo to Databricks SQL > New Query > Use the above queries")
