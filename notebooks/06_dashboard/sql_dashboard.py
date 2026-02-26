# Databricks notebook source
# Sales Analytics Dashboard - Display Key Metrics

from pyspark.sql.functions import sum, count, col

# ============================================================================
# CONFIGURATION
# ============================================================================
ADLS_ACCOUNT_NAME = dbutils.secrets.get("databricks-secrets", "adls-account-name")
CONTAINER_NAME = "rawdata"
ROOT_PATH = f"abfss://{CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
GOLD_BASE_PATH = f"{ROOT_PATH}/gold"

# ============================================================================
# DISPLAY DASHBOARD
# ============================================================================
print("=" * 80)
print("                    SALES ANALYTICS DASHBOARD")
print("=" * 80)

# Read Gold layer tables
regional_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/regional_performance")
daily_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/daily_revenue")
top_products_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/top_products")
customer_ltv_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/customer_ltv")

# Calculate KPIs
total_revenue = regional_df.agg(sum("total_revenue")).collect()[0][0]
total_orders = regional_df.agg(sum("total_orders")).collect()[0][0]
total_customers = regional_df.agg(sum("unique_customers")).collect()[0][0]

print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                              KEY METRICS                                   ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  💰 TOTAL REVENUE:     ${total_revenue:>15,.2f}                                     ║
║  📦 TOTAL ORDERS:      {total_orders:>15,}                                     ║
║  👥 UNIQUE CUSTOMERS:   {total_customers:>15,}                                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

# Display Regional Performance
print("\n" + "=" * 80)
print("📍 REGIONAL PERFORMANCE")
print("=" * 80)
display(regional_df.orderBy(col("total_revenue").desc()))

# Display Top Products
print("\n" + "=" * 80)
print("🏆 TOP 10 PRODUCTS BY REVENUE")
print("=" * 80)
display(top_products_df.orderBy(col("revenue").desc()).limit(10))

# Display Daily Revenue Trend
print("\n" + "=" * 80)
print("📈 DAILY REVENUE TREND")
print("=" * 80)
daily_grouped = daily_df.groupBy("event_date").agg(sum("revenue").alias("revenue")).orderBy("event_date")
display(daily_grouped)

# Display Customer LTV
print("\n" + "=" * 80)
print("⭐ TOP 10 CUSTOMERS BY LIFETIME VALUE")
print("=" * 80)
display(customer_ltv_df.orderBy(col("lifetime_value").desc()).limit(10))

print("\n" + "=" * 80)
print("✅ Dashboard Complete!")
print("=" * 80)
