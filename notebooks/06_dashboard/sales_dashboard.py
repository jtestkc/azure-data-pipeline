# Databricks notebook source
# Databricks Dashboard - Sales Analytics Visualization
# Displays key metrics from Gold layer

from pyspark.sql.functions import sum, count, avg, col
import matplotlib.pyplot as plt

# ============================================================================
# CONFIGURATION
# ============================================================================
ADLS_ACCOUNT_NAME = dbutils.secrets.get("kv-secrets", "adls-account-name")
CONTAINER_NAME = "rawdata"
ROOT_PATH = f"abfss://{CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"

GOLD_BASE_PATH = f"{ROOT_PATH}/gold"

# ============================================================================
# READ GOLD LAYER DATA
# ============================================================================
print("=" * 70)
print("📊 LOADING GOLD LAYER DATA FOR DASHBOARD")
print("=" * 70)

# Daily Revenue
daily_revenue_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/daily_revenue")
print(f"\n✅ Daily Revenue: {daily_revenue_df.count()} records")

# Regional Performance
regional_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/regional_performance")
print(f"✅ Regional Performance: {regional_df.count()} records")

# Top Products
top_products_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/top_products")
print(f"✅ Top Products: {top_products_df.count()} records")

# Customer LTV
customer_ltv_df = spark.read.format("delta").load(f"{GOLD_BASE_PATH}/customer_ltv")
print(f"✅ Customer LTV: {customer_ltv_df.count()} records")

# ============================================================================
# DASHBOARD VISUALIZATIONS
# ============================================================================
print("\n" + "=" * 70)
print("📊 KEY METRICS DASHBOARD")
print("=" * 70)

# Total Revenue
total_revenue = regional_df.agg(sum("total_revenue")).collect()[0][0]
total_orders = regional_df.agg(sum("total_orders")).collect()[0][0]
unique_customers = regional_df.agg(sum("unique_customers")).collect()[0][0]

print(f"\n💰 TOTAL REVENUE: ${total_revenue:,.2f}")
print(f"📦 TOTAL ORDERS: {total_orders:,}")
print(f"👥 UNIQUE CUSTOMERS: {unique_customers:,}")

# Display Regional Performance Table
print("\n" + "=" * 70)
print("📍 REGIONAL PERFORMANCE")
print("=" * 70)
display(regional_df)

# Display Daily Revenue
print("\n" + "=" * 70)
print("📅 DAILY REVENUE TREND")
print("=" * 70)
display(daily_revenue_df.orderBy("event_date", "region"))

# Display Top Products
print("\n" + "=" * 70)
print("🏆 TOP PRODUCTS BY REVENUE")
print("=" * 70)
display(top_products_df.orderBy("revenue", ascending=False).limit(20))

# Display Top Customers
print("\n" + "=" * 70)
print("⭐ TOP CUSTOMERS BY LIFETIME VALUE")
print("=" * 70)
display(customer_ltv_df.orderBy("lifetime_value", ascending=False).limit(20))

# ============================================================================
# PIE CHART: Revenue by Region
# ============================================================================
print("\n" + "=" * 70)
print("🥧 REVENUE BY REGION (Pie Chart)")
print("=" * 70)

# Convert to Pandas for matplotlib
regional_pd = regional_df.toPandas()

fig, ax = plt.subplots(figsize=(10, 6))
ax.pie(regional_pd['total_revenue'], labels=regional_pd['region'], autopct='%1.1f%%', startangle=90)
ax.set_title('Revenue by Region')
plt.tight_layout()
display(fig)

# ============================================================================
# BAR CHART: Top 10 Products
# ============================================================================
print("\n" + "=" * 70)
print("📊 TOP 10 PRODUCTS (Bar Chart)")
print("=" * 70)

top10_products = top_products_df.orderBy("revenue", ascending=False).limit(10).toPandas()

fig, ax = plt.subplots(figsize=(12, 6))
ax.barh(top10_products['product_name'], top10_products['revenue'])
ax.set_xlabel('Revenue ($)')
ax.set_title('Top 10 Products by Revenue')
ax.invert_yaxis()
plt.tight_layout()
display(fig)

# ============================================================================
# LINE CHART: Daily Revenue Trend
# ============================================================================
print("\n" + "=" * 70)
print("📈 DAILY REVENUE TREND (Line Chart)")
print("=" * 70)

daily_pd = daily_revenue_df.groupBy("event_date").agg(sum("revenue").alias("total_revenue")).orderBy("event_date").toPandas()

fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(daily_pd['event_date'], daily_pd['total_revenue'], marker='o')
ax.set_xlabel('Date')
ax.set_ylabel('Revenue ($)')
ax.set_title('Daily Revenue Trend')
plt.xticks(rotation=45)
plt.tight_layout()
display(fig)

print("\n" + "=" * 70)
print("✅ DASHBOARD COMPLETE!")
print("=" * 70)
