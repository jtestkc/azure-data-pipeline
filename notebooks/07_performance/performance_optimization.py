# Databricks Notebook: Performance Optimization
# Section 7: Performance Optimization
# Shuffle bottleneck analysis, Photon comparison, Auto Loader

from pyspark.sql.functions import col, spark_partition_id
from pyspark.sql import SparkSession

# ============================================================================
# CONFIGURATION
# ============================================================================
CATALOG_NAME = "sales_catalog"

# ============================================================================
# SECTION 7.1: SHUFFLE BOTTLENECK ANALYSIS
# ============================================================================
print("=== SECTION 7.1: SHUFFLE BOTTLENECK ANALYSIS ===\n")

# Check current shuffle configuration
print("Current Shuffle Configuration:")
print(f"spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"spark.default.parallelism: {spark.conf.get('spark.default.parallelism')}")

# Analyze data distribution
silver_df = spark.read.format("delta").table(f"{CATALOG_NAME}.silver.orders")

# Check partition distribution
partition_analysis = silver_df.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id") \
    .count() \
    .orderBy("partition_id")

print("\nPartition Distribution:")
display(partition_analysis)

# Identify skewed partitions
partition_stats = partition_analysis.agg(
    {"count": "avg"}).collect()[0]
avg_count = partition_stats['avg(count)']

skewed_partitions = partition_analysis.filter(
    col("count") > avg_count * 2
)

if skewed_partitions.count() > 0:
    print("\n⚠️ Skewed Partitions Detected:")
    display(skewed_partitions)
    print("\nRecommendations:")
    print("1. Increase spark.sql.shuffle.partitions")
    print("2. Use salting technique for skewed joins")
    print("3. Consider different partitioning column")
else:
    print("\n✓ Partition distribution is balanced")

# Fix: Adjust shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "16")
spark.conf.set("spark.default.parallelism", "16")

print("\nAdjusted Shuffle Configuration:")
print(f"spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"spark.default.parallelism: {spark.conf.get('spark.default.parallelism')}")

# ============================================================================
# SECTION 7.2: PHOTON RUNTIME COMPARISON
# ============================================================================
print("\n=== SECTION 7.2: PHOTON RUNTIME COMPARISON ===\n")

# Check if Photon is enabled
spark_version = spark.version
photon_enabled = "photon" in spark_version.lower() or "photon" in spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "").lower()

print(f"Spark Version: {spark_version}")
print(f"Photon Enabled: {photon_enabled}")

# Run benchmark query
query = """
SELECT region, event_date, 
       sum(total_price) as revenue, 
       count(*) as order_count
FROM sales_catalog.silver.orders
GROUP BY region, event_date
ORDER BY event_date DESC
"""

import time

# Run with current config
start_time = time.time()
result = spark.sql(query)
result.collect()
baseline_time = time.time() - start_time

print(f"\nQuery Execution Time: {baseline_time:.2f}s")

# Photon-specific optimizations
photon_configs = {
    "spark.databricks.photon.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
}

print("\nPhoton Optimizations Applied:")
for key, value in photon_configs.items():
    spark.conf.set(key, value)
    print(f"  {key}: {value}")

# ============================================================================
# SECTION 7.3: AUTO LOADER IMPLEMENTATION
# ============================================================================
print("\n=== SECTION 7.3: AUTO LOADER IMPLEMENTATION ===\n")

# Auto Loader configuration for incremental data loading
auto_loader_config = {
    "format": "cloudFiles",
    "schema": "infer",
    "cloudFiles.format": "json",
    "cloudFiles.schemaLocation": "/mnt/datalake/schemas/bronze_orders",
    "cloudFiles.notificationEnabled": "true",
    "cloudFiles.backfillInterval": "10s"
}

# Create Auto Loader stream
auto_loader_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/datalake/schemas/bronze_orders")
    .option("cloudFiles.includeExistingFiles", "true")
    .load("/mnt/datalake/raw/orders/")
)

print("Auto Loader Configuration:")
for key, value in auto_loader_config.items():
    print(f"  {key}: {value}")

# Write to Delta with Auto Loader
output_path = "/mnt/datalake/bronze/orders_autoloader"
checkpoint_path = "/mnt/datalake/checkpoints/autoloader"

query = (
    auto_loader_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .start(output_path)
)

print(f"\nAuto Loader Stream Started")
print(f"Output Path: {output_path}")
print(f"Checkpoint: {checkpoint_path}")

# ============================================================================
# PERFORMANCE RECOMMENDATIONS SUMMARY
# ============================================================================
print("\n=== PERFORMANCE RECOMMENDATIONS ===\n")

recommendations = [
    {
        "area": "Shuffle Partitions",
        "recommendation": "Set spark.sql.shuffle.partitions based on data size (16-64 for small/medium)",
        "current": spark.conf.get('spark.sql.shuffle.partitions'),
        "suggested": "16-64"
    },
    {
        "area": "Photon Runtime",
        "recommendation": "Enable Photon for 2-3x query performance improvement",
        "current": photon_enabled,
        "suggested": "true"
    },
    {
        "area": "Auto Loader",
        "recommendation": "Use Auto Loader for incremental file ingestion",
        "current": "Disabled",
        "suggested": "Enabled"
    },
    {
        "area": "ZORDER",
        "recommendation": "ZORDER frequently filtered columns (region, event_date)",
        "current": "Not configured",
        "suggested": "Configured"
    },
    {
        "area": "Data Skipping",
        "recommendation": "Ensure partition and ZORDER columns align with query filters",
        "current": "Configured",
        "suggested": "Review queries"
    }
]

rec_df = spark.createDataFrame(recommendations)
display(rec_df)
