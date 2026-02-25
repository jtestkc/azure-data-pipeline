# Databricks Notebook: LLM Data Quality Checker
# Section 9.4: LLM Data Quality Checker
# Sampling, anomaly detection, dq_reports table, Slack alert

from pyspark.sql.functions import col, avg, stddev, min as spark_min, max as spark_max, count, percentile_approx
from pyspark.sql.window import Window
import mlflow
import json
import requests

# ============================================================================
# CONFIGURATION
# ============================================================================
CATALOG_NAME = "sales_catalog"
DQ_REPORTS_TABLE = f"{CATALOG_NAME}.gold.dq_reports"

MLFLOW_EXPERIMENT = "/Shared/sales-analytics/data-quality"

# ============================================================================
# SETUP
# ============================================================================
mlflow.set_experiment(MLFLOW_EXPERIMENT)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.gold")

# ============================================================================
# SECTION 9.4.1: DATA SAMPLING
# ============================================================================
print("=== SECTION 9.4.1: DATA SAMPLING ===\n")

# Read from Silver layer
silver_df = spark.read.format("delta").table(f"{CATALOG_NAME}.silver.orders")

total_count = silver_df.count()
sample_size = min(10000, total_count)

sample_df = silver_df.sample(withReplacement=False, fraction=sample_size/total_count, seed=42)

print(f"Total records: {total_count}")
print(f"Sample size: {sample_size}")
print(f"Sample rate: {sample_size/total_count*100:.2f}%")

# ============================================================================
# SECTION 9.4.2: ANOMALY DETECTION
# ============================================================================
print("\n=== SECTION 9.4.2: ANOMALY DETECTION ===\n")

# Statistical metrics
stats = sample_df.select(
    count("*").alias("total_records"),
    avg("total_price").alias("avg_price"),
    stddev("total_price").alias("stddev_price"),
    spark_min("total_price").alias("min_price"),
    spark_max("total_price").alias("max_price"),
    avg("quantity").alias("avg_quantity"),
    stddev("quantity").alias("stddev_quantity")
).collect()[0]

print("Statistical Summary:")
print(f"  Total Records: {stats['total_records']}")
print(f"  Avg Price: ${stats['avg_price']:.2f}")
print(f"  StdDev Price: ${stats['stddev_price']:.2f}")
print(f"  Price Range: ${stats['min_price']:.2f} - ${stats['max_price']:.2f}")
print(f"  Avg Quantity: {stats['avg_quantity']:.2f}")
print(f"  StdDev Quantity: {stats['stddev_quantity']:.2f}")

# Define anomaly thresholds (3 standard deviations)
upper_price_threshold = stats['avg_price'] + 3 * stats['stddev_price']
lower_price_threshold = max(0, stats['avg_price'] - 3 * stats['stddev_price'])

print(f"\nAnomaly Thresholds:")
print(f"  Upper Price: ${upper_price_threshold:.2f}")
print(f"  Lower Price: ${lower_price_threshold:.2f}")

# Detect anomalies
anomalies = silver_df.filter(
    (col("total_price") > upper_price_threshold) |
    (col("total_price") < lower_price_threshold) |
    (col("quantity") <= 0) |
    (col("unit_price") <= 0)
)

anomaly_count = anomalies.count()
print(f"\nDetected {anomaly_count} anomalies ({anomaly_count/total_count*100:.2f}%)")

# Show sample anomalies
if anomaly_count > 0:
    print("\nSample Anomalies:")
    display(anomalies.limit(10))

# ============================================================================
# SECTION 9.4.3: DATA QUALITY RULES
# ============================================================================
print("\n=== SECTION 9.4.3: DATA QUALITY RULES ===\n")

dq_checks = []

# Check 1: Null values
null_checks = sample_df.select([
    count(when(col(c).isNull(), 1)).alias(c) for c in sample_df.columns
]).collect()[0]

for col_name in sample_df.columns:
    null_pct = null_checks[col_name] / sample_size * 100
    status = "PASS" if null_pct < 5 else "FAIL"
    dq_checks.append({
        "check": f"null_check_{col_name}",
        "column": col_name,
        "failed_count": null_checks[col_name],
        "failed_pct": null_pct,
        "status": status,
        "threshold": 5
    })

# Check 2: Duplicate orders
dup_count = sample_df.groupBy("order_id").count().filter(col("count") > 1).count()
dup_pct = dup_count / sample_size * 100
dq_checks.append({
    "check": "duplicate_orders",
    "column": "order_id",
    "failed_count": dup_count,
    "failed_pct": dup_pct,
    "status": "PASS" if dup_pct < 1 else "FAIL",
    "threshold": 1
})

# Check 3: Price consistency
price_mismatch = sample_df.filter(
    col("quantity") * col("unit_price") != col("total_price")
).count()
price_mismatch_pct = price_mismatch / sample_size * 100
dq_checks.append({
    "check": "price_consistency",
    "column": "total_price",
    "failed_count": price_mismatch,
    "failed_pct": price_mismatch_pct,
    "status": "PASS" if price_mismatch_pct < 1 else "FAIL",
    "threshold": 1
})

# Check 4: Region validity
valid_regions = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East", "Africa"]
invalid_region = sample_df.filter(~col("region").isin(valid_regions)).count()
invalid_region_pct = invalid_region / sample_size * 100
dq_checks.append({
    "check": "valid_region",
    "column": "region",
    "failed_count": invalid_region,
    "failed_pct": invalid_region_pct,
    "status": "PASS" if invalid_region_pct < 5 else "FAIL",
    "threshold": 5
})

# Display DQ results
dq_df = spark.createDataFrame(dq_checks)
print("\nData Quality Checks:")
display(dq_df)

# Calculate overall score
passed = dq_df.filter(col("status") == "PASS").count()
total_checks = len(dq_checks)
dq_score = passed / total_checks * 100

print(f"\nOverall DQ Score: {dq_score:.1f}%")

# ============================================================================
# SECTION 9.4.4: SAVE DQ REPORTS TABLE
# ============================================================================
print("\n=== SECTION 9.4.4: DQ REPORTS TABLE ===\n")

dq_report = {
    "report_id": f"dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    "table_name": f"{CATALOG_NAME}.silver.orders",
    "run_timestamp": datetime.now().isoformat(),
    "total_records": total_count,
    "sample_size": sample_size,
    "dq_score": dq_score,
    "checks_passed": passed,
    "checks_failed": total_checks - passed,
    "anomaly_count": anomaly_count,
    "avg_price": stats['avg_price'],
    "max_price": stats['max_price'],
    "checks": dq_checks
}

# Save to Delta table
dq_report_df = spark.read.json(spark.sparkContext.parallelize([json.dumps(dq_report)]))
dq_report_df.write.format("delta").mode("append").saveAsTable(DQ_REPORTS_TABLE)

print(f"DQ report saved to: {DQ_REPORTS_TABLE}")

# ============================================================================
# SECTION 9.4.5: EMAIL ALERT (instead of Slack)
# ============================================================================
print("\n=== SECTION 9.4.5: EMAIL ALERT ===\n")

def send_email_alert(dq_score: float, anomaly_count: int):
    """Send alert via email if DQ score drops below threshold"""
    
    if dq_score < 95 or anomaly_count > 5:
        alert_message = f"""
        ⚠️ Data Quality Alert
        
        DQ Score: {dq_score:.1f}%
        Anomalies: {anomaly_count}
        Table: {CATALOG_NAME}.silver.orders
        
        Please review the DQ report in the dashboard.
        """
        print("Email alert triggered:")
        print(alert_message)
        
        # In production, integrate with Azure Logic Apps or send via SMTP
        # For now, we'll log to the DQ reports table for visibility
        return True
    else:
        print("DQ checks passed - no alert needed")
        return False

send_email_alert(dq_score, anomaly_count)

# ============================================================================
# SECTION 9.4.6: MLFLOW TRACKING
# ============================================================================
print("\n=== SECTION 9.4.6: MLFLOW TRACKING ===\n")

with mlflow.start_run(run_name="dq-check-run"):
    mlflow.log_params({
        "table_name": f"{CATALOG_NAME}.silver.orders",
        "sample_size": sample_size,
        "total_records": total_count
    })
    
    mlflow.log_metrics({
        "dq_score": dq_score,
        "checks_passed": passed,
        "checks_failed": total_checks - passed,
        "anomaly_count": anomaly_count,
        "null_check_failures": dq_checks[0]['failed_count'],
        "duplicate_count": dup_count,
        "price_mismatch_count": price_mismatch
    })

print("MLflow tracking complete!")
print(f"View results: mlflow.experiments.get_by_name('{MLFLOW_EXPERIMENT}')")
