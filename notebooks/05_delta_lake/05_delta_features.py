# Databricks Notebook: Delta Lake Features Demo
# Section 5: Delta Lake Features
# Time travel, OPTIMIZE + ZORDER, Delta constraints

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

# ============================================================================
# CONFIGURATION
# ============================================================================
CATALOG_NAME = "sales_catalog"
TABLE_PATH = f"{CATALOG_NAME}.silver.orders"

# ============================================================================
# SECTION 5.1: TIME TRAVEL
# ============================================================================
print("=== SECTION 5.1: TIME TRAVEL ===\n")

delta_table = DeltaTable.forName(spark, TABLE_PATH)

# Show transaction history
print("Transaction History:")
display(delta_table.history(10))

# Read using timestamp
print("\n--- Time Travel by Timestamp ---")
# Replace with actual timestamp
# df_v1 = spark.read.format("delta").option("timestampAsOf", "2024-01-15T10:00:00").load(TABLE_PATH)

# Read using version number
print("Current version:", delta_table.version())
df_current = spark.read.format("delta").option("versionAsOf", delta_table.version()).load(TABLE_PATH)
print(f"Records at current version: {df_current.count()}")

# ============================================================================
# SECTION 5.2: OPTIMIZE + ZORDER
# ============================================================================
print("\n=== SECTION 5.2: OPTIMIZE + ZORDER ===\n")

# OPTIMIZE: Coalesce small files
spark.sql(f"OPTIMIZE {TABLE_PATH}")

# ZORDER: Index commonly filtered columns for faster queries
# ZORDER by region and event_date (frequently filtered in queries)
spark.sql(f"OPTIMIZE {TABLE_PATH} ZORDER BY (region, event_date)")

# Verify optimization
print("After OPTIMIZE:")
file_stats = spark.sql(f"DESCRIBE DETAIL {TABLE_PATH}").collect()[0]
print(f"  Number of files: {file_stats['numFiles']}")
print(f"  Table size: {file_stats['sizeInBytes'] / 1024 / 1024:.2f} MB")

# ============================================================================
# SECTION 5.3: DELTA CONSTRAINTS
# ============================================================================
print("\n=== SECTION 5.3: DELTA CONSTRAINTS ===\n")

# Add constraint: price must be greater than 0
spark.sql(f"""
    ALTER TABLE {TABLE_PATH} 
    ADD CONSTRAINT positive_price 
    CHECK (unit_price > 0 AND total_price > 0)
""")

# Add constraint: quantity must be positive
spark.sql(f"""
    ALTER TABLE {TABLE_PATH} 
    ADD CONSTRAINT positive_quantity 
    CHECK (quantity > 0)
""")

# Verify constraints
print("Table Constraints:")
constraints = spark.sql(f"DESCRIBE EXTENDED {TABLE_PATH}")
constraints.filter(col("col_name") == "DetailedTableOutput").show(truncate=False)

# Try inserting invalid data (will fail)
print("\n--- Testing Constraint ---")
try:
    invalid_df = spark.createDataFrame([(
        "TEST-001", "C001", "P001", "Test Product", 
        -1, 10.0, -10.0, "US", "2024-01-15 10:00:00", "2024-01-15", "2024-01-15 10:00:00"
    )], 
        ["order_id", "customer_id", "product_id", "product_name", 
         "quantity", "unit_price", "total_price", "region", 
         "order_timestamp", "event_date", "ingestion_timestamp"])
    
    invalid_df.write.format("delta").mode("append").saveAsTable(TABLE_PATH)
    print("ERROR: Invalid data was inserted!")
except Exception as e:
    print(f"Correctly rejected invalid data: {str(e)[:100]}")

# List all constraints
print("\nTable Constraints:")
constraints_df = spark.sql(f"""
    SELECT constraint_name, constraint_type, details 
    FROM delta_table_constraints('{TABLE_PATH}')
""")
display(constraints_df)

# ============================================================================
# BONUS: VACUUM (remove old files)
# ============================================================================
print("\n=== BONUS: VACUUM ===\n")

# Keep last 7 days of data for time travel
spark.sql(f"VACUUM {TABLE_PATH} RETAIN 168 HOURS")

print("Vacuum completed - removed old files not needed for time travel")
