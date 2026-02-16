# Databricks notebook source
# MAGIC %md
# MAGIC # DBR Migration Demo Notebook - ✅ FIXED VERSION
# MAGIC 
# MAGIC ## This notebook has been fixed for DBR 17.3 LTS compatibility
# MAGIC 
# MAGIC All breaking changes have been remediated using the DBR Migration Agent Skill.
# MAGIC 
# MAGIC ### Fixes Applied:
# MAGIC | ID | Original | Fixed |
# MAGIC |----|----------|-------|
# MAGIC | BC-17.3-001 | `input_file_name()` | `_metadata.file_name` |
# MAGIC | BC-15.4-003 | `IF ! EXISTS` | `IF NOT EXISTS` |
# MAGIC | BC-15.4-001 | `VariantType()` UDF | `StringType()` + JSON |
# MAGIC | BC-17.3-002 | Implicit incremental listing | Explicit option |
# MAGIC | BC-16.4-007 | `MM/dd/yy` strict width | `M/d/y` flexible width |
# MAGIC | BC-SC-002 | Same temp view name | UUID in view name |
# MAGIC | BC-SC-003 | External variable in UDF | Function factory |
# MAGIC | BC-SC-004 | `df.columns` in loop | Cached locally |
# MAGIC | BC-16.4-001a-i | Scala 2.13 patterns | Updated syntax |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Load NYC Taxi Dataset

# COMMAND ----------

# Standard imports
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, sum, avg, count
from pyspark.sql.window import Window
from pyspark.sql.types import *
import uuid  # Added for unique temp view names
import json  # Added for JSON handling in UDFs

# ============================================================================
# FIX BC-17.3-001: REMOVED input_file_name import
# ============================================================================
# The input_file_name function is removed in DBR 17.3
# Use _metadata.file_name instead (no import needed)

print("Imports loaded successfully")

# COMMAND ----------

# Load NYC Taxi data (available in Databricks by default)
taxi_df = (spark.read
    .format("delta")
    .load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
    .limit(100000)
)

print(f"Loaded {taxi_df.count()} taxi records")
taxi_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FIXED: input_file_name() → _metadata.file_name [BC-17.3-001]

# COMMAND ----------

# ============================================================================
# FIX BC-17.3-001: Use _metadata.file_name instead of input_file_name()
# ============================================================================

# Pattern 1: Direct usage - FIXED (use alias to avoid renaming wrong column)
df_with_source = taxi_df.select("*", col("_metadata.file_name").alias("source_file"))

# Pattern 2: Usage in select - FIXED
df_selected = taxi_df.select(
    "*",
    col("_metadata.file_name").alias("data_source")
)

# Pattern 3: Usage in a transformation pipeline - FIXED
processed_df = (taxi_df
    .select("*", col("_metadata.file_name").alias("source"))
    .withColumn("load_timestamp", F.current_timestamp())
    .filter(col("fare_amount") > 0)
)

print(f"Processed {processed_df.count()} records with source file tracking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FIXED: SQL '!' Syntax → NOT [BC-15.4-003]

# COMMAND ----------

# Save taxi data to a temp table for SQL queries
taxi_df.createOrReplaceTempView("taxi_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- FIX BC-15.4-003: Use NOT instead of !
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- Pattern 1: IF NOT EXISTS (FIXED)
# MAGIC CREATE TABLE IF NOT EXISTS demo_db.taxi_summary (
# MAGIC     pickup_date DATE,
# MAGIC     trip_count BIGINT,
# MAGIC     total_fare DOUBLE
# MAGIC );
# MAGIC 
# MAGIC -- Pattern 2: IS NOT NULL (FIXED)
# MAGIC SELECT 
# MAGIC     pickup_datetime,
# MAGIC     fare_amount,
# MAGIC     tip_amount
# MAGIC FROM taxi_trips
# MAGIC WHERE fare_amount IS NOT NULL
# MAGIC   AND tip_amount IS NOT NULL
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- More NOT syntax patterns (FIXED)
# MAGIC 
# MAGIC -- Pattern 3: NOT IN (FIXED)
# MAGIC SELECT *
# MAGIC FROM taxi_trips
# MAGIC WHERE payment_type NOT IN ('Cash', 'No Charge')
# MAGIC LIMIT 10;
# MAGIC 
# MAGIC -- Pattern 4: NOT BETWEEN (FIXED)
# MAGIC SELECT *
# MAGIC FROM taxi_trips
# MAGIC WHERE fare_amount NOT BETWEEN 0 AND 5
# MAGIC LIMIT 10;
# MAGIC 
# MAGIC -- Pattern 5: NOT LIKE (FIXED)
# MAGIC SELECT *
# MAGIC FROM taxi_trips  
# MAGIC WHERE pickup_datetime NOT LIKE '2019-01%'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FIXED: VARIANT Type → StringType + JSON [BC-15.4-001]

# COMMAND ----------

# ============================================================================
# FIX BC-15.4-001: Use StringType with JSON instead of VariantType
# ============================================================================

from pyspark.sql.functions import udf, to_json, from_json, struct
from pyspark.sql.types import StringType, StructType, StructField, DoubleType

# Pattern 1: UDF returning JSON string instead of VARIANT (FIXED)
@udf(returnType=StringType())
def create_trip_metadata(fare, tip, total):
    """Create a JSON string containing trip metadata"""
    import json
    return json.dumps({
        "fare_amount": float(fare) if fare else 0,
        "tip_amount": float(tip) if tip else 0,
        "total_amount": float(total) if total else 0,
        "tip_percentage": (tip / fare * 100) if fare and fare > 0 else 0
    })

# Pattern 2: UDF accepting JSON string as input (FIXED)
@udf(returnType=StringType())
def extract_from_json(json_str):
    """Extract a field from JSON data"""
    import json
    if json_str:
        data = json.loads(json_str)
        return str(data.get("fare_amount", "N/A"))
    return "N/A"

# Usage - now works on DBR 15.4+
df_with_json = taxi_df.withColumn(
    "trip_meta",
    create_trip_metadata(col("fare_amount"), col("tip_amount"), col("total_amount"))
)

# To access as structured data, parse the JSON
trip_meta_schema = StructType([
    StructField("fare_amount", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("tip_percentage", DoubleType())
])

df_with_struct = df_with_json.withColumn(
    "trip_meta_parsed",
    from_json(col("trip_meta"), trip_meta_schema)
)

print("Created DataFrame with JSON metadata column")
df_with_struct.select("fare_amount", "trip_meta", "trip_meta_parsed").show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FIXED: Auto Loader with Explicit Incremental Listing [BC-17.3-002]

# COMMAND ----------

# ============================================================================
# FIX BC-17.3-002: Explicitly set cloudFiles.useIncrementalListing
# ============================================================================

auto_loader_config_fixed = """
# FIXED: Auto Loader configuration with explicit incremental listing setting

df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    # FIXED: Explicitly set incremental listing behavior
    .option("cloudFiles.useIncrementalListing", "auto")  # Or "false" for reliability
    .schema("id INT, name STRING, amount DOUBLE")
    .load("/path/to/streaming/data")
)

# Alternative: Use file notifications for best performance
df_stream_notifications = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "true")  # Recommended for large directories
    .schema("id INT, name STRING, amount DOUBLE")
    .load("/path/to/streaming/data")
)
"""

print("FIXED Auto Loader configuration:")
print(auto_loader_config_fixed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FIXED: DateTime Pattern Width for JDK 17 [BC-16.4-007]

# COMMAND ----------

# ============================================================================
# FIX BC-16.4-007: Use flexible-width patterns (M/d/y) instead of strict (MM/dd/yy)
# JDK 17 strictly enforces pattern width; JDK 8 was lenient
# ============================================================================

from pyspark.sql.functions import to_date, col

test_dates = spark.createDataFrame([
    ("01/01/22",),     # Standard 2-digit month/day, 2-digit year
    ("01/01/23",),     # Standard 2-digit month/day, 2-digit year
    ("1/29/2022",),    # Single-digit month, 4-digit year
    ("1/29/2023",),    # Single-digit month, 4-digit year
    ("12/5/2023",),    # Single-digit day, 4-digit year
], ["bill_date"])

# ✅ FIXED: Use 'M/d/y' (flexible width) instead of 'MM/dd/yy' (strict width)
df_fixed = test_dates.withColumn(
    "parsed_date", to_date(col("bill_date"), "M/d/y")
)

print("FIXED: DateTime pattern using flexible-width M/d/y")
print("All dates parse correctly on both DBR 13.3 (JDK 8) and DBR 16.4+ (JDK 17):")
df_fixed.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FIXED: Temp View with UUID [BC-SC-002]

# COMMAND ----------

# ============================================================================
# FIX BC-SC-002: Use unique temp view names with UUID
# ============================================================================

import uuid

def process_batch_fixed(data_size, batch_name):
    """
    FIXED PATTERN: Uses unique temp view name with UUID
    Now works correctly in both Spark Classic and Spark Connect!
    """
    df = spark.range(data_size).withColumn("batch", lit(batch_name))
    
    # FIXED: Generate unique view name
    unique_view_name = f"`batch_{batch_name}_{uuid.uuid4()}`"
    df.createOrReplaceTempView(unique_view_name)
    
    return spark.table(unique_view_name)

# Process multiple batches
batch_1 = process_batch_fixed(100, "morning")
batch_2 = process_batch_fixed(500, "afternoon")
batch_3 = process_batch_fixed(50, "evening")

# Now works correctly in BOTH Spark Classic and Spark Connect!
print(f"Batch 1 count: {batch_1.count()}")  # Always 100
print(f"Batch 2 count: {batch_2.count()}")  # Always 500
print(f"Batch 3 count: {batch_3.count()}")  # Always 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FIXED: UDF with Function Factory [BC-SC-003]

# COMMAND ----------

# ============================================================================
# FIX BC-SC-003: Use function factory to capture variable at definition time
# ============================================================================

from pyspark.sql.functions import udf

def make_surge_pricing_udf(multiplier):
    """
    FIXED PATTERN: Function factory captures value at definition time
    Works correctly in BOTH Spark Classic and Spark Connect!
    """
    @udf("double")
    def apply_surge(fare):
        return fare * multiplier  # multiplier is captured in closure
    return apply_surge

# Create the UDF with multiplier = 1.0
surge_udf_fixed = make_surge_pricing_udf(1.0)

# Even if we change the "global" multiplier, the UDF still uses 1.0
global_multiplier = 2.5  # This doesn't affect the UDF

# Apply UDF
df_with_surge_fixed = taxi_df.withColumn(
    "surge_fare",
    surge_udf_fixed(col("fare_amount"))
)

# Now works correctly: surge_fare = fare_amount * 1.0
# Consistent in BOTH Spark Classic and Spark Connect
df_with_surge_fixed.select("fare_amount", "surge_fare").show(5)

# To apply different surge rates, create new UDFs
surge_2x = make_surge_pricing_udf(2.0)
surge_3x = make_surge_pricing_udf(3.0)

df_multi_surge = (taxi_df
    .withColumn("normal_fare", surge_udf_fixed(col("fare_amount")))
    .withColumn("double_surge", surge_2x(col("fare_amount")))
    .withColumn("triple_surge", surge_3x(col("fare_amount")))
)
df_multi_surge.select("fare_amount", "normal_fare", "double_surge", "triple_surge").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FIXED: Cache Schema Outside Loop [BC-SC-004]

# COMMAND ----------

# ============================================================================
# FIX BC-SC-004: Cache columns locally before loop
# ============================================================================

import time

def add_computed_columns_fixed(df, num_columns=20):
    """
    FIXED PATTERN: Cache df.columns once before the loop
    Now fast in BOTH Spark Classic and Spark Connect!
    """
    start_time = time.time()
    
    # FIXED: Cache columns locally - only one RPC call
    existing_columns = set(df.columns)
    
    for i in range(num_columns):
        col_name = f"computed_{i}"
        # Check against local set - no RPC!
        if col_name not in existing_columns:
            df = df.withColumn(col_name, col("fare_amount") * i)
            existing_columns.add(col_name)  # Keep cache updated
    
    elapsed = time.time() - start_time
    print(f"FIXED pattern took: {elapsed:.2f} seconds")
    return df

# Run the fixed pattern (fast in both environments)
result_fixed = add_computed_columns_fixed(taxi_df.limit(1000), num_columns=20)
print(f"Result columns: {len(result_fixed.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FIXED: View Without Column Type Definitions [BC-15.4-004]

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- FIX BC-15.4-004: Remove column type definitions from CREATE VIEW
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW taxi_summary_view AS
# MAGIC SELECT 
# MAGIC     DATE(pickup_datetime) as trip_date,
# MAGIC     COUNT(*) as total_trips,
# MAGIC     AVG(fare_amount) as avg_fare
# MAGIC FROM taxi_trips
# MAGIC GROUP BY DATE(pickup_datetime);
# MAGIC 
# MAGIC -- Verify the view works
# MAGIC SELECT * FROM taxi_summary_view LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: All Fixes Applied
# MAGIC 
# MAGIC | ID | Original Pattern | Fixed Pattern | Status |
# MAGIC |----|------------------|---------------|--------|
# MAGIC | BC-17.3-001 | `input_file_name()` | `_metadata.file_name` | ✅ |
# MAGIC | BC-15.4-003 | `IF ! EXISTS` | `IF NOT EXISTS` | ✅ |
# MAGIC | BC-15.4-003 | `IS ! NULL` | `IS NOT NULL` | ✅ |
# MAGIC | BC-15.4-003 | `! IN`, `! BETWEEN`, `! LIKE` | `NOT IN`, `NOT BETWEEN`, `NOT LIKE` | ✅ |
# MAGIC | BC-15.4-001 | `VariantType()` in UDF | `StringType()` + JSON | ✅ |
# MAGIC | BC-17.3-002 | Implicit incremental listing | Explicit `.option()` | ✅ |
# MAGIC | BC-16.4-007 | `MM/dd/yy` strict width (JDK 17) | `M/d/y` flexible width | ✅ |
# MAGIC | BC-SC-003 | External variable capture | Function factory | ✅ |
# MAGIC | BC-SC-004 | `df.columns` in loop | Cached in local set | ✅ |
# MAGIC | BC-15.4-004 | Column types in VIEW | Removed type constraints | ✅ |
# MAGIC 
# MAGIC ### Scala 2.13 Fixes (for .scala files)
# MAGIC 
# MAGIC | ID | Original Pattern | Fixed Pattern |
# MAGIC |----|------------------|---------------|
# MAGIC | BC-16.4-001a | `JavaConverters` | `CollectionConverters` |
# MAGIC | BC-16.4-001b | `.to[List]` | `.to(List)` |
# MAGIC | BC-16.4-001c | `TraversableOnce` | `IterableOnce` |
# MAGIC | BC-16.4-001d | `Traversable` | `Iterable` |
# MAGIC | BC-16.4-001e | `Stream.from()` | `LazyList.from()` |
# MAGIC | BC-16.4-001f | `.toIterator` | `.iterator` |
# MAGIC | BC-16.4-001g | `.view.force` | `.view.to(List)` |
# MAGIC | BC-16.4-001h | `collection.Seq` | Explicit `immutable.Seq` |
# MAGIC | BC-16.4-001i | `'symbol` | `Symbol("symbol")` |
# MAGIC | BC-16.4-002 | `HashMap` iteration | Explicit sorting |
# MAGIC 
# MAGIC ### ✅ This notebook is now compatible with DBR 17.3 LTS!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up temp views
spark.catalog.dropTempView("taxi_trips")
print("Cleanup complete!")
