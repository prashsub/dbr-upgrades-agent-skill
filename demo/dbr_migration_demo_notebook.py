# Databricks notebook source
# MAGIC %md
# MAGIC # DBR Migration Demo Notebook
# MAGIC 
# MAGIC ## ⚠️ WARNING: This notebook contains INTENTIONAL breaking changes!
# MAGIC 
# MAGIC This notebook demonstrates code patterns that will break or behave unexpectedly when upgrading from DBR 13.3 LTS to DBR 17.3 LTS.
# MAGIC 
# MAGIC **Purpose:** Use this notebook to demo the DBR Migration Agent Skill's ability to:
# MAGIC 1. **SCAN** - Find all breaking change patterns
# MAGIC 2. **FIX** - Apply automatic remediations
# MAGIC 3. **VALIDATE** - Verify fixes are correct
# MAGIC 
# MAGIC ### Breaking Changes Included:
# MAGIC | ID | Severity | Description |
# MAGIC |----|----------|-------------|
# MAGIC | BC-17.3-001 | HIGH | `input_file_name()` removed |
# MAGIC | BC-17.3-002 | HIGH | Auto Loader incremental listing default |
# MAGIC | BC-15.4-001 | HIGH | VARIANT in Python UDF |
# MAGIC | BC-15.4-003 | MEDIUM | '!' syntax for NOT |
# MAGIC | BC-SC-002 | HIGH | Temp view name overwriting (Spark Connect) |
# MAGIC | BC-SC-003 | HIGH | UDF late binding (Spark Connect) |
# MAGIC | BC-SC-004 | MEDIUM | Schema access in loops (Spark Connect) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Load NYC Taxi Dataset

# COMMAND ----------

# Standard imports
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, sum, avg, count
from pyspark.sql.window import Window
from pyspark.sql.types import *

# ============================================================================
# BC-17.3-001: BREAKING CHANGE - input_file_name() is REMOVED in DBR 17.3
# ============================================================================
# This import will fail on DBR 17.3!
from pyspark.sql.functions import input_file_name

print("Imports loaded successfully")

# COMMAND ----------

# Load NYC Taxi data (available in Databricks by default)
# Using the samples dataset

taxi_df = (spark.read
    .format("delta")
    .load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
    .limit(100000)  # Limit for demo purposes
)

print(f"Loaded {taxi_df.count()} taxi records")
taxi_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Breaking Change 1: input_file_name() [BC-17.3-001]
# MAGIC 
# MAGIC **Issue:** The `input_file_name()` function was deprecated in DBR 13.3 and **completely removed** in DBR 17.3.
# MAGIC 
# MAGIC **Error on DBR 17.3:** `AnalysisException: Undefined function: input_file_name`

# COMMAND ----------

# ============================================================================
# BC-17.3-001: BREAKING - input_file_name() usage
# ============================================================================
# This code will FAIL on DBR 17.3!

# Pattern 1: Direct usage in withColumn
df_with_source = taxi_df.withColumn("source_file", input_file_name())

# Pattern 2: Usage in select
df_selected = taxi_df.select(
    "*",
    input_file_name().alias("data_source")
)

# Pattern 3: Usage in a transformation pipeline
processed_df = (taxi_df
    .withColumn("source", input_file_name())
    .withColumn("load_timestamp", F.current_timestamp())
    .filter(col("fare_amount") > 0)
)

print(f"Processed {processed_df.count()} records with source file tracking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Breaking Change 2: SQL '!' Syntax [BC-15.4-003]
# MAGIC 
# MAGIC **Issue:** Using `!` instead of `NOT` outside boolean expressions is **disallowed** in DBR 15.4+.
# MAGIC 
# MAGIC **Error:** `ParseException: Syntax error at or near '!'`

# COMMAND ----------

# ============================================================================
# BC-15.4-003: BREAKING - '!' syntax instead of NOT
# ============================================================================
# These SQL statements will FAIL on DBR 15.4+!

# Save taxi data to a temp table for SQL queries
taxi_df.createOrReplaceTempView("taxi_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- BC-15.4-003: BREAKING - '!' syntax in SQL
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- Pattern 1: IF ! EXISTS (WILL FAIL on DBR 15.4+)
# MAGIC CREATE TABLE IF ! EXISTS demo_db.taxi_summary (
# MAGIC     pickup_date DATE,
# MAGIC     trip_count BIGINT,
# MAGIC     total_fare DOUBLE
# MAGIC );
# MAGIC 
# MAGIC -- Pattern 2: IS ! NULL (WILL FAIL on DBR 15.4+)
# MAGIC SELECT 
# MAGIC     pickup_datetime,
# MAGIC     fare_amount,
# MAGIC     tip_amount
# MAGIC FROM taxi_trips
# MAGIC WHERE fare_amount IS ! NULL
# MAGIC   AND tip_amount IS ! NULL
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- More '!' syntax patterns that will break
# MAGIC 
# MAGIC -- Pattern 3: ! IN (WILL FAIL on DBR 15.4+)
# MAGIC SELECT *
# MAGIC FROM taxi_trips
# MAGIC WHERE payment_type ! IN ('Cash', 'No Charge')
# MAGIC LIMIT 10;
# MAGIC 
# MAGIC -- Pattern 4: ! BETWEEN (WILL FAIL on DBR 15.4+)
# MAGIC SELECT *
# MAGIC FROM taxi_trips
# MAGIC WHERE fare_amount ! BETWEEN 0 AND 5
# MAGIC LIMIT 10;
# MAGIC 
# MAGIC -- Pattern 5: ! LIKE (WILL FAIL on DBR 15.4+)
# MAGIC SELECT *
# MAGIC FROM taxi_trips  
# MAGIC WHERE pickup_datetime ! LIKE '2019-01%'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Breaking Change 3: VARIANT Type in Python UDF [BC-15.4-001]
# MAGIC 
# MAGIC **Issue:** Python UDFs using `VARIANT` type as input or output **throw an exception** in DBR 15.4+.
# MAGIC 
# MAGIC **Error:** `AnalysisException: VARIANT type is not supported in Python UDFs`

# COMMAND ----------

# ============================================================================
# BC-15.4-001: BREAKING - VARIANT type in Python UDF
# ============================================================================
# This code will FAIL on DBR 15.4+!

from pyspark.sql.functions import udf
from pyspark.sql.types import VariantType, StringType, StructType, StructField

# Pattern 1: UDF returning VARIANT (WILL FAIL on DBR 15.4+)
@udf(returnType=VariantType())
def create_trip_metadata(fare, tip, total):
    """Create a VARIANT containing trip metadata"""
    return {
        "fare_amount": fare,
        "tip_amount": tip,
        "total_amount": total,
        "tip_percentage": (tip / fare * 100) if fare > 0 else 0
    }

# Pattern 2: UDF accepting VARIANT as input (WILL FAIL on DBR 15.4+)
@udf(returnType=StringType())
def extract_from_variant(variant_data):
    """Extract a field from VARIANT data"""
    if variant_data:
        return str(variant_data.get("fare_amount", "N/A"))
    return "N/A"

# Usage that will fail
df_with_variant = taxi_df.withColumn(
    "trip_meta",
    create_trip_metadata(col("fare_amount"), col("tip_amount"), col("total_amount"))
)

print("Created DataFrame with VARIANT column")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Breaking Change 4: Auto Loader Incremental Listing [BC-17.3-002]
# MAGIC 
# MAGIC **Issue:** Default for `cloudFiles.useIncrementalListing` changed from `auto` to `false` in DBR 17.3.
# MAGIC 
# MAGIC **Impact:** Jobs may be slower (full directory listing) but more reliable.

# COMMAND ----------

# ============================================================================
# BC-17.3-002: BEHAVIORAL CHANGE - Auto Loader incremental listing
# ============================================================================
# This code works but may behave differently on DBR 17.3

# Simulated Auto Loader setup (won't actually run without streaming source)
# The issue is that the old default was "auto" and new default is "false"

auto_loader_config = """
# This Auto Loader configuration relies on the OLD default behavior
# On DBR 17.3, incremental listing is disabled by default

df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    # Missing: .option("cloudFiles.useIncrementalListing", "auto")
    # In DBR 13.3: defaults to "auto" (incremental listing)
    # In DBR 17.3: defaults to "false" (full listing - slower but reliable)
    .schema("id INT, name STRING, amount DOUBLE")
    .load("/path/to/streaming/data")
)
"""

print("Auto Loader configuration (simulated):")
print(auto_loader_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Connect Anti-Pattern 1: Temp View Overwriting [BC-SC-002]
# MAGIC 
# MAGIC **Issue:** In Spark Connect (Serverless, DBR 14.3+), DataFrames store references to temp views by name. If the view is replaced, all DataFrames see the new data.
# MAGIC 
# MAGIC **Impact:** Unexpected data changes in DataFrames.

# COMMAND ----------

# ============================================================================
# BC-SC-002: SPARK CONNECT ISSUE - Temp view name reuse
# ============================================================================
# This code has DIFFERENT behavior in Spark Connect vs Classic!

def process_batch(data_size, batch_name):
    """
    BAD PATTERN: Reuses the same temp view name
    In Spark Connect, previous DataFrames will see new data!
    """
    # Create sample data
    df = spark.range(data_size).withColumn("batch", lit(batch_name))
    
    # Always uses the same view name - DANGEROUS in Spark Connect!
    df.createOrReplaceTempView("current_batch")
    
    return spark.table("current_batch")

# Process multiple batches
batch_1 = process_batch(100, "morning")
batch_2 = process_batch(500, "afternoon")
batch_3 = process_batch(50, "evening")

# In Spark Classic: Each batch_X has its original data
# In Spark Connect: ALL batches now point to "evening" data (50 rows)!
print(f"Batch 1 count: {batch_1.count()}")  # Expected: 100, Connect: 50
print(f"Batch 2 count: {batch_2.count()}")  # Expected: 500, Connect: 50
print(f"Batch 3 count: {batch_3.count()}")  # Expected: 50, Connect: 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Connect Anti-Pattern 2: UDF Late Binding [BC-SC-003]
# MAGIC 
# MAGIC **Issue:** In Spark Connect, UDFs capture external variables at **execution time**, not definition time.
# MAGIC 
# MAGIC **Impact:** UDFs may use unexpected variable values.

# COMMAND ----------

# ============================================================================
# BC-SC-003: SPARK CONNECT ISSUE - UDF late variable binding
# ============================================================================
# This code has DIFFERENT behavior in Spark Connect vs Classic!

from pyspark.sql.functions import udf

# Global multiplier that changes
fare_multiplier = 1.0

@udf("double")
def apply_surge_pricing(fare):
    """
    BAD PATTERN: Captures external variable
    In Spark Connect, uses value at execution time, not definition time!
    """
    return fare * fare_multiplier

# Create the UDF when multiplier is 1.0
surge_udf = apply_surge_pricing

# Change the multiplier AFTER UDF creation
fare_multiplier = 2.5  # Surge pricing!

# Apply UDF
df_with_surge = taxi_df.withColumn(
    "surge_fare",
    surge_udf(col("fare_amount"))
)

# In Spark Classic: surge_fare = fare_amount * 1.0 (captured at definition)
# In Spark Connect: surge_fare = fare_amount * 2.5 (captured at execution)
df_with_surge.select("fare_amount", "surge_fare").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Connect Anti-Pattern 3: Schema Access in Loops [BC-SC-004]
# MAGIC 
# MAGIC **Issue:** In Spark Connect, `df.columns` triggers an RPC call. In loops, this causes severe performance degradation.
# MAGIC 
# MAGIC **Impact:** Code that works in milliseconds can take minutes.

# COMMAND ----------

# ============================================================================
# BC-SC-004: SPARK CONNECT ISSUE - Schema access in loops
# ============================================================================
# This code is EXTREMELY SLOW in Spark Connect!

import time

# BAD PATTERN: Checking columns inside loop
def add_computed_columns_bad(df, num_columns=20):
    """
    BAD: Calls df.columns on every iteration
    In Spark Connect, each call is an RPC - very slow!
    """
    start_time = time.time()
    
    for i in range(num_columns):
        col_name = f"computed_{i}"
        # This check triggers RPC in Spark Connect!
        if col_name not in df.columns:
            df = df.withColumn(col_name, col("fare_amount") * i)
    
    elapsed = time.time() - start_time
    print(f"BAD pattern took: {elapsed:.2f} seconds")
    return df

# Run the bad pattern (will be slow in Spark Connect)
result_bad = add_computed_columns_bad(taxi_df.limit(1000), num_columns=20)
print(f"Result columns: {len(result_bad.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Breaking Pattern: View Column Definitions [BC-15.4-004]

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- BC-15.4-004: BREAKING - Column type definitions in CREATE VIEW
# MAGIC -- ============================================================================
# MAGIC -- This syntax is DISALLOWED in DBR 15.4+!
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW taxi_summary_view (
# MAGIC     trip_date DATE NOT NULL,
# MAGIC     total_trips BIGINT DEFAULT 0,
# MAGIC     avg_fare DOUBLE
# MAGIC ) AS
# MAGIC SELECT 
# MAGIC     DATE(pickup_datetime) as trip_date,
# MAGIC     COUNT(*) as total_trips,
# MAGIC     AVG(fare_amount) as avg_fare
# MAGIC FROM taxi_trips
# MAGIC GROUP BY DATE(pickup_datetime);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: All Breaking Changes in This Notebook
# MAGIC 
# MAGIC | Line | Breaking Change | Code Pattern | Fix |
# MAGIC |------|-----------------|--------------|-----|
# MAGIC | Cmd 3 | BC-17.3-001 | `from pyspark.sql.functions import input_file_name` | Remove import |
# MAGIC | Cmd 5 | BC-17.3-001 | `input_file_name()` | Use `_metadata.file_name` |
# MAGIC | Cmd 7-8 | BC-15.4-003 | `IF ! EXISTS`, `IS ! NULL` | Use `NOT` |
# MAGIC | Cmd 9 | BC-15.4-003 | `! IN`, `! BETWEEN`, `! LIKE` | Use `NOT IN`, etc. |
# MAGIC | Cmd 11 | BC-15.4-001 | `@udf(returnType=VariantType())` | Use StringType + JSON |
# MAGIC | Cmd 13 | BC-17.3-002 | Auto Loader without explicit incremental | Add option explicitly |
# MAGIC | Cmd 15 | BC-SC-002 | `createOrReplaceTempView("same_name")` | Use UUID in name |
# MAGIC | Cmd 17 | BC-SC-003 | UDF capturing external variable | Use function factory |
# MAGIC | Cmd 19 | BC-SC-004 | `df.columns` in loop | Cache columns locally |
# MAGIC | Cmd 21 | BC-15.4-004 | `CREATE VIEW (col TYPE NOT NULL)` | Remove type constraints |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Demo Instructions
# MAGIC 
# MAGIC ### Using Databricks Assistant to Fix This Notebook
# MAGIC 
# MAGIC 1. **Open Databricks Assistant** (agent mode)
# MAGIC 
# MAGIC 2. **Scan for breaking changes:**
# MAGIC    ```
# MAGIC    Scan this notebook for breaking changes when upgrading to DBR 17.3
# MAGIC    ```
# MAGIC 
# MAGIC 3. **Review findings** - Assistant should identify all patterns above
# MAGIC 
# MAGIC 4. **Apply fixes:**
# MAGIC    ```
# MAGIC    Fix all the breaking changes you found in this notebook
# MAGIC    ```
# MAGIC 
# MAGIC 5. **Validate fixes:**
# MAGIC    ```
# MAGIC    Validate that all breaking changes have been fixed
# MAGIC    ```
# MAGIC 
# MAGIC ### Expected Fixes
# MAGIC 
# MAGIC | Original | Fixed |
# MAGIC |----------|-------|
# MAGIC | `input_file_name()` | `_metadata.file_name` |
# MAGIC | `IF ! EXISTS` | `IF NOT EXISTS` |
# MAGIC | `IS ! NULL` | `IS NOT NULL` |
# MAGIC | `@udf(returnType=VariantType())` | `@udf(returnType=StringType())` |
# MAGIC | `createOrReplaceTempView("name")` | `createOrReplaceTempView(f"name_{uuid}")` |
# MAGIC | External variable in UDF | Function factory pattern |
# MAGIC | `df.columns` in loop | `columns = set(df.columns)` outside loop |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up temp views
spark.catalog.dropTempView("taxi_trips")
spark.catalog.dropTempView("current_batch")

print("Cleanup complete!")
