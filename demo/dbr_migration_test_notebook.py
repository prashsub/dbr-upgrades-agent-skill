# Databricks notebook source
# MAGIC %md
# MAGIC # DBR Migration Test Notebook
# MAGIC 
# MAGIC **Purpose:** A compact notebook with ALL 17 breaking changes for testing the Agent Skill.
# MAGIC 
# MAGIC ## Breaking Changes Summary
# MAGIC 
# MAGIC | Category | Count | IDs |
# MAGIC |----------|-------|-----|
# MAGIC | 🔴 Auto-Fix | 7 | BC-17.3-001, BC-15.4-003, BC-16.4-001a-e |
# MAGIC | 🟡 Manual Review | 6 | BC-15.4-001, BC-15.4-004, BC-SC-001/002/003/004 |
# MAGIC | ⚙️ Config | 4 | BC-13.3-002, BC-15.4-002, BC-16.4-004, BC-17.3-002 |
# MAGIC 
# MAGIC ## Usage
# MAGIC 
# MAGIC 1. Run Agent Skill: `Scan this notebook for breaking changes`
# MAGIC 2. Apply fixes: `Fix all the breaking changes`
# MAGIC 3. Validate: `Validate all fixes were applied`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import *
import time
import sys
import os

# Add utils to path for imports
sys.path.append(os.path.join(os.getcwd(), 'utils'))

# Import test helpers from utils module
from utils.dbr_test_helpers import (
    generate_test_data,
    create_breaking_change_sample,
    get_dbr_version,
    is_breaking_change_applicable,
    BreakingChangeTestResult,
    get_legacy_config_settings,
    generate_test_report
)

# BC-17.3-001: This import will FAIL on DBR 17.3
from pyspark.sql.functions import input_file_name

# Display DBR version information
version_info = get_dbr_version(spark)
print("=" * 60)
print("DBR VERSION INFORMATION")
print("=" * 60)
print(f"DBR Version: {version_info['dbr_version']}")
print(f"Full Version: {version_info['dbr_full']}")
print(f"Spark Version: {version_info['spark_version']}")
print(f"Is LTS: {version_info['is_lts']}")
print("=" * 60)
print()

# Load NYC Taxi dataset
taxi_df = (spark.read.format("delta")
    .load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
    .limit(10000))

print(f"Loaded {taxi_df.count()} records")

# Initialize test results tracking
test_results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🔴 AUTO-FIX CHANGES
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-17.3-001: input_file_name() Removed

# COMMAND ----------

# BC-17.3-001: All these patterns will FAIL on DBR 17.3

# Pattern 1: withColumn
df1 = taxi_df.withColumn("source_file", input_file_name())

# Pattern 2: select
df2 = taxi_df.select("*", input_file_name().alias("file_source"))

# Pattern 3: SQL string
df3 = spark.sql("SELECT input_file_name() as src, * FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow` LIMIT 10")

print("input_file_name() patterns executed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-15.4-003: '!' Syntax for NOT

# COMMAND ----------

taxi_df.createOrReplaceTempView("taxi_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BC-15.4-003: All these patterns FAIL on DBR 15.4+
# MAGIC 
# MAGIC -- Pattern 1: IF ! EXISTS
# MAGIC CREATE TABLE IF ! EXISTS default.test_taxi_summary (id INT);
# MAGIC 
# MAGIC -- Pattern 2: IS ! NULL
# MAGIC SELECT * FROM taxi_trips WHERE fare_amount IS ! NULL LIMIT 5;
# MAGIC 
# MAGIC -- Pattern 3: ! IN
# MAGIC SELECT * FROM taxi_trips WHERE payment_type ! IN ('Cash') LIMIT 5;
# MAGIC 
# MAGIC -- Pattern 4: ! BETWEEN
# MAGIC SELECT * FROM taxi_trips WHERE fare_amount ! BETWEEN 0 AND 5 LIMIT 5;
# MAGIC 
# MAGIC -- Pattern 5: ! LIKE
# MAGIC SELECT * FROM taxi_trips WHERE vendor_id ! LIKE 'CMT%' LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-16.4-001a-e: Scala 2.13 Changes (shown as strings)

# COMMAND ----------

# BC-16.4-001a: JavaConverters (Scala)
scala_1a = """
import scala.collection.JavaConverters._
val scalaList = javaList.asScala.toList
"""

# BC-16.4-001b: .to[Collection] syntax (Scala)
scala_1b = """
val list = Seq(1, 2, 3).to[List]
val set = Seq(1, 2, 3).to[Set]
"""

# BC-16.4-001c: TraversableOnce (Scala)
scala_1c = """
def process(items: TraversableOnce[String]): Unit = items.foreach(println)
"""

# BC-16.4-001d: Traversable (Scala)
scala_1d = """
def process(data: Traversable[Int]): Int = data.sum
"""

# BC-16.4-001e: Stream (Scala)
scala_1e = """
val numbers = Stream.from(1)
val randoms = Stream.continually(scala.util.Random.nextInt())
"""

print("Scala 2.13 breaking patterns defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🟡 MANUAL REVIEW CHANGES
# MAGIC 
# MAGIC **⚠️ AGENT: These items should be FLAGGED for developer review, with specific guidance on what to check.**
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-15.4-001: VARIANT in Python UDF (15.4 only - fixed in 16.4+)

# COMMAND ----------

# =============================================================================
# BC-15.4-001: [MANUAL REVIEW] VARIANT UDF
# FAILS on DBR 15.4 only, works on 16.4+
# 
# REVIEW: If target is 15.4, use StringType + json.dumps instead
# FIX: @udf(returnType=StringType()) + return json.dumps({...})
# =============================================================================
@udf(returnType=VariantType())
def create_trip_variant(fare, tip):
    return {"fare": fare, "tip": tip, "total": fare + tip if fare and tip else 0}

df_variant = taxi_df.withColumn("trip_data", create_trip_variant(col("fare_amount"), col("tip_amount")))
print("VARIANT UDF created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-15.4-004: VIEW Column Type Definition

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- BC-15.4-004: [MANUAL REVIEW] VIEW Column Type Definition
# MAGIC -- FAILS on DBR 15.4+ - column types/constraints not allowed in CREATE VIEW
# MAGIC --
# MAGIC -- REVIEW: Remove type definitions, move constraints to SELECT
# MAGIC -- FIX: CREATE VIEW name AS SELECT CAST(...) FROM ... WHERE ... IS NOT NULL
# MAGIC -- =============================================================================
# MAGIC CREATE OR REPLACE VIEW taxi_view (
# MAGIC     trip_date DATE NOT NULL,
# MAGIC     total_trips BIGINT DEFAULT 0,
# MAGIC     avg_fare DOUBLE
# MAGIC ) AS
# MAGIC SELECT DATE(pickup_datetime), COUNT(*), AVG(fare_amount)
# MAGIC FROM taxi_trips GROUP BY DATE(pickup_datetime);

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-SC-001: Lazy Schema Analysis (Spark Connect)

# COMMAND ----------

# =============================================================================
# BC-SC-001: [MANUAL REVIEW] Lazy Schema Analysis (Spark Connect)
# In Spark Connect, errors appear at ACTION time, not transformation time
#
# REVIEW: Check if try/except blocks rely on catching errors at transformation
# FIX: Add _ = df.columns after transformation to force early validation
# =============================================================================
def risky_transform():
    try:
        # Typo: "far_amount" instead of "fare_amount"
        result = taxi_df.withColumn("total", col("far_amount") + col("tip_amount"))
        # FIX: Add this line to catch errors early in Spark Connect:
        # _ = result.columns
        print("Transform created - error won't appear until action in Spark Connect")
    except Exception as e:
        print(f"Caught: {e}")

risky_transform()

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-SC-002: Temp View Name Reuse

# COMMAND ----------

# =============================================================================
# BC-SC-002: [MANUAL REVIEW] Temp View Name Reuse (Spark Connect)
# Same view name used multiple times - in Spark Connect, all DFs see latest data
#
# REVIEW: Check if temp view names are reused in loops or repeated function calls
# FIX: Use unique names with UUID: f"batch_{name}_{uuid.uuid4().hex[:8]}"
# =============================================================================
def process_data(size, name):
    df = spark.range(size).withColumn("batch", lit(name))
    df.createOrReplaceTempView("current_batch")  # Same name every time - PROBLEM!
    return spark.table("current_batch")

batch_a = process_data(100, "morning")
batch_b = process_data(50, "evening")

# In Spark Connect: both show 50 rows!
print(f"Batch A: {batch_a.count()}, Batch B: {batch_b.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-SC-003: UDF Late Binding

# COMMAND ----------

# =============================================================================
# BC-SC-003: [MANUAL REVIEW] UDF Late Binding (Spark Connect)
# External variable captured at EXECUTION time, not definition time
#
# REVIEW: Check if UDFs reference variables defined outside the function
# FIX: Use function factory pattern to capture value at creation time
#      def make_udf(val): @udf(...) def fn(x): return x * val; return fn
# =============================================================================
multiplier = 1.0

@udf("double")
def apply_multiplier(value):
    return value * multiplier  # Captures external variable - PROBLEM!

multiplier = 2.5  # Changed AFTER UDF defined

df_mult = taxi_df.withColumn("adjusted_fare", apply_multiplier(col("fare_amount")))
# Classic: uses 1.0, Connect: uses 2.5!
df_mult.select("fare_amount", "adjusted_fare").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-SC-004: Schema Access in Loops

# COMMAND ----------

# =============================================================================
# BC-SC-004: [MANUAL REVIEW] Schema Access in Loops (Spark Connect)
# df.columns/df.schema triggers RPC call - very slow in loops
#
# REVIEW: Check if df.columns, df.schema, or df.dtypes is accessed inside loops
# FIX: Cache schema outside loop: cached_cols = df.columns; for c in cached_cols:
# =============================================================================
def add_columns_slow(df):
    start = time.time()
    for i in range(10):
        if f"col_{i}" not in df.columns:  # RPC call each iteration - SLOW!
            df = df.withColumn(f"col_{i}", lit(i))
    print(f"Slow pattern: {time.time() - start:.2f}s")
    return df

result = add_columns_slow(taxi_df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ⚙️ CONFIG CHANGES
# MAGIC 
# MAGIC **⚠️ AGENT: These are behavioral changes. Flag them as "test first, add config if results differ."**
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-13.3-002: Parquet Timestamp NTZ

# COMMAND ----------

# =============================================================================
# BC-13.3-002: [CONFIG FLAG] Parquet Timestamp NTZ Inference
# Default behavior changed - timestamps may be inferred differently
#
# TEST: Read Parquet files and verify timestamp values are correct
# FIX IF NEEDED: spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
# =============================================================================
try:
    val = spark.conf.get("spark.sql.parquet.inferTimestampNTZ.enabled")
    print(f"BC-13.3-002: Current inferTimestampNTZ: {val}")
except:
    print("BC-13.3-002: inferTimestampNTZ not explicitly set (using DBR default)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-15.4-002: JDBC useNullCalendar

# COMMAND ----------

# =============================================================================
# BC-15.4-002: [CONFIG FLAG] JDBC useNullCalendar
# Default changed to true - JDBC timestamps may be handled differently
#
# TEST: Read from JDBC and verify timestamp values match source system
# FIX IF NEEDED: spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")
# =============================================================================
try:
    val = spark.conf.get("spark.sql.legacy.jdbc.useNullCalendar")
    print(f"BC-15.4-002: Current useNullCalendar: {val}")
except:
    print("BC-15.4-002: useNullCalendar not explicitly set (using DBR default)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-16.4-004: MERGE materializeSource=none

# COMMAND ----------

# =============================================================================
# BC-16.4-004: [CONFIG FLAG] MERGE materializeSource=none Disallowed
# Setting to "none" throws error in DBR 16.4+
#
# SCAN: Search for merge.materializeSource.*none in codebase
# FIX: Remove the setting OR change to "auto"
# =============================================================================
# spark.conf.set("spark.databricks.delta.merge.materializeSource", "none")  # ❌ ERROR!
print("BC-16.4-004: materializeSource='none' is no longer allowed - use 'auto' instead")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-17.3-002: Auto Loader Incremental Listing

# COMMAND ----------

# =============================================================================
# BC-17.3-002: [CONFIG FLAG] Auto Loader Incremental Listing Default Changed
# Default changed from "auto" to "false" - may be slower but more reliable
#
# TEST: Run Auto Loader jobs and check if performance is acceptable
# FIX IF NEEDED: Add .option("cloudFiles.useIncrementalListing", "auto")
# =============================================================================
auto_loader_code = """
# Old code (relies on implicit "auto"):
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(path)

# New code (explicit setting for old behavior):
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.useIncrementalListing", "auto").load(path)
"""
print("BC-17.3-002: Auto Loader incremental listing")
print(auto_loader_code)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 SUMMARY
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Breaking Changes in This Notebook
# MAGIC 
# MAGIC ### Expected Agent Behavior
# MAGIC 
# MAGIC | ID | Type | Pattern | Agent Action |
# MAGIC |----|------|---------|--------------|
# MAGIC | BC-17.3-001 | 🔴 Auto-Fix | `input_file_name()` | **FIX** → `_metadata.file_name` |
# MAGIC | BC-15.4-003 | 🔴 Auto-Fix | `!` syntax for NOT | **FIX** → `NOT` keyword |
# MAGIC | BC-16.4-001a | 🔴 Auto-Fix | `JavaConverters` | **FIX** → `CollectionConverters` |
# MAGIC | BC-16.4-001b | 🔴 Auto-Fix | `.to[List]` | **FIX** → `.to(List)` |
# MAGIC | BC-16.4-001c | 🔴 Auto-Fix | `TraversableOnce` | **FIX** → `IterableOnce` |
# MAGIC | BC-16.4-001d | 🔴 Auto-Fix | `Traversable` | **FIX** → `Iterable` |
# MAGIC | BC-16.4-001e | 🔴 Auto-Fix | `Stream.from()` | **FIX** → `LazyList.from()` |
# MAGIC | BC-15.4-001 | 🟡 Manual | `VariantType()` UDF | **FLAG** - Skip if target ≥16.4 |
# MAGIC | BC-15.4-004 | 🟡 Manual | VIEW column types | **FLAG** - Remove types, cast in SELECT |
# MAGIC | BC-SC-001 | 🟡 Manual | Lazy schema analysis | **FLAG** - Add `df.columns` for validation |
# MAGIC | BC-SC-002 | 🟡 Manual | Temp view reuse | **FLAG** - Add UUID to view names |
# MAGIC | BC-SC-003 | 🟡 Manual | UDF late binding | **FLAG** - Use function factory pattern |
# MAGIC | BC-SC-004 | 🟡 Manual | Schema in loop | **FLAG** - Cache columns outside loop |
# MAGIC | BC-13.3-002 | ⚙️ Config | Parquet timestamp | **FLAG** - Test timestamps first |
# MAGIC | BC-15.4-002 | ⚙️ Config | JDBC timestamp | **FLAG** - Test JDBC reads first |
# MAGIC | BC-16.4-004 | ⚙️ Config | MERGE source=none | **FLAG** - Remove or use "auto" |
# MAGIC | BC-17.3-002 | ⚙️ Config | Auto Loader listing | **FLAG** - Test performance first |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Agent Skill
# MAGIC 
# MAGIC ```
# MAGIC 1. Scan: "Scan this notebook for breaking changes when upgrading to DBR 17.3"
# MAGIC 2. Fix: "Fix all the breaking changes you found"
# MAGIC 3. Validate: "Validate that all breaking changes have been addressed"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Breaking Change Applicability Check
# MAGIC 
# MAGIC Using helper functions to check which breaking changes apply to current DBR version

# COMMAND ----------

# Check applicability of each breaking change
bc_ids = [
    "BC-17.3-001", "BC-15.4-003", "BC-15.4-001", "BC-15.4-004",
    "BC-SC-001", "BC-SC-002", "BC-SC-003", "BC-SC-004",
    "BC-13.3-002", "BC-15.4-002", "BC-16.4-004", "BC-17.3-002",
    "BC-16.4-001a"
]

print("=" * 70)
print("BREAKING CHANGE APPLICABILITY")
print("=" * 70)
print()

for bc_id in bc_ids:
    applicable = is_breaking_change_applicable(spark, bc_id)
    bc_sample = create_breaking_change_sample(spark, bc_id)
    status = "✅ APPLIES" if applicable else "❌ N/A"
    print(f"{status} | {bc_id}: {bc_sample.get('description', 'Unknown')}")
    print(f"         Severity: {bc_sample.get('severity', 'UNKNOWN')}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Test Data Using Helper Functions

# COMMAND ----------

# Generate test data using helper function
test_df = generate_test_data(spark, num_rows=50)
print("Generated test data:")
test_df.show(5)

# Validate schema using helper function
from utils.dbr_test_helpers import validate_dataframe_schema

expected_cols = ["id", "value", "category", "created_at"]
is_valid, message = validate_dataframe_schema(test_df, expected_cols)
print(f"\nSchema Validation: {'✅ PASSED' if is_valid else '❌ FAILED'}")
print(f"Message: {message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Results Summary

# COMMAND ----------

# Create sample test results for demonstration
sample_results = [
    BreakingChangeTestResult("BC-17.3-001", "input_file_name() removed"),
    BreakingChangeTestResult("BC-15.4-003", "! syntax for NOT disallowed"),
    BreakingChangeTestResult("BC-SC-002", "Temp view name reuse")
]

# Simulate test results
sample_results[0].add_test_result(True, "Successfully replaced with _metadata.file_name")
sample_results[0].add_test_result(True, "All imports updated")

sample_results[1].add_test_result(True, "Replaced ! with NOT in SQL")
sample_results[1].add_test_result(False, "Found additional ! usage in line 123")

sample_results[2].add_test_result(False, "Duplicate temp view name detected")

# Generate and display report
report = generate_test_report(sample_results)
print(report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recommended Legacy Configurations

# COMMAND ----------

# Display recommended legacy configurations
configs = get_legacy_config_settings()

print("=" * 70)
print("RECOMMENDED LEGACY CONFIGURATIONS")
print("=" * 70)
print()
print("Add these to your cluster/notebook if timestamps or MERGE behave differently:")
print()
for key, value in configs.items():
    print(f"  {key} = {value}")
print()
print("To apply these configs, uncomment and run:")
print("# from utils.dbr_test_helpers import apply_legacy_configs")
print("# result = apply_legacy_configs(spark)")
print("# print(result)")

# COMMAND ----------

# Cleanup
spark.catalog.dropTempView("taxi_trips")
spark.catalog.dropTempView("current_batch")
print("Cleanup complete!")
