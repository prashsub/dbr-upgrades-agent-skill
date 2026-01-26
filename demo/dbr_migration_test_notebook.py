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

# BC-17.3-001: This import will FAIL on DBR 17.3
from pyspark.sql.functions import input_file_name

# Load NYC Taxi dataset
taxi_df = (spark.read.format("delta")
    .load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
    .limit(10000))

print(f"Loaded {taxi_df.count()} records")

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
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-15.4-001: VARIANT in Python UDF (15.4 only - fixed in 16.4+)

# COMMAND ----------

# BC-15.4-001: VARIANT UDF - FAILS on DBR 15.4 only, works on 16.4+
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
# MAGIC -- BC-15.4-004: Column types in VIEW - FAILS on DBR 15.4+
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

# BC-SC-001: In Spark Connect, this error appears LATE, not immediately
def risky_transform():
    try:
        # Typo: "far_amount" instead of "fare_amount"
        result = taxi_df.withColumn("total", col("far_amount") + col("tip_amount"))
        print("Transform created - error won't appear until action in Spark Connect")
    except Exception as e:
        print(f"Caught: {e}")

risky_transform()

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-SC-002: Temp View Name Reuse

# COMMAND ----------

# BC-SC-002: Same view name reused - causes issues in Spark Connect
def process_data(size, name):
    df = spark.range(size).withColumn("batch", lit(name))
    df.createOrReplaceTempView("current_batch")  # Same name every time!
    return spark.table("current_batch")

batch_a = process_data(100, "morning")
batch_b = process_data(50, "evening")

# In Spark Connect: both show 50 rows!
print(f"Batch A: {batch_a.count()}, Batch B: {batch_b.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-SC-003: UDF Late Binding

# COMMAND ----------

# BC-SC-003: External variable capture - different behavior in Spark Connect
multiplier = 1.0

@udf("double")
def apply_multiplier(value):
    return value * multiplier  # Captures external variable

multiplier = 2.5  # Changed AFTER UDF defined

df_mult = taxi_df.withColumn("adjusted_fare", apply_multiplier(col("fare_amount")))
# Classic: uses 1.0, Connect: uses 2.5!
df_mult.select("fare_amount", "adjusted_fare").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-SC-004: Schema Access in Loops

# COMMAND ----------

# BC-SC-004: df.columns in loop - SLOW in Spark Connect
def add_columns_slow(df):
    start = time.time()
    for i in range(10):
        if f"col_{i}" not in df.columns:  # RPC call each iteration in Connect!
            df = df.withColumn(f"col_{i}", lit(i))
    print(f"Slow pattern: {time.time() - start:.2f}s")
    return df

result = add_columns_slow(taxi_df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ⚙️ CONFIG CHANGES
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-13.3-002: Parquet Timestamp NTZ

# COMMAND ----------

# BC-13.3-002: Parquet timestamp inference changed
# Fix: spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
try:
    val = spark.conf.get("spark.sql.parquet.inferTimestampNTZ.enabled")
    print(f"BC-13.3-002: Current inferTimestampNTZ: {val}")
except:
    print("BC-13.3-002: inferTimestampNTZ not explicitly set (using DBR default)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-15.4-002: JDBC useNullCalendar

# COMMAND ----------

# BC-15.4-002: JDBC timestamp handling changed
# Fix: spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")
try:
    val = spark.conf.get("spark.sql.legacy.jdbc.useNullCalendar")
    print(f"BC-15.4-002: Current useNullCalendar: {val}")
except:
    print("BC-15.4-002: useNullCalendar not explicitly set (using DBR default)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-16.4-004: MERGE materializeSource=none

# COMMAND ----------

# BC-16.4-004: This setting will FAIL in DBR 16.4+
# spark.conf.set("spark.databricks.delta.merge.materializeSource", "none")  # ❌ ERROR!
print("BC-16.4-004: materializeSource='none' is no longer allowed - use 'auto' instead")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BC-17.3-002: Auto Loader Incremental Listing

# COMMAND ----------

# BC-17.3-002: Default changed from "auto" to "false"
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
# MAGIC | ID | Type | Pattern | Status |
# MAGIC |----|------|---------|--------|
# MAGIC | BC-17.3-001 | 🔴 Auto-Fix | `input_file_name()` | ❌ Breaks on 17.3 |
# MAGIC | BC-15.4-003 | 🔴 Auto-Fix | `!` syntax for NOT | ❌ Breaks on 15.4+ |
# MAGIC | BC-16.4-001a | 🔴 Auto-Fix | `JavaConverters` | ❌ Deprecated 16.4+ |
# MAGIC | BC-16.4-001b | 🔴 Auto-Fix | `.to[List]` | ❌ Fails 16.4+ |
# MAGIC | BC-16.4-001c | 🔴 Auto-Fix | `TraversableOnce` | ❌ Fails 16.4+ |
# MAGIC | BC-16.4-001d | 🔴 Auto-Fix | `Traversable` | ❌ Fails 16.4+ |
# MAGIC | BC-16.4-001e | 🔴 Auto-Fix | `Stream.from()` | ❌ Deprecated 16.4+ |
# MAGIC | BC-15.4-001 | 🟡 Manual | `VariantType()` UDF | ⚠️ 15.4 only |
# MAGIC | BC-15.4-004 | 🟡 Manual | VIEW column types | ❌ Breaks on 15.4+ |
# MAGIC | BC-SC-001 | 🟡 Manual | Lazy schema | ⚠️ Spark Connect |
# MAGIC | BC-SC-002 | 🟡 Manual | Temp view reuse | ⚠️ Spark Connect |
# MAGIC | BC-SC-003 | 🟡 Manual | UDF late binding | ⚠️ Spark Connect |
# MAGIC | BC-SC-004 | 🟡 Manual | Schema in loop | ⚠️ Spark Connect |
# MAGIC | BC-13.3-002 | ⚙️ Config | Parquet timestamp | ⚠️ Behavior change |
# MAGIC | BC-15.4-002 | ⚙️ Config | JDBC timestamp | ⚠️ Behavior change |
# MAGIC | BC-16.4-004 | ⚙️ Config | MERGE source=none | ❌ Not allowed |
# MAGIC | BC-17.3-002 | ⚙️ Config | Auto Loader listing | ⚠️ Behavior change |

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

# Cleanup
spark.catalog.dropTempView("taxi_trips")
spark.catalog.dropTempView("current_batch")
print("Cleanup complete!")
