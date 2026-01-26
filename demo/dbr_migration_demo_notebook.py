# Databricks notebook source
# MAGIC %md
# MAGIC # 📚 DBR Migration Training & Demo Notebook
# MAGIC 
# MAGIC ## Purpose
# MAGIC 
# MAGIC This notebook serves **two purposes**:
# MAGIC 
# MAGIC 1. **📖 Training Document** - Learn about all breaking changes between DBR 13.3 LTS → 17.3 LTS
# MAGIC 2. **🔧 Demo Notebook** - See the Agent Skill scan, fix, and validate these changes
# MAGIC 
# MAGIC ## How to Use This Notebook
# MAGIC 
# MAGIC | Use Case | Instructions |
# MAGIC |----------|--------------|
# MAGIC | **Learning** | Read through each section to understand what changed and why |
# MAGIC | **Demo** | Run the Agent Skill on this notebook to see it detect and fix issues |
# MAGIC | **Reference** | Search for specific BC-IDs when you encounter issues |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📊 Breaking Changes Overview
# MAGIC 
# MAGIC | Category | Count | Action |
# MAGIC |----------|-------|--------|
# MAGIC | 🔴 **Auto-Fix** | 7 | Agent automatically applies fix |
# MAGIC | 🟡 **Manual Review** | 6 | Agent flags for developer decision |
# MAGIC | ⚙️ **Config** | 4 | Test first, add config if needed |
# MAGIC 
# MAGIC ### Quick Reference Table
# MAGIC 
# MAGIC | ID | Severity | Change | DBR | Auto-Fix? |
# MAGIC |----|----------|--------|-----|-----------|
# MAGIC | BC-17.3-001 | 🔴 HIGH | `input_file_name()` removed | 17.3 | ✅ Yes |
# MAGIC | BC-15.4-003 | 🟡 MEDIUM | `!` syntax for NOT | 15.4 | ✅ Yes |
# MAGIC | BC-16.4-001a-e | 🔴 HIGH | Scala 2.13 collection changes | 16.4 | ✅ Yes |
# MAGIC | BC-15.4-001 | 🟢 LOW | VARIANT in UDF *(15.4 only)* | 15.4 | ❌ Fixed in 16.4 |
# MAGIC | BC-15.4-004 | 🟢 LOW | VIEW column types | 15.4 | ❌ Manual |
# MAGIC | BC-SC-001 | 🟢 LOW | Lazy schema analysis | 13.3+ | ❌ Manual |
# MAGIC | BC-SC-002 | 🟡 MEDIUM | Temp view name reuse | 14.3+ | ❌ Manual |
# MAGIC | BC-SC-003 | 🟢 LOW | UDF late binding | 14.3+ | ❌ Manual |
# MAGIC | BC-SC-004 | 🟢 LOW | Schema access in loops | 13.3+ | ❌ Manual |
# MAGIC | BC-13.3-002 | 🟢 LOW | Parquet timestamp NTZ | 13.3 | ⚙️ Config |
# MAGIC | BC-15.4-002 | 🟢 LOW | JDBC useNullCalendar | 15.4 | ⚙️ Config |
# MAGIC | BC-16.4-004 | 🟢 LOW | MERGE materializeSource | 16.4 | ⚙️ Config |
# MAGIC | BC-17.3-002 | 🟡 MEDIUM | Auto Loader incremental listing | 17.3 | ⚙️ Config |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🔧 SETUP
# MAGIC ---

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
taxi_df = (spark.read
    .format("delta")
    .load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
    .limit(100000)  # Limit for demo purposes
)

print(f"Loaded {taxi_df.count()} taxi records")
taxi_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🔴 SECTION 1: AUTO-FIXABLE CODE CHANGES
# MAGIC 
# MAGIC These changes will **fail immediately** on the new DBR version, but the Agent Skill can **automatically fix** them.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-17.3-001: `input_file_name()` Removed
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC The `input_file_name()` function was **deprecated in DBR 13.3** and is **completely removed in DBR 17.3** (Spark 4.0).
# MAGIC 
# MAGIC | DBR Version | Status |
# MAGIC |-------------|--------|
# MAGIC | 13.3 LTS | ⚠️ Deprecated (works with warning) |
# MAGIC | 14.3 LTS | ⚠️ Deprecated (works with warning) |
# MAGIC | 15.4 LTS | ⚠️ Deprecated (works with warning) |
# MAGIC | 16.4 LTS | ⚠️ Deprecated (works with warning) |
# MAGIC | **17.3 LTS** | ❌ **REMOVED** - `AnalysisException: Undefined function` |
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 17.3 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/17.3lts.html)
# MAGIC - [Spark 4.0 Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-33-to-40)
# MAGIC - [File Metadata Column Documentation](https://docs.databricks.com/en/ingestion/file-metadata-column.html)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Detection Pattern (Regex):**
# MAGIC ```
# MAGIC \binput_file_name\s*\(
# MAGIC ```
# MAGIC 
# MAGIC **Matches:**
# MAGIC - `input_file_name()`
# MAGIC - `input_file_name ()`
# MAGIC - `input_file_name(  )`
# MAGIC 
# MAGIC ### ✅ How the Agent Fixes It
# MAGIC 
# MAGIC | Context | Before | After |
# MAGIC |---------|--------|-------|
# MAGIC | DataFrame API | `input_file_name()` | `col("_metadata.file_name")` |
# MAGIC | SQL String | `input_file_name()` | `_metadata.file_name` |
# MAGIC | Pure SQL | `input_file_name()` | `_metadata.file_name` |

# COMMAND ----------

# ============================================================================
# BC-17.3-001: BREAKING - input_file_name() usage
# ============================================================================
# This code will FAIL on DBR 17.3!

# ❌ Pattern 1: Direct usage in withColumn
df_with_source = taxi_df.withColumn("source_file", input_file_name())

# ❌ Pattern 2: Usage in select
df_selected = taxi_df.select(
    "*",
    input_file_name().alias("data_source")
)

# ❌ Pattern 3: Usage in a transformation pipeline
processed_df = (taxi_df
    .withColumn("source", input_file_name())
    .withColumn("load_timestamp", F.current_timestamp())
    .filter(col("fare_amount") > 0)
)

print(f"Processed {processed_df.count()} records with source file tracking")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ CORRECT CODE (After Fix)
# MAGIC 
# MAGIC ```python
# MAGIC # Pattern 1: Direct usage in withColumn
# MAGIC df_with_source = taxi_df.withColumn("source_file", col("_metadata.file_name"))
# MAGIC 
# MAGIC # Pattern 2: Usage in select
# MAGIC df_selected = taxi_df.select(
# MAGIC     "*",
# MAGIC     col("_metadata.file_name").alias("data_source")
# MAGIC )
# MAGIC 
# MAGIC # Pattern 3: Usage in a transformation pipeline
# MAGIC processed_df = (taxi_df
# MAGIC     .withColumn("source", col("_metadata.file_name"))
# MAGIC     .withColumn("load_timestamp", F.current_timestamp())
# MAGIC     .filter(col("fare_amount") > 0)
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC > ⚠️ **Note:** Remember to remove `input_file_name` from your imports!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## BC-15.4-003: `!` Syntax for NOT Disallowed
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC Using `!` as a shorthand for `NOT` in SQL (outside of boolean expressions) is **no longer allowed** in DBR 15.4+.
# MAGIC 
# MAGIC | Pattern | DBR 13.3 | DBR 15.4+ |
# MAGIC |---------|----------|-----------|
# MAGIC | `IF ! EXISTS` | ✅ Works | ❌ `ParseException` |
# MAGIC | `IS ! NULL` | ✅ Works | ❌ `ParseException` |
# MAGIC | `! IN (...)` | ✅ Works | ❌ `ParseException` |
# MAGIC | `! BETWEEN` | ✅ Works | ❌ `ParseException` |
# MAGIC | `! LIKE` | ✅ Works | ❌ `ParseException` |
# MAGIC | `!= ` | ✅ Works | ✅ Still works (comparison operator) |
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 15.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/15.4lts.html)
# MAGIC - [Spark SQL ANSI Compliance](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Detection Patterns (Regex):**
# MAGIC ```
# MAGIC (IF|IS)\s*!(?!\s*=)     -- Matches IF ! and IS ! but NOT !=
# MAGIC \s!\s*(IN|BETWEEN|LIKE|EXISTS)\b   -- Matches ! IN, ! BETWEEN, etc.
# MAGIC ```
# MAGIC 
# MAGIC ### ✅ How the Agent Fixes It
# MAGIC 
# MAGIC | Before | After |
# MAGIC |--------|-------|
# MAGIC | `IF ! EXISTS` | `IF NOT EXISTS` |
# MAGIC | `IS ! NULL` | `IS NOT NULL` |
# MAGIC | `! IN` | `NOT IN` |
# MAGIC | `! BETWEEN` | `NOT BETWEEN` |
# MAGIC | `! LIKE` | `NOT LIKE` |

# COMMAND ----------

# Save taxi data to a temp table for SQL queries
taxi_df.createOrReplaceTempView("taxi_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- BC-15.4-003: BREAKING - '!' syntax in SQL
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- ❌ Pattern 1: IF ! EXISTS (WILL FAIL on DBR 15.4+)
# MAGIC CREATE TABLE IF ! EXISTS demo_db.taxi_summary (
# MAGIC     pickup_date DATE,
# MAGIC     trip_count BIGINT,
# MAGIC     total_fare DOUBLE
# MAGIC );
# MAGIC 
# MAGIC -- ❌ Pattern 2: IS ! NULL (WILL FAIL on DBR 15.4+)
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
# MAGIC -- ❌ Pattern 3: ! IN (WILL FAIL on DBR 15.4+)
# MAGIC SELECT *
# MAGIC FROM taxi_trips
# MAGIC WHERE payment_type ! IN ('Cash', 'No Charge')
# MAGIC LIMIT 10;
# MAGIC 
# MAGIC -- ❌ Pattern 4: ! BETWEEN (WILL FAIL on DBR 15.4+)
# MAGIC SELECT *
# MAGIC FROM taxi_trips
# MAGIC WHERE fare_amount ! BETWEEN 0 AND 5
# MAGIC LIMIT 10;
# MAGIC 
# MAGIC -- ❌ Pattern 5: ! LIKE (WILL FAIL on DBR 15.4+)
# MAGIC SELECT *
# MAGIC FROM taxi_trips  
# MAGIC WHERE pickup_datetime ! LIKE '2019-01%'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ CORRECT SQL (After Fix)
# MAGIC 
# MAGIC ```sql
# MAGIC -- Pattern 1: IF NOT EXISTS
# MAGIC CREATE TABLE IF NOT EXISTS demo_db.taxi_summary (...);
# MAGIC 
# MAGIC -- Pattern 2: IS NOT NULL
# MAGIC SELECT * FROM taxi_trips WHERE fare_amount IS NOT NULL;
# MAGIC 
# MAGIC -- Pattern 3: NOT IN
# MAGIC SELECT * FROM taxi_trips WHERE payment_type NOT IN ('Cash', 'No Charge');
# MAGIC 
# MAGIC -- Pattern 4: NOT BETWEEN
# MAGIC SELECT * FROM taxi_trips WHERE fare_amount NOT BETWEEN 0 AND 5;
# MAGIC 
# MAGIC -- Pattern 5: NOT LIKE
# MAGIC SELECT * FROM taxi_trips WHERE pickup_datetime NOT LIKE '2019-01%';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🟡 SECTION 2: MANUAL REVIEW ITEMS
# MAGIC 
# MAGIC These patterns **require developer decision**. The agent flags them but doesn't auto-fix because the correct action depends on context.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-15.4-001: VARIANT Type in Python UDF
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC Python UDFs using `VARIANT` type as input or output **threw an exception in DBR 15.4 only**.
# MAGIC 
# MAGIC | DBR Version | VARIANT in UDF |
# MAGIC |-------------|----------------|
# MAGIC | 13.3 LTS | N/A (VARIANT not available) |
# MAGIC | 14.3 LTS | ✅ Works |
# MAGIC | **15.4 LTS** | ❌ **FAILS** |
# MAGIC | 16.4 LTS | ✅ Works |
# MAGIC | 17.3 LTS | ✅ Works |
# MAGIC 
# MAGIC > ✅ **RESOLVED:** If upgrading to 16.4 or 17.3, **no action needed!**
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 15.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/15.4lts.html)
# MAGIC - [VARIANT Data Type Documentation](https://docs.databricks.com/en/sql/language-manual/data-types/variant-type.html)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Detection Pattern:**
# MAGIC ```
# MAGIC VariantType\s*\(
# MAGIC ```
# MAGIC 
# MAGIC ### 📋 Decision Matrix
# MAGIC 
# MAGIC | Target DBR | Action |
# MAGIC |------------|--------|
# MAGIC | 15.4 | ✅ **FIX**: Use StringType + json.dumps |
# MAGIC | 16.4+ | ❌ **No action needed** |

# COMMAND ----------

# ============================================================================
# BC-15.4-001: VARIANT type in Python UDF (15.4 only)
# ============================================================================
# This code FAILS on DBR 15.4 only - works fine on 16.4+!

from pyspark.sql.functions import udf
from pyspark.sql.types import VariantType, StringType

# ❌ Pattern 1: UDF returning VARIANT (fails on 15.4 only)
@udf(returnType=VariantType())
def create_trip_metadata(fare, tip, total):
    """Create a VARIANT containing trip metadata"""
    return {
        "fare_amount": fare,
        "tip_amount": tip,
        "total_amount": total,
        "tip_percentage": (tip / fare * 100) if fare > 0 else 0
    }

# Usage
df_with_variant = taxi_df.withColumn(
    "trip_meta",
    create_trip_metadata(col("fare_amount"), col("tip_amount"), col("total_amount"))
)

print("Created DataFrame with VARIANT column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ FIX (For DBR 15.4 Only)
# MAGIC 
# MAGIC ```python
# MAGIC import json
# MAGIC from pyspark.sql.types import StringType
# MAGIC 
# MAGIC @udf(returnType=StringType())
# MAGIC def create_trip_metadata(fare, tip, total):
# MAGIC     return json.dumps({
# MAGIC         "fare_amount": float(fare) if fare else 0,
# MAGIC         "tip_amount": float(tip) if tip else 0,
# MAGIC         "total_amount": float(total) if total else 0,
# MAGIC         "tip_percentage": (tip / fare * 100) if fare > 0 else 0
# MAGIC     })
# MAGIC 
# MAGIC # To use as VARIANT later:
# MAGIC # df.select(parse_json(col("trip_meta")))
# MAGIC ```
# MAGIC 
# MAGIC > **Note:** If upgrading directly to 16.4 or 17.3, your VARIANT UDFs will work!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## BC-15.4-004: VIEW Column Type Definitions
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC Specifying column types directly in `CREATE VIEW` is **no longer allowed** in DBR 15.4+.
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 15.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/15.4lts.html)
# MAGIC - [CREATE VIEW Documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Detection Pattern:**
# MAGIC ```
# MAGIC CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+\w+\s*\([^)]*\b(INT|STRING|BIGINT|DOUBLE|BOOLEAN|NOT\s+NULL|DEFAULT)\b
# MAGIC ```
# MAGIC 
# MAGIC ### 📋 Decision Matrix
# MAGIC 
# MAGIC | Pattern | Action |
# MAGIC |---------|--------|
# MAGIC | `(col INT, ...)` | Remove types, cast in SELECT |
# MAGIC | `(col NOT NULL)` | Remove constraint, use WHERE in SELECT |
# MAGIC | `(col DEFAULT x)` | Remove default, use COALESCE in SELECT |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- BC-15.4-004: BREAKING - Column type definitions in CREATE VIEW
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- ❌ BROKEN (DBR 15.4+)
# MAGIC CREATE OR REPLACE VIEW taxi_summary_view (
# MAGIC     trip_date DATE NOT NULL,       -- Type + constraint not allowed!
# MAGIC     total_trips BIGINT DEFAULT 0,  -- DEFAULT not allowed!
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
# MAGIC ### ✅ CORRECT SQL (After Fix)
# MAGIC 
# MAGIC ```sql
# MAGIC -- Move all type handling to the SELECT
# MAGIC CREATE OR REPLACE VIEW taxi_summary_view AS
# MAGIC SELECT 
# MAGIC     CAST(DATE(pickup_datetime) AS DATE) as trip_date,
# MAGIC     COALESCE(COUNT(*), 0) as total_trips,
# MAGIC     AVG(fare_amount) as avg_fare
# MAGIC FROM taxi_trips
# MAGIC WHERE DATE(pickup_datetime) IS NOT NULL  -- Replaces NOT NULL constraint
# MAGIC GROUP BY DATE(pickup_datetime);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## BC-SC-001: Lazy Schema Analysis (Spark Connect)
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC In **Spark Connect** (used by Serverless and Databricks Connect), schema validation happens **lazily** instead of **eagerly**.
# MAGIC 
# MAGIC | Behavior | Spark Classic | Spark Connect |
# MAGIC |----------|--------------|---------------|
# MAGIC | When errors appear | At transformation time | At action time |
# MAGIC | Try/except catches errors | ✅ Immediately | ❌ Only at `.show()`, `.collect()`, etc. |
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
# MAGIC - [Databricks Connect](https://docs.databricks.com/en/dev-tools/databricks-connect/index.html)
# MAGIC - [Serverless Compute Documentation](https://docs.databricks.com/en/compute/serverless.html)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Pattern:** try/except blocks around DataFrame transformations
# MAGIC 
# MAGIC **Flag Reason:** Errors may slip through in Spark Connect mode
# MAGIC 
# MAGIC ### 📋 Decision Matrix
# MAGIC 
# MAGIC | Your Code Pattern | Action |
# MAGIC |-------------------|--------|
# MAGIC | try/except around transformations | ⚠️ Add `df.columns` after transformation |
# MAGIC | No try/except | Consider adding validation points |

# COMMAND ----------

# ============================================================================
# BC-SC-001: SPARK CONNECT - Lazy Schema Analysis
# ============================================================================
# In Spark Connect, errors are detected LATE, not immediately

def analyze_with_misspelled_column():
    """
    ❌ PROBLEM: This try/except won't catch the error in Spark Connect!
    """
    df = taxi_df.select("fare_amount", "tip_amount")
    
    try:
        # Typo: "far_amount" instead of "fare_amount"
        transformed = df.withColumn("total", col("far_amount") + col("tip_amount"))
        print("Transformation created (no error in Spark Connect...)")
        
        # In Spark Connect, error only appears HERE:
        # transformed.show()  # <-- AnalysisException thrown here, not above
        
    except Exception as e:
        print(f"Error caught: {e}")  # Won't catch anything in Connect!

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ BEST PRACTICE: Force Early Validation
# MAGIC 
# MAGIC ```python
# MAGIC def analyze_with_validation(df):
# MAGIC     """
# MAGIC     ✅ GOOD: Forces schema validation immediately
# MAGIC     """
# MAGIC     transformed = df.withColumn("total", col("fare_amount") + col("tip_amount"))
# MAGIC     
# MAGIC     # Force schema resolution - errors caught here!
# MAGIC     _ = transformed.columns
# MAGIC     
# MAGIC     return transformed
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## BC-SC-002: Temp View Name Reuse (Spark Connect)
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC In **Spark Connect**, temp views use **name-based lookup** instead of embedded query plans. If you reuse a view name, **all DataFrames pointing to that name see the new data**.
# MAGIC 
# MAGIC | Scenario | Spark Classic | Spark Connect |
# MAGIC |----------|--------------|---------------|
# MAGIC | View name reused | Each DF keeps original data | ❌ All DFs see new data! |
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
# MAGIC - [Temp Views in Databricks](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html#temporary-views)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC The agent tracks all `createOrReplaceTempView` calls and flags when:
# MAGIC - Same **literal name** used multiple times
# MAGIC - Same **variable name** used multiple times
# MAGIC 
# MAGIC ### 📋 Decision Matrix
# MAGIC 
# MAGIC | Pattern | Risk | Action |
# MAGIC |---------|------|--------|
# MAGIC | Same name in a loop | 🔴 HIGH | ✅ Add UUID |
# MAGIC | Same name in function called multiple times | 🔴 HIGH | ✅ Add UUID |
# MAGIC | Different names for different purposes | 🟢 LOW | ❌ No action |
# MAGIC | View created once, used throughout | 🟢 LOW | ❌ No action |

# COMMAND ----------

# ============================================================================
# BC-SC-002: SPARK CONNECT - Temp view name reuse
# ============================================================================

def process_batch(data_size, batch_name):
    """
    ❌ BAD PATTERN: Reuses the same temp view name
    """
    df = spark.range(data_size).withColumn("batch", lit(batch_name))
    
    # DANGER: Same view name used every time!
    df.createOrReplaceTempView("current_batch")
    
    return spark.table("current_batch")

# Process multiple batches
batch_1 = process_batch(100, "morning")
batch_2 = process_batch(500, "afternoon")
batch_3 = process_batch(50, "evening")

# ❌ In Spark Connect: ALL batches now show 50 rows!
print(f"Batch 1 count: {batch_1.count()}")  # Classic: 100, Connect: 50
print(f"Batch 2 count: {batch_2.count()}")  # Classic: 500, Connect: 50
print(f"Batch 3 count: {batch_3.count()}")  # Classic: 50, Connect: 50

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ FIX: Use Unique View Names
# MAGIC 
# MAGIC ```python
# MAGIC import uuid
# MAGIC 
# MAGIC def process_batch(data_size, batch_name):
# MAGIC     """
# MAGIC     ✅ GOOD: Unique view name for each call
# MAGIC     """
# MAGIC     df = spark.range(data_size).withColumn("batch", lit(batch_name))
# MAGIC     
# MAGIC     # Unique name with UUID
# MAGIC     unique_view = f"batch_{batch_name}_{uuid.uuid4().hex[:8]}"
# MAGIC     df.createOrReplaceTempView(unique_view)
# MAGIC     
# MAGIC     return spark.table(unique_view)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## BC-SC-003: UDF Late Binding (Spark Connect)
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC In **Spark Connect**, UDFs capture external variables at **execution time**, not **definition time**.
# MAGIC 
# MAGIC | Scenario | Spark Classic | Spark Connect |
# MAGIC |----------|--------------|---------------|
# MAGIC | Variable changes after UDF defined | UDF uses old value | ❌ UDF uses new value! |
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
# MAGIC - [User-Defined Functions (UDFs)](https://docs.databricks.com/en/udf/index.html)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Pattern:** `@udf` decorators (flagged for manual review)
# MAGIC 
# MAGIC **Developer must check:** Does the UDF reference variables defined outside the function?
# MAGIC 
# MAGIC ### 📋 Decision Matrix
# MAGIC 
# MAGIC | UDF References | Action |
# MAGIC |----------------|--------|
# MAGIC | Only function parameters | ❌ No action |
# MAGIC | Hardcoded constants | ❌ No action |
# MAGIC | External variables that NEVER change | ⚠️ Consider fixing |
# MAGIC | External variables that CHANGE | ✅ **Must fix!** |

# COMMAND ----------

# ============================================================================
# BC-SC-003: SPARK CONNECT - UDF late variable binding
# ============================================================================

from pyspark.sql.functions import udf

# External variable that changes
fare_multiplier = 1.0

@udf("double")
def apply_surge_pricing(fare):
    """
    ❌ BAD: Captures external variable
    """
    return fare * fare_multiplier  # Which value of fare_multiplier?

# Create UDF when multiplier is 1.0
surge_udf = apply_surge_pricing

# Change multiplier AFTER UDF creation
fare_multiplier = 2.5

# Apply UDF
df_with_surge = taxi_df.withColumn(
    "surge_fare",
    surge_udf(col("fare_amount"))
)

# ❌ In Spark Connect: surge_fare = fare_amount * 2.5 (not 1.0!)
df_with_surge.select("fare_amount", "surge_fare").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ FIX: Function Factory Pattern
# MAGIC 
# MAGIC ```python
# MAGIC def make_multiplier_udf(multiplier):
# MAGIC     """
# MAGIC     ✅ GOOD: Value captured at factory call time
# MAGIC     """
# MAGIC     @udf("double")
# MAGIC     def apply_surge(fare):
# MAGIC         return fare * multiplier  # Captured in closure
# MAGIC     return apply_surge
# MAGIC 
# MAGIC # Create with specific values
# MAGIC multiply_by_1 = make_multiplier_udf(1.0)
# MAGIC multiply_by_2_5 = make_multiplier_udf(2.5)
# MAGIC 
# MAGIC # Now it doesn't matter if variables change later
# MAGIC df.withColumn("surge_1", multiply_by_1(col("fare")))
# MAGIC df.withColumn("surge_2_5", multiply_by_2_5(col("fare")))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## BC-SC-004: Schema Access in Loops (Spark Connect)
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC In **Spark Connect**, accessing `df.columns`, `df.schema`, or `df.dtypes` triggers an **RPC call** to the server.
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
# MAGIC - [DataFrame Schema Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.schema.html)
# MAGIC 
# MAGIC | Operation | Spark Classic | Spark Connect |
# MAGIC |-----------|--------------|---------------|
# MAGIC | `df.columns` | Local (instant) | RPC call (~50-100ms) |
# MAGIC | In a loop of 100 iterations | Instant | 5-10 seconds! |
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Pattern:** `\.(columns|schema|dtypes)\b`
# MAGIC 
# MAGIC **Developer must check:** Is this inside a loop?
# MAGIC 
# MAGIC ### 📋 Decision Matrix
# MAGIC 
# MAGIC | Location | Action |
# MAGIC |----------|--------|
# MAGIC | Inside `for`/`while` loop | ✅ **Cache outside loop** |
# MAGIC | Inside frequently-called function | ✅ **Cache before calls** |
# MAGIC | At module top level (once) | ❌ No action |
# MAGIC | In conditional that runs once | ❌ No action |

# COMMAND ----------

# ============================================================================
# BC-SC-004: SPARK CONNECT - Schema access in loops
# ============================================================================

import time

def add_computed_columns_bad(df, num_columns=20):
    """
    ❌ BAD: Calls df.columns on every iteration
    """
    start_time = time.time()
    
    for i in range(num_columns):
        col_name = f"computed_{i}"
        # This triggers RPC in Spark Connect - SLOW!
        if col_name not in df.columns:
            df = df.withColumn(col_name, col("fare_amount") * i)
    
    elapsed = time.time() - start_time
    print(f"BAD pattern took: {elapsed:.2f} seconds")
    return df

# Run the bad pattern
result_bad = add_computed_columns_bad(taxi_df.limit(1000), num_columns=20)
print(f"Result columns: {len(result_bad.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ FIX: Cache Schema Outside Loop
# MAGIC 
# MAGIC ```python
# MAGIC def add_computed_columns_good(df, num_columns=20):
# MAGIC     """
# MAGIC     ✅ GOOD: Caches columns once
# MAGIC     """
# MAGIC     start_time = time.time()
# MAGIC     
# MAGIC     # Cache schema ONCE
# MAGIC     existing_columns = set(df.columns)
# MAGIC     
# MAGIC     for i in range(num_columns):
# MAGIC         col_name = f"computed_{i}"
# MAGIC         # No RPC call - just checking local set!
# MAGIC         if col_name not in existing_columns:
# MAGIC             df = df.withColumn(col_name, col("fare_amount") * i)
# MAGIC             existing_columns.add(col_name)  # Update local cache
# MAGIC     
# MAGIC     elapsed = time.time() - start_time
# MAGIC     print(f"GOOD pattern took: {elapsed:.2f} seconds")
# MAGIC     return df
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ⚙️ SECTION 3: CONFIGURATION CHANGES
# MAGIC 
# MAGIC These are **behavioral changes** where defaults changed. Code may run but produce different results.
# MAGIC 
# MAGIC **Important:** Test your code first! Only add config if results are wrong.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-13.3-002: Parquet Timestamp NTZ
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC Default for `spark.sql.parquet.inferTimestampNTZ.enabled` changed, affecting how Parquet files with `TIMESTAMP_NTZ` are read.
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 13.3 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/13.3lts.html)
# MAGIC - [Timestamp Types (TIMESTAMP_NTZ)](https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html)
# MAGIC - [Parquet File Configuration](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
# MAGIC 
# MAGIC ### 📋 When to Add Config
# MAGIC 
# MAGIC | Scenario | Action |
# MAGIC |----------|--------|
# MAGIC | Timestamps look correct | ❌ No config needed |
# MAGIC | Timestamps shifted by hours | ✅ Add config below |
# MAGIC | Different timestamp type in schema | ✅ Add config below |
# MAGIC 
# MAGIC ### ✅ Config Setting
# MAGIC 
# MAGIC ```python
# MAGIC # Add at START of notebook if timestamps are wrong
# MAGIC spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
# MAGIC ```

# COMMAND ----------

print("=== BC-13.3-002: Parquet Timestamp NTZ ===")
try:
    print("Current setting:", spark.conf.get("spark.sql.parquet.inferTimestampNTZ.enabled"))
except:
    print("Current setting: not explicitly set (using DBR default)")
print()
print("To restore DBR 13.3 behavior:")
print('spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## BC-15.4-002: JDBC useNullCalendar
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC Default for `spark.sql.legacy.jdbc.useNullCalendar` changed to `true`, affecting JDBC timestamp handling.
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 15.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/15.4lts.html)
# MAGIC - [JDBC Data Source Configuration](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
# MAGIC 
# MAGIC ### 📋 When to Add Config
# MAGIC 
# MAGIC | Scenario | Action |
# MAGIC |----------|--------|
# MAGIC | JDBC timestamps match source system | ❌ No config needed |
# MAGIC | JDBC timestamps off by hours | ✅ Add config below |
# MAGIC 
# MAGIC ### ✅ Config Setting
# MAGIC 
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")
# MAGIC ```

# COMMAND ----------

print("=== BC-15.4-002: JDBC useNullCalendar ===")
try:
    print("Current setting:", spark.conf.get("spark.sql.legacy.jdbc.useNullCalendar"))
except:
    print("Current setting: not explicitly set (using DBR default)")
print()
print("To restore previous behavior:")
print('spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## BC-16.4-004: MERGE materializeSource=none Disallowed
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC Setting `merge.materializeSource` to `none` is **no longer allowed** and will throw an error.
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 16.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/16.4lts.html)
# MAGIC - [MERGE INTO Documentation](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html)
# MAGIC - [Delta Lake Table Properties](https://docs.delta.io/latest/table-properties.html)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Pattern:** `merge\.materializeSource.*none`
# MAGIC 
# MAGIC ### ✅ Fix
# MAGIC 
# MAGIC | Before | After |
# MAGIC |--------|-------|
# MAGIC | `"none"` | Remove the setting entirely |
# MAGIC | `"none"` | Change to `"auto"` |

# COMMAND ----------

print("=== BC-16.4-004: MERGE materializeSource ===")
print()
print("❌ BROKEN (will throw error):")
print('spark.conf.set("spark.databricks.delta.merge.materializeSource", "none")')
print()
print("✅ FIX: Remove the line OR use 'auto':")
print('spark.conf.set("spark.databricks.delta.merge.materializeSource", "auto")')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## BC-17.3-002: Auto Loader Incremental Listing
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC | Setting | DBR 13.3 | DBR 17.3 |
# MAGIC |---------|----------|----------|
# MAGIC | `cloudFiles.useIncrementalListing` | `"auto"` | `"false"` |
# MAGIC 
# MAGIC **Impact:** Auto Loader may be slower (full directory listing) but more reliable.
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 17.3 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/17.3lts.html)
# MAGIC - [Auto Loader Documentation](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
# MAGIC - [Auto Loader Options Reference](https://docs.databricks.com/en/ingestion/auto-loader/options.html)
# MAGIC 
# MAGIC ### 📋 When to Add Config
# MAGIC 
# MAGIC | Scenario | Action |
# MAGIC |----------|--------|
# MAGIC | Auto Loader performance acceptable | ❌ No config needed |
# MAGIC | Auto Loader much slower | ✅ Add config below |
# MAGIC 
# MAGIC ### ✅ Config Setting
# MAGIC 
# MAGIC ```python
# MAGIC df = (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "parquet")
# MAGIC     .option("cloudFiles.useIncrementalListing", "auto")  # Restore old behavior
# MAGIC     .schema(schema)
# MAGIC     .load(path)
# MAGIC )
# MAGIC ```

# COMMAND ----------

auto_loader_example = """
# ❌ OLD CODE (relies on implicit default)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .load("/path/to/data")
)

# ✅ NEW CODE (explicit setting for old behavior)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useIncrementalListing", "auto")  # ← Add this
    .load("/path/to/data")
)
"""
print("=== BC-17.3-002: Auto Loader Incremental Listing ===")
print(auto_loader_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🔷 SECTION 4: SCALA 2.13 CHANGES (DBR 16.4+)
# MAGIC 
# MAGIC These affect **Scala notebooks and libraries** only. Shown as Python strings for reference.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-16.4-001a: JavaConverters → CollectionConverters
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC `scala.collection.JavaConverters` is deprecated in Scala 2.13, replaced by `scala.jdk.CollectionConverters`.
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 16.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/16.4lts.html)
# MAGIC - [Scala 2.13 Release Notes](https://www.scala-lang.org/news/2.13.0/)
# MAGIC - [Scala 2.13 Collection Migration](https://docs.scala-lang.org/overviews/core/collections-migration-213.html)
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC 
# MAGIC ```
# MAGIC import\s+scala\.collection\.JavaConverters
# MAGIC ```
# MAGIC 
# MAGIC ### ✅ Fix
# MAGIC 
# MAGIC | Before | After |
# MAGIC |--------|-------|
# MAGIC | `import scala.collection.JavaConverters._` | `import scala.jdk.CollectionConverters._` |

# COMMAND ----------

scala_example_1a = '''
// ❌ BEFORE (deprecated in Scala 2.13)
import scala.collection.JavaConverters._
val scalaList = javaList.asScala.toList

// ✅ AFTER
import scala.jdk.CollectionConverters._
val scalaList = javaList.asScala.toList
'''
print("=== BC-16.4-001a: JavaConverters ===")
print(scala_example_1a)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-16.4-001b: .to[Collection] → .to(Collection)
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 16.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/16.4lts.html)
# MAGIC - [Scala 2.13 Collections Changes](https://docs.scala-lang.org/overviews/core/collections-migration-213.html)
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC 
# MAGIC ```
# MAGIC \.to\s*\[\s*(List|Set|Vector|Seq|Array)\s*\]
# MAGIC ```
# MAGIC 
# MAGIC ### ✅ Fix
# MAGIC 
# MAGIC | Before | After |
# MAGIC |--------|-------|
# MAGIC | `seq.to[List]` | `seq.to(List)` |
# MAGIC | `seq.to[Set]` | `seq.to(Set)` |
# MAGIC | `seq.to[Vector]` | `seq.to(Vector)` |

# COMMAND ----------

scala_example_1b = '''
// ❌ BEFORE (compile error in Scala 2.13)
val list = seq.to[List]
val set = seq.to[Set]

// ✅ AFTER
val list = seq.to(List)
val set = seq.to(Set)
'''
print("=== BC-16.4-001b: .to[Collection] Syntax ===")
print(scala_example_1b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-16.4-001c: TraversableOnce → IterableOnce
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 16.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/16.4lts.html)
# MAGIC - [Scala 2.13 Collections Changes](https://docs.scala-lang.org/overviews/core/collections-migration-213.html)
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC 
# MAGIC ```
# MAGIC \bTraversableOnce\b
# MAGIC ```
# MAGIC 
# MAGIC ### ✅ Fix: Replace `TraversableOnce` with `IterableOnce`

# COMMAND ----------

scala_example_1c = '''
// ❌ BEFORE (compile error in Scala 2.13)
def process(items: TraversableOnce[String]): Unit = {
  items.foreach(println)
}

// ✅ AFTER
def process(items: IterableOnce[String]): Unit = {
  items.iterator.foreach(println)
}
'''
print("=== BC-16.4-001c: TraversableOnce ===")
print(scala_example_1c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-16.4-001d: Traversable → Iterable
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 16.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/16.4lts.html)
# MAGIC - [Scala 2.13 Collections Changes](https://docs.scala-lang.org/overviews/core/collections-migration-213.html)
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC 
# MAGIC ```
# MAGIC \bTraversable\b(?!Once)
# MAGIC ```
# MAGIC 
# MAGIC ### ✅ Fix: Replace `Traversable` with `Iterable`

# COMMAND ----------

scala_example_1d = '''
// ❌ BEFORE (compile error in Scala 2.13)
def process(data: Traversable[Int]): Int = data.sum

// ✅ AFTER
def process(data: Iterable[Int]): Int = data.sum
'''
print("=== BC-16.4-001d: Traversable ===")
print(scala_example_1d)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-16.4-001e: Stream → LazyList
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 16.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/16.4lts.html)
# MAGIC - [Scala 2.13 LazyList Documentation](https://www.scala-lang.org/api/2.13.0/scala/collection/immutable/LazyList.html)
# MAGIC - [Scala 2.13 Collections Changes](https://docs.scala-lang.org/overviews/core/collections-migration-213.html)
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC 
# MAGIC ```
# MAGIC \bStream\s*\.\s*(from|continually|iterate|empty|cons)
# MAGIC ```
# MAGIC 
# MAGIC ### ✅ Fix: Replace `Stream` with `LazyList`

# COMMAND ----------

scala_example_1e = '''
// ❌ BEFORE (deprecated in Scala 2.13)
val numbers = Stream.from(1)
val randoms = Stream.continually(scala.util.Random.nextInt())

// ✅ AFTER
val numbers = LazyList.from(1)
val randoms = LazyList.continually(scala.util.Random.nextInt())
'''
print("=== BC-16.4-001e: Stream → LazyList ===")
print(scala_example_1e)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 COMPLETE SUMMARY
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Reference: All Breaking Changes
# MAGIC 
# MAGIC ### 🔴 Auto-Fix (7 patterns)
# MAGIC 
# MAGIC | ID | Pattern | Fix | File Types |
# MAGIC |----|---------|-----|------------|
# MAGIC | BC-17.3-001 | `input_file_name()` | `_metadata.file_name` | .py, .sql, .scala |
# MAGIC | BC-15.4-003 | `IF !`, `IS !`, `! IN` | Use `NOT` | .sql |
# MAGIC | BC-16.4-001a | `JavaConverters` | `CollectionConverters` | .scala |
# MAGIC | BC-16.4-001b | `.to[List]` | `.to(List)` | .scala |
# MAGIC | BC-16.4-001c | `TraversableOnce` | `IterableOnce` | .scala |
# MAGIC | BC-16.4-001d | `Traversable` | `Iterable` | .scala |
# MAGIC | BC-16.4-001e | `Stream.from()` | `LazyList.from()` | .scala |
# MAGIC 
# MAGIC ### 🟡 Manual Review (6 patterns)
# MAGIC 
# MAGIC | ID | Pattern | Decision |
# MAGIC |----|---------|----------|
# MAGIC | BC-15.4-001 | `VariantType()` in UDF | Skip if upgrading to 16.4+ |
# MAGIC | BC-15.4-004 | `CREATE VIEW (col TYPE)` | Remove types, cast in SELECT |
# MAGIC | BC-SC-001 | try/except around transforms | Add `df.columns` for validation |
# MAGIC | BC-SC-002 | Same temp view name reused | Add UUID to names |
# MAGIC | BC-SC-003 | UDF captures external variable | Use function factory |
# MAGIC | BC-SC-004 | `df.columns` in loop | Cache outside loop |
# MAGIC 
# MAGIC ### ⚙️ Config Settings (4 patterns)
# MAGIC 
# MAGIC | ID | Test First | Config If Needed |
# MAGIC |----|------------|------------------|
# MAGIC | BC-13.3-002 | Check Parquet timestamps | `spark.sql.parquet.inferTimestampNTZ.enabled` = `false` |
# MAGIC | BC-15.4-002 | Check JDBC timestamps | `spark.sql.legacy.jdbc.useNullCalendar` = `false` |
# MAGIC | BC-16.4-004 | Check for `"none"` setting | Remove or change to `"auto"` |
# MAGIC | BC-17.3-002 | Check Auto Loader speed | `cloudFiles.useIncrementalListing` = `"auto"` |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🎯 Demo Instructions: Using Databricks Assistant
# MAGIC 
# MAGIC ### Step 1: Scan for Breaking Changes
# MAGIC 
# MAGIC Open Databricks Assistant (agent mode) and say:
# MAGIC 
# MAGIC ```
# MAGIC Scan this notebook for breaking changes when upgrading to DBR 17.3
# MAGIC ```
# MAGIC 
# MAGIC **Expected Result:** Assistant identifies all 17 breaking change patterns
# MAGIC 
# MAGIC ### Step 2: Review the Findings
# MAGIC 
# MAGIC Assistant will categorize findings:
# MAGIC - 🔴 **HIGH** - Will fail immediately, can be auto-fixed
# MAGIC - 🟡 **MEDIUM** - May cause issues, needs review
# MAGIC - ⚙️ **CONFIG** - Behavioral change, test first
# MAGIC 
# MAGIC ### Step 3: Apply Auto-Fixes
# MAGIC 
# MAGIC ```
# MAGIC Fix all the auto-fixable breaking changes in this notebook
# MAGIC ```
# MAGIC 
# MAGIC ### Step 4: Review Manual Items
# MAGIC 
# MAGIC For each manual review item, ask:
# MAGIC 
# MAGIC ```
# MAGIC Help me fix [BC-SC-002] - the temp view name reuse in the process_batch function
# MAGIC ```
# MAGIC 
# MAGIC ### Step 5: Validate
# MAGIC 
# MAGIC ```
# MAGIC Validate that all breaking changes have been addressed
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up temp views
spark.catalog.dropTempView("taxi_trips")
spark.catalog.dropTempView("current_batch")

print("Cleanup complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 📚 Additional Resources
# MAGIC 
# MAGIC | Resource | Description |
# MAGIC |----------|-------------|
# MAGIC | [BREAKING-CHANGES-EXPLAINED.md](../developer-guide/BREAKING-CHANGES-EXPLAINED.md) | Detailed guide with decision matrices |
# MAGIC | [SKILL.md](../databricks-dbr-migration/SKILL.md) | Agent Skill specification |
# MAGIC | [QUICK-REFERENCE.md](../databricks-dbr-migration/references/QUICK-REFERENCE.md) | ❌/🔍/✅ format reference |
# MAGIC | [Databricks Release Notes](https://docs.databricks.com/release-notes/runtime/supported.html) | Official documentation |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC *Last Updated: January 2026*
