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
# MAGIC | 🔴 **Auto-Fix** | 10 | Agent automatically applies fix |
# MAGIC | 🔧 **Assisted Fix** | 11 | Agent provides fix snippet, developer reviews |
# MAGIC | 🟡 **Manual Review** | 10 | Agent flags for developer decision |
# MAGIC 
# MAGIC ### Quick Reference Table
# MAGIC 
# MAGIC | ID | Severity | Change | DBR | Category |
# MAGIC |----|----------|--------|-----|----------|
# MAGIC | BC-17.3-001 | 🔴 HIGH | `input_file_name()` removed | 17.3 | 🔴 Auto-Fix |
# MAGIC | BC-15.4-003 | 🟡 MEDIUM | `!` syntax for NOT | 15.4 | 🔴 Auto-Fix |
# MAGIC | BC-16.4-001a-i | 🔴 HIGH | Scala 2.13 collection changes | 16.4 | 🔴 Auto-Fix |
# MAGIC | BC-SC-002 | 🟡 MEDIUM | Temp view name reuse | 13.3+ | 🔧 Assisted Fix |
# MAGIC | BC-SC-003 | 🟢 LOW | UDF late binding | 14.3+ | 🔧 Assisted Fix |
# MAGIC | BC-SC-004 | 🟢 LOW | Schema access in loops | 13.3+ | 🔧 Assisted Fix |
# MAGIC | BC-17.3-005 | 🟢 LOW | Decimal precision defaults | 17.3 | 🔧 Assisted Fix |
# MAGIC | BC-13.3-002 | 🟢 LOW | Parquet timestamp NTZ | 13.3 | 🔧 Assisted Fix |
# MAGIC | BC-15.4-002 | 🟢 LOW | JDBC useNullCalendar | 15.4 | 🔧 Assisted Fix |
# MAGIC | BC-15.4-005 | 🟢 LOW | JDBC read timestamp handling | 15.4 | 🔧 Assisted Fix |
# MAGIC | BC-16.4-003 | 🟡 MEDIUM | Data source cache options | 16.4 | 🔧 Assisted Fix |
# MAGIC | BC-16.4-004 | 🟢 LOW | MERGE materializeSource | 16.4 | 🔧 Assisted Fix |
# MAGIC | BC-16.4-006 | 🟡 MEDIUM | Auto Loader cleanSource | 16.4 | 🔧 Assisted Fix |
# MAGIC | BC-17.3-002 | 🟡 MEDIUM | Auto Loader incremental listing | 17.3 | 🔧 Assisted Fix |
# MAGIC | BC-13.3-001 | 🔴 HIGH | MERGE INTO type casting (ANSI) | 13.3 | 🟡 Manual Review |
# MAGIC | BC-16.4-002 | 🔴 HIGH | HashMap/HashSet ordering | 16.4 | 🟡 Manual Review |
# MAGIC | BC-15.4-001 | 🟡 MEDIUM | VARIANT in UDF | 15.4+ | 🟡 Manual Review |
# MAGIC | BC-15.4-004 | 🟢 LOW | VIEW column types | 15.4 | 🟡 Manual Review |
# MAGIC | BC-15.4-006 | 🟡 MEDIUM | VIEW schema binding mode | 15.4 | 🟡 Manual Review |
# MAGIC | BC-13.3-003 | 🟡 MEDIUM | overwriteSchema + dynamic partition | 13.3 | 🟡 Manual Review |
# MAGIC | BC-SC-001 | 🔴 HIGH | Lazy schema analysis | 13.3+ | 🟡 Manual Review |
# MAGIC | BC-17.3-003 | 🟢 LOW | Spark Connect null handling | 17.3 | 🟡 Manual Review |

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
# MAGIC ### 💡 In Simple Terms
# MAGIC 
# MAGIC There's a function called `input_file_name()` that many ETL pipelines use to find out **which file a row of data originally came from**. For example, if you read 10 CSV files into a DataFrame, `input_file_name()` tells you "this row came from `file_3.csv`."
# MAGIC 
# MAGIC **The problem:** This function has been **completely deleted** in DBR 17.3. If your code calls it, the notebook will crash immediately with an error. It won't just give a warning -- it will **stop your job**.
# MAGIC 
# MAGIC **The fix:** Use `_metadata.file_name` instead. This is a built-in hidden column that Spark adds to every DataFrame automatically. It does the same thing, but it's the officially supported way.
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
# MAGIC 
# MAGIC > **From [DBR 17.3 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/17.3lts.html):**
# MAGIC >
# MAGIC > *"The `input_file_name` function has been deprecated since Databricks Runtime 13.3 LTS because it is unreliable. The function is no longer supported in Databricks Runtime 17.3 LTS and above because it is unreliable. Use `_metadata.file_name` instead."*
# MAGIC 
# MAGIC - [File Metadata Column Documentation](https://docs.databricks.com/en/ingestion/file-metadata-column.html)
# MAGIC - [Spark 4.0 Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-33-to-40)
# MAGIC - SPARK-53507: Add breaking change info to errors
# MAGIC 
# MAGIC ### Why `_metadata.file_name` is Better
# MAGIC 
# MAGIC | Feature | `input_file_name()` | `_metadata.file_name` |
# MAGIC |---------|---------------------|----------------------|
# MAGIC | Reliability | Unreliable with reused exchanges | Always correct |
# MAGIC | Performance | Blocks certain optimizations | Part of metadata column |
# MAGIC | Future support | Removed in Spark 4.0 | Supported long-term |
# MAGIC | Extra metadata | Only file name | Also: `file_path`, `file_size`, `file_modification_time` |
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
# MAGIC 
# MAGIC **SQL equivalent:**
# MAGIC ```sql
# MAGIC -- BEFORE
# MAGIC SELECT input_file_name(), col1, col2 FROM my_table
# MAGIC 
# MAGIC -- AFTER
# MAGIC SELECT _metadata.file_name, col1, col2 FROM my_table
# MAGIC ```

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
# MAGIC ## BC-13.3-001: MERGE INTO Type Casting (ANSI Mode)
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC MERGE INTO and UPDATE operations now follow `spark.sql.storeAssignmentPolicy` (default: ANSI). Values that **overflow** the target column type now **throw an error** instead of silently storing NULL.
# MAGIC 
# MAGIC | DBR Version | Overflow Behavior |
# MAGIC |-------------|-------------------|
# MAGIC | < 13.3 | ⚠️ Silently stores NULL |
# MAGIC | **13.3+** | ❌ **CAST_OVERFLOW error** |
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC - [DBR 13.3 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/13.3lts.html)
# MAGIC - [ANSI Mode](https://docs.databricks.com/en/sql/language-manual/sql-ref-ansi-compliance.html)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Detection Pattern:** `\bMERGE\s+INTO\b`
# MAGIC 
# MAGIC Flags all MERGE INTO statements for review of type casting compatibility.
# MAGIC 
# MAGIC ### 📋 Decision Matrix
# MAGIC 
# MAGIC | Source Type | Target Type | Action |
# MAGIC |-------------|-------------|--------|
# MAGIC | BIGINT | INT | ⚠️ Potential overflow - add explicit CAST with bounds check |
# MAGIC | DOUBLE | DECIMAL | ⚠️ Precision loss - verify or widen target |
# MAGIC | Same types | Same types | ✅ No action needed |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- BC-13.3-001: MERGE INTO with potential type overflow
# MAGIC -- ============================================================================
# MAGIC 
# MAGIC -- ❌ POTENTIAL ISSUE: If source.value > 2147483647, this will throw CAST_OVERFLOW
# MAGIC -- MERGE INTO target_table AS t
# MAGIC -- USING source_table AS s
# MAGIC -- ON t.id = s.id
# MAGIC -- WHEN MATCHED THEN UPDATE SET t.int_column = s.bigint_column;  -- BIGINT → INT
# MAGIC 
# MAGIC -- ✅ FIX: Add explicit bounds checking
# MAGIC -- MERGE INTO target_table AS t
# MAGIC -- USING source_table AS s  
# MAGIC -- ON t.id = s.id
# MAGIC -- WHEN MATCHED THEN UPDATE SET
# MAGIC --     t.int_column = CASE 
# MAGIC --         WHEN s.bigint_column > 2147483647 THEN NULL 
# MAGIC --         WHEN s.bigint_column < -2147483648 THEN NULL
# MAGIC --         ELSE CAST(s.bigint_column AS INT) 
# MAGIC --     END;
# MAGIC 
# MAGIC SELECT 'BC-13.3-001: MERGE INTO type casting - review all MERGE statements' as guidance;

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
# MAGIC # 🔧 SECTION 2: ASSISTED FIX & MANUAL REVIEW ITEMS
# MAGIC 
# MAGIC This section covers patterns that **require developer involvement**:
# MAGIC - **🔧 Assisted Fix** — Agent provides a suggested code snippet; developer reviews and applies
# MAGIC - **🟡 Manual Review** — Agent flags the pattern; developer decides on the fix using their judgment
# MAGIC 
# MAGIC Each item below is labeled with its category.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟡 BC-15.4-001: VARIANT Type in Python UDF [Manual Review]
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC Python UDFs using `VARIANT` type as input or output **may throw exceptions** starting in DBR 15.4+.
# MAGIC 
# MAGIC | DBR Version | VARIANT in UDF |
# MAGIC |-------------|----------------|
# MAGIC | 13.3 LTS | N/A (VARIANT not available) |
# MAGIC | 14.3 LTS | ✅ Works |
# MAGIC | **15.4+ LTS** | ⚠️ **May fail** - test your specific use case |
# MAGIC 
# MAGIC > ⚠️ **REVIEW RECOMMENDED:** Test VARIANT UDFs on your target DBR version. Consider using StringType + JSON as a safer alternative.
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
# MAGIC | Scenario | Action |
# MAGIC |----------|--------|
# MAGIC | UDF uses VariantType | ⚠️ **TEST** on target DBR or use StringType + json.dumps |
# MAGIC | Complex nested VARIANT | ✅ **FIX**: Use StringType + json.dumps for safety |

# COMMAND ----------

# ============================================================================
# BC-15.4-001: VARIANT type in Python UDF
# ============================================================================
# This code MAY FAIL on DBR 15.4+ - test on your target version!

from pyspark.sql.functions import udf
from pyspark.sql.types import VariantType, StringType

# ⚠️ Pattern 1: UDF returning VARIANT (may throw exception in 15.4+)
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
# MAGIC ### ✅ RECOMMENDED FIX (Safer Alternative)
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
# MAGIC > **Note:** Using StringType + JSON is the safest approach for cross-version compatibility.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🟡 BC-15.4-004: VIEW Column Type Definitions [Manual Review]
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
# MAGIC ## 🟡 BC-SC-001: Lazy Schema Analysis (Spark Connect) [Manual Review]
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
# MAGIC ## 🔧 BC-SC-002: Temp View Name Reuse (Spark Connect) [Assisted Fix]
# MAGIC 
# MAGIC ### 💡 In Simple Terms
# MAGIC 
# MAGIC Think of a temp view like a **whiteboard label** in a shared office. In Spark Classic, each person who writes on the whiteboard gets their own copy -- even if they use the same label. So if Alice writes "batch" with 100 rows and Bob writes "batch" with 500 rows, they each keep their own data.
# MAGIC 
# MAGIC In **Spark Connect** (Serverless), the whiteboard is **shared**. If Alice and Bob both use the label "batch," whoever writes last wins -- and Alice's reference now points to Bob's data!
# MAGIC 
# MAGIC **When does this apply?** Only if you use **Serverless compute** or **Databricks Connect**. If you're on Classic clusters, this doesn't affect you today (but it will if you switch later).
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
# MAGIC 
# MAGIC > **From [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic):**
# MAGIC >
# MAGIC > *"DataFrames that reference temp views are resolved by name in Spark Connect. If a temp view is replaced, all DataFrames referencing that view name will reflect the new data."*
# MAGIC 
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
# MAGIC 
# MAGIC ### 💡 How to Spot This
# MAGIC 
# MAGIC Search for `createOrReplaceTempView` in your code. If the same string appears in multiple calls, or if the call is inside a loop or a function called multiple times, you have a potential issue.

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
# MAGIC **Option A: UUID suffix (guaranteed unique)**
# MAGIC ```python
# MAGIC import uuid
# MAGIC 
# MAGIC def process_batch(data_size, batch_name):
# MAGIC     """
# MAGIC     ✅ GOOD: Unique view name for each call (UUID suffix)
# MAGIC     """
# MAGIC     df = spark.range(data_size).withColumn("batch", lit(batch_name))
# MAGIC     
# MAGIC     # Unique name with UUID -- no collisions possible
# MAGIC     unique_view = f"batch_{batch_name}_{uuid.uuid4().hex[:8]}"
# MAGIC     df.createOrReplaceTempView(unique_view)
# MAGIC     
# MAGIC     return spark.table(unique_view)
# MAGIC ```
# MAGIC 
# MAGIC **Option B: Descriptive unique names (easier to debug)**
# MAGIC ```python
# MAGIC def process_batch(data_size, batch_name):
# MAGIC     """
# MAGIC     ✅ GOOD: Unique view name using meaningful identifiers
# MAGIC     """
# MAGIC     df = spark.range(data_size).withColumn("batch", lit(batch_name))
# MAGIC     
# MAGIC     # Descriptive name -- unique because batch_name is unique per call
# MAGIC     unique_view = f"batch_{batch_name}_{data_size}"
# MAGIC     df.createOrReplaceTempView(unique_view)
# MAGIC     
# MAGIC     return spark.table(unique_view)
# MAGIC ```
# MAGIC 
# MAGIC **Expected result after fix:**
# MAGIC ```
# MAGIC Batch 1 count: 100   ✓ Correct on both Classic and Connect
# MAGIC Batch 2 count: 500   ✓ Correct on both Classic and Connect
# MAGIC Batch 3 count: 50    ✓ Correct on both Classic and Connect
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🔧 BC-SC-003: UDF Late Binding (Spark Connect) [Assisted Fix]
# MAGIC 
# MAGIC ### 💡 In Simple Terms
# MAGIC 
# MAGIC Imagine you're at a restaurant. You tell the waiter "I want the daily special" at noon when it's grilled salmon. But the kitchen doesn't prepare your order until 6pm, by which time the daily special has changed to pasta.
# MAGIC 
# MAGIC - In **Spark Classic**: You get grilled salmon (what was the special when you ordered).
# MAGIC - In **Spark Connect**: You get pasta (what's the special when the kitchen runs your order).
# MAGIC 
# MAGIC The same thing happens with UDFs. If your UDF references a variable like `fare_multiplier`, Spark Classic captures its value at definition time, but Spark Connect captures it when the UDF actually runs -- which could be much later, after the variable has changed.
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
# MAGIC 
# MAGIC > **From [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic):**
# MAGIC >
# MAGIC > *"UDFs in Spark Connect are serialized and sent to the server at execution time. Any variables captured by the UDF closure are evaluated at execution time, not at definition time."*
# MAGIC 
# MAGIC - [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
# MAGIC - [User-Defined Functions (UDFs)](https://docs.databricks.com/en/udf/index.html)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Pattern:** `@udf` decorators (flagged for review)
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
# MAGIC 
# MAGIC ### 💡 How to Spot This (Red Flag)
# MAGIC 
# MAGIC If you see this pattern, it's a red flag:
# MAGIC ```python
# MAGIC some_variable = ...           # Defined outside UDF
# MAGIC @udf("double")
# MAGIC def my_udf(x):
# MAGIC     return x * some_variable  # ← References outer variable
# MAGIC ```

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
# MAGIC The factory pattern creates a UDF with a specific value **baked in**. Changing the original variable later has no effect.
# MAGIC 
# MAGIC ```python
# MAGIC # THE FACTORY: A function that CREATES a UDF with a specific multiplier baked in
# MAGIC def make_surge_udf(multiplier):
# MAGIC     """
# MAGIC     GOOD PATTERN: The 'multiplier' parameter is captured at the moment
# MAGIC     you call make_surge_udf(). It's now locked in -- changing the
# MAGIC     original variable later has no effect on this UDF.
# MAGIC     """
# MAGIC     @udf("double")
# MAGIC     def apply_surge(fare):
# MAGIC         return fare * multiplier  # Uses the locked-in value, not a global variable
# MAGIC     return apply_surge
# MAGIC 
# MAGIC # Step 1: Set the variable to 1.0
# MAGIC fare_multiplier = 1.0
# MAGIC 
# MAGIC # Step 2: Create the UDF by passing fare_multiplier INTO the factory
# MAGIC # The factory captures the CURRENT value (1.0) as a snapshot
# MAGIC surge_udf = make_surge_udf(fare_multiplier)  # Locks in 1.0
# MAGIC 
# MAGIC # Step 3: Change the variable AFTER the UDF was created
# MAGIC fare_multiplier = 2.5  # Doesn't matter! The UDF already locked in 1.0
# MAGIC 
# MAGIC # Step 4: Apply the UDF -- it ALWAYS uses 1.0, regardless of fare_multiplier
# MAGIC df.withColumn("surge_fare", surge_udf(col("fare_amount")))
# MAGIC # Uses 1.0 on BOTH Classic and Connect ✓
# MAGIC 
# MAGIC # You can also create multiple UDFs with different locked-in values:
# MAGIC udf_normal  = make_surge_udf(1.0)   # Always multiplies by 1.0
# MAGIC udf_peak    = make_surge_udf(2.5)   # Always multiplies by 2.5
# MAGIC udf_holiday = make_surge_udf(3.0)   # Always multiplies by 3.0
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🔧 BC-SC-004: Schema Access in Loops (Spark Connect) [Assisted Fix]
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
# MAGIC 
# MAGIC ## 🟡 BC-17.3-003: Spark Connect Null Handling in Literals [Manual Review]
# MAGIC 
# MAGIC ### 💡 In Simple Terms
# MAGIC 
# MAGIC Imagine you have a list: `[1, null, 3]`. In Spark Classic, constructing this as an array literal would **silently replace** the null with a default value (like `0`), giving you `[1, 0, 3]`. In Spark Connect, the null is **preserved** as `[1, null, 3]`.
# MAGIC 
# MAGIC The new behavior is actually **more correct** -- a null should stay a null. But if your downstream code assumed nulls were already gone (e.g., doing `sum()` without handling nulls), you might get unexpected results.
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC In **Spark Connect** (Serverless), `array()`, `map()`, and `struct()` literal constructions now **preserve nulls** instead of coercing them to type defaults.
# MAGIC 
# MAGIC | Construction | Spark Classic | Spark Connect |
# MAGIC |-------------|--------------|---------------|
# MAGIC | `array(lit(1), lit(None), lit(3))` | `[1, 0, 3]` | `[1, null, 3]` |
# MAGIC | `struct(lit("a"), lit(None))` | `("a", "")` | `("a", null)` |
# MAGIC | `map(lit("k"), lit(None))` | `{"k": 0}` | `{"k": null}` |
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC 
# MAGIC > **From [DBR 17.3 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/17.3lts.html):**
# MAGIC >
# MAGIC > *"Spark Connect literal handling has been updated to correctly represent null values in array, map, and struct constructions. Previously, null values were coerced to type defaults."*
# MAGIC 
# MAGIC - SPARK-53553: Spark Connect literal null handling
# MAGIC - [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Detection Patterns:**
# MAGIC ```
# MAGIC \b(array|map|struct)\s*\(
# MAGIC ```
# MAGIC 
# MAGIC The agent flags these for human review when nullable columns are used inside literal constructions.
# MAGIC 
# MAGIC ### 📋 When Do You Need to Act?
# MAGIC 
# MAGIC | Scenario | Action |
# MAGIC |----------|--------|
# MAGIC | Downstream code already handles nulls | ❌ No action -- new behavior is better |
# MAGIC | Downstream `sum()`/`avg()` that assumed no nulls | ⚠️ Add `coalesce()` |
# MAGIC | Schema comparisons in tests | ⚠️ Update expected results |
# MAGIC | Data written to Delta tables with NOT NULL constraints | ⚠️ Add `coalesce()` |

# COMMAND ----------

# ============================================================================
# BC-17.3-003: SPARK CONNECT - Null handling in literal constructions
# ============================================================================

from pyspark.sql.functions import array, struct, coalesce

# ❌ PROBLEM: In Spark Connect, nulls in array/struct literals are now preserved
# In Spark Classic this would coerce None → default (0 for int, "" for string)
# In Spark Connect this preserves the null

example_df = taxi_df.limit(5).select(
    col("fare_amount"),
    col("tip_amount"),
    # This array may contain null if tip_amount is null
    array(col("fare_amount"), col("tip_amount")).alias("amounts_array"),
    # This struct may contain null fields
    struct(col("fare_amount"), col("tip_amount")).alias("amounts_struct")
)

print("=== BC-17.3-003: Null Handling in Literals ===")
print("If tip_amount is null, Spark Connect preserves the null in array/struct.")
print("Spark Classic would coerce it to 0.")
example_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ FIX: Explicit Null Handling with `coalesce()`
# MAGIC 
# MAGIC Only apply this fix if your downstream logic assumes nulls were already removed.
# MAGIC 
# MAGIC ```python
# MAGIC from pyspark.sql.functions import array, struct, coalesce, lit
# MAGIC 
# MAGIC # AFTER: Explicitly handle nulls (only if you need to!)
# MAGIC safe_array = array(
# MAGIC     coalesce(col("fare_amount"), lit(0.0)),
# MAGIC     coalesce(col("tip_amount"), lit(0.0))
# MAGIC ).alias("amounts_array")
# MAGIC 
# MAGIC safe_struct = struct(
# MAGIC     coalesce(col("fare_amount"), lit(0.0)).alias("fare"),
# MAGIC     coalesce(col("tip_amount"), lit(0.0)).alias("tip")
# MAGIC ).alias("amounts_struct")
# MAGIC ```
# MAGIC 
# MAGIC > **Remember:** The new null-preserving behavior is often **more correct**. Only add `coalesce()` if your downstream code specifically needs the old behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🔧 BC-17.3-005: Spark Connect Decimal Precision [Assisted Fix]
# MAGIC 
# MAGIC ### 💡 In Simple Terms
# MAGIC 
# MAGIC When you write `col("amount").cast("decimal")` without specifying precision and scale, Spark has to pick a default. In Spark Classic, this default varies. In Spark Connect, it always uses `SYSTEM_DEFAULT (38, 18)` -- which is a very wide decimal with 38 total digits and 18 after the decimal point.
# MAGIC 
# MAGIC **The important thing:** This change only affects **execution plans** (the logical blueprint Spark generates). It does **NOT** change your actual query results. However, if you have tests that compare plan outputs or if you rely on specific precision for downstream schema validation, you'll need to update.
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC In **Spark Connect**, decimal precision for literals in `array()`, `map()`, and `struct()` defaults to `SYSTEM_DEFAULT (38, 18)` instead of the narrower inferred precision.
# MAGIC 
# MAGIC | Context | Spark Classic | Spark Connect |
# MAGIC |---------|--------------|---------------|
# MAGIC | `array(lit(1.5), lit(2.5))` | `DecimalType(2,1)` in plan | `DecimalType(38,18)` in plan |
# MAGIC | Query result | `1.5, 2.5` | `1.5, 2.5` (same!) |
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC 
# MAGIC > **From [DBR 17.3 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/17.3lts.html):**
# MAGIC >
# MAGIC > *"Spark Connect uses SYSTEM_DEFAULT precision for decimal types in literal constructions. This affects only the logical plan representation, not the actual query results."*
# MAGIC 
# MAGIC - [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)
# MAGIC 
# MAGIC ### 🔍 How the Agent Detects It
# MAGIC 
# MAGIC **Detection Patterns:**
# MAGIC ```
# MAGIC DecimalType\s*\(
# MAGIC \.cast\s*\(\s*["']decimal["']\s*\)
# MAGIC ```
# MAGIC 
# MAGIC The risky pattern is bare `cast("decimal")` without explicit precision.
# MAGIC 
# MAGIC ### 📋 Quick Guide to Choosing Precision/Scale
# MAGIC 
# MAGIC | Use Case | Recommended | Example |
# MAGIC |----------|-------------|---------|
# MAGIC | Currency (USD/EUR) | `DecimalType(10, 2)` | `$12345678.90` |
# MAGIC | Tax rates / percentages | `DecimalType(5, 4)` | `0.0825` |
# MAGIC | Scientific measurements | `DecimalType(20, 10)` | `1234567890.1234567890` |
# MAGIC | General-purpose / unsure | `DecimalType(38, 18)` | Maximum precision |

# COMMAND ----------

# ============================================================================
# BC-17.3-005: SPARK CONNECT - Decimal precision in literal constructions
# ============================================================================

from pyspark.sql.types import DecimalType

# ❌ RISKY PATTERN: Bare cast without explicit precision
# In Spark Connect, this will use DecimalType(38, 18) instead of inferred precision
risky_df = taxi_df.limit(5).select(
    col("fare_amount"),
    col("fare_amount").cast("decimal").alias("fare_bare_cast"),  # ← No precision specified!
)

# ✅ GOOD PATTERN: Explicit precision and scale
safe_df = taxi_df.limit(5).select(
    col("fare_amount"),
    col("fare_amount").cast(DecimalType(10, 2)).alias("fare_explicit"),  # ← Explicit!
)

print("=== BC-17.3-005: Decimal Precision ===")
print("Bare cast (risky -- plan changes in Spark Connect):")
risky_df.printSchema()
risky_df.show()
print("\nExplicit precision (safe -- consistent on Classic and Connect):")
safe_df.printSchema()
safe_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ FIX: Always Specify Explicit Precision
# MAGIC 
# MAGIC ```python
# MAGIC from pyspark.sql.types import DecimalType
# MAGIC 
# MAGIC # ❌ BEFORE: Bare cast (plan will differ in Spark Connect)
# MAGIC df.withColumn("amount", col("amount").cast("decimal"))
# MAGIC 
# MAGIC # ✅ AFTER: Explicit precision and scale
# MAGIC df.withColumn("amount", col("amount").cast(DecimalType(10, 2)))
# MAGIC ```
# MAGIC 
# MAGIC > **Note:** This only affects execution plans, not query results. No action needed unless you have plan-comparison tests or strict schema validation.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🔧 SECTION 3: ASSISTED FIX — CONFIGURATION CHANGES
# MAGIC 
# MAGIC These are **behavioral changes** where Spark configuration defaults changed. Code may run but produce different results.
# MAGIC 
# MAGIC The agent provides a **commented config line** as the fix snippet. **Test your code first** — only uncomment the config if results are wrong.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 BC-13.3-002: Parquet Timestamp NTZ [Assisted Fix]
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
# MAGIC ## 🔧 BC-15.4-002: JDBC useNullCalendar [Assisted Fix]
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
# MAGIC ## 🔧 BC-15.4-005: JDBC Read Timestamp Handling [Assisted Fix]
# MAGIC 
# MAGIC ### 💡 In Simple Terms
# MAGIC 
# MAGIC When Spark reads timestamps from a JDBC database (like Oracle, SQL Server, or PostgreSQL), it needs to know how to interpret the raw timestamp bytes. Before DBR 15.4, Spark used the **Gregorian calendar** to interpret these bytes. Starting in DBR 15.4, the default changed to a "**null calendar**" (proleptic Gregorian), which handles edge-case dates differently.
# MAGIC 
# MAGIC **Who is affected?** Only teams that read from external databases using JDBC (`.format("jdbc")` or `.jdbc()`). If you only read from Delta, Parquet, or CSV files, skip this.
# MAGIC 
# MAGIC **The risk:** Timestamps from JDBC may shift by hours or days for dates before October 15, 1582 (when the Gregorian calendar was adopted). For modern dates, the difference is usually zero, but you should verify.
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC Default for `spark.sql.legacy.jdbc.useNullCalendar` changed to `true` in DBR 15.4.
# MAGIC 
# MAGIC | Setting | DBR < 15.4 | DBR 15.4+ |
# MAGIC |---------|-----------|-----------|
# MAGIC | `useNullCalendar` default | `false` (Gregorian) | `true` (null/proleptic) |
# MAGIC 
# MAGIC ### 📚 Official Documentation
# MAGIC 
# MAGIC > **From [DBR 15.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/15.4lts.html):**
# MAGIC >
# MAGIC > *"The default value of `spark.sql.legacy.jdbc.useNullCalendar` has been changed to `true`. This may affect timestamp interpretation when reading from JDBC sources."*
# MAGIC 
# MAGIC - [JDBC Data Source Configuration](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
# MAGIC 
# MAGIC ### 📋 3-Step Testing Process
# MAGIC 
# MAGIC | Step | Action | What to Check |
# MAGIC |------|--------|---------------|
# MAGIC | 1 | Run JDBC read on DBR 17.3 | Do timestamps match your source database? |
# MAGIC | 2 | If wrong, add config | `spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")` |
# MAGIC | 3 | Re-run and verify | Timestamps now match source |
# MAGIC 
# MAGIC > **Difference from BC-15.4-002:** BC-15.4-002 covers the general `useNullCalendar` config setting. BC-15.4-005 specifically covers JDBC read patterns (`.jdbc()` or `.format("jdbc")`) that may be affected by this config change.

# COMMAND ----------

# ============================================================================
# BC-15.4-005: JDBC Read Timestamp Handling
# ============================================================================

print("=== BC-15.4-005: JDBC Read Timestamp Handling ===")
print()

jdbc_example = """
# ❌ JDBC read that may have different timestamp behavior on DBR 15.4+
df = (spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://host:5432/database")
    .option("dbtable", "transactions")
    .option("user", "username")
    .option("password", "password")
    .load()
)

# ✅ FIX: If timestamps are wrong, add this BEFORE the JDBC read:
# spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")

# Then re-run the JDBC read and verify timestamps match your source database.
"""
print(jdbc_example)
print("Test first on DBR 17.3. Only add the config if timestamps differ from source.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🔧 BC-16.4-004: MERGE materializeSource=none Disallowed [Assisted Fix]
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
# MAGIC ## 🔧 BC-17.3-002: Auto Loader Incremental Listing [Assisted Fix]
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
# MAGIC ## BC-16.4-001f: .toIterator → .iterator
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC ```
# MAGIC \.toIterator\b
# MAGIC ```

# COMMAND ----------

scala_example_1f = '''
// ❌ BEFORE (deprecated in Scala 2.13)
val iter = myList.toIterator

// ✅ AFTER
val iter = myList.iterator
'''
print("=== BC-16.4-001f: .toIterator Deprecated ===")
print(scala_example_1f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-16.4-001g: .view.force → .view.to(List)
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC ```
# MAGIC \.view\s*\.\s*force\b
# MAGIC ```

# COMMAND ----------

scala_example_1g = '''
// ❌ BEFORE (deprecated in Scala 2.13)
val result = myList.view.map(_ * 2).force

// ✅ AFTER
val result = myList.view.map(_ * 2).to(List)
// or
val result = myList.view.map(_ * 2).toList
'''
print("=== BC-16.4-001g: .view.force Deprecated ===")
print(scala_example_1g)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-16.4-001h: collection.Seq Changed to Immutable
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC ```
# MAGIC \bcollection\.Seq\b
# MAGIC ```
# MAGIC 
# MAGIC **Impact:** In Scala 2.13, `collection.Seq` refers to `immutable.Seq` by default.

# COMMAND ----------

scala_example_1h = '''
// ❌ BEFORE (may be mutable in Scala 2.12)
import scala.collection.Seq
val mySeq: Seq[Int] = ???

// ✅ AFTER (be explicit)
import scala.collection.immutable.Seq  // or mutable.Seq if needed
val mySeq: Seq[Int] = ???
'''
print("=== BC-16.4-001h: collection.Seq Changed ===")
print(scala_example_1h)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-16.4-001i: Symbol Literals Deprecated
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC ```
# MAGIC '[a-zA-Z_][a-zA-Z0-9_]*
# MAGIC ```

# COMMAND ----------

scala_example_1i = '''
// ❌ BEFORE (deprecated in Scala 2.13)
val sym = 'mySymbol

// ✅ AFTER
val sym = Symbol("mySymbol")
'''
print("=== BC-16.4-001i: Symbol Literals Deprecated ===")
print(scala_example_1i)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BC-16.4-002: HashMap/HashSet Ordering Changed
# MAGIC 
# MAGIC ### 📖 What Changed
# MAGIC 
# MAGIC In Scala 2.13, HashMap and HashSet **may iterate in different order** than in 2.12.
# MAGIC 
# MAGIC ### 🔍 Detection Pattern
# MAGIC ```
# MAGIC \b(HashMap|HashSet)\s*[\[\(]
# MAGIC ```
# MAGIC 
# MAGIC ### 📋 Impact
# MAGIC - Code relying on implicit iteration order will break
# MAGIC - Tests comparing collections by order will fail
# MAGIC - Serialization depending on iteration order affected

# COMMAND ----------

scala_example_2 = '''
// ❌ BAD: Relies on iteration order (WILL DIFFER in 2.13!)
val map = HashMap("a" -> 1, "b" -> 2, "c" -> 3)
map.foreach { case (k, v) => println(s"$k: $v") }  // Order may differ!

// ✅ GOOD: Explicit ordering
map.toSeq.sortBy(_._1).foreach { case (k, v) => println(s"$k: $v") }

// ✅ GOOD: Use order-preserving collection
import scala.collection.immutable.ListMap
val orderedMap = ListMap("a" -> 1, "b" -> 2, "c" -> 3)
'''
print("=== BC-16.4-002: HashMap/HashSet Ordering ===")
print(scala_example_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 COMPLETE SUMMARY
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Reference: All Breaking Changes
# MAGIC 
# MAGIC ### 🔴 Auto-Fix (10 patterns)
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
# MAGIC | BC-16.4-001f | `.toIterator` | `.iterator` | .scala |
# MAGIC | BC-16.4-001g | `.view.force` | `.view.to(List)` | .scala |
# MAGIC | BC-16.4-001i | `'symbol` literal | `Symbol("symbol")` | .scala |
# MAGIC 
# MAGIC ### 🔧 Assisted Fix (11 patterns)
# MAGIC 
# MAGIC Agent provides exact fix snippet in scan output. Developer reviews and decides.
# MAGIC 
# MAGIC | ID | Pattern | Suggested Fix |
# MAGIC |----|---------|---------------|
# MAGIC | BC-SC-002 | Temp view name reuse | UUID-suffixed view names |
# MAGIC | BC-SC-003 | UDF captures external variable | Factory wrapper pattern |
# MAGIC | BC-SC-004 | `df.columns` in loop | Cache outside loop |
# MAGIC | BC-17.3-005 | `DecimalType` without precision | Explicit `DecimalType(p, s)` |
# MAGIC | BC-13.3-002 | Parquet timestamp NTZ | Commented `inferTimestampNTZ` config |
# MAGIC | BC-15.4-002 | JDBC useNullCalendar | Commented `useNullCalendar` config |
# MAGIC | BC-15.4-005 | JDBC read patterns | Commented `useNullCalendar` config |
# MAGIC | BC-16.4-003 | Cached data source reads | Commented cache config |
# MAGIC | BC-16.4-004 | `materializeSource=none` | Replace with `"auto"` |
# MAGIC | BC-16.4-006 | Auto Loader `cleanSource` | Commented config with test guidance |
# MAGIC | BC-17.3-002 | Auto Loader incremental listing | `.option("cloudFiles.useIncrementalListing", "auto")` |
# MAGIC 
# MAGIC ### 🟡 Manual Review (10 patterns)
# MAGIC 
# MAGIC Human judgment required. No fix snippet generated.
# MAGIC 
# MAGIC | ID | Pattern | Decision |
# MAGIC |----|---------|----------|
# MAGIC | BC-13.3-001 | `MERGE INTO` | Review type casting for overflow |
# MAGIC | BC-13.3-003 | `overwriteSchema` + dynamic partition | Separate operations |
# MAGIC | BC-13.3-004 | MERGE/UPDATE type mismatch | Review ANSI store assignment policy |
# MAGIC | BC-15.4-001 | `VariantType()` in UDF | Use StringType + JSON |
# MAGIC | BC-15.4-004 | `CREATE VIEW (col TYPE)` | Remove types, cast in SELECT |
# MAGIC | BC-15.4-006 | `CREATE VIEW` | Review schema binding changes |
# MAGIC | BC-16.4-002 | `HashMap`/`HashSet` | Don't rely on iteration order |
# MAGIC | BC-16.4-001h | `collection.Seq` | Use explicit immutable/mutable |
# MAGIC | BC-SC-001 | try/except around transforms | Add `df.columns` for validation |
# MAGIC | BC-17.3-003 | `array`/`map`/`struct` literals | Review null preservation behavior |

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
# MAGIC **Expected Result:** Assistant identifies the breaking change patterns demonstrated in this notebook (see summary tables for pattern IDs that have code examples vs. those listed for reference only)
# MAGIC 
# MAGIC ### Step 2: Review the Findings
# MAGIC 
# MAGIC Assistant will categorize findings into three tiers:
# MAGIC - 🔴 **Auto-Fix** - Safe to apply automatically, no review needed
# MAGIC - 🔧 **Assisted Fix** - Suggested fix snippet provided, developer reviews and decides
# MAGIC - 🟡 **Manual Review** - Human judgment required, no fix snippet
# MAGIC 
# MAGIC ### Step 3: Apply Auto-Fixes
# MAGIC 
# MAGIC ```
# MAGIC Fix all the auto-fixable breaking changes in this notebook
# MAGIC ```
# MAGIC 
# MAGIC ### Step 4: Review Assisted Fix Suggestions
# MAGIC 
# MAGIC For each assisted fix item, the agent provides a copy-paste-ready code snippet.
# MAGIC Review the suggestions and apply as appropriate:
# MAGIC 
# MAGIC ```
# MAGIC Show me the assisted fix for BC-SC-002 - the temp view name reuse
# MAGIC ```
# MAGIC 
# MAGIC ### Step 5: Review Manual Items
# MAGIC 
# MAGIC For manual review items, ask for guidance:
# MAGIC 
# MAGIC ```
# MAGIC Help me understand BC-17.3-003 - the null handling in my array constructions
# MAGIC ```
# MAGIC 
# MAGIC ### Step 6: Validate
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
# MAGIC *Last Updated: February 2026*
