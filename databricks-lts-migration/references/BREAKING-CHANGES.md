# Complete Breaking Changes Reference

This document contains all breaking changes between Databricks Runtime 13.3 LTS and 17.3 LTS with detailed remediation.

---

## DBR 13.3 LTS Breaking Changes

### BC-13.3-001: MERGE INTO and UPDATE Type Casting

**Severity:** HIGH  
**Category:** Delta Lake

**What Changed:**  
Delta UPDATE and MERGE operations now follow `spark.sql.storeAssignmentPolicy` (default: ANSI). Values that overflow the target column type throw an error instead of silently storing NULL.

**Error Message:**
```
CAST_OVERFLOW: Cannot safely cast 'value' from LongType to IntType
```

**Impact:**  
- MERGE INTO statements with implicit type conversions
- UPDATE statements storing values that exceed column range
- ETL pipelines with loose type handling

**Remediation:**

Option 1: Widen the column type
```sql
-- Enable column mapping first
ALTER TABLE target_table SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);

-- Rename old column
ALTER TABLE target_table RENAME COLUMN id TO id_old;

-- Add new column with wider type
ALTER TABLE target_table ADD COLUMN id BIGINT;

-- Migrate data
UPDATE target_table SET id = id_old;

-- Drop old column
ALTER TABLE target_table DROP COLUMN id_old;
```

Option 2: Explicit safe casting in MERGE
```sql
MERGE INTO target AS t
USING source AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
    t.value = CASE 
        WHEN s.value > 2147483647 THEN NULL 
        ELSE CAST(s.value AS INT) 
    END
```

---

### BC-13.3-002: Parquet Timestamp Schema Inference

**Severity:** MEDIUM  
**Category:** Parquet / Delta Lake

**What Changed:**  
`int64` timestamp columns annotated with `isAdjustedToUTC=false` now infer as `TIMESTAMP_NTZ` (timestamp without timezone) instead of `TIMESTAMP`.

**Error Message:**
```
DeltaAnalysisException: Your table schema requires manual enablement of the following table feature(s): timestampNtz
```

**Impact:**  
- Reading external Parquet files
- Creating Delta tables from Parquet sources
- Tables with timezone-naive timestamps

**Remediation:**

Option 1: Disable new inference (preserve old behavior)
```sql
SET spark.sql.parquet.inferTimestampNTZ.enabled = false;
```

Option 2: Enable timestampNtz feature on table
```sql
ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.feature.timestampNtz' = 'supported'
);
```

Option 3: Explicit schema definition
```python
from pyspark.sql.types import StructType, StructField, TimestampType

schema = StructType([
    StructField("event_time", TimestampType(), True)  # Forces TIMESTAMP
])

df = spark.read.schema(schema).parquet("/path/to/files")
```

---

### BC-13.3-003: Block Schema Overwrite with Dynamic Partitions

**Severity:** MEDIUM  
**Category:** Delta Lake

**What Changed:**  
Cannot set `overwriteSchema` to `true` in combination with dynamic partition overwrites.

**Error Message:**
```
DeltaAnalysisException: Cannot set overwriteSchema to true when using dynamic partition overwrite mode
```

**Impact:**  
- ETL jobs using both schema evolution and partition overwrites
- Prevents table corruption from schema mismatch

**Remediation:**

Separate schema evolution from partition overwrites:
```python
# Step 1: Evolve schema first (if needed)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
df_sample.write.format("delta").mode("append").save("/path/to/table")

# Step 2: Then do partition overwrites
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .save("/path/to/table")
```

---

### BC-13.3-004: File Modification Detection

**Severity:** LOW  
**Category:** Query Execution

**What Changed:**  
Queries return an error if a file is modified between query planning and execution.

**Impact:**  
- Long-running queries over mutable file sources
- ETL reading from actively updated directories

**Remediation:**  
- Use Delta Lake for ACID guarantees
- Ensure file immutability during query execution
- Use checkpointing for streaming workloads

---

## DBR 14.3 LTS Breaking Changes

### BC-14.3-001: Thriftserver Features Removed

**Severity:** LOW  
**Category:** Thriftserver / JDBC

**What Changed:**  
The following Thriftserver configurations are no longer supported:
- Hive auxiliary JARs (`hive.aux.jars.path`)
- Hive global init file (`.hiverc`) via `hive.server2.global.init.file.location`

**Impact:**  
- Thriftserver connections using legacy Hive configuration
- Custom UDF loading via aux JARs

**Remediation:**

Option 1: Use cluster init scripts
```bash
#!/bin/bash
# init_script.sh
cp /dbfs/path/to/custom.jar /databricks/jars/
```

Option 2: Use Unity Catalog volumes
```sql
CREATE VOLUME my_catalog.my_schema.jars;
-- Upload JAR to volume, then reference in cluster libraries
```

Option 3: Use cluster-scoped libraries
Configure JAR libraries directly in cluster configuration.

---

## DBR 15.4 LTS Breaking Changes

### BC-15.4-001: VARIANT Type with Python UDF/UDAF/UDTF

**Severity:** HIGH (BREAKING)  
**Category:** Python UDF

**What Changed:**  
Calling any Python UDF, UDAF, or UDTF that uses `VARIANT` type as an argument or return value throws an exception.

**Error Message:**
```
AnalysisException: VARIANT type is not supported in Python UDFs
```

**Impact:**  
- All Python functions accepting VARIANT input
- All Python functions returning VARIANT output
- Semi-structured data processing in Python

**Remediation:**

Option 1: Use STRING with JSON parsing
```python
from pyspark.sql.functions import udf, col, to_json, from_json
from pyspark.sql.types import StringType, StructType, StructField

# Before (fails in 15.4+)
# @udf(returnType=VariantType())
# def process_variant(v):
#     return v

# After (works)
@udf(returnType=StringType())
def process_json(json_str):
    import json
    data = json.loads(json_str)
    # Process data
    return json.dumps(data)

# Convert VARIANT to STRING, process, convert back
df.withColumn("json_str", to_json(col("variant_col"))) \
  .withColumn("processed", process_json(col("json_str")))
```

Option 2: Use SQL functions instead
```sql
-- Use SQL VARIANT functions instead of Python UDFs
SELECT 
    variant_get(data, '$.field', 'STRING') as field_value,
    parse_json(to_json(data)) as reparsed
FROM my_table
```

Option 3: Use Scala UDFs
```scala
// Scala UDFs can still use VARIANT
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.VariantType

val processVariant = udf((v: Any) => {
    // Process variant
    v
})
```

---

### BC-15.4-002: JDBC useNullCalendar Default Changed

**Severity:** MEDIUM  
**Category:** JDBC

**What Changed:**  
`spark.sql.legacy.jdbc.useNullCalendar` default changed from `false` to `true`.

**Impact:**  
- JDBC queries returning TIMESTAMP values
- BI tools connecting via JDBC
- Applications parsing JDBC timestamp results

**Remediation:**

```sql
-- Preserve old behavior
SET spark.sql.legacy.jdbc.useNullCalendar = false;
```

Or in cluster Spark config:
```
spark.sql.legacy.jdbc.useNullCalendar false
```

---

### BC-15.4-003: Disallow '!' Syntax for NOT

**Severity:** MEDIUM  
**Category:** SQL Syntax

**What Changed:**  
Using `!` as a synonym for `NOT` outside boolean expressions is disallowed.

**Error Message:**
```
ParseException: Syntax error at or near '!'
```

**Affected Patterns:**
| Before (Invalid) | After (Valid) |
|-----------------|---------------|
| `CREATE TABLE IF ! EXISTS` | `CREATE TABLE IF NOT EXISTS` |
| `IS ! NULL` | `IS NOT NULL` |
| `! IN (1, 2)` | `NOT IN (1, 2)` |
| `! BETWEEN 1 AND 10` | `NOT BETWEEN 1 AND 10` |
| `! LIKE '%pattern%'` | `NOT LIKE '%pattern%'` |
| `! EXISTS (SELECT ...)` | `NOT EXISTS (SELECT ...)` |

**Remediation:**

Search and replace in SQL files:
```bash
# Find occurrences
grep -rn "IF\s*!" --include="*.sql" .
grep -rn "IS\s*!" --include="*.sql" .
grep -rn "\s!\s*IN" --include="*.sql" .
grep -rn "\s!\s*BETWEEN" --include="*.sql" .
grep -rn "\s!\s*LIKE" --include="*.sql" .
grep -rn "\s!\s*EXISTS" --include="*.sql" .

# Note: ! is still valid in boolean expressions: WHERE !flag
```

---

### BC-15.4-004: View Column Definition Syntax Disallowed

**Severity:** LOW  
**Category:** SQL Views

**What Changed:**  
Column types, NOT NULL constraints, or DEFAULT specifications in CREATE VIEW are disallowed.

**Before (Invalid):**
```sql
CREATE VIEW my_view (
    id INT NOT NULL,
    name STRING DEFAULT 'unknown'
) AS SELECT id, name FROM source
```

**After (Valid):**
```sql
CREATE VIEW my_view (id, name) AS 
SELECT 
    CAST(id AS INT) as id,
    COALESCE(name, 'unknown') as name 
FROM source
```

---

### BC-15.4-005: CHECK Constraint Error Class Changed

**Severity:** LOW  
**Category:** Error Handling

**What Changed:**  
`ALTER TABLE ADD CONSTRAINT` with invalid column now returns `UNRESOLVED_COLUMN.WITH_SUGGESTION` instead of `INTERNAL_ERROR`.

**Impact:**  
Error handling code catching `INTERNAL_ERROR` for constraint operations.

**Remediation:**  
Update exception handling to catch `UNRESOLVED_COLUMN.WITH_SUGGESTION`.

---

## DBR 16.4 LTS Breaking Changes

### BC-16.4-001: Scala 2.13 Collection Incompatibility

**Severity:** CRITICAL  
**Category:** Scala

**What Changed:**  
Scala 2.13 introduces major collection library changes. This affects API parameters and return types across the standard library.

**Impact:**  
- ALL Scala code using collections
- Third-party libraries compiled for Scala 2.12
- UDFs and UDAFs written in Scala

**Key Changes:**
| Scala 2.12 | Scala 2.13 |
|------------|------------|
| `collection.Seq` | `collection.immutable.Seq` |
| `TraversableOnce` | `IterableOnce` |
| `JavaConverters` | `CollectionConverters` |
| `.to[List]` | `.to(List)` |

**Remediation:**

See [SCALA-213-GUIDE.md](SCALA-213-GUIDE.md) for complete migration instructions.

Quick fix with compatibility library:
```scala
// Add to build.sbt
libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0"

// Add import
import scala.collection.compat._
```

---

### BC-16.4-002: Hash Algorithm Ordering Changed

**Severity:** HIGH  
**Category:** Scala

**What Changed:**  
HashMap and Set may order elements differently during iteration in Scala 2.13.

**Impact:**  
- Code relying on implicit iteration order
- Tests comparing collection contents by order
- Serialization depending on iteration order

**Remediation:**

```scala
// Don't rely on iteration order
val map = HashMap("a" -> 1, "b" -> 2)

// BAD: Order-dependent
map.foreach(println)  // Order may differ in 2.13

// GOOD: Explicit ordering when needed
map.toSeq.sortBy(_._1).foreach(println)

// GOOD: Use order-preserving collection
import scala.collection.immutable.ListMap
val orderedMap = ListMap("a" -> 1, "b" -> 2)
```

---

### BC-16.4-003: Data Source Cached Plans Fix

**Severity:** MEDIUM  
**Category:** Query Planning

**What Changed:**  
Table reads now respect options for all data source plans when cached, not just the first cached plan.

**Impact:**  
- Queries with different options on same table
- Read options now consistently applied

**Remediation:**

```sql
-- Restore old behavior if needed
SET spark.sql.legacy.readFileSourceTableCacheIgnoreOptions = true;
```

---

### BC-16.4-004: MERGE Source Materialization Cannot Be Disabled

**Severity:** LOW  
**Category:** Delta Lake

**What Changed:**  
Setting `merge.materializeSource` to `none` now throws an error.

**Error Message:**
```
DeltaAnalysisException: merge.materializeSource cannot be set to 'none'
```

**Remediation:**  
Remove the configuration or set to a valid value (`auto` or `all`).

---

### BC-16.4-005: Json4s Downgrade

**Severity:** LOW  
**Category:** Libraries

**What Changed:**  
Json4s downgraded from 4.0.7 to 3.7.0-M11 for Scala 2.13 compatibility.

**Impact:**  
Applications using Json4s 4.x-specific APIs.

**Remediation:**  
Review Json4s API usage and adjust for 3.7.x compatibility.

---

## DBR 17.3 LTS Breaking Changes

### BC-17.3-001: input_file_name Function Removed

**Severity:** HIGH  
**Category:** SQL Functions

**What Changed:**  
The `input_file_name()` function, deprecated since DBR 13.3, is completely removed in DBR 17.3.

**Error Message:**
```
AnalysisException: Undefined function: input_file_name
```

**Impact:**  
- All queries using `input_file_name()`
- Views containing `input_file_name()`
- ETL pipelines tracking source files

**Remediation:**

SQL:
```sql
-- Before (fails in 17.3)
SELECT input_file_name(), col1, col2 FROM my_table

-- After (works)
SELECT _metadata.file_name, col1, col2 FROM my_table
```

Python:
```python
# Before (fails in 17.3)
from pyspark.sql.functions import input_file_name
df.withColumn("source", input_file_name())

# After (works)
df.select("*", "_metadata.file_name")
# or
df.select("*", df["_metadata"]["file_name"].alias("source"))
```

Scala:
```scala
// Before (fails in 17.3)
import org.apache.spark.sql.functions.input_file_name
df.withColumn("source", input_file_name())

// After (works)
df.select(col("*"), col("_metadata.file_name").as("source"))
```

**Recreating Views:**
```sql
-- Drop old view
DROP VIEW IF EXISTS my_view;

-- Create new view with _metadata
CREATE VIEW my_view AS
SELECT _metadata.file_name as source_file, * 
FROM my_table;
```

---

### BC-17.3-002: Auto Loader Incremental Listing Default Changed

**Severity:** HIGH  
**Category:** Auto Loader / Streaming

**What Changed:**  
Default for `cloudFiles.useIncrementalListing` changed from `auto` to `false`. Full directory listings are now performed instead of incremental.

**Impact:**  
- Auto Loader jobs may be slower (full listings)
- Fixes issue where files with non-lexicographic names were skipped
- More reliable file discovery

**Remediation:**

Option 1: Preserve old behavior (if files are lexicographically ordered)
```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.useIncrementalListing", "auto") \
    .load("/path/to/files")
```

Option 2: Migrate to file events (recommended for performance)
```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.useNotifications", "true") \
    .load("/path/to/files")
```

**When to keep new default (false):**
- Files don't follow lexicographic naming
- Files may be uploaded out of order
- Reliability is more important than speed

---

### BC-17.3-003: Null Values Preserved in Literals (Spark Connect)

**Severity:** MEDIUM  
**Category:** Spark Connect

**What Changed:**  
In Spark Connect mode, null values inside array, map, and struct literals are now preserved instead of being replaced with protobuf default values (0, empty string, false).

**Impact:**  
- Spark Connect applications
- Applications expecting default value substitution

**Remediation:**

Handle nulls explicitly:
```python
# Before: nulls might become 0 or ""
# After: nulls remain null

# Explicit null handling
from pyspark.sql.functions import coalesce, lit

df.withColumn("value", coalesce(col("value"), lit(0)))
```

---

### BC-17.3-004: Null Struct Handling in Delta

**Severity:** MEDIUM  
**Category:** Delta Lake

**What Changed:**  
Null struct values are now correctly preserved when dropping NullType columns. Previously, null structs were incorrectly replaced with non-null structs having all null fields.

**Impact:**  
- Applications checking for null structs
- Data quality checks on struct columns

**Before (incorrect):**
```
struct_col = struct(null, null)  -- Non-null struct with null fields
```

**After (correct):**
```
struct_col = null  -- Null struct remains null
```

**Remediation:**

Update null checks:
```sql
-- Check for null struct (entire struct is null)
SELECT * FROM table WHERE struct_col IS NULL

-- Check for struct with null fields
SELECT * FROM table WHERE struct_col.field1 IS NULL
```

---

### BC-17.3-005: Decimal Precision in Spark Connect

**Severity:** MEDIUM  
**Category:** Spark Connect

**What Changed:**  
Decimal precision and scale in array/map literals changed to `SYSTEM_DEFAULT` (38, 18) in Spark Connect mode.

**Impact:**  
- Logical plan analysis (not query results)
- Plan comparison tests

**Remediation:**  
Specify explicit precision/scale if needed for plan comparison.

---

## Spark Connect Behavioral Changes (DBR 13.3+)

Spark Connect is used in Scala notebooks (DBR 13.3+), Python notebooks (DBR 14.3+), Serverless compute, and Databricks Connect. These behavioral differences require code changes when migrating from Spark Classic.

Source: [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)

---

### BC-SC-001: Lazy Schema Analysis (Deferred Error Detection)

**Severity:** HIGH  
**Category:** Spark Connect

**What Changed:**  
Spark Connect defers schema analysis and name resolution to execution time. In Spark Classic, analysis happens eagerly during logical plan construction.

| Aspect | Spark Classic | Spark Connect |
|--------|---------------|---------------|
| Transformations (filter, select, limit) | Eager analysis | Lazy analysis |
| Schema access (df.columns, df.schema) | Eager | Triggers RPC request |
| Error detection | Immediate on transformation | Delayed until action |

**Impact:**  
- Invalid column references don't throw errors until action is called
- Try/except blocks around transformations won't catch analysis errors
- Debugging becomes harder as errors appear later in execution

**Example of Changed Behavior:**
```python
# Spark Classic: This throws an error immediately
# Spark Connect: No error until df.show() or df.collect()
df = spark.sql("select 1 as a, 2 as b").filter("c > 1")  # Column 'c' doesn't exist!
# ... many lines of code later ...
df.show()  # Error thrown HERE in Spark Connect
```

**Remediation:**

Trigger eager analysis explicitly when you need immediate error detection:
```python
try:
    df = spark.sql("select 1 as a, 2 as b").filter("c > 1")
    df.columns  # Trigger analysis to catch errors early
except Exception as e:
    print(f"Error: {repr(e)}")
```

```scala
import org.apache.spark.SparkThrowable

try {
  val df = spark.sql("select 1 as a, 2 as b").filter(col("c") > 1)
  df.columns  // Trigger eager analysis
} catch {
  case e: SparkThrowable => println(s"Error: ${e.getMessage}")
}
```

---

### BC-SC-002: Temporary View Name Resolution (View Overwriting)

**Severity:** HIGH  
**Category:** Spark Connect

**What Changed:**  
In Spark Connect, DataFrames store only a **reference to the temporary view by name**. If the view is later replaced, the DataFrame data will change because it looks up the view by name at execution time.

In Spark Classic, the logical plan of the temp view is embedded into the DataFrame's plan at creation time. Subsequent view replacements don't affect the original DataFrame.

**Impact:**  
- Reusing temp view names causes unexpected data changes
- DataFrames may return different results than expected
- Hard to debug data inconsistencies

**Example of Changed Behavior:**
```python
# Spark Classic: df10 always returns 10 rows
# Spark Connect: df10 returns 100 rows after second call!

def create_view_and_df(x):
    spark.range(x).createOrReplaceTempView("temp_view")
    return spark.table("temp_view")

df10 = create_view_and_df(10)
df100 = create_view_and_df(100)

# In Spark Connect, this returns 100, not 10!
print(len(df10.collect()))  # Expected: 10, Actual: 100
```

**Remediation:**

Always use unique temporary view names (include UUID):
```python
import uuid

def create_view_and_df(x):
    temp_view_name = f"`temp_view_{uuid.uuid4()}`"  # Unique name
    spark.range(x).createOrReplaceTempView(temp_view_name)
    return spark.table(temp_view_name)

df10 = create_view_and_df(10)
df100 = create_view_and_df(100)
assert len(df10.collect()) == 10  # Works correctly now
```

```scala
import java.util.UUID

def createTempViewAndDataFrame(x: Int) = {
  val tempViewName = s"`temp_view_${UUID.randomUUID()}`"
  spark.range(x).createOrReplaceTempView(tempViewName)
  spark.table(tempViewName)
}

val df10 = createTempViewAndDataFrame(10)
val df100 = createTempViewAndDataFrame(100)
assert(df10.collect().length == 10)  // Works correctly
```

---

### BC-SC-003: UDF Serialization Timing (Late Binding)

**Severity:** HIGH  
**Category:** Spark Connect / Python UDF

**What Changed:**  
In Spark Connect, Python UDFs are lazy—serialization and registration are deferred until execution time. External variables captured by UDFs are bound at execution time, not creation time.

In Spark Classic, UDFs are eagerly created and variables are captured at UDF creation time.

**Impact:**  
- UDFs capture variable values at execution time, not definition time
- Modifying variables after UDF creation affects UDF behavior
- Non-deterministic behavior in loops or functions

**Example of Changed Behavior:**
```python
from pyspark.sql.functions import udf

x = 123

@udf("INT")
def foo():
    return x

df = spark.range(1).select(foo())
x = 456
df.show()  # Spark Classic: 123, Spark Connect: 456!
```

**Remediation:**

Use a function factory (closure) to capture variable values at definition time:
```python
from pyspark.sql.functions import udf

def make_udf(value):
    """Captures 'value' at function creation time"""
    def foo():
        return value
    return udf(foo)

x = 123
foo_udf = make_udf(x)  # Captures 123
x = 456
df = spark.range(1).select(foo_udf())
df.show()  # Correctly prints 123
```

```scala
def makeUDF(value: Int) = udf(() => value)

var x = 123
val fooUDF = makeUDF(x)  // Captures current value
x = 456
val df = spark.range(1).select(fooUDF())
df.show()  // Correctly prints 123
```

---

### BC-SC-004: Excessive Schema Access Performance

**Severity:** MEDIUM  
**Category:** Spark Connect / Performance

**What Changed:**  
In Spark Connect, every schema access (`df.columns`, `df.schema`) triggers an RPC request to the server. In Spark Classic, this information is local.

**Impact:**  
- Loops with schema access become extremely slow
- Creating many DataFrames with schema checks causes performance degradation
- Interactive notebook cells may timeout

**Example of Performance Problem:**
```python
df = spark.range(10)
for i in range(200):
    if str(i) not in df.columns:  # BAD: RPC on every iteration!
        df = df.withColumn(str(i), col("id") + i)
```

**Remediation:**

Cache schema information locally:
```python
df = spark.range(10)
columns = set(df.columns)  # Cache column names locally

for i in range(200):
    new_col = str(i)
    if new_col not in columns:  # Check local set, no RPC
        df = df.withColumn(new_col, col("id") + i)
        columns.add(new_col)
```

For struct field access, use schema object directly:
```python
from pyspark.sql.types import StructType

# BAD: Creates intermediate DataFrames and triggers analysis
struct_fields = {
    col.name: df.select(col.name + ".*").columns  
    for col in df.schema if isinstance(col.dataType, StructType)
}

# GOOD: Access fields from schema directly
struct_fields = {
    col.name: [f.name for f in col.dataType.fields]
    for col in df.schema if isinstance(col.dataType, StructType)
}
```

---

## Quick Reference: All Configuration Flags

```sql
-- Preserve JDBC timestamp behavior (changed in 15.4)
SET spark.sql.legacy.jdbc.useNullCalendar = false;

-- Preserve Parquet timestamp inference (changed in 13.3)
SET spark.sql.parquet.inferTimestampNTZ.enabled = false;

-- Preserve data source cache behavior (changed in 16.4)  
SET spark.sql.legacy.readFileSourceTableCacheIgnoreOptions = true;
```

Auto Loader (set in readStream options, not globally):
```python
.option("cloudFiles.useIncrementalListing", "auto")
```
