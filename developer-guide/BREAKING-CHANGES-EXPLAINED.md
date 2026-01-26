# Breaking Changes Explained: DBR 13.3 → 17.3

This guide explains each breaking change in simple terms, with examples showing:
- ❌ **The Problem**: Code that will break
- 🔍 **How It's Detected**: The pattern the agent looks for
- ✅ **The Fix**: How the code should be changed

---

## Table of Contents

| Severity | ID | Name | DBR Version |
|----------|-----|------|-------------|
| 🔴 HIGH | [BC-17.3-001](#bc-173-001-input_file_name-removed) | input_file_name() Removed | 17.3 |
| 🔴 HIGH | [BC-15.4-001](#bc-154-001-variant-type-in-python-udf) | VARIANT Type in Python UDF | 15.4 |
| 🔴 HIGH | [BC-16.4-001a](#bc-164-001a-scala-javaconverters) | Scala JavaConverters | 16.4 |
| 🔴 HIGH | [BC-16.4-001c](#bc-164-001c-scala-traversableonce) | Scala TraversableOnce | 16.4 |
| 🔴 HIGH | [BC-16.4-001d](#bc-164-001d-scala-traversable) | Scala Traversable | 16.4 |
| 🟡 MEDIUM | [BC-15.4-003](#bc-154-003--syntax-for-not) | '!' Syntax for NOT | 15.4 |
| 🟡 MEDIUM | [BC-16.4-001b](#bc-164-001b-scala-tocollection-syntax) | Scala .to[Collection] | 16.4 |
| 🟡 MEDIUM | [BC-16.4-001e](#bc-164-001e-scala-stream-lazy) | Scala Stream (Lazy) | 16.4 |
| 🟡 MEDIUM | [BC-17.3-002](#bc-173-002-auto-loader-incremental-listing) | Auto Loader Incremental Listing | 17.3 |
| 🟡 MEDIUM | [BC-SC-002](#bc-sc-002-temp-view-name-reuse) | Temp View Name Reuse | 13.3+ |
| 🟢 LOW | [BC-13.3-002](#bc-133-002-parquet-timestamp-ntz) | Parquet Timestamp NTZ | 13.3 |
| 🟢 LOW | [BC-15.4-002](#bc-154-002-jdbc-usenullcalendar) | JDBC useNullCalendar | 15.4 |
| 🟢 LOW | [BC-15.4-004](#bc-154-004-view-column-type-definition) | View Column Type Definition | 15.4 |
| 🟢 LOW | [BC-16.4-004](#bc-164-004-merge-materializesourcenone) | MERGE materializeSource=none | 16.4 |
| 🟢 LOW | [BC-SC-003](#bc-sc-003-udf-external-variable-capture) | UDF External Variable Capture | 14.3+ |
| 🟢 LOW | [BC-SC-004](#bc-sc-004-schema-access-in-loops) | Schema Access in Loops | 13.3+ |

---

## 🔴 HIGH Severity Changes

These will cause immediate failures when running on the new DBR version.

---

### BC-17.3-001: input_file_name() Removed

**What Changed:** The `input_file_name()` function has been completely removed from Spark 4.0 (DBR 17.3). It was deprecated since DBR 13.3.

**Why It Matters:** Any code using this function will throw a `AnalysisException: Undefined function` error.

#### ❌ The Problem

**Python (DataFrame API):**
```python
from pyspark.sql.functions import input_file_name, col

# This will fail in DBR 17.3
df = spark.read.parquet("/data/files/")
df_with_source = df.withColumn("source_file", input_file_name())
```

**Python (SQL String):**
```python
# This will also fail
result = spark.sql("SELECT input_file_name() as source, * FROM my_table")
```

**SQL:**
```sql
-- This will fail
SELECT input_file_name() as source_file, * FROM parquet.`/data/files/`
```

**Scala:**
```scala
import org.apache.spark.sql.functions.input_file_name

// This will fail
val df = spark.read.parquet("/data/files/")
val dfWithSource = df.withColumn("source_file", input_file_name())
```

#### 🔍 How It's Detected

The agent scans for this regex pattern:
```regex
\binput_file_name\s*\(
```

This matches:
- `input_file_name()`
- `input_file_name ()`
- `input_file_name(  )`

#### ✅ The Fix

**Python (DataFrame API) - BEFORE:**
```python
from pyspark.sql.functions import input_file_name, col

df_with_source = df.withColumn("source_file", input_file_name())
```

**Python (DataFrame API) - AFTER:**
```python
from pyspark.sql.functions import col

df_with_source = df.withColumn("source_file", col("_metadata.file_name"))
```

**Python (SQL String) - BEFORE:**
```python
result = spark.sql("SELECT input_file_name() as source, * FROM my_table")
```

**Python (SQL String) - AFTER:**
```python
result = spark.sql("SELECT _metadata.file_name as source, * FROM my_table")
```

> ⚠️ **Note:** Inside SQL strings, the agent uses `_metadata.file_name` (not `col()`). In DataFrame API, it uses `col("_metadata.file_name")`.

**SQL - BEFORE:**
```sql
SELECT input_file_name() as source_file, * FROM parquet.`/data/files/`
```

**SQL - AFTER:**
```sql
SELECT _metadata.file_name as source_file, * FROM parquet.`/data/files/`
```

---

### BC-15.4-001: VARIANT Type in Python UDF

**What Changed:** The `VARIANT` data type cannot be used as input or output for Python UDFs, UDAFs, or UDTFs.

**Why It Matters:** Attempting to use `VariantType()` in a UDF will throw a runtime error.

#### ❌ The Problem

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import VariantType

# This will fail in DBR 15.4+
@udf(returnType=VariantType())
def create_metadata(value):
    return {"key": value, "timestamp": "2024-01-01"}
```

#### 🔍 How It's Detected

```regex
VariantType\s*\(
```

#### ✅ The Fix

**BEFORE:**
```python
from pyspark.sql.types import VariantType

@udf(returnType=VariantType())
def create_metadata(fare, tip):
    return {
        "fare_amount": fare,
        "tip_amount": tip
    }
```

**AFTER:**
```python
from pyspark.sql.types import StringType
import json

@udf(returnType=StringType())
def create_metadata(fare, tip):
    return json.dumps({
        "fare_amount": float(fare) if fare else 0,
        "tip_amount": float(tip) if tip else 0
    })

# To use the result as a variant later:
# df.select(parse_json(create_metadata_udf(col("fare"), col("tip"))))
```

---

### BC-16.4-001a: Scala JavaConverters

**What Changed:** In Scala 2.13 (DBR 16.4), `scala.collection.JavaConverters` is deprecated and replaced with `scala.jdk.CollectionConverters`.

**Why It Matters:** Code will show deprecation warnings and may not compile in future versions.

#### ❌ The Problem

```scala
import scala.collection.JavaConverters._

val javaList = new java.util.ArrayList[String]()
javaList.add("a")
val scalaList = javaList.asScala.toList
```

#### 🔍 How It's Detected

```regex
import\s+scala\.collection\.JavaConverters
```

#### ✅ The Fix

**BEFORE:**
```scala
import scala.collection.JavaConverters._

val scalaList = javaList.asScala.toList
```

**AFTER:**
```scala
import scala.jdk.CollectionConverters._

val scalaList = javaList.asScala.toList
```

---

### BC-16.4-001c: Scala TraversableOnce

**What Changed:** In Scala 2.13, `TraversableOnce` has been renamed to `IterableOnce`.

**Why It Matters:** Code using `TraversableOnce` will not compile.

#### ❌ The Problem

```scala
def processItems(items: TraversableOnce[String]): Unit = {
  items.foreach(println)
}
```

#### 🔍 How It's Detected

```regex
\bTraversableOnce\b
```

#### ✅ The Fix

**BEFORE:**
```scala
def processItems(items: TraversableOnce[String]): Unit = {
  items.foreach(println)
}
```

**AFTER:**
```scala
def processItems(items: IterableOnce[String]): Unit = {
  items.foreach(println)
}
```

---

### BC-16.4-001d: Scala Traversable

**What Changed:** In Scala 2.13, `Traversable` has been renamed to `Iterable`.

**Why It Matters:** Code using `Traversable` will not compile.

#### ❌ The Problem

```scala
def processCollection(data: Traversable[Int]): Int = {
  data.sum
}
```

#### 🔍 How It's Detected

```regex
\bTraversable\b(?!Once)
```

The `(?!Once)` ensures we don't match `TraversableOnce` (which is handled separately).

#### ✅ The Fix

**BEFORE:**
```scala
def processCollection(data: Traversable[Int]): Int = {
  data.sum
}
```

**AFTER:**
```scala
def processCollection(data: Iterable[Int]): Int = {
  data.sum
}
```

---

## 🟡 MEDIUM Severity Changes

These may cause failures in specific scenarios or produce incorrect results.

---

### BC-15.4-003: '!' Syntax for NOT

**What Changed:** Using `!` as a shorthand for `NOT` in SQL (outside of boolean expressions) is no longer allowed.

**Why It Matters:** SQL queries will fail with a parse error.

#### ❌ The Problem

```sql
-- These will all fail in DBR 15.4+
CREATE TABLE IF ! EXISTS my_table (id INT);

SELECT * FROM my_table WHERE status IS ! NULL;

SELECT * FROM my_table WHERE id ! IN (1, 2, 3);

SELECT * FROM my_table WHERE price ! BETWEEN 10 AND 100;

SELECT * FROM my_table WHERE name ! LIKE '%test%';
```

#### 🔍 How It's Detected

Two patterns:
```regex
(IF|IS)\s*!(?!\s*=)    -- Matches IF ! and IS ! but not !=
\s!\s*(IN|BETWEEN|LIKE|EXISTS)\b   -- Matches ! IN, ! BETWEEN, etc.
```

#### ✅ The Fix

**BEFORE:**
```sql
CREATE TABLE IF ! EXISTS my_table (id INT);
SELECT * FROM my_table WHERE status IS ! NULL;
SELECT * FROM my_table WHERE id ! IN (1, 2, 3);
SELECT * FROM my_table WHERE price ! BETWEEN 10 AND 100;
SELECT * FROM my_table WHERE name ! LIKE '%test%';
```

**AFTER:**
```sql
CREATE TABLE IF NOT EXISTS my_table (id INT);
SELECT * FROM my_table WHERE status IS NOT NULL;
SELECT * FROM my_table WHERE id NOT IN (1, 2, 3);
SELECT * FROM my_table WHERE price NOT BETWEEN 10 AND 100;
SELECT * FROM my_table WHERE name NOT LIKE '%test%';
```

---

### BC-16.4-001b: Scala .to[Collection] Syntax

**What Changed:** In Scala 2.13, the `.to[Collection]` syntax changed to `.to(Collection)`.

**Why It Matters:** Code will not compile with the old syntax.

#### ❌ The Problem

```scala
val list = Seq(1, 2, 3)
val result = list.to[List]
val setResult = list.to[Set]
val vectorResult = list.to[Vector]
```

#### 🔍 How It's Detected

```regex
\.to\s*\[\s*(List|Set|Vector|Seq|Array)\s*\]
```

#### ✅ The Fix

**BEFORE:**
```scala
val list = Seq(1, 2, 3)
val result = list.to[List]
val setResult = list.to[Set]
val vectorResult = list.to[Vector]
```

**AFTER:**
```scala
val list = Seq(1, 2, 3)
val result = list.to(List)
val setResult = list.to(Set)
val vectorResult = list.to(Vector)
```

---

### BC-16.4-001e: Scala Stream (Lazy)

**What Changed:** In Scala 2.13, `Stream` is deprecated and replaced with `LazyList`.

**Why It Matters:** Code using `Stream` will show deprecation warnings.

#### ❌ The Problem

```scala
val numbers = Stream.from(1)
val evens = Stream.continually(scala.util.Random.nextInt())
```

#### 🔍 How It's Detected

```regex
\bStream\s*\.\s*(from|continually|iterate|empty|cons)
```

#### ✅ The Fix

**BEFORE:**
```scala
val numbers = Stream.from(1)
val evens = Stream.continually(scala.util.Random.nextInt())
```

**AFTER:**
```scala
val numbers = LazyList.from(1)
val evens = LazyList.continually(scala.util.Random.nextInt())
```

---

### BC-17.3-002: Auto Loader Incremental Listing

**What Changed:** The default value for `cloudFiles.useIncrementalListing` changed from `auto` to `false` in DBR 17.3.

**Why It Matters:** Auto Loader may behave differently if you relied on the implicit default.

#### ❌ The Problem

```python
# This code relied on implicit incremental listing behavior
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .load("/data/incoming/")
)
```

#### 🔍 How It's Detected

```regex
cloudFiles\.useIncrementalListing
```

This flags any explicit usage for review to ensure settings are intentional.

#### ✅ The Fix

**BEFORE (implicit default):**
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .load("/data/incoming/")
)
```

**AFTER (explicit setting):**
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useIncrementalListing", "auto")  # Explicit setting
    .load("/data/incoming/")
)
```

---

### BC-SC-002: Temp View Name Reuse

**What Changed:** In Spark Connect mode, temporary views use name-based lookup instead of embedded query plans. Reusing the same temp view name across sessions can cause conflicts.

**Why It Matters:** In concurrent or shared sessions, one session's temp view could be overwritten by another.

#### ❌ The Problem

```python
def process_batch(batch_df, batch_name):
    # Danger: Same view name used for every batch!
    batch_df.createOrReplaceTempView("current_batch")
    return spark.sql("SELECT * FROM current_batch WHERE status = 'active'")

# In concurrent execution, batches can overwrite each other's views
for batch in batches:
    process_batch(batch, batch.name)
```

#### 🔍 How It's Detected

The agent tracks all temp view creations in a file and flags when the **same name or variable** is used multiple times:

```python
# Detected: "current_batch" used on lines 5 and 10
df1.createOrReplaceTempView("current_batch")  # Line 5
df2.createOrReplaceTempView("current_batch")  # Line 10 - FLAGGED!
```

#### ✅ The Fix

**BEFORE:**
```python
def process_batch(batch_df, batch_name):
    batch_df.createOrReplaceTempView("current_batch")
    return spark.sql("SELECT * FROM current_batch")
```

**AFTER:**
```python
import uuid

def process_batch(batch_df, batch_name):
    unique_view = f"batch_{batch_name}_{uuid.uuid4()}"
    batch_df.createOrReplaceTempView(unique_view)
    return spark.sql(f"SELECT * FROM {unique_view}")
```

---

## 🟢 LOW Severity Changes

These are informational or may cause subtle behavior differences.

---

### BC-13.3-002: Parquet Timestamp NTZ

**What Changed:** The default behavior for `spark.sql.parquet.inferTimestampNTZ.enabled` changed, affecting how Parquet files with `TIMESTAMP_NTZ` are read.

**Why It Matters:** Timestamp values may be interpreted differently than before.

#### 🔍 How It's Detected

```regex
spark\.sql\.parquet\.inferTimestampNTZ
```

#### ✅ The Fix

Set the configuration explicitly to maintain consistent behavior:

```python
# To restore DBR 13.3 behavior:
spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
```

---

### BC-15.4-002: JDBC useNullCalendar

**What Changed:** The default for `spark.sql.legacy.jdbc.useNullCalendar` changed to `true`.

**Why It Matters:** JDBC timestamp handling may be different.

#### 🔍 How It's Detected

```regex
spark\.sql\.legacy\.jdbc\.useNullCalendar
```

#### ✅ The Fix

```python
# To restore previous behavior:
spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")
```

---

### BC-15.4-004: View Column Type Definition

**What Changed:** Specifying column types in `CREATE VIEW` is no longer allowed.

**Why It Matters:** Views with type specifications will fail to create.

#### ❌ The Problem

```sql
-- This will fail in DBR 15.4+
CREATE VIEW my_view (
    id INT,
    name STRING NOT NULL,
    price DOUBLE DEFAULT 0.0
) AS SELECT * FROM my_table;
```

#### 🔍 How It's Detected

```regex
CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+\w+\s*\([^)]*\b(INT|STRING|BIGINT|DOUBLE|BOOLEAN|NOT\s+NULL|DEFAULT)\b
```

#### ✅ The Fix

**BEFORE:**
```sql
CREATE VIEW my_view (
    id INT,
    name STRING NOT NULL
) AS SELECT * FROM my_table;
```

**AFTER:**
```sql
CREATE VIEW my_view AS 
SELECT 
    CAST(id AS INT) as id,
    name
FROM my_table 
WHERE name IS NOT NULL;
```

---

### BC-16.4-004: MERGE materializeSource=none

**What Changed:** Setting `merge.materializeSource` to `none` is no longer allowed.

**Why It Matters:** MERGE operations with this setting will fail.

#### 🔍 How It's Detected

```regex
merge\.materializeSource.*none
```

#### ✅ The Fix

Remove the configuration or use a different value:

```python
# BEFORE:
spark.conf.set("spark.databricks.delta.merge.materializeSource", "none")

# AFTER: Remove the setting (let Spark choose automatically)
# Or use "auto" if you need to set it explicitly
spark.conf.set("spark.databricks.delta.merge.materializeSource", "auto")
```

---

### BC-SC-003: UDF External Variable Capture

**What Changed:** In Spark Connect, UDFs serialize external variables at **execution time**, not at **definition time**.

**Why It Matters:** Variables captured by UDFs may have different values than expected.

#### ❌ The Problem

```python
multiplier = 1.0

@udf("double")
def apply_multiplier(value):
    return value * multiplier  # Captures multiplier at EXECUTION time!

multiplier = 2.5  # Changed after UDF definition

# In Spark Connect: multiplier will be 2.5 (not 1.0!)
df.withColumn("result", apply_multiplier(col("value")))
```

#### 🔍 How It's Detected

```regex
@udf\s*\(
```

This flags all UDFs for manual review to check for external variable capture.

#### ✅ The Fix

**BEFORE:**
```python
multiplier = 1.0

@udf("double")
def apply_multiplier(value):
    return value * multiplier

multiplier = 2.5
```

**AFTER (Function Factory Pattern):**
```python
def make_multiplier_udf(multiplier):
    @udf("double")
    def apply_multiplier(value):
        return value * multiplier  # Captures at factory call time
    return apply_multiplier

# Create UDF with specific multiplier
multiply_by_1 = make_multiplier_udf(1.0)
multiply_by_2_5 = make_multiplier_udf(2.5)

df.withColumn("result", multiply_by_1(col("value")))
```

---

### BC-SC-004: Schema Access in Loops

**What Changed:** In Spark Connect, accessing `df.columns`, `df.schema`, or `df.dtypes` triggers an RPC call to the server each time.

**Why It Matters:** Accessing schema properties in loops can cause significant performance degradation.

#### ❌ The Problem

```python
# BAD: df.columns is called on every iteration!
for col_name in df.columns:
    if df.columns.count(col_name) > 1:  # Another RPC call!
        print(f"Duplicate: {col_name}")
```

#### 🔍 How It's Detected

```regex
\.(columns|schema|dtypes)\b
```

This flags for review to ensure schema access isn't in a loop.

#### ✅ The Fix

**BEFORE:**
```python
for col_name in df.columns:
    if col_name in df.columns:
        process(col_name)
```

**AFTER:**
```python
# Cache the schema once
cached_columns = df.columns

for col_name in cached_columns:
    if col_name in cached_columns:
        process(col_name)
```

---

## Summary: Detection Patterns

| ID | Pattern | File Types |
|----|---------|------------|
| BC-17.3-001 | `\binput_file_name\s*\(` | .py, .sql, .scala |
| BC-15.4-001 | `VariantType\s*\(` | .py |
| BC-15.4-003 | `(IF\|IS)\s*!(?!\s*=)` | .sql |
| BC-15.4-003b | `\s!\s*(IN\|BETWEEN\|LIKE\|EXISTS)\b` | .sql |
| BC-16.4-001a | `import\s+scala\.collection\.JavaConverters` | .scala |
| BC-16.4-001b | `\.to\s*\[\s*(List\|Set\|...)\s*\]` | .scala |
| BC-16.4-001c | `\bTraversableOnce\b` | .scala |
| BC-16.4-001d | `\bTraversable\b(?!Once)` | .scala |
| BC-16.4-001e | `\bStream\s*\.\s*(from\|...)` | .scala |
| BC-17.3-002 | `cloudFiles\.useIncrementalListing` | .py, .sql, .scala |
| BC-SC-002 | Same temp view name used multiple times | .py, .scala |
| BC-SC-003 | `@udf\s*\(` | .py |
| BC-SC-004 | `\.(columns\|schema\|dtypes)\b` | .py |

---

## Quick Reference: What Gets Auto-Fixed vs Manual Review

| Category | Auto-Fixed | Manual Review |
|----------|------------|---------------|
| **BC-17.3-001** | ✅ DataFrame API, SQL strings | Triple-quoted SQL in complex contexts |
| **BC-15.4-001** | ❌ | ✅ Requires logic rewrite |
| **BC-15.4-003** | ✅ All `!` → `NOT` patterns | - |
| **BC-16.4-001a-e** | ✅ All Scala 2.13 changes | - |
| **BC-SC-002** | ❌ | ✅ Flagged for review |
| **BC-SC-003** | ❌ | ✅ Flagged for review |
| **BC-SC-004** | ❌ | ✅ Flagged for review |

---

*Last Updated: January 2026*
