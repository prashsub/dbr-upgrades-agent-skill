# Breaking Changes Explained: DBR 13.3 → 17.3

This guide explains each breaking change in simple terms, with examples showing:
- ❌ **The Problem**: Code that will break
- 🔍 **How It's Detected**: The pattern the agent looks for
- ✅ **The Fix**: How the code should be changed

---

## Table of Contents

### HIGH Severity
| ID | Name | DBR Version |
|-----|------|-------------|
| [BC-17.3-001](#bc-173-001-input_file_name-removed) | input_file_name() Removed | 17.3 |
| [BC-13.3-001](#bc-133-001-merge-into-type-casting) | MERGE INTO Type Casting (ANSI Mode) | 13.3 |
| [BC-16.4-001a](#bc-164-001a-scala-javaconverters) | Scala JavaConverters | 16.4 |
| [BC-16.4-001c](#bc-164-001c-scala-traversableonce) | Scala TraversableOnce | 16.4 |
| [BC-16.4-001d](#bc-164-001d-scala-traversable) | Scala Traversable | 16.4 |
| [BC-16.4-002](#bc-164-002-scala-hashmaphashset-ordering) | Scala HashMap/HashSet Ordering | 16.4 |
| [BC-SC-001](#bc-sc-001-spark-connect-lazy-analysis) | Spark Connect Lazy Analysis | 13.3+ |

### MEDIUM Severity
| ID | Name | DBR Version |
|-----|------|-------------|
| [BC-15.4-001](#bc-154-001-variant-type-in-python-udf) | VARIANT Type in Python UDF | 15.4+ |
| [BC-15.4-003](#bc-154-003--syntax-for-not) | '!' Syntax for NOT | 15.4 |
| [BC-16.4-001b](#bc-164-001b-scala-tocollection-syntax) | Scala .to[Collection] | 16.4 |
| [BC-16.4-001e](#bc-164-001e-scala-stream-lazy) | Scala Stream (Lazy) | 16.4 |
| [BC-16.4-001f](#bc-164-001f-scala-toiterator) | Scala .toIterator | 16.4 |
| [BC-16.4-001g](#bc-164-001g-scala-viewforce) | Scala .view.force | 16.4 |
| [BC-16.4-001h](#bc-164-001h-scala-collectionseq) | Scala collection.Seq | 16.4 |
| [BC-17.3-002](#bc-173-002-auto-loader-incremental-listing) | Auto Loader Incremental Listing | 17.3 |
| [BC-13.3-003](#bc-133-003-overwriteschema-with-dynamic-partition) | overwriteSchema + Dynamic Partition | 13.3 |
| [BC-SC-002](#bc-sc-002-temp-view-name-reuse) | Temp View Name Reuse | 13.3+ |

### LOW Severity
| ID | Name | DBR Version |
|-----|------|-------------|
| [BC-16.4-001i](#bc-164-001i-scala-symbol-literals) | Scala Symbol Literals | 16.4 |
| [BC-13.3-002](#bc-133-002-parquet-timestamp-ntz) | Parquet Timestamp NTZ | 13.3 |
| [BC-15.4-002](#bc-154-002-jdbc-usenullcalendar) | JDBC useNullCalendar | 15.4 |
| [BC-15.4-004](#bc-154-004-view-column-type-definition) | View Column Type Definition | 15.4 |
| [BC-16.4-004](#bc-164-004-merge-materializesourcenone) | MERGE materializeSource=none | 16.4 |
| [BC-SC-003](#bc-sc-003-udf-external-variable-capture) | UDF External Variable Capture | 14.3+ |
| [BC-SC-004](#bc-sc-004-schema-access-in-loops) | Schema Access in Loops | 13.3+ |

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

### BC-13.3-001: MERGE INTO Type Casting

**What Changed:** In DBR 13.3+ with ANSI mode enabled, MERGE INTO operations that involve implicit type casting may throw `CAST_OVERFLOW` errors instead of silently truncating or converting values.

**Why It Matters:** MERGE operations that previously succeeded may now fail if there are type mismatches between source and target columns.

#### ❌ The Problem

```sql
-- If source.amount is BIGINT and target.amount is INT,
-- values > 2147483647 will throw CAST_OVERFLOW in ANSI mode
MERGE INTO target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.amount = source.amount
```

#### 🔍 How It's Detected

```regex
\bMERGE\s+INTO\b
```

This flags all MERGE statements for review of type compatibility.

#### ✅ The Fix

Add explicit bounds checking or type casting:

**BEFORE:**
```sql
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.amount = source.amount
```

**AFTER:**
```sql
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED AND source.amount BETWEEN -2147483648 AND 2147483647
THEN UPDATE SET target.amount = CAST(source.amount AS INT)
```

---

### BC-SC-001: Spark Connect Lazy Analysis

**What Changed:** In Spark Connect mode, schema analysis is deferred until action execution. Exceptions like `AnalysisException` won't be thrown until you call an action (e.g., `.show()`, `.collect()`).

**Why It Matters:** Try/except blocks around transformations won't catch schema errors. Error handling that worked in classic Spark may not work in Spark Connect.

#### ❌ The Problem

```python
try:
    # In classic Spark, this throws immediately if column doesn't exist
    df = spark.table("my_table").select("nonexistent_column")
except AnalysisException:
    df = spark.table("my_table").select("default_column")
# In Spark Connect, the exception is NOT caught here!
```

#### 🔍 How It's Detected

```regex
except\s+.*(?:AnalysisException|SparkException|IllegalArgumentException)
```

This flags exception handling around Spark operations for review.

#### ✅ The Fix

Trigger eager analysis with `.schema` or `.columns` if you need to catch errors:

**BEFORE:**
```python
try:
    df = spark.table("my_table").select("maybe_column")
except AnalysisException:
    df = spark.table("my_table").select("default_column")
```

**AFTER:**
```python
df = spark.table("my_table")
if "maybe_column" in df.columns:  # Triggers analysis
    result = df.select("maybe_column")
else:
    result = df.select("default_column")
```

---

### BC-15.4-001: VARIANT Type in Python UDF

**What Changed:** The `VARIANT` data type may cause exceptions when used as input or output for Python UDFs, UDAFs, or UDTFs in DBR 15.4+.

> ⚠️ **REVIEW REQUIRED:** VARIANT UDF behavior may vary by DBR version. Test on your target DBR version, or use the safer StringType + JSON approach shown below.

**Why It Matters:** Using `VariantType()` in a UDF may throw runtime errors depending on DBR version. For cross-version compatibility, consider using StringType with JSON serialization.

#### ❌ The Problem

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import VariantType

# May fail or behave unexpectedly - test on target DBR version
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

### BC-16.4-001f: Scala .toIterator

**What Changed:** In Scala 2.13, `.toIterator` is deprecated in favor of `.iterator`.

**Why It Matters:** Code will show deprecation warnings.

#### ❌ The Problem

```scala
val list = List(1, 2, 3)
val iter = list.toIterator
```

#### 🔍 How It's Detected

```regex
\.toIterator\b
```

#### ✅ The Fix

**BEFORE:**
```scala
val iter = list.toIterator
```

**AFTER:**
```scala
val iter = list.iterator
```

---

### BC-16.4-001g: Scala .view.force

**What Changed:** In Scala 2.13, `.view.force` is deprecated. Use `.view.to(Collection)` instead.

**Why It Matters:** Code will show deprecation warnings.

#### ❌ The Problem

```scala
val result = list.view.map(_ * 2).force
```

#### 🔍 How It's Detected

```regex
\.view\s*\.\s*force\b
```

#### ✅ The Fix

**BEFORE:**
```scala
val result = list.view.map(_ * 2).force
```

**AFTER:**
```scala
val result = list.view.map(_ * 2).to(List)
```

---

### BC-16.4-001h: Scala collection.Seq

**What Changed:** In Scala 2.13, `scala.collection.Seq` now refers to `scala.collection.immutable.Seq` by default (previously referred to the mutable variant).

**Why It Matters:** Code that relies on `collection.Seq` being mutable may behave differently.

#### ❌ The Problem

```scala
import scala.collection.Seq
// In 2.12: Seq was mutable by default
// In 2.13: Seq is immutable by default
```

#### 🔍 How It's Detected

```regex
\bcollection\.Seq\b
```

#### ✅ The Fix

Use explicit imports:
```scala
// For immutable (now the default)
import scala.collection.immutable.Seq

// For mutable (if you need it)
import scala.collection.mutable.Seq
```

---

### BC-16.4-001i: Scala Symbol Literals

**What Changed:** In Scala 2.13, symbol literals (`'symbolName`) are deprecated.

**Why It Matters:** Code will show deprecation warnings.

#### ❌ The Problem

```scala
val sym = 'mySymbol
df.col('columnName)
```

#### 🔍 How It's Detected

```regex
'[a-zA-Z_][a-zA-Z0-9_]*\b
```

#### ✅ The Fix

**BEFORE:**
```scala
val sym = 'mySymbol
```

**AFTER:**
```scala
val sym = Symbol("mySymbol")
```

---

### BC-16.4-002: Scala HashMap/HashSet Ordering

**What Changed:** In Scala 2.13, `HashMap` and `HashSet` iteration order changed and is no longer deterministic.

**Why It Matters:** Code that relies on iteration order will produce different results.

#### ❌ The Problem

```scala
val map = HashMap("a" -> 1, "b" -> 2, "c" -> 3)
// DON'T rely on iteration order - it changed in 2.13!
map.keys.foreach(println)
```

#### 🔍 How It's Detected

```regex
\b(HashMap|HashSet)\s*[\[\(]
```

#### ✅ The Fix

If order matters, use `ListMap`, `LinkedHashSet`, or explicit sorting:

**BEFORE:**
```scala
val map = HashMap("a" -> 1, "b" -> 2, "c" -> 3)
map.keys.foreach(println)  // Order not guaranteed!
```

**AFTER:**
```scala
// Option 1: Use ListMap for insertion order
import scala.collection.immutable.ListMap
val map = ListMap("a" -> 1, "b" -> 2, "c" -> 3)

// Option 2: Sort explicitly
HashMap("a" -> 1, "b" -> 2, "c" -> 3).toSeq.sortBy(_._1).foreach(println)
```

---

### BC-17.3-002: Auto Loader Incremental Listing

**What Changed:** The default value for `cloudFiles.useIncrementalListing` changed from `auto` to `false` in DBR 17.3.

**Why It Matters:** Auto Loader may have different performance characteristics if you relied on the implicit default.

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
format\s*\(\s*["']cloudFiles["']\s*\)
```

This flags any Auto Loader usage for review to check if the new default affects performance.

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

### BC-13.3-003: overwriteSchema with Dynamic Partition

**What Changed:** In DBR 13.3+, you cannot combine `overwriteSchema=true` with `partitionOverwriteMode='dynamic'` in the same write operation.

**Why It Matters:** Write operations using both options will fail.

#### ❌ The Problem

```python
# This combination will fail in DBR 13.3+
df.write \
    .option("overwriteSchema", "true") \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .saveAsTable("my_table")
```

#### 🔍 How It's Detected

```regex
overwriteSchema.*true
```

This flags uses of `overwriteSchema=true` for review to check if combined with dynamic partition mode.

#### ✅ The Fix

Separate schema evolution from partition overwrites into distinct operations:

**BEFORE:**
```python
df.write \
    .option("overwriteSchema", "true") \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .saveAsTable("my_table")
```

**AFTER:**
```python
# Step 1: Evolve schema with full overwrite (if needed)
df.write \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("my_table")

# Step 2: Use dynamic partition for subsequent writes
df.write \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .saveAsTable("my_table")
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
| BC-13.3-001 | `\bMERGE\s+INTO\b` | .py, .sql, .scala |
| BC-13.3-003 | `overwriteSchema.*true` | .py, .scala |
| BC-15.4-001 | `VariantType\s*\(` | .py |
| BC-15.4-003 | `(IF\|IS)\s*!(?!\s*=)` | .sql |
| BC-15.4-003b | `\s!\s*(IN\|BETWEEN\|LIKE\|EXISTS)\b` | .sql |
| BC-16.4-001a | `import\s+scala\.collection\.JavaConverters` | .scala |
| BC-16.4-001b | `\.to\s*\[\s*(List\|Set\|...)\s*\]` | .scala |
| BC-16.4-001c | `\bTraversableOnce\b` | .scala |
| BC-16.4-001d | `\bTraversable\b(?!Once)` | .scala |
| BC-16.4-001e | `\bStream\s*\.\s*(from\|...)` | .scala |
| BC-16.4-001f | `\.toIterator\b` | .scala |
| BC-16.4-001g | `\.view\s*\.\s*force\b` | .scala |
| BC-16.4-001h | `\bcollection\.Seq\b` | .scala |
| BC-16.4-001i | `'[a-zA-Z_][a-zA-Z0-9_]*\b` | .scala |
| BC-16.4-002 | `\b(HashMap\|HashSet)\s*[\[\(]` | .scala |
| BC-17.3-002 | `format\s*\(\s*["']cloudFiles["']\s*\)` | .py, .scala |
| BC-SC-001 | `except.*AnalysisException` | .py, .scala |
| BC-SC-002 | Same temp view name used multiple times | .py, .scala |
| BC-SC-003 | `@udf\s*\(` | .py |
| BC-SC-004 | `\.(columns\|schema\|dtypes)\b` | .py |

---

## Quick Reference: What Gets Auto-Fixed vs Manual Review

| Category | Auto-Fixed | Manual Review |
|----------|------------|---------------|
| **BC-17.3-001** | ✅ DataFrame API, SQL strings | Triple-quoted SQL in complex contexts |
| **BC-13.3-001** | ❌ | ✅ Review type casting in MERGE |
| **BC-13.3-003** | ❌ | ✅ Review overwriteSchema usage |
| **BC-15.4-001** | ❌ | ✅ Test on target DBR or use StringType |
| **BC-15.4-003** | ✅ All `!` → `NOT` patterns | - |
| **BC-16.4-001a-i** | ✅ All Scala 2.13 changes | BC-16.4-001h (collection.Seq) needs review |
| **BC-16.4-002** | ❌ | ✅ Review if iteration order matters |
| **BC-SC-001** | ❌ | ✅ Review exception handling logic |
| **BC-SC-002** | ❌ | ✅ Flagged for review |
| **BC-SC-003** | ❌ | ✅ Flagged for review |
| **BC-SC-004** | ❌ | ✅ Flagged for review |
| **BC-13.3-002** | ❌ | ⚙️ Config setting |
| **BC-15.4-002** | ❌ | ⚙️ Config setting |
| **BC-16.4-004** | ❌ | ⚙️ Config setting |
| **BC-17.3-002** | ❌ | ⚙️ Config setting |

---

## 📋 Developer Action Guide: Manual Review Items

When the scanner flags something for **manual review**, follow these step-by-step guides.

---

### 🔍 Manual Review: BC-SC-002 (Temp View Name Reuse)

**Why it's flagged:** The same temp view name appears multiple times in your code.

**Step-by-Step Review:**

1. **Check if views are in a loop or function called multiple times:**
   ```python
   # IF you see this pattern → FIX NEEDED
   for item in items:
       df.createOrReplaceTempView("temp_view")  # ⚠️ Same name reused!
   ```

2. **Check if views are independent operations:**
   ```python
   # IF you see this pattern → LIKELY OKAY (but verify)
   df1.createOrReplaceTempView("customers")
   # ... other code ...
   df2.createOrReplaceTempView("orders")  # Different name = OK
   ```

3. **Decision Matrix:**

   | Scenario | Action |
   |----------|--------|
   | Same name in a loop | ✅ **FIX**: Add UUID to name |
   | Same name in a function called multiple times | ✅ **FIX**: Add UUID to name |
   | Different names for different purposes | ❌ **No action needed** |
   | View created once and used throughout | ❌ **No action needed** |

4. **Apply the fix:**
   ```python
   import uuid
   unique_name = f"temp_view_{uuid.uuid4().hex[:8]}"
   df.createOrReplaceTempView(unique_name)
   ```

---

### 🔍 Manual Review: BC-SC-003 (UDF External Variable Capture)

**Why it's flagged:** Your UDF may be capturing variables that could change.

**Step-by-Step Review:**

1. **Look at the UDF code - does it reference variables defined OUTSIDE the function?**
   ```python
   threshold = 100  # ← External variable
   
   @udf("boolean")
   def is_above_threshold(value):
       return value > threshold  # ← References external variable!
   ```

2. **Decision Matrix:**

   | What the UDF references | Action |
   |------------------------|--------|
   | Only function parameters | ❌ **No action needed** |
   | Constants (hardcoded values) | ❌ **No action needed** |
   | External variables that NEVER change | ⚠️ **Consider fixing** (defensive) |
   | External variables that CHANGE after UDF definition | ✅ **FIX**: Use function factory |

3. **Apply the fix (Function Factory Pattern):**
   ```python
   # BEFORE (problematic in Spark Connect)
   threshold = 100
   @udf("boolean")
   def is_above(value):
       return value > threshold
   
   # AFTER (safe)
   def make_threshold_udf(threshold_value):
       @udf("boolean")
       def is_above(value):
           return value > threshold_value  # Captured at creation
       return is_above
   
   is_above_100 = make_threshold_udf(100)
   ```

---

### 🔍 Manual Review: BC-SC-004 (Schema Access in Loops)

**Why it's flagged:** You're accessing `df.columns`, `df.schema`, or `df.dtypes`.

**Step-by-Step Review:**

1. **Check if schema access is INSIDE a loop:**
   ```python
   # PROBLEMATIC - N RPC calls in Spark Connect!
   for i in range(100):
       if "col_name" in df.columns:  # ← Called 100 times!
           ...
   ```

2. **Check if schema access is OUTSIDE loops:**
   ```python
   # OKAY - Only 1 RPC call
   columns = df.columns  # ← Called once
   for col in columns:
       ...
   ```

3. **Decision Matrix:**

   | Location | Action |
   |----------|--------|
   | Inside a `for`/`while` loop | ✅ **FIX**: Cache outside loop |
   | Inside a function called repeatedly | ✅ **FIX**: Cache before calls |
   | At module/notebook top level (once) | ❌ **No action needed** |
   | In a conditional that runs once | ❌ **No action needed** |

4. **Apply the fix:**
   ```python
   # BEFORE
   for col in df.columns:  # RPC on each iteration
       if col in df.columns:  # Another RPC!
           process(col)
   
   # AFTER
   cached_columns = df.columns  # One RPC
   for col in cached_columns:
       if col in cached_columns:  # No RPC
           process(col)
   ```

---

### 🔍 Manual Review: BC-15.4-004 (View Column Type Definition)

**Why it's flagged:** Your `CREATE VIEW` statement has column type definitions.

**Step-by-Step Review:**

1. **Identify the problematic syntax:**
   ```sql
   -- Look for types, NOT NULL, or DEFAULT in the column list
   CREATE VIEW my_view (
       id INT,           -- ← Type definition (not allowed)
       name STRING NOT NULL,  -- ← NOT NULL constraint (not allowed)
       value DOUBLE DEFAULT 0.0  -- ← DEFAULT value (not allowed)
   ) AS SELECT ...
   ```

2. **Apply the fix - Move constraints to the SELECT:**
   ```sql
   -- AFTER: No column definitions, constraints in query
   CREATE VIEW my_view AS
   SELECT
       CAST(id AS INT) as id,
       name,
       COALESCE(value, 0.0) as value
   FROM source_table
   WHERE name IS NOT NULL;  -- Replaces NOT NULL constraint
   ```

---

## ⚙️ Developer Action Guide: Configuration Settings

These breaking changes are about **default behavior changes**. You may not need to do anything if your code works correctly.

### When to Add Configuration Settings

| Scenario | Action |
|----------|--------|
| Code works fine on new DBR | ❌ **No config needed** |
| Code behaves differently (but correctly) | ❌ **No config needed** (adapt to new behavior) |
| Code breaks or gives wrong results | ✅ **Add config to restore old behavior** |
| You want guaranteed consistent behavior across DBR versions | ✅ **Add config explicitly** |

---

### ⚙️ Config: BC-13.3-002 (Parquet Timestamp NTZ)

**What changed:** How Parquet files infer `TIMESTAMP_NTZ` type.

**Test if you need this:**
```python
# Read a Parquet file and check timestamp columns
df = spark.read.parquet("/path/to/data")
df.printSchema()  # Check if timestamp types are what you expect
```

**Where to add the config:**

| Location | How to Add |
|----------|------------|
| **Notebook (session)** | `spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")` |
| **Cluster config** | Add to Spark config: `spark.sql.parquet.inferTimestampNTZ.enabled false` |
| **Job config** | Add to job's Spark configuration in the job JSON/UI |
| **Init script** | Add `spark.conf.set(...)` to cluster's init script |

**Full example:**
```python
# At the START of your notebook/job
spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")

# Then run your Parquet reads
df = spark.read.parquet("/path/to/data")
```

---

### ⚙️ Config: BC-15.4-002 (JDBC useNullCalendar)

**What changed:** Default for `spark.sql.legacy.jdbc.useNullCalendar` changed to `true`.

**Test if you need this:**
```python
# Read from JDBC and check timestamp values
df = spark.read.jdbc(url, table)
df.select("timestamp_column").show()
# Compare with expected values from your source system
```

**Apply the fix (only if timestamps are wrong):**
```python
# To restore DBR 13.3 behavior:
spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")

# Then run your JDBC reads
df = spark.read.jdbc(url, table)
```

---

### ⚙️ Config: BC-16.4-004 (MERGE materializeSource=none)

**What changed:** Setting `merge.materializeSource` to `none` now throws an error.

**Find the problematic code:**
```python
# Search your code for this line - it will FAIL:
spark.conf.set("spark.databricks.delta.merge.materializeSource", "none")
```

**Apply the fix:**
```python
# Option 1: Remove the setting entirely (RECOMMENDED)
# Just delete the spark.conf.set line - let Spark use default

# Option 2: Change to "auto"
spark.conf.set("spark.databricks.delta.merge.materializeSource", "auto")
```

---

### ⚙️ Config: BC-17.3-002 (Auto Loader Incremental Listing)

**What changed:** Default for `cloudFiles.useIncrementalListing` changed from `auto` to `false`.

**Test if you need this:**
1. Run your Auto Loader job on new DBR version
2. Check if performance is acceptable
3. Check if all files are being processed correctly

**If you need the old behavior (for performance):**
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useIncrementalListing", "auto")  # ← Add this line
    .schema(schema)
    .load(path)
)
```

---

## 🔄 Complete Developer Workflow

### Step 1: Run the Scanner
```bash
# Using Databricks Assistant or locally
python scan-breaking-changes.py /path/to/notebooks --target-version 17.3
```

### Step 2: Review the Report

| Finding Type | What It Means | Developer Action |
|--------------|---------------|------------------|
| 🔴 **HIGH + AUTO-FIX** | Will break, can be auto-fixed | Run `apply-fixes.py` |
| 🟡 **MEDIUM + MANUAL** | May break, needs review | Follow guides above |
| ⚙️ **CONFIG** | Behavior changed | Test first, add config if needed |

### Step 3: Apply Auto-Fixes
```bash
# For auto-fixable issues
python apply-fixes.py /path/to/notebooks --target-version 17.3
```

### Step 4: Manual Review Checklist

For each **MANUAL REVIEW** finding:

- [ ] Locate the flagged line in your code
- [ ] Identify which category (BC-SC-002, BC-SC-003, BC-SC-004, or BC-15.4-004)
- [ ] Follow the decision matrix in this guide
- [ ] If fix needed, apply the fix pattern shown
- [ ] If no fix needed, document why (false positive)
- [ ] Mark as reviewed in your tracking system

### Step 5: Test Configuration Settings

For each **CONFIG** finding:

- [ ] Run your code on the new DBR version **without** adding config
- [ ] Check if results are correct
- [ ] If results differ, decide: adapt code OR add config
- [ ] If adding config, choose where (notebook, cluster, or job level)
- [ ] Document any config settings added

### Step 6: Validate
```bash
# Run validation to ensure all issues are resolved
python validate-migration.py /path/to/notebooks
```

---

## 📊 Summary: Action by Category

| Category | Patterns | Primary Action |
|----------|----------|----------------|
| **Auto-Fix** | 10 patterns | Run `apply-fixes.py` |
| **Manual Review** | 12 patterns | Follow guides in this document |
| **Config Settings** | 4 patterns | Test first, add config only if needed |

### Auto-Fix Patterns (10)
- BC-17.3-001: `input_file_name()`
- BC-15.4-003: `!` syntax for NOT
- BC-16.4-001a: JavaConverters
- BC-16.4-001b: `.to[Collection]`
- BC-16.4-001c: TraversableOnce
- BC-16.4-001d: Traversable
- BC-16.4-001e: Stream → LazyList
- BC-16.4-001f: `.toIterator`
- BC-16.4-001g: `.view.force`
- BC-16.4-001i: Symbol literals

### Manual Review Patterns (12)
- BC-13.3-001: MERGE INTO type casting
- BC-13.3-003: overwriteSchema + dynamic partition
- BC-15.4-001: VARIANT type in UDF
- BC-15.4-004: View column type definition
- BC-16.4-001h: `collection.Seq` import
- BC-16.4-002: HashMap/HashSet ordering
- BC-SC-001: Spark Connect lazy analysis
- BC-SC-002: Temp view name reuse
- BC-SC-003: UDF external variable capture
- BC-SC-004: Schema access in loops

### Config Setting Patterns (4)
- BC-13.3-002: Parquet Timestamp NTZ
- BC-15.4-002: JDBC useNullCalendar
- BC-16.4-004: MERGE materializeSource
- BC-17.3-002: Auto Loader incremental listing

---

*Last Updated: January 2026*
