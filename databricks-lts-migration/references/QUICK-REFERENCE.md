# Quick Reference: Breaking Changes

A condensed reference for all breaking changes. Use this to quickly explain issues to users.

---

## 🔴 HIGH Severity (Immediate Failures)

### BC-17.3-001: input_file_name() Removed
❌ `df.withColumn("src", input_file_name())`  
🔍 `\binput_file_name\s*\(`  
✅ `df.withColumn("src", col("_metadata.file_name"))`  
✅ SQL: `SELECT _metadata.file_name FROM ...`

### BC-15.4-001: VARIANT in Python UDF
❌ `@udf(returnType=VariantType())`  
🔍 `VariantType\s*\(`  
✅ Use `StringType()` + `json.dumps()`, then `parse_json()` later

### BC-16.4-001a: Scala JavaConverters
❌ `import scala.collection.JavaConverters._`  
🔍 `import\s+scala\.collection\.JavaConverters`  
✅ `import scala.jdk.CollectionConverters._`

### BC-16.4-001c: Scala TraversableOnce
❌ `def process(items: TraversableOnce[T])`  
🔍 `\bTraversableOnce\b`  
✅ `def process(items: IterableOnce[T])`

### BC-16.4-001d: Scala Traversable
❌ `def process(data: Traversable[Int])`  
🔍 `\bTraversable\b(?!Once)`  
✅ `def process(data: Iterable[Int])`

---

## 🟡 MEDIUM Severity (Potential Issues)

### BC-15.4-003: '!' Syntax for NOT
❌ `CREATE TABLE IF ! EXISTS t`  
❌ `WHERE status IS ! NULL`  
❌ `WHERE id ! IN (1,2,3)`  
🔍 `(IF|IS)\s*!(?!\s*=)` and `\s!\s*(IN|BETWEEN|LIKE|EXISTS)`  
✅ `IF NOT EXISTS`, `IS NOT NULL`, `NOT IN`

### BC-16.4-001b: Scala .to[Collection]
❌ `list.to[List]`  
🔍 `\.to\s*\[\s*(List|Set|Vector|Seq|Array)\s*\]`  
✅ `list.to(List)`

### BC-16.4-001e: Scala Stream
❌ `Stream.from(1)`  
🔍 `\bStream\s*\.\s*(from|continually|iterate)`  
✅ `LazyList.from(1)`

### BC-17.3-002: Auto Loader Default Changed
❌ Implicit `cloudFiles.useIncrementalListing` behavior  
🔍 `cloudFiles\.useIncrementalListing`  
✅ Set explicitly: `.option("cloudFiles.useIncrementalListing", "auto")`

### BC-SC-002: Temp View Reuse [MANUAL REVIEW]
❌ Same view name used multiple times in file  
🔍 Track `createOrReplaceTempView` calls, flag duplicates  
✅ Use unique names: `f"view_{uuid.uuid4()}"`

---

## 🟢 LOW Severity (Subtle Changes)

### BC-13.3-002: Parquet Timestamp NTZ
🔍 `spark\.sql\.parquet\.inferTimestampNTZ`  
✅ Set `spark.sql.parquet.inferTimestampNTZ.enabled = false` for old behavior

### BC-15.4-002: JDBC Null Calendar
🔍 `spark\.sql\.legacy\.jdbc\.useNullCalendar`  
✅ Set `spark.sql.legacy.jdbc.useNullCalendar = false` for old behavior

### BC-15.4-004: View Column Types
❌ `CREATE VIEW v (id INT, name STRING) AS SELECT ...`  
🔍 `CREATE\s+VIEW.*\([^)]*\b(INT|STRING|BIGINT)\b`  
✅ Use `CAST()` in the SELECT instead

### BC-16.4-004: MERGE materializeSource
🔍 `merge\.materializeSource.*none`  
✅ Remove setting or use `"auto"`

### BC-SC-003: UDF Variable Capture [MANUAL REVIEW]
❌ UDF captures external variable that changes later  
🔍 `@udf\s*\(`  
✅ Use function factory pattern to capture at definition time

### BC-SC-004: Schema in Loops [MANUAL REVIEW]
❌ `for col in df.columns:` (RPC on each iteration)  
🔍 `\.(columns|schema|dtypes)\b`  
✅ Cache first: `cols = df.columns; for col in cols:`

---

## Auto-Fix Summary

| ID | Auto-Fixed | Notes |
|----|------------|-------|
| BC-17.3-001 | ✅ | DataFrame API & SQL strings |
| BC-15.4-001 | ❌ | Requires logic rewrite |
| BC-15.4-003 | ✅ | All `!` → `NOT` |
| BC-16.4-001a-e | ✅ | All Scala 2.13 changes |
| BC-17.3-002 | ❌ | Informational only |
| BC-SC-002/3/4 | ❌ | Flagged for manual review |
