# Quick Reference: Breaking Changes

A condensed reference for all breaking changes. Use this to quickly explain issues to users.

---

## 🔴 HIGH Severity (Immediate Failures)

### BC-17.3-001: input_file_name() Removed
❌ `df.withColumn("src", input_file_name())`  
🔍 `\binput_file_name\s*\(`  
✅ `df.withColumn("src", col("_metadata.file_name"))`  
✅ SQL: `SELECT _metadata.file_name FROM ...`

### BC-13.3-001: MERGE INTO Type Casting (ANSI Mode)
❌ `MERGE INTO target SET int_col = bigint_col` *(overflow throws error)*  
🔍 `\bMERGE\s+INTO\b`  
✅ Add explicit bounds checking: `CASE WHEN val > 2147483647 THEN NULL ELSE CAST(val AS INT) END`

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

### BC-16.4-002: HashMap/HashSet Ordering Changed
❌ `HashMap("a" -> 1, "b" -> 2).foreach(...)` *(order may differ)*  
🔍 `\b(HashMap|HashSet)\s*[\[\(]`  
✅ `map.toSeq.sortBy(_._1).foreach(...)` or use `ListMap`

### BC-SC-001: Spark Connect Lazy Analysis
❌ `try: df.withColumn("x", col("bad_col"))` *(error at action, not transform)*  
🔍 `try\s*:` near DataFrame transforms  
✅ Add `_ = df.columns` after transform to force early validation

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

### BC-16.4-001f: Scala .toIterator
❌ `list.toIterator`  
🔍 `\.toIterator\b`  
✅ `list.iterator`

### BC-16.4-001g: Scala .view.force
❌ `list.view.map(_ * 2).force`  
🔍 `\.view\s*\.\s*force\b`  
✅ `list.view.map(_ * 2).to(List)`

### BC-16.4-001h: Scala collection.Seq Changed
❌ `import scala.collection.Seq` *(now immutable)*  
🔍 `\bcollection\.Seq\b`  
✅ Use explicit `immutable.Seq` or `mutable.Seq`

### BC-13.3-003: overwriteSchema + Dynamic Partition
❌ Using both `overwriteSchema=true` and `partitionOverwriteMode=dynamic`  
🔍 `overwriteSchema.*true` near partition operations  
✅ Separate into two operations: schema evolution first, then partition overwrite

### BC-17.3-002: Auto Loader Default Changed
❌ Implicit `cloudFiles.useIncrementalListing` behavior  
🔍 `format\s*\(\s*[\"']cloudFiles[\"']\s*\)`  
✅ Set explicitly: `.option("cloudFiles.useIncrementalListing", "auto")`

### BC-15.4-006: VIEW Schema Binding Mode
❌ View schema binding mode changed  
🔍 `CREATE\s+(OR\s+REPLACE\s+)?VIEW`  
✅ Review schema evolution behavior on target DBR

### BC-16.4-003: Data Source Cache Options
❌ Cached reads may ignore options  
🔍 `spark\.sql\.legacy\.readFileSourceTableCacheIgnoreOptions`  
✅ Set `spark.sql.legacy.readFileSourceTableCacheIgnoreOptions = true`

### BC-16.4-006: Auto Loader cleanSource Behavior
❌ cleanSource file deletion timing changed  
🔍 `cloudFiles\.cleanSource`  
✅ Review file cleanup behavior and timing

---

## 🟢 LOW Severity (Subtle Changes)

### BC-15.4-001: VARIANT in Python UDF [REVIEW]
⚠️ `@udf(returnType=VariantType())` *(may fail in 15.4+)*  
🔍 `VariantType\s*\(`  
✅ Test on target DBR or use `StringType()` + `json.dumps()`, then `parse_json()` later

### BC-15.4-004: View Column Types
❌ `CREATE VIEW v (id INT, name STRING) AS SELECT ...`  
🔍 `CREATE\s+VIEW.*\([^)]*\b(INT|STRING|BIGINT)\b`  
✅ Use `CAST()` in the SELECT instead

### BC-13.3-002: Parquet Timestamp NTZ
🔍 `spark\.sql\.parquet\.inferTimestampNTZ`  
✅ Set `spark.sql.parquet.inferTimestampNTZ.enabled = false` for old behavior

### BC-13.3-004: ANSI Store Assignment Policy
🔍 `spark\.sql\.storeAssignmentPolicy`  
✅ Review type assignment behavior in MERGE/UPDATE

### BC-15.4-002: JDBC Null Calendar
🔍 `spark\.sql\.legacy\.jdbc\.useNullCalendar`  
✅ Set `spark.sql.legacy.jdbc.useNullCalendar = false` for old behavior

### BC-15.4-005: JDBC Reads
🔍 `\.jdbc\(|\.format\s*\(\s*[\"']jdbc[\"']\s*\)`  
✅ Run self-comparison test (read with `useNullCalendar=true` vs `false`, diff with `exceptAll`). Fix only if diff > 0

### BC-16.4-004: MERGE materializeSource
🔍 `merge\.materializeSource.*none`  
✅ Remove setting or use `"auto"`

### BC-16.4-001i: Scala Symbol Literals
❌ `val sym = 'mySymbol`  
🔍 `'[a-zA-Z_][a-zA-Z0-9_]*`  
✅ `val sym = Symbol("mySymbol")`

### BC-16.4-005: Json4s Library
🔍 `import\s+org\.json4s`  
✅ Review json4s usage for compatibility

### BC-17.3-003: Spark Connect Null Handling
❌ `array(lit(null))` *(may behave differently in Connect)*  
🔍 `(array|map|struct)\s*\(`  
✅ Handle null values explicitly

### BC-17.3-004: Spark Connect Decimal Precision
❌ `DecimalType()` without precision  
🔍 `DecimalType\s*\(`  
✅ Specify precision and scale explicitly

### BC-14.3-001: Thriftserver hive.aux.jars.path
🔍 `hive\.aux\.jars\.path`  
✅ Config removed - use alternative approach

### BC-SC-003: UDF Variable Capture [MANUAL REVIEW]
❌ UDF captures external variable that changes later  
🔍 `@udf\s*\(`  
✅ Use function factory pattern to capture at definition time

### BC-SC-004: Schema in Loops [MANUAL REVIEW]
❌ `for col in df.columns:` (RPC on each iteration in Connect)  
🔍 `\.(columns|schema|dtypes)\b`  
✅ Cache first: `cols = df.columns; for col in cols:`

---

## Auto-Fix Summary

| ID | Auto-Fixed | Notes |
|----|------------|-------|
| BC-17.3-001 | ✅ | DataFrame API & SQL strings |
| BC-15.4-003 | ✅ | All `!` → `NOT` |
| BC-16.4-001a | ✅ | JavaConverters → CollectionConverters |
| BC-16.4-001b | ✅ | `.to[List]` → `.to(List)` |
| BC-16.4-001c | ✅ | TraversableOnce → IterableOnce |
| BC-16.4-001d | ✅ | Traversable → Iterable |
| BC-16.4-001e | ✅ | Stream → LazyList |
| BC-16.4-001f | ✅ | `.toIterator` → `.iterator` |
| BC-16.4-001g | ✅ | `.view.force` → `.view.to(List)` |
| BC-16.4-001i | ✅ | `'symbol` → `Symbol("symbol")` |
| BC-13.3-001 | ❌ | Manual - review type casting |
| BC-15.4-001 | ❌ | Manual - test or rewrite |
| BC-17.3-002 | ❌ | Config - test first |
| BC-SC-* | ❌ | Manual review required |

---

## Pattern Counts by Category

| Category | Count | IDs |
|----------|-------|-----|
| 🔴 Auto-Fix | 10 | BC-17.3-001, BC-15.4-003, BC-16.4-001a-i |
| 🟠 Assisted Fix | 11 | BC-SC-002/003, BC-17.3-005, BC-13.3-002/004, BC-15.4-002/005, BC-16.4-003/004/006, BC-17.3-002 |
| 🟡 Manual Review | 10 | BC-13.3-001/003, BC-15.4-001/004/006, BC-16.4-002, BC-SC-001/004, BC-17.3-003/004 |
| **Total** | **31** | All patterns |
