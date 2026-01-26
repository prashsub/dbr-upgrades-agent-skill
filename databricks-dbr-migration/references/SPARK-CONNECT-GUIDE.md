# Spark Connect Migration Guide

This guide covers behavioral differences between Spark Connect and Spark Classic that affect code migration for Databricks Runtime upgrades.

**Applies to:**
- Scala notebooks on DBR 13.3+ (standard compute)
- Python notebooks on DBR 14.3+ (standard compute)
- Serverless compute (all versions)
- Databricks Connect

Source: [Compare Spark Connect to Spark Classic - Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)

---

## Overview: Lazy vs Eager Execution

The fundamental difference is when analysis (schema resolution, name lookup) occurs:

| Aspect | Spark Classic | Spark Connect |
|--------|---------------|---------------|
| Query execution | Lazy | Lazy |
| Schema analysis | **Eager** | **Lazy** |
| Schema access (df.columns) | Local | Triggers RPC |
| Temporary views | Plan embedded | Name lookup |
| UDF serialization | At creation | At execution |

---

## Breaking Change 1: Lazy Schema Analysis (BC-SC-001)

### Problem

In Spark Connect, schema analysis is deferred until execution time. This means invalid column references don't throw errors until an action is called.

### Impact

```python
# Spark Classic: Error thrown HERE (immediately)
df = spark.sql("select 1 as a").filter("nonexistent_col > 1")

# Spark Connect: No error yet! Error only when show() is called
df = spark.sql("select 1 as a").filter("nonexistent_col > 1")
# ... 100 lines of code later ...
df.show()  # Error thrown HERE
```

This breaks error handling patterns that expect immediate feedback:

```python
# This pattern doesn't work in Spark Connect!
try:
    df = df.filter("bad_column > 0")  # No error in Spark Connect
except Exception as e:
    handle_error(e)  # Never reached!
```

### Solution

Trigger eager analysis explicitly when you need immediate error detection:

```python
# Option 1: Access schema
try:
    df = df.filter("bad_column > 0")
    df.columns  # Triggers analysis RPC
except Exception as e:
    handle_error(e)

# Option 2: Use schema access
try:
    df = df.filter("bad_column > 0")
    _ = df.schema  # Triggers analysis
except Exception as e:
    handle_error(e)

# Option 3: Check if streaming
try:
    df = df.filter("bad_column > 0")
    _ = df.isStreaming  # Triggers analysis
except Exception as e:
    handle_error(e)
```

---

## Breaking Change 2: Temporary View Name Resolution (BC-SC-002)

### Problem

In Spark Connect, DataFrames store only a **reference by name** to temporary views. If the view is replaced, all DataFrames referencing it see the new data.

### Impact

```python
# Spark Classic behavior:
def bad_pattern(x):
    spark.range(x).createOrReplaceTempView("my_view")
    return spark.table("my_view")

df10 = bad_pattern(10)   # DataFrame plan embeds the view definition
df100 = bad_pattern(100) # View replaced, but df10 is unaffected
assert len(df10.collect()) == 10  # ✅ Works in Classic

# Spark Connect behavior:
df10 = bad_pattern(10)   # DataFrame stores reference to "my_view"
df100 = bad_pattern(100) # View replaced!
assert len(df10.collect()) == 10  # ❌ FAILS! Returns 100!
```

### Solution

Always use unique view names with UUID:

```python
import uuid

def good_pattern(x):
    # Generate unique view name
    view_name = f"`temp_view_{uuid.uuid4()}`"
    spark.range(x).createOrReplaceTempView(view_name)
    return spark.table(view_name)

df10 = good_pattern(10)
df100 = good_pattern(100)
assert len(df10.collect()) == 10   # ✅ Works correctly
assert len(df100.collect()) == 100 # ✅ Works correctly
```

**Scala version:**
```scala
import java.util.UUID

def goodPattern(x: Int) = {
  val viewName = s"`temp_view_${UUID.randomUUID()}`"
  spark.range(x).createOrReplaceTempView(viewName)
  spark.table(viewName)
}
```

---

## Breaking Change 3: UDF Late Binding (BC-SC-003)

### Problem

In Spark Connect, Python UDFs are serialized at **execution time**, not creation time. This means external variables are captured when the UDF runs, not when it's defined.

### Impact

```python
from pyspark.sql.functions import udf

x = 123

@udf("INT")
def my_udf():
    return x  # Captures x

df = spark.range(1).select(my_udf())

x = 456  # Modify x AFTER UDF definition

# Spark Classic: Returns 123 (captured at definition)
# Spark Connect: Returns 456 (captured at execution)
df.show()
```

### Solution

Use a function factory (closure) to capture values at definition time:

```python
from pyspark.sql.functions import udf

def make_udf(captured_value):
    """Factory function that captures value in closure"""
    @udf("INT")
    def inner_udf():
        return captured_value  # This is now bound at make_udf() call time
    return inner_udf

x = 123
my_udf = make_udf(x)  # Captures 123 in closure

x = 456  # Modifying x has no effect

df = spark.range(1).select(my_udf())
df.show()  # Returns 123 in both Classic and Connect
```

**More complex example with parameters:**
```python
def make_multiplier_udf(multiplier):
    """Factory for a UDF that multiplies by a fixed value"""
    @udf("INT")
    def multiply(value):
        return value * multiplier
    return multiply

double_udf = make_multiplier_udf(2)
triple_udf = make_multiplier_udf(3)

df.select(double_udf(col("x")), triple_udf(col("x")))
```

**Scala version:**
```scala
def makeUDF(value: Int) = udf(() => value)

var x = 123
val myUDF = makeUDF(x)  // Captures 123
x = 456
spark.range(1).select(myUDF()).show()  // Returns 123
```

---

## Breaking Change 4: Schema Access Performance (BC-SC-004)

### Problem

In Spark Connect, every `df.columns` or `df.schema` access triggers an RPC to the server. In loops, this causes severe performance degradation.

### Impact

```python
# BAD: O(n) RPC calls!
df = spark.range(10)
for i in range(200):
    if str(i) not in df.columns:  # RPC call on EVERY iteration!
        df = df.withColumn(str(i), col("id") + i)
# This can take minutes instead of seconds!
```

### Solution

Cache schema information locally:

```python
# GOOD: Only 1 RPC call
df = spark.range(10)
columns = set(df.columns)  # Single RPC, cache locally

for i in range(200):
    new_col = str(i)
    if new_col not in columns:  # Check local set, no RPC
        df = df.withColumn(new_col, col("id") + i)
        columns.add(new_col)  # Update local cache
```

**For struct field access:**
```python
from pyspark.sql.types import StructType

# BAD: Creates intermediate DataFrames and triggers analysis
struct_fields = {}
for col_schema in df.schema:
    if isinstance(col_schema.dataType, StructType):
        # This creates a new DataFrame and triggers analysis!
        struct_fields[col_schema.name] = df.select(col_schema.name + ".*").columns

# GOOD: Access fields directly from schema object
struct_fields = {
    col_schema.name: [f.name for f in col_schema.dataType.fields]
    for col_schema in df.schema
    if isinstance(col_schema.dataType, StructType)
}
```

---

## Detection Patterns

Use these grep patterns to find potential Spark Connect issues:

```bash
# Find temp views without UUID (potential BC-SC-002)
grep -rn "createOrReplaceTempView" --include="*.py" --include="*.scala" .

# Find UDFs that may capture external variables (potential BC-SC-003)
grep -rn "@udf" --include="*.py" .

# Find schema access in loops (potential BC-SC-004)
grep -rn "\.columns" --include="*.py" . | grep -E "(for|while)"

# Find try/except around transformations (potential BC-SC-001)
grep -rn -A5 "try:" --include="*.py" . | grep -E "(filter|select|withColumn)"
```

---

## Migration Checklist

### For Spark Connect Migration

- [ ] Search for `createOrReplaceTempView` - add UUID to view names
- [ ] Review UDFs for external variable capture - use factory pattern
- [ ] Check for `df.columns`/`df.schema` in loops - cache locally
- [ ] Review try/except blocks around transformations - add explicit analysis triggers
- [ ] Test all error handling paths

### Environments Affected

| Environment | Spark Connect? | Since Version |
|-------------|----------------|---------------|
| Scala notebooks (standard) | Yes | DBR 13.3 |
| Python notebooks (standard) | Yes | DBR 14.3 |
| Serverless compute | Yes | All |
| Databricks Connect | Yes | All |
| Spark Classic (jobs) | No | N/A |

---

## Quick Reference

| Issue | Classic Behavior | Connect Behavior | Fix |
|-------|------------------|------------------|-----|
| Schema analysis | Eager | Lazy | Call `df.columns` to force |
| Temp views | Plan embedded | Name lookup | Use UUID in names |
| UDF variables | Captured at creation | Captured at execution | Use factory function |
| Schema access | Local | RPC call | Cache in local variable |

---

## References

- [Compare Spark Connect to Spark Classic - Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)
- [Spark Connect Architecture](https://spark.apache.org/docs/latest/spark-connect-overview.html)
