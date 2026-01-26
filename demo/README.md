# DBR Migration Agent Skill Demo

This folder contains demo notebooks to showcase the DBR Migration Agent Skill's capabilities.

## Files

| File | Description |
|------|-------------|
| `dbr_migration_demo_notebook.py` | **BEFORE** - Contains intentional breaking changes |
| `dbr_migration_demo_notebook_FIXED.py` | **AFTER** - All breaking changes remediated |

## Breaking Changes Demonstrated

| ID | Pattern | Severity | Category |
|----|---------|----------|----------|
| BC-17.3-001 | `input_file_name()` | HIGH | Function Removed |
| BC-15.4-003 | `IF ! EXISTS`, `IS ! NULL` | MEDIUM | SQL Syntax |
| BC-15.4-001 | `VariantType()` in UDF | HIGH | Python UDF |
| BC-17.3-002 | Auto Loader implicit config | HIGH | Streaming |
| BC-SC-002 | Temp view name reuse | HIGH | Spark Connect |
| BC-SC-003 | UDF late binding | HIGH | Spark Connect |
| BC-SC-004 | `df.columns` in loop | MEDIUM | Spark Connect |
| BC-15.4-004 | Column types in VIEW | LOW | SQL Syntax |

## Demo Instructions

### Option 1: Live Demo with Databricks Assistant

1. **Upload** `dbr_migration_demo_notebook.py` to your Databricks workspace

2. **Open** the notebook and launch **Databricks Assistant** (agent mode)

3. **Scan** for breaking changes:
   ```
   Scan this notebook for breaking changes when upgrading to DBR 17.3
   ```

4. **Review findings** - Assistant should identify all 10 breaking patterns

5. **Apply fixes**:
   ```
   Fix all the breaking changes you found
   ```

6. **Validate**:
   ```
   Validate that all breaking changes have been fixed
   ```

7. **Compare** with `dbr_migration_demo_notebook_FIXED.py` to verify fixes

### Option 2: Side-by-Side Comparison

1. Open both notebooks side-by-side
2. Walk through each section showing:
   - **BEFORE**: The breaking pattern
   - **AFTER**: The fixed code
3. Explain why each change is necessary

### Option 3: Run-Through Demo

1. **Run BEFORE notebook on DBR 13.3** - Works (mostly)
2. **Run BEFORE notebook on DBR 17.3** - Fails at various points
3. **Run FIXED notebook on DBR 17.3** - Works completely

## Key Demo Points

### 1. input_file_name() Removal (BC-17.3-001)

**Before:**
```python
from pyspark.sql.functions import input_file_name
df.withColumn("source", input_file_name())
```

**After:**
```python
df.select("*", "_metadata.file_name").withColumnRenamed("file_name", "source")
```

### 2. SQL NOT Syntax (BC-15.4-003)

**Before:**
```sql
CREATE TABLE IF ! EXISTS my_table ...
WHERE col IS ! NULL
```

**After:**
```sql
CREATE TABLE IF NOT EXISTS my_table ...
WHERE col IS NOT NULL
```

### 3. VARIANT in Python UDF (BC-15.4-001)

**Before:**
```python
@udf(returnType=VariantType())
def my_udf(value):
    return {"key": value}
```

**After:**
```python
@udf(returnType=StringType())
def my_udf(value):
    import json
    return json.dumps({"key": value})
```

### 4. Spark Connect - Temp Views (BC-SC-002)

**Before:**
```python
df.createOrReplaceTempView("my_view")  # Same name = data changes!
```

**After:**
```python
import uuid
df.createOrReplaceTempView(f"my_view_{uuid.uuid4()}")
```

### 5. Spark Connect - UDF Binding (BC-SC-003)

**Before:**
```python
multiplier = 1.0
@udf("double")
def apply_mult(x):
    return x * multiplier  # Captures at execution time!
```

**After:**
```python
def make_udf(mult):
    @udf("double")
    def apply_mult(x):
        return x * mult  # Captured in closure
    return apply_mult
```

## NYC Taxi Dataset

The demo uses the NYC Taxi dataset available in Databricks:
```
/databricks-datasets/nyctaxi/tables/nyctaxi_yellow
```

This dataset is pre-loaded in all Databricks workspaces.

## Expected Demo Duration

| Demo Type | Duration |
|-----------|----------|
| Quick overview | 10-15 min |
| Full walkthrough | 30-45 min |
| Hands-on workshop | 60-90 min |

## Troubleshooting

### Notebook won't run on DBR 13.3

Some patterns (like `VariantType`) were introduced after 13.3. Comment out those cells for a 13.3 demo.

### NYC Taxi data not found

Use this alternative:
```python
taxi_df = spark.range(1000).withColumn("fare_amount", lit(10.0))
```

### Spark Connect patterns don't show different behavior

These only differ on Serverless compute or Databricks Connect. On classic clusters, both patterns work.
