---
name: databricks-lts-migration
description: Find, fix, and validate breaking changes when upgrading Databricks Runtime between LTS versions (13.3 to 17.3). Use this skill when users ask to scan code for DBR compatibility issues, automatically fix breaking changes, validate migrations, or upgrade Databricks workflows. Covers Spark 3.4 to 4.0, Scala 2.12 to 2.13, Delta Lake, Auto Loader, Python UDFs, and SQL syntax.
license: Apache-2.0
compatibility: Requires file system access. Works with Databricks notebooks, Python, SQL, and Scala files.
metadata:
  databricks-skill-author: Databricks Solution Architect
  databricks-skill-version: "2.0.0"
  databricks-skill-category: platform-migration
  databricks-skill-last-updated: "2026-01-23"
allowed-tools: Read Write Bash(grep:*) Bash(find:*) Bash(python:*)
---

# Databricks LTS Migration Agent

This skill enables agents to **find**, **fix**, and **validate** breaking changes when upgrading Databricks Runtime from 13.3 LTS to 17.3 LTS.

## Agent Capabilities

1. **SCAN** - Find breaking changes in code
2. **FIX** - Apply automatic remediations
3. **VALIDATE** - Verify fixes are correct

---

## Capability 1: SCAN - Find Breaking Changes

When user asks to scan code for breaking changes, follow these steps:

### Step 1: Identify Target Files

Find all relevant files in the specified path:

```bash
# Find Python files
find /path/to/scan -name "*.py" -type f

# Find SQL files
find /path/to/scan -name "*.sql" -type f

# Find Scala files
find /path/to/scan -name "*.scala" -type f
```

### Step 2: Search for Breaking Change Patterns

Search for each pattern and report findings:

#### HIGH SEVERITY PATTERNS

**BC-17.3-001: input_file_name() [REMOVED in 17.3]**
```bash
grep -rn "input_file_name\s*(" --include="*.py" --include="*.sql" --include="*.scala" /path/to/scan
```

**BC-15.4-001: VARIANT in Python UDF [FAILS in 15.4+]**
```bash
grep -rn "VariantType" --include="*.py" /path/to/scan
```

**BC-16.4-001: Scala JavaConverters [DEPRECATED in 16.4]**
```bash
grep -rn "scala.collection.JavaConverters" --include="*.scala" /path/to/scan
```

#### MEDIUM SEVERITY PATTERNS

**BC-15.4-003: '!' syntax for NOT [DISALLOWED in 15.4]**
```bash
grep -rn "IF\s*!" --include="*.sql" /path/to/scan
grep -rn "IS\s*!" --include="*.sql" /path/to/scan
grep -rn "\s!\s*IN\b" --include="*.sql" /path/to/scan
```

**BC-16.4-001b: Scala .to[Collection] syntax [CHANGED in 16.4]**
```bash
grep -rn "\.to\[" --include="*.scala" /path/to/scan
```

### Step 3: Report Findings

Format findings as:
```
## Scan Results for [path]

### HIGH Severity (Must Fix)
- BC-17.3-001: input_file_name() found in:
  - file.py:42: df.withColumn("src", input_file_name())

### MEDIUM Severity (Should Fix)  
- BC-15.4-003: '!' syntax found in:
  - query.sql:15: CREATE TABLE IF ! EXISTS

### Summary
- Files scanned: X
- HIGH severity: Y findings
- MEDIUM severity: Z findings
```

---

## Capability 2: FIX - Apply Automatic Remediations

When user asks to fix breaking changes, apply these transformations:

### Fix BC-17.3-001: input_file_name() → _metadata.file_name

**Python files:**
```python
# BEFORE
from pyspark.sql.functions import input_file_name
df.withColumn("source", input_file_name())

# AFTER
df.select("*", "_metadata.file_name").withColumnRenamed("file_name", "source")
# OR
df.select("*", df["_metadata"]["file_name"].alias("source"))
```

**SQL files:**
```sql
-- BEFORE
SELECT input_file_name(), col1, col2 FROM my_table

-- AFTER
SELECT _metadata.file_name, col1, col2 FROM my_table
```

**Scala files:**
```scala
// BEFORE
import org.apache.spark.sql.functions.input_file_name
df.withColumn("source", input_file_name())

// AFTER
df.select(col("*"), col("_metadata.file_name").as("source"))
```

### Fix BC-15.4-003: '!' → 'NOT'

**Search and replace patterns:**
| Find | Replace |
|------|---------|
| `IF ! EXISTS` | `IF NOT EXISTS` |
| `IF !EXISTS` | `IF NOT EXISTS` |
| `IS ! NULL` | `IS NOT NULL` |
| `IS !NULL` | `IS NOT NULL` |
| ` ! IN ` | ` NOT IN ` |
| ` ! BETWEEN ` | ` NOT BETWEEN ` |
| ` ! LIKE ` | ` NOT LIKE ` |
| ` ! EXISTS` | ` NOT EXISTS` |

### Fix BC-15.4-001: VARIANT in Python UDF

**Convert VARIANT UDF to STRING with JSON:**
```python
# BEFORE
from pyspark.sql.types import VariantType
@udf(returnType=VariantType())
def process_data(v):
    return modified_v

# AFTER
from pyspark.sql.types import StringType
from pyspark.sql.functions import to_json, from_json
@udf(returnType=StringType())
def process_data(json_str):
    import json
    data = json.loads(json_str)
    # process data
    return json.dumps(data)

# Usage: df.withColumn("result", from_json(process_data(to_json(col("variant_col"))), schema))
```

### Fix BC-16.4-001: Scala 2.13 Collection Changes

**Import fix:**
```scala
// BEFORE
import scala.collection.JavaConverters._

// AFTER
import scala.jdk.CollectionConverters._
```

**Conversion syntax fix:**
```scala
// BEFORE
collection.to[List]
collection.to[Set]
collection.to[Vector]

// AFTER
collection.to(List)
collection.to(Set)
collection.to(Vector)
```

### Applying Fixes

Use the file editing tool to apply changes:

1. Read the file content
2. Apply the transformation
3. Write the updated content
4. Report the change made

**Example fix report:**
```
## Fixes Applied

### file.py
- Line 5: Removed `from pyspark.sql.functions import input_file_name`
- Line 42: Changed `input_file_name()` to `_metadata.file_name`

### query.sql  
- Line 15: Changed `IF ! EXISTS` to `IF NOT EXISTS`
- Line 28: Changed `IS ! NULL` to `IS NOT NULL`

Total: 4 fixes in 2 files
```

---

## Capability 3: VALIDATE - Verify Fixes

After applying fixes, validate they are correct:

### Step 1: Syntax Validation

**Verify no breaking patterns remain:**
```bash
# Should return no results after fixes
grep -rn "input_file_name\s*(" --include="*.py" --include="*.sql" --include="*.scala" /path/to/scan
grep -rn "(IF|IS)\s*!" --include="*.sql" /path/to/scan
grep -rn "VariantType" --include="*.py" /path/to/scan
grep -rn "scala.collection.JavaConverters" --include="*.scala" /path/to/scan
```

### Step 2: Replacement Validation

**Verify correct replacements exist:**
```bash
# Should find _metadata.file_name replacements
grep -rn "_metadata.file_name" --include="*.py" --include="*.sql" --include="*.scala" /path/to/scan

# Should find NOT instead of !
grep -rn "IF NOT EXISTS\|IS NOT NULL\|NOT IN\|NOT BETWEEN\|NOT LIKE" --include="*.sql" /path/to/scan

# Should find new Scala imports
grep -rn "scala.jdk.CollectionConverters" --include="*.scala" /path/to/scan
```

### Step 3: Code Structure Validation

**For Python files, verify:**
- Import statements are valid
- Function signatures are correct
- Column references use proper syntax

**For SQL files, verify:**
- SQL syntax is valid
- Keywords are properly spaced
- Identifiers are correctly quoted if needed

**For Scala files, verify:**
- Imports compile (no deprecated imports)
- Collection operations use new syntax
- Type annotations are present where needed

### Step 4: Generate Validation Report

```
## Validation Report

### Breaking Patterns Check
✅ No input_file_name() found
✅ No '!' syntax for NOT found  
✅ No VariantType in Python UDFs found
✅ No deprecated Scala imports found

### Replacement Verification
✅ Found 3 instances of _metadata.file_name
✅ Found 5 instances of NOT syntax
✅ Found 2 instances of CollectionConverters

### Summary
Status: ✅ PASSED - Ready for DBR 17.3 upgrade
Files validated: 15
All breaking changes resolved
```

---

## Quick Reference: All Breaking Changes

### Code-Level Breaking Changes

| ID | Severity | Pattern | Fix |
|----|----------|---------|-----|
| BC-17.3-001 | HIGH | `input_file_name()` | `_metadata.file_name` |
| BC-15.4-001 | HIGH | `VariantType` in Python UDF | Use STRING + JSON |
| BC-16.4-001 | HIGH | `JavaConverters` | `CollectionConverters` |
| BC-13.3-001 | HIGH | MERGE/UPDATE overflow | Widen column type |
| BC-15.4-003 | MEDIUM | `IF !`, `IS !`, `! IN` | Use `NOT` |
| BC-16.4-001b | MEDIUM | `.to[List]` | `.to(List)` |
| BC-17.3-002 | MEDIUM | Auto Loader incremental | Set option explicitly |
| BC-13.3-002 | MEDIUM | Parquet TIMESTAMP_NTZ | Set inferTimestampNTZ=false |

### Spark Connect Behavioral Changes (Serverless/Connect Users)

| ID | Severity | Behavior | Best Practice |
|----|----------|----------|---------------|
| BC-SC-001 | HIGH | Lazy schema analysis | Call `df.columns` to trigger early error detection |
| BC-SC-002 | HIGH | Temp view name lookup | Use UUID in temp view names |
| BC-SC-003 | HIGH | UDF late binding | Use function factory to capture variables |
| BC-SC-004 | MEDIUM | Schema access RPC | Cache `df.columns` locally in loops |

Source: [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)

---

## Spark Connect Best Practices

When using Serverless compute, Databricks Connect, or notebooks on DBR 13.3+, follow these patterns:

### 1. Unique Temp View Names
```python
import uuid
def create_temp_view(df, base_name):
    view_name = f"`{base_name}_{uuid.uuid4()}`"
    df.createOrReplaceTempView(view_name)
    return spark.table(view_name)
```

### 2. UDF Variable Capture
```python
def make_udf(captured_value):
    """Factory function captures value at definition time"""
    @udf("INT")
    def my_udf():
        return captured_value
    return my_udf

my_udf = make_udf(123)  # Captures 123
```

### 3. Trigger Eager Analysis
```python
try:
    df = df.filter("nonexistent_col > 0")
    df.columns  # Forces analysis, catches error immediately
except Exception as e:
    print(f"Analysis error: {e}")
```

### 4. Avoid Schema Access in Loops
```python
columns = set(df.columns)  # Cache once
for i in range(100):
    if str(i) not in columns:  # Check local set
        df = df.withColumn(str(i), lit(i))
        columns.add(str(i))
```

---

## Example Agent Workflow

**User:** "Scan my notebooks folder for DBR 17.3 compatibility issues and fix them"

**Agent actions:**

1. **SCAN**: Search for breaking patterns
   ```bash
   grep -rn "input_file_name" ./notebooks/
   ```
   Found: `etl_job.py:42`

2. **FIX**: Apply remediation
   - Read `etl_job.py`
   - Replace `input_file_name()` with `_metadata.file_name`
   - Write updated file

3. **VALIDATE**: Verify fix
   ```bash
   grep -rn "input_file_name" ./notebooks/  # Should be empty
   grep -rn "_metadata.file_name" ./notebooks/  # Should find replacement
   ```
   
4. **REPORT**: 
   ```
   ✅ Fixed 1 breaking change in etl_job.py
   - Replaced input_file_name() with _metadata.file_name at line 42
   - Validated: No breaking patterns remain
   ```

---

## Additional Resources

- [references/QUICK-REFERENCE.md](references/QUICK-REFERENCE.md) - **Quick lookup** of all breaking changes (❌/🔍/✅ format)
- [references/BREAKING-CHANGES.md](references/BREAKING-CHANGES.md) - Complete breaking changes with code examples
- [references/SPARK-CONNECT-GUIDE.md](references/SPARK-CONNECT-GUIDE.md) - Spark Connect vs Classic behavioral differences
- [references/MIGRATION-CHECKLIST.md](references/MIGRATION-CHECKLIST.md) - Full migration checklist
- [references/SCALA-213-GUIDE.md](references/SCALA-213-GUIDE.md) - Scala 2.13 migration details
- [scripts/scan-breaking-changes.py](scripts/scan-breaking-changes.py) - Automated scanner
- [scripts/apply-fixes.py](scripts/apply-fixes.py) - Automatic fix application
- [scripts/validate-migration.py](scripts/validate-migration.py) - Validation script
- [assets/fix-patterns.json](assets/fix-patterns.json) - Machine-readable fix patterns
