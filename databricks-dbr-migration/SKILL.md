---
name: databricks-dbr-migration
description: Find, fix, and validate breaking changes when upgrading Databricks Runtime between LTS versions (13.3 to 17.3). Use this skill when users ask to scan code for DBR compatibility issues, automatically fix breaking changes, validate migrations, or upgrade Databricks workflows. Covers Spark 3.4 to 4.0, Scala 2.12 to 2.13, Delta Lake, Auto Loader, Python UDFs, and SQL syntax.
license: Apache-2.0
compatibility: Requires file system access. Works with Databricks notebooks, Python, SQL, and Scala files.
metadata:
  databricks-skill-author: Databricks Solution Architect
  databricks-skill-version: "3.6.0"
  databricks-skill-category: platform-migration
  databricks-skill-last-updated: "2026-01-26"
allowed-tools: Read Write Bash(grep:*) Bash(find:*) Bash(python:*)
---

# Databricks LTS Migration Agent

This skill enables agents to **find**, **fix**, and **validate** breaking changes when upgrading Databricks Runtime from 13.3 LTS to 17.3 LTS.

## Agent Capabilities

1. **SCAN** - Find ALL breaking changes in code
2. **FIX** - Apply automatic remediations (7 patterns)
3. **FLAG** - Explicitly flag items requiring manual review (6 patterns) or configuration testing (4 patterns)
4. **VALIDATE** - Verify fixes are correct
5. **SUMMARIZE** - Add a summary markdown cell to the notebook

---

## CRITICAL: Add Summary as Markdown Cell

**After scanning or fixing, ALWAYS add a summary as a NEW MARKDOWN CELL at the end of the notebook:**

### For SCAN Results - Add This Markdown Cell:

````markdown
# MAGIC %md
# MAGIC ## 📋 DBR Migration Scan Results
# MAGIC 
# MAGIC **Scan Date:** YYYY-MM-DD HH:MM  
# MAGIC **Target DBR Version:** 17.3
# MAGIC 
# MAGIC ### Summary
# MAGIC | Category | Count |
# MAGIC |----------|-------|
# MAGIC | 🔴 Auto-Fix | X |
# MAGIC | 🟡 Manual Review | Y |
# MAGIC | ⚙️ Config Check | Z |
# MAGIC 
# MAGIC ### 🔴 Auto-Fix Required
# MAGIC | Line | BC-ID | Pattern | Fix |
# MAGIC |------|-------|---------|-----|
# MAGIC | 42 | BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |
# MAGIC 
# MAGIC ### 🟡 Manual Review Required
# MAGIC | Line | BC-ID | Issue | Action |
# MAGIC |------|-------|-------|--------|
# MAGIC | 55,85 | BC-SC-002 | Temp view reuse | Add UUID to view names |
# MAGIC 
# MAGIC ### ⚙️ Config Check (Test First)
# MAGIC | Line | BC-ID | Issue | Config If Needed |
# MAGIC |------|-------|-------|------------------|
# MAGIC | 30 | BC-17.3-002 | Auto Loader | `.option("cloudFiles.useIncrementalListing", "auto")` |
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Run: `@databricks-dbr-migration fix all auto-fixable issues`
# MAGIC 2. Review manual items above
# MAGIC 3. Test config changes on DBR 17.3
````

### For FIX Results - Add This Markdown Cell:

````markdown
# MAGIC %md
# MAGIC ## ✅ DBR Migration Fix Results
# MAGIC 
# MAGIC **Fix Date:** YYYY-MM-DD HH:MM  
# MAGIC **Target DBR Version:** 17.3
# MAGIC 
# MAGIC ### Summary
# MAGIC | Status | Count |
# MAGIC |--------|-------|
# MAGIC | ✅ Fixed | X |
# MAGIC | 🟡 Manual Review Still Required | Y |
# MAGIC | ⚙️ Config Check Still Required | Z |
# MAGIC 
# MAGIC ### ✅ Auto-Fixes Applied
# MAGIC | Line | BC-ID | Pattern | Applied Fix |
# MAGIC |------|-------|---------|-------------|
# MAGIC | 42 | BC-17.3-001 | `input_file_name()` | Replaced with `_metadata.file_name` |
# MAGIC | 89 | BC-15.4-003 | `IF !condition` | Replaced with `IF NOT condition` |
# MAGIC 
# MAGIC ### 🟡 Manual Review Still Required
# MAGIC | Line | BC-ID | Issue | Action |
# MAGIC |------|-------|-------|--------|
# MAGIC | 55,85 | BC-SC-002 | Temp view reuse | Add UUID to view names |
# MAGIC 
# MAGIC ### ⚙️ Config Check Still Required
# MAGIC | Line | BC-ID | Issue | Config If Needed |
# MAGIC |------|-------|-------|------------------|
# MAGIC | 30 | BC-17.3-002 | Auto Loader | `.option("cloudFiles.useIncrementalListing", "auto")` |
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Review manual items above
# MAGIC 2. Test on DBR 17.3 cluster
# MAGIC 3. Validate config changes as needed
# MAGIC 4. Run: `@databricks-dbr-migration validate all fixes were applied correctly`
````

### Example Agent Actions

**After scanning:** Add a new cell at the end of the notebook with the scan summary.

**After fixing:** Add a new cell at the end of the notebook with the fix summary showing what was auto-fixed and what still needs manual attention.

---

## CRITICAL: Target Version Awareness

**IMPORTANT:** When the user specifies a target DBR version, filter findings accordingly:

| Target Version | Skip These Patterns |
|----------------|---------------------|
| **17.3** or **16.4** | Skip `BC-15.4-001` (VariantType in UDF) - **FIXED in 16.4** |
| **15.4** | Flag ALL patterns including BC-15.4-001 |

**Example:** If user says "check compatibility for DBR 17.3" or "upgrade to 16.4":
- ✅ Do NOT flag `VariantType()` in UDFs (BC-15.4-001) - it works in 16.4+
- ✅ Still flag all other patterns

---

## CRITICAL: Three Categories of Findings

When scanning code, categorize ALL findings into these three categories and handle them appropriately:

### 🔴 Category 1: AUTO-FIX (7 patterns)
**Action: Automatically apply the fix**

| ID | Pattern | Fix |
|----|---------|-----|
| BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |
| BC-15.4-003 | `IF !`, `IS !`, `! IN`, etc. | Replace `!` with `NOT` |
| BC-16.4-001a | `JavaConverters` | Replace with `CollectionConverters` |
| BC-16.4-001b | `.to[List]` | Replace with `.to(List)` |
| BC-16.4-001c | `TraversableOnce` | Replace with `IterableOnce` |
| BC-16.4-001d | `Traversable` | Replace with `Iterable` |
| BC-16.4-001e | `Stream.from()` | Replace with `LazyList.from()` |

### 🟡 Category 2: MANUAL REVIEW (6 patterns)
**Action: FLAG for developer review with specific guidance**

| ID | Pattern | Flag Message |
|----|---------|--------------|
| BC-15.4-001 | `VariantType()` in UDF | **⚠️ SKIP if target is 16.4+ or 17.3** - VARIANT UDFs work in 16.4+. Only flag if target is 15.4. |
| BC-15.4-004 | `CREATE VIEW (col TYPE)` | **FLAG:** Column types in VIEW not allowed in 15.4+. Remove types, use CAST in SELECT |
| BC-SC-001 | try/except around DataFrame transforms | **FLAG:** In Spark Connect, errors appear at action time, not transform time. Add `_ = df.columns` after transforms for early validation |
| BC-SC-002 | Same temp view name used multiple times | **FLAG:** In Spark Connect, view name reuse causes data conflicts. Add UUID: `f"view_{uuid.uuid4().hex[:8]}"` |
| BC-SC-003 | UDF referencing external variables | **FLAG:** In Spark Connect, UDFs capture variables at execution time. Use function factory pattern |
| BC-SC-004 | `df.columns` / `df.schema` in loops | **FLAG:** In Spark Connect, schema access triggers RPC. Cache outside loop: `cols = df.columns` |

### ⚙️ Category 3: CONFIG CHECK (4 patterns)
**Action: FLAG for testing - only add config if results differ**

| ID | Pattern | Flag Message |
|----|---------|--------------|
| BC-13.3-002 | Parquet with timestamps | **FLAG:** Test Parquet timestamp reads. If wrong, add: `spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")` |
| BC-15.4-002 | JDBC reads | **FLAG:** Test JDBC timestamp reads. If wrong, add: `spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")` |
| BC-16.4-004 | `materializeSource.*none` | **FLAG:** `materializeSource=none` not allowed in 16.4+. Remove or change to `auto` |
| BC-17.3-002 | Auto Loader without explicit incremental | **FLAG:** Auto Loader default changed. Test performance; if slow, add: `.option("cloudFiles.useIncrementalListing", "auto")` |

---

## Capability 1: SCAN - Find Breaking Changes

When user asks to scan code for breaking changes, follow these steps:

### Step 1: Identify Target Files

Find all relevant files in the specified path **including subdirectories**:

```bash
# Find Python files (recursively scans all subdirectories)
find /path/to/scan -name "*.py" -type f

# Find SQL files
find /path/to/scan -name "*.sql" -type f

# Find Scala files
find /path/to/scan -name "*.scala" -type f
```

**Important for Multi-File Projects:**
- The `find` command recursively searches all subdirectories (e.g., `utils/`, `src/`, etc.)
- Scan ALL Python files found, including helper modules and utility packages
- Check `import` and `from` statements to identify dependencies
- If a notebook imports from local modules (e.g., `from utils.helpers import foo`), scan those imported files too
- Look for package structures with `__init__.py` files

**Example Multi-File Structure:**
```
project/
├── main_notebook.py          ← Scan this
├── utils/
│   ├── __init__.py           ← Scan this
│   └── dbr_helpers.py        ← Scan this (imported by main_notebook)
└── config/
    └── settings.py           ← Scan this if imported
```

### Step 2: Search for Breaking Change Patterns

Search for each pattern and report findings:

#### HIGH SEVERITY PATTERNS

**BC-17.3-001: input_file_name() [REMOVED in 17.3]**
```bash
grep -rn "input_file_name\s*(" --include="*.py" --include="*.sql" --include="*.scala" /path/to/scan
```

**BC-15.4-001: VARIANT in Python UDF [SKIP if target is 16.4+ or 17.3]**
```bash
grep -rn "VariantType" --include="*.py" /path/to/scan
```
> ⚠️ **SKIP THIS CHECK** if user's target version is 16.4 or 17.3 - VARIANT UDFs work in 16.4+!

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

### Step 3: Search for MANUAL REVIEW Patterns

**BC-15.4-001: VARIANT in UDF [SKIP if target is 16.4+ or 17.3]**
> ⚠️ **Only scan for this if target version is 15.4.** Skip if upgrading to 16.4 or 17.3.
```bash
# Only run this if target is 15.4:
grep -rn "VariantType\s*(" --include="*.py" /path/to/scan
```

**BC-15.4-004: VIEW Column Type Definition**
```bash
grep -rn "CREATE.*VIEW.*\(.*\(INT\|STRING\|BIGINT\|DOUBLE\|NOT NULL\|DEFAULT\)" --include="*.sql" /path/to/scan
```

**BC-SC-002: Temp View Name Reuse**
```bash
grep -rn "createOrReplaceTempView\|createTempView" --include="*.py" --include="*.scala" /path/to/scan
# Then check if same name appears multiple times in same file
```

**BC-SC-003: UDF with External Variables**
```bash
grep -rn "@udf" --include="*.py" /path/to/scan
# Then check if function body references variables defined outside
```

**BC-SC-004: Schema Access in Loops**
```bash
grep -rn "\.columns\|\.schema\|\.dtypes" --include="*.py" /path/to/scan
# Then check if inside for/while loop
```

### Step 4: Search for CONFIG CHECK Patterns

**BC-13.3-002: Parquet Timestamp**
```bash
grep -rn "\.parquet\|read.parquet" --include="*.py" /path/to/scan
```

**BC-15.4-002: JDBC**
```bash
grep -rn "\.jdbc\|read.jdbc" --include="*.py" /path/to/scan
```

**BC-16.4-004: MERGE materializeSource=none**
```bash
grep -rn "materializeSource.*none" --include="*.py" --include="*.sql" /path/to/scan
```

**BC-17.3-002: Auto Loader**
```bash
grep -rn "cloudFiles" --include="*.py" /path/to/scan
```

### Step 5: Report Findings with THREE CATEGORIES

Format findings as:
```
## Scan Results for [path]

### 🔴 AUTO-FIX (Will be fixed automatically)
- BC-17.3-001: input_file_name() found in:
  - file.py:42: df.withColumn("src", input_file_name())
  - **FIX:** Replace with `_metadata.file_name`

- BC-15.4-003: '!' syntax found in:
  - query.sql:15: CREATE TABLE IF ! EXISTS
  - **FIX:** Replace `!` with `NOT`

### 🟡 MANUAL REVIEW REQUIRED
- BC-SC-002: Temp view name reuse found in:
  - etl.py:25: df.createOrReplaceTempView("batch")
  - etl.py:35: df2.createOrReplaceTempView("batch")  <-- Same name!
  - **REVIEW:** In Spark Connect, both DataFrames will see the latest data.
  - **FIX IF NEEDED:** Add UUID: `f"batch_{uuid.uuid4().hex[:8]}"`

- BC-SC-003: UDF with external variable found in:
  - process.py:10: multiplier = 1.0
  - process.py:12: @udf(...) def calc(x): return x * multiplier
  - **REVIEW:** In Spark Connect, UDF captures `multiplier` at execution time, not definition.
  - **FIX IF NEEDED:** Use function factory pattern

### ⚙️ CONFIG CHECK (Test first, add config only if needed)
- BC-17.3-002: Auto Loader found in:
  - streaming.py:50: spark.readStream.format("cloudFiles")...
  - **TEST:** Check if job performance is acceptable on new DBR
  - **FIX IF NEEDED:** Add `.option("cloudFiles.useIncrementalListing", "auto")`

### Summary
- Files scanned: X
- 🔴 AUTO-FIX: Y findings (will be fixed)
- 🟡 MANUAL REVIEW: Z findings (developer decision required)
- ⚙️ CONFIG CHECK: W findings (test first)
```

### Step 6: Add Scan Summary as Markdown Cell

**ALWAYS add the scan results as a new markdown cell at the end of the notebook:**

- Add a new cell with `# MAGIC %md` prefix
- Include scan date and target DBR version
- Include summary table with counts by category
- Include detailed findings tables
- Include next steps for the developer

**Example:** See the markdown cell format in the "CRITICAL: Add Summary as Markdown Cell" section above.

---

## Capability 2: FIX - Apply Automatic Remediations

When user asks to fix breaking changes, apply these transformations.

### CRITICAL: Add Fix Summary as Markdown Cell

**After applying fixes, ALWAYS add a summary as a NEW MARKDOWN CELL at the end of the notebook:**

### For FIX Results - Add This Markdown Cell:

````markdown
# MAGIC %md
# MAGIC ## ✅ DBR Migration Fix Summary
# MAGIC 
# MAGIC **Fix Date:** YYYY-MM-DD HH:MM  
# MAGIC **Target DBR Version:** 17.3
# MAGIC 
# MAGIC ### Summary
# MAGIC | Status | Count |
# MAGIC |--------|-------|
# MAGIC | ✅ Fixed | X |
# MAGIC | 🟡 Manual Review (unchanged) | Y |
# MAGIC | ⚙️ Config Check (unchanged) | Z |
# MAGIC 
# MAGIC ### ✅ Changes Applied
# MAGIC 
# MAGIC #### BC-17.3-001: input_file_name() → _metadata.file_name
# MAGIC | Line | Before | After |
# MAGIC |------|--------|-------|
# MAGIC | 5 | `from pyspark.sql.functions import input_file_name` | (removed) |
# MAGIC | 42 | `input_file_name()` | `col("_metadata.file_name")` |
# MAGIC 
# MAGIC #### BC-15.4-003: ! → NOT
# MAGIC | Line | Before | After |
# MAGIC |------|--------|-------|
# MAGIC | 15 | `IF ! EXISTS` | `IF NOT EXISTS` |
# MAGIC | 28 | `IS ! NULL` | `IS NOT NULL` |
# MAGIC 
# MAGIC ### 🟡 Manual Review Still Required
# MAGIC | Line | BC-ID | Issue | Action Needed |
# MAGIC |------|-------|-------|---------------|
# MAGIC | 55,85 | BC-SC-002 | Temp view reuse | Add UUID to view names |
# MAGIC 
# MAGIC ### ⚙️ Config Check Still Required
# MAGIC | Line | BC-ID | Issue | Test Then Add |
# MAGIC |------|-------|-------|---------------|
# MAGIC | 30 | BC-17.3-002 | Auto Loader | Test performance first |
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Review the changes above
# MAGIC 2. Address manual review items
# MAGIC 3. Test on DBR 17.3
# MAGIC 4. Run: `@databricks-dbr-migration validate all fixes`
````

### Multi-File Fix Strategy

**If breaking changes are found in multiple files:**
1. **Fix all files in a single session** - Don't just fix the main file and ignore imported modules
2. **Maintain import compatibility** - If you fix `input_file_name()` in a utility module, ensure the calling code still works
3. **Update imports** - Remove deprecated imports from ALL files (e.g., `from pyspark.sql.functions import input_file_name`)
4. **Test cross-file dependencies** - After fixing, verify imports still resolve correctly
5. **Report all changes** - List every file modified with a summary of changes

**Example Multi-File Fix:**
```
Fixed 3 files:
✅ demo/main_notebook.py - Replaced input_file_name() (2 occurrences)
✅ demo/utils/helpers.py - Replaced input_file_name() (1 occurrence), removed import
✅ demo/utils/__init__.py - No changes needed (no breaking changes found)
```

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

> ⚠️ **SKIP if target is 16.4+ or 17.3** - No fix needed! VARIANT UDFs work in 16.4+.
> See [official docs](https://learn.microsoft.com/en-us/azure/databricks/udf/python#variants-with-udf).

**For DBR 15.4 only - Convert VARIANT UDF to STRING with JSON:**
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
4. **Track each change made** (line, before, after)
5. **Add fix summary as a new markdown cell at end of notebook**
6. Report to user

**After all fixes are applied, add a summary cell and report:**
```
✅ Fixes applied!

Changes made:
- ✅ 4 fixes applied
- 🟡 1 item still needs manual review
- ⚙️ 1 item needs config testing

📋 Summary added as new cell at end of notebook.

Run `@databricks-dbr-migration validate all fixes` to verify.
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
| BC-15.4-001 | **SKIP** | `VariantType` in Python UDF | **Skip if target is 16.4+/17.3** - only an issue on 15.4 |
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

### 1. SCAN all patterns
```bash
# Auto-fix patterns
grep -rn "input_file_name" ./notebooks/
grep -rn "IF\s*!\|IS\s*!" ./notebooks/

# Manual review patterns  
grep -rn "createOrReplaceTempView" ./notebooks/
grep -rn "@udf" ./notebooks/

# Config patterns
grep -rn "cloudFiles" ./notebooks/
```

### 2. Categorize findings
```
Found:
- etl_job.py:42 - input_file_name() [AUTO-FIX]
- etl_job.py:55 - createOrReplaceTempView("batch") [MANUAL REVIEW - check for reuse]
- etl_job.py:85 - createOrReplaceTempView("batch") [MANUAL REVIEW - same name reused!]
- streaming.py:30 - cloudFiles [CONFIG CHECK]
```

### 3. FIX auto-fixable items
- Read `etl_job.py`
- Replace `input_file_name()` with `_metadata.file_name`
- Write updated file

### 4. FLAG manual review items
```
🟡 MANUAL REVIEW REQUIRED: BC-SC-002 in etl_job.py

The temp view name "batch" is used on both line 55 and line 85.

In Spark Connect (Serverless), this causes data conflicts - both DataFrames 
will see the data from the LAST createOrReplaceTempView call.

RECOMMENDATION: Add UUID to view names:
  - Line 55: `f"batch_{uuid.uuid4().hex[:8]}"`
  - Line 85: Use a different base name, or add UUID

Do you want me to apply this fix? (Y/N)
```

### 5. FLAG config items
```
⚙️ CONFIG CHECK: BC-17.3-002 in streaming.py

Auto Loader is used without explicit `useIncrementalListing` setting.
The default changed from "auto" to "false" in DBR 17.3.

TEST FIRST: Run the streaming job on DBR 17.3 and check performance.

IF SLOWER: Add `.option("cloudFiles.useIncrementalListing", "auto")`

No automatic fix applied - testing required first.
```

### 6. ADD SCAN SUMMARY as Markdown Cell

**Add a new markdown cell at the end of the notebook with the scan summary:**

```python
# MAGIC %md
# MAGIC ## 📋 DBR Migration Scan Results
# MAGIC 
# MAGIC **Scan Date:** 2026-01-26 10:30  
# MAGIC **Target DBR Version:** 17.3
# MAGIC 
# MAGIC ### Summary
# MAGIC | Category | Count |
# MAGIC |----------|-------|
# MAGIC | 🔴 Auto-Fix | 1 |
# MAGIC | 🟡 Manual Review | 1 |
# MAGIC | ⚙️ Config Check | 1 |
# MAGIC 
# MAGIC ### 🔴 Auto-Fix Required
# MAGIC | Line | BC-ID | Pattern | Fix |
# MAGIC |------|-------|---------|-----|
# MAGIC | 42 | BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |
# MAGIC 
# MAGIC ### 🟡 Manual Review Required
# MAGIC | Line | BC-ID | Issue | Action |
# MAGIC |------|-------|-------|--------|
# MAGIC | 55,85 | BC-SC-002 | Temp view "batch" reused | Add UUID |
# MAGIC 
# MAGIC ### ⚙️ Config Check (Test First)
# MAGIC | Line | BC-ID | Issue | Config If Needed |
# MAGIC |------|-------|-------|------------------|
# MAGIC | 30 | BC-17.3-002 | Auto Loader | `.option("cloudFiles.useIncrementalListing", "auto")` |
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Run: `@databricks-dbr-migration fix all auto-fixable issues`
# MAGIC 2. Review manual items above
# MAGIC 3. Test config changes on DBR 17.3
```

### 7. REPORT to User

After adding the summary cell, tell the user:
```
✅ Scan complete! 

📋 Summary added as new cell at end of notebook.

Summary:
- 🔴 Auto-fixable: 1 issue
- 🟡 Manual review: 1 issue  
- ⚙️ Config check: 1 issue

Run `@databricks-dbr-migration fix all auto-fixable issues` to apply fixes.
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
