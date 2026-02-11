# Fix Workflow

Instructions for applying automatic fixes and generating fix summaries. Includes markdown cell template, multi-file strategy, and code transformations for all auto-fix patterns.

---

## Capability 2: FIX - Apply Automatic Remediations

When user asks to fix breaking changes, apply these transformations.

### CRITICAL: Add Fix Summary as Markdown Cell

**After applying fixes, ALWAYS add a summary as a NEW MARKDOWN CELL at the end of the notebook.**

**Use the template from `assets/markdown-templates/fix-summary.md` and replace all variables.**

**IMPORTANT: The `{ASSISTED_FIX_SNIPPETS}` variable MUST be populated with actual code blocks — follow the same process as Step 6 in the SCAN capability. Go back to each assisted fix finding, read the actual code, and generate copy-paste-ready fix snippets. DO NOT leave this blank.**

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
# MAGIC | 🔧 Assisted Fix (suggested) | Y |
# MAGIC | 🟡 Manual Review (unchanged) | Z |
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
# MAGIC ### 🔧 Assisted Fix (Suggested Fixes Provided)
# MAGIC | Line | BC-ID | Issue | Suggested Fix |
# MAGIC |------|-------|-------|---------------|
# MAGIC | 55,85 | BC-SC-002 | Temp view reuse | UUID-suffixed view names (see snippet below) |
# MAGIC | 30 | BC-17.3-002 | Auto Loader | `.option("cloudFiles.useIncrementalListing", "auto")` (see snippet below) |
# MAGIC 
# MAGIC ### 🔧 Suggested Fix Snippets (copy-paste ready)
# MAGIC 
# MAGIC #### 🔧 BC-SC-002-DUP: Temp view `"batch"` reused (lines 55, 85)
# MAGIC **Original (line 55):** `df_batch1.createOrReplaceTempView("batch")`
# MAGIC **Original (line 85):** `df_batch2.createOrReplaceTempView("batch")`
# MAGIC **Replacement (copy-paste this):**
# MAGIC ```python
# MAGIC import uuid
# MAGIC batch_view_1 = f"batch_{uuid.uuid4().hex[:8]}"
# MAGIC df_batch1.createOrReplaceTempView(batch_view_1)
# MAGIC batch_view_2 = f"batch_{uuid.uuid4().hex[:8]}"
# MAGIC df_batch2.createOrReplaceTempView(batch_view_2)
# MAGIC ```
# MAGIC ⚠️ Also update downstream `spark.table("batch")` and SQL references.
# MAGIC 
# MAGIC #### 🔧 BC-17.3-002: Auto Loader incremental listing (line 30)
# MAGIC **Original:** `spark.readStream.format("cloudFiles").load(path)`
# MAGIC **Replacement:**
# MAGIC ```python
# MAGIC spark.readStream.format("cloudFiles").option("cloudFiles.useIncrementalListing", "auto").load(path)
# MAGIC ```
# MAGIC ℹ️ Explicitly sets listing behavior. Test with your data volume.
# MAGIC 
# MAGIC ### 🟡 Manual Review Still Required
# MAGIC | Line | BC-ID | Issue | Action Needed |
# MAGIC |------|-------|-------|---------------|
# MAGIC | 90 | BC-17.3-003 | Nulls in array/struct | Review null behavior |
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Review the changes above
# MAGIC 2. **Review and apply the assisted fix snippets above**
# MAGIC 3. Address manual review items
# MAGIC 4. Test on DBR 17.3
# MAGIC 5. Run: `@databricks-dbr-migration validate all fixes`
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
from pyspark.sql.functions import input_file_name, col
df.withColumn("source", input_file_name())

# AFTER (use col().alias() to safely add the metadata column)
from pyspark.sql.functions import col
df.select("*", col("_metadata.file_name").alias("source"))
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

> ⚠️ **REVIEW REQUIRED** - VARIANT UDFs may have issues. Test on target DBR or use the safer StringType + JSON approach below.
> See [official docs](https://learn.microsoft.com/en-us/azure/databricks/udf/python#variants-with-udf) for current guidance.

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
- ✅ 4 auto-fixes applied
- 🔧 Assisted fix suggestions provided for developer review
- 🟡 Manual review items flagged

📋 Summary added as new cell at end of notebook.

Run `@databricks-dbr-migration validate all fixes` to verify.
```
