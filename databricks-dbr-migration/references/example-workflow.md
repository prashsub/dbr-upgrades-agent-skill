# Example Agent Workflow

A complete end-to-end example showing how the agent should handle a scan-and-fix request. Includes scan commands, categorization, fix application, and scan summary cell generation.

---

## Example Agent Workflow

**User:** "Scan my notebooks folder for DBR 17.3 compatibility issues and fix them"

**Agent actions:**

### 1. SCAN all patterns
```bash
# Auto-fix patterns (scan .py, .ipynb, .sql, .scala)
grep -rn "input_file_name" --include="*.py" --include="*.ipynb" --include="*.sql" --include="*.scala" ./notebooks/
grep -rn "IF\s*!\|IS\s*!" --include="*.sql" --include="*.ipynb" ./notebooks/

# Manual review patterns  
grep -rn "createOrReplaceTempView" --include="*.py" --include="*.ipynb" --include="*.scala" ./notebooks/
grep -rn "@udf" --include="*.py" --include="*.ipynb" ./notebooks/

# Config patterns
grep -rn "cloudFiles" --include="*.py" --include="*.ipynb" ./notebooks/
```

### 2. Categorize findings
```
Found:
- etl_job.py:42 - input_file_name() [AUTO-FIX]
- etl_job.py:55 - createOrReplaceTempView("batch") [ASSISTED FIX - duplicate name]
- etl_job.py:85 - createOrReplaceTempView("batch") [ASSISTED FIX - same name reused!]
- streaming.py:30 - cloudFiles [ASSISTED FIX]
```

### 3. FIX auto-fixable items
- Read `etl_job.py`
- Replace `input_file_name()` with `_metadata.file_name`
- Write updated file

### 4. FLAG assisted fix items (with suggested code snippets)
```
🔧 ASSISTED FIX: BC-SC-002 in etl_job.py

The temp view name "batch" is used on both line 55 and line 85.

SUGGESTED FIX (copy-paste ready):
┌─────────────────────────────────────────────────────
│ import uuid
│ # Line 55:
│ unique_view = f"batch_{uuid.uuid4().hex[:8]}"
│ df.createOrReplaceTempView(unique_view)
│ # Line 85:
│ unique_view_2 = f"batch_{uuid.uuid4().hex[:8]}"
│ df2.createOrReplaceTempView(unique_view_2)
└─────────────────────────────────────────────────────

🔧 ASSISTED FIX: BC-17.3-002 in streaming.py

SUGGESTED FIX (copy-paste ready):
┌─────────────────────────────────────────────────────
│ .option("cloudFiles.useIncrementalListing", "auto")
└─────────────────────────────────────────────────────
ℹ️ Test first: Only add if Auto Loader is slower on DBR 17.3.
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
# MAGIC | 🔧 Assisted Fix | 2 |
# MAGIC | 🟡 Manual Review | 0 |
# MAGIC 
# MAGIC ### 🔴 Auto-Fix Required
# MAGIC | Line | BC-ID | Pattern | Fix |
# MAGIC |------|-------|---------|-----|
# MAGIC | 42 | BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |
# MAGIC 
# MAGIC ### 🔧 Assisted Fix (Review Suggested Code)
# MAGIC | Line | BC-ID | Issue | Suggested Fix |
# MAGIC |------|-------|-------|---------------|
# MAGIC | 55,85 | BC-SC-002 | Temp view "batch" reused | UUID-suffixed names (see snippet below) |
# MAGIC | 30 | BC-17.3-002 | Auto Loader | `.option("cloudFiles.useIncrementalListing", "auto")` (see snippet below) |
# MAGIC 
# MAGIC ### 🔧 Suggested Fix Snippets (copy-paste ready)
# MAGIC 
# MAGIC #### 🔧 BC-SC-002-DUP: Temp view `"batch"` reused (lines 55, 85)
# MAGIC 
# MAGIC **Original (line 55):** `df_batch1.createOrReplaceTempView("batch")`
# MAGIC **Original (line 85):** `df_batch2.createOrReplaceTempView("batch")`
# MAGIC 
# MAGIC **Replacement (copy-paste this):**
# MAGIC ```python
# MAGIC import uuid
# MAGIC 
# MAGIC # Line 55:
# MAGIC batch_view_1 = f"batch_{uuid.uuid4().hex[:8]}"
# MAGIC df_batch1.createOrReplaceTempView(batch_view_1)
# MAGIC 
# MAGIC # Line 85:
# MAGIC batch_view_2 = f"batch_{uuid.uuid4().hex[:8]}"
# MAGIC df_batch2.createOrReplaceTempView(batch_view_2)
# MAGIC ```
# MAGIC ⚠️ Also update `spark.table("batch")` and SQL references to use the new unique names.
# MAGIC 
# MAGIC #### 🔧 BC-17.3-002: Auto Loader incremental listing (line 30)
# MAGIC 
# MAGIC **Original (line 30):**
# MAGIC ```python
# MAGIC spark.readStream.format("cloudFiles").load(path)
# MAGIC ```
# MAGIC 
# MAGIC **Replacement (copy-paste this):**
# MAGIC ```python
# MAGIC spark.readStream.format("cloudFiles").option("cloudFiles.useIncrementalListing", "auto").load(path)
# MAGIC ```
# MAGIC ℹ️ This explicitly sets Auto Loader listing behavior. Test with your data volume.
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Run: `@databricks-dbr-migration fix all auto-fixable issues`
# MAGIC 2. **Review and apply the assisted fix snippets above**
# MAGIC 3. Test on DBR 17.3
```

### 7. REPORT to User

After adding the summary cell, tell the user:
```
✅ Scan complete! 

📋 Summary added as new cell at end of notebook.

Summary:
- 🔴 Auto-fixable: 1 issue
- 🔧 Assisted fix: 2 issues with suggested code
- 🟡 Manual review: 0 issues

Run `@databricks-dbr-migration fix all auto-fixable issues` to apply fixes.
```
