---
name: databricks-dbr-migration
description: Find, fix, and validate breaking changes when upgrading Databricks Runtime between LTS versions (13.3 to 17.3). Use this skill when users ask to scan code for DBR compatibility issues, automatically fix breaking changes, validate migrations, or upgrade Databricks workflows. Covers Spark 3.4 to 4.0, Scala 2.12 to 2.13, Delta Lake, Auto Loader, Python UDFs, and SQL syntax.
license: Apache-2.0
compatibility: Requires file system access. Works with Databricks notebooks, Python, SQL, and Scala files.
metadata:
  databricks-skill-author: Databricks Solution Architect
  databricks-skill-version: "5.0.0"
  databricks-skill-category: platform-migration
  databricks-skill-last-updated: "2026-02-10"
allowed-tools: Read Write Bash(grep:*) Bash(find:*) Bash(python:*)
---

# Databricks LTS Migration Agent

This skill enables agents to **find**, **fix**, and **validate** breaking changes when upgrading Databricks Runtime from 13.3 LTS to 17.3 LTS.

## Agent Capabilities

1. **SCAN** - Find breaking changes in code (32 patterns in scanner script, plus special detection for temp view reuse)
2. **FIX** - Apply automatic remediations (10 Scala/SQL patterns via apply-fixes.py)
3. **FLAG** - Explicitly flag items requiring manual review or configuration testing
4. **VALIDATE** - Verify fixes are correct (12 critical patterns checked)
5. **SUMMARIZE** - Add a summary markdown cell to the notebook

> **Note:** The workspace profiler (`developer-guide/workspace-profiler.py`) has the most comprehensive coverage with 35 patterns. The agent scanner implements 32 regex-based patterns plus special detection for BC-SC-002 (temp view reuse).

---

## CRITICAL: Add Summary as Markdown Cell

**After scanning, fixing, or validating, ALWAYS add a summary as a NEW MARKDOWN CELL at the end of the notebook.**

### How to Add Summaries

1. **Load the appropriate template** from `assets/markdown-templates/`
2. **Replace all template variables** with actual findings (counts, line numbers, BC-IDs)
3. **Add as a new cell** at the end of the notebook

### Available Templates

| Action | Template File | When to Use |
|--------|---------------|-------------|
| **SCAN** | `assets/markdown-templates/scan-summary.md` | After scanning code for breaking changes |
| **FIX** | `assets/markdown-templates/fix-summary.md` | After applying automatic fixes |
| **VALIDATE** | `assets/markdown-templates/validation-report.md` | After validating fixes were applied correctly |

### Template Variables

**All templates use variables like:**
- `{SCAN_DATE}` / `{FIX_DATE}` / `{VALIDATION_DATE}` → Current date/time (YYYY-MM-DD HH:MM)
- `{TARGET_VERSION}` → Target DBR version (e.g., "17.3")
- `{AUTO_FIX_COUNT}` → Number of auto-fixable issues
- `{ASSISTED_FIX_COUNT}` → Number of assisted fix items (snippet provided, developer decides)
- `{MANUAL_REVIEW_COUNT}` → Number of manual review items
- `{AUTO_FIX_ITEMS}` / `{APPLIED_FIXES}` → Table rows with findings/fixes
- `{ASSISTED_FIX_ITEMS}` → Table rows for assisted fix items
- `{ASSISTED_FIX_SNIPPETS}` → Code snippet blocks for each assisted fix
- `{ASSISTED_FIX_VALIDATION}` → Validation status for each assisted fix
- `{MANUAL_REVIEW_ITEMS}` → Table rows for manual review items

**Example table row format:**
```
| 42 | BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |
```

### Example Agent Action (SCAN)

```python
# 1. Read template
with open('assets/markdown-templates/scan-summary.md', 'r') as f:
    template = f.read()

# 2. Replace variables with actual data
summary = template.replace('{SCAN_DATE}', '2026-01-26 14:30')
summary = summary.replace('{TARGET_VERSION}', '17.3')
summary = summary.replace('{AUTO_FIX_COUNT}', '5')
summary = summary.replace('{ASSISTED_FIX_COUNT}', '2')
summary = summary.replace('{MANUAL_REVIEW_COUNT}', '1')
summary = summary.replace('{AUTO_FIX_ITEMS}', 
    '| 42 | BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |\n' +
    '| 89 | BC-15.4-003 | `IF !condition` | Replace with `IF NOT condition` |')
# ... replace other variables ...

# 3. Add as new markdown cell at end of notebook
```

> 📝 **See `assets/markdown-templates/README.md` for complete template documentation.**

### CRITICAL: How to Generate `{ASSISTED_FIX_SNIPPETS}` — Actual Copy-Paste Fixes

**This is the most important variable.** The agent MUST read the actual code around each finding and generate a real, copy-paste-ready replacement — NOT a generic example. Each snippet must use the exact variable names, function names, view names, return types, and expressions from the developer's code.

**Workflow for EACH Assisted Fix finding:**
1. **READ** the code: Read 5-10 lines around the detected pattern to understand context
2. **EXTRACT** real names: Pull the actual variable names, function names, view names, UDF return types, column names, etc. from the code
3. **GENERATE** the fix: Write the replacement code using those exact extracted names — the developer should be able to copy-paste it directly
4. **SHOW** before → after: Include the original line(s) as a comment, then the fixed version
5. **ADD** a review note: Explain what to verify before applying

**Per-BC-ID generation instructions:**

#### BC-SC-002: Temp View Reuse
1. Read the file and find all `createOrReplaceTempView("X")` calls
2. Identify which view name `"X"` is used more than once
3. For EACH duplicate occurrence, generate the replacement using the actual view name and the actual DataFrame variable name from that line
4. Warn about downstream `spark.table("X")` or `spark.sql("SELECT ... FROM X")` calls

```
# MAGIC #### 🔧 BC-SC-002: Temp view `"{actual_view_name}"` reused (lines {line1}, {line2})
# MAGIC
# MAGIC **Original (line {line1}):** `{actual_df_var}.createOrReplaceTempView("{actual_view_name}")`
# MAGIC **Original (line {line2}):** `{actual_df_var_2}.createOrReplaceTempView("{actual_view_name}")`
# MAGIC
# MAGIC ```python
# MAGIC import uuid
# MAGIC
# MAGIC # Line {line1}:
# MAGIC {actual_view_name}_1 = f"{actual_view_name}_{uuid.uuid4().hex[:8]}"
# MAGIC {actual_df_var}.createOrReplaceTempView({actual_view_name}_1)
# MAGIC
# MAGIC # Line {line2}:
# MAGIC {actual_view_name}_2 = f"{actual_view_name}_{uuid.uuid4().hex[:8]}"
# MAGIC {actual_df_var_2}.createOrReplaceTempView({actual_view_name}_2)
# MAGIC ```
# MAGIC ⚠️ Also update `spark.table("{actual_view_name}")` and SQL references to use the new unique names.
```

#### BC-SC-003: UDF Late Binding
1. Read the `@udf(...)` decorator to get the return type (e.g., `"double"`, `StringType()`)
2. Read the function definition: name, parameters, body
3. Identify which variables in the body are defined OUTSIDE the function
4. Generate a factory function that takes those external variables as parameters
5. Show the original code and the replacement side by side

```
# MAGIC #### 🔧 BC-SC-003: UDF `{actual_func_name}` captures external variable `{actual_var_name}`
# MAGIC
# MAGIC **Original (lines {def_line}-{end_line}):**
# MAGIC ```python
# MAGIC {paste the original @udf + def block exactly as found}
# MAGIC ```
# MAGIC
# MAGIC **Replacement (copy-paste this):**
# MAGIC ```python
# MAGIC def make_{actual_func_name}_udf({actual_var_name}):
# MAGIC     @udf({actual_return_type})
# MAGIC     def {actual_func_name}({actual_params}):
# MAGIC         {actual_body_using_param_instead_of_external_var}
# MAGIC     return {actual_func_name}
# MAGIC
# MAGIC {actual_func_name} = make_{actual_func_name}_udf({actual_var_name})
# MAGIC ```
# MAGIC ⚠️ Confirm `{actual_var_name}` should be locked at its value when the UDF is created ({value_at_definition}), not at execution time ({value_later}).
```

#### BC-SC-004: Schema Access in Loops
1. Read the loop and find where `df.columns`, `df.schema`, or `df.dtypes` is accessed
2. Generate a cache statement BEFORE the loop using the actual DataFrame variable name
3. Replace the in-loop access with the cached variable

```
# MAGIC #### 🔧 BC-SC-004: `{actual_df}.columns` accessed inside loop (line {line})
# MAGIC
# MAGIC **Original:** `{paste the actual loop code with df.columns}`
# MAGIC
# MAGIC **Replacement (copy-paste this):**
# MAGIC ```python
# MAGIC # Add BEFORE the loop:
# MAGIC cached_columns = set({actual_df}.columns)
# MAGIC
# MAGIC # Inside loop — replace {actual_df}.columns with cached_columns:
# MAGIC {paste the fixed loop code}
# MAGIC ```
# MAGIC ℹ️ In Spark Connect, `.columns` triggers an RPC each call. Caching avoids repeated round-trips.
```

#### BC-17.3-005: Decimal Precision
1. Read the line with the `DecimalType` or `.cast("decimal")` call
2. If already explicit `DecimalType(p, s)` — note it and skip
3. If bare — copy the original expression and show the fixed version

```
# MAGIC #### 🔧 BC-17.3-005: Decimal precision at line {line}
# MAGIC
# MAGIC **Original:** `{paste the actual expression}`
# MAGIC **Replacement:** `{same expression with explicit DecimalType(10, 2)}`
# MAGIC
# MAGIC ℹ️ Adjust (10, 2) to match your data's precision needs. This only affects execution plans, not results.
```

#### BC-17.3-002: Auto Loader Incremental Listing
1. Read the Auto Loader code block
2. Check if `.option("cloudFiles.useIncrementalListing", ...)` already exists
3. If not, generate the fixed version with the option inserted

```
# MAGIC #### 🔧 BC-17.3-002: Auto Loader at line {line}
# MAGIC
# MAGIC **Original:**
# MAGIC ```python
# MAGIC {paste the actual readStream...format("cloudFiles")...load() block}
# MAGIC ```
# MAGIC
# MAGIC **Replacement (copy-paste this):**
# MAGIC ```python
# MAGIC {same block with .option("cloudFiles.useIncrementalListing", "auto") inserted}
# MAGIC ```
# MAGIC ℹ️ Test first: Only add if Auto Loader is slower on DBR 17.3.
```

#### BC-15.4-005 / BC-15.4-002 / BC-13.3-002 / BC-16.4-003: Config-based Fixes
1. Read the actual read operation to identify what data source is being used
2. Generate a commented config line to place BEFORE the read operation

```
# MAGIC #### 🔧 {BC-ID}: {description} at line {line}
# MAGIC
# MAGIC **Original:** `{paste the actual read line}`
# MAGIC
# MAGIC **Add BEFORE the read (uncomment after testing if behavior differs):**
# MAGIC ```python
# MAGIC # {the specific spark.conf.set line for this BC-ID}
# MAGIC ```
# MAGIC ℹ️ Test first on DBR 17.3 and compare results. Only uncomment if behavior changed.
```

#### BC-16.4-004: MERGE materializeSource=none
1. Read the actual config line
2. Generate the replacement with `"auto"` instead of `"none"`

```
# MAGIC #### 🔧 BC-16.4-004: materializeSource=none at line {line}
# MAGIC
# MAGIC **Original:** `{paste the actual line}`
# MAGIC **Replacement:** `{same line with "none" replaced by "auto"}`
```

**Key rules:**
- **NEVER use placeholder names** like `[view_name]` or `{udf_name}`. Use the ACTUAL names from the code.
- **ALWAYS read the surrounding code** before generating a snippet. Do not guess names.
- **ALWAYS show the original code** so the developer can see what's being replaced.
- **For BC-SC-003:** Copy the actual function body and only change the variable reference to use the factory parameter.
- **For BC-SC-002:** Use the actual DataFrame variable name from the line (e.g., `df_batch1`, not `df`).
- **If no findings for a BC-ID:** Omit that BC-ID's snippet block entirely.

---

## CRITICAL: Migration Path Awareness

**IMPORTANT:** When scanning, consider both the SOURCE (current) and TARGET DBR versions. Only flag patterns that are relevant to the migration path.

### Source Version Filtering

If the user is migrating FROM a specific version, skip patterns that were already addressed:

| Migration Path | Patterns to Flag |
|----------------|------------------|
| **13.3 → 17.3** | BC-14.3-*, BC-15.4-*, BC-16.4-*, BC-17.3-* (skip BC-13.3-*) |
| **14.3 → 17.3** | BC-15.4-*, BC-16.4-*, BC-17.3-* |
| **15.4 → 17.3** | BC-16.4-*, BC-17.3-* |
| **Any → 16.4** | All patterns up to BC-16.4-* |

**Example:** User says "I'm upgrading from 13.3 to 17.3" - skip BC-13.3-* patterns because they're already working on 13.3.

### Target Version Filtering

| Target Version | Patterns to Flag |
|----------------|------------------|
| **17.3** | All patterns up to 17.3 |
| **16.4** | All except BC-17.3-* patterns |
| **15.4** | All except BC-16.4-* and BC-17.3-* patterns |

**BC-15.4-001 Guidance (VariantType in UDF):**
- ⚠️ Flag as MEDIUM severity regardless of version
- Recommend testing on target DBR or using StringType + JSON as safer alternative
- Do NOT claim it's "fixed" in later versions - behavior may vary

---

## CRITICAL: Three Tiers of Findings

When scanning code, categorize ALL findings into these three categories and handle them appropriately:

### 🔴 Category 1: AUTO-FIX (10 patterns)
**Action: Automatically apply the fix. No developer review needed.**

| ID | Pattern | Fix |
|----|---------|-----|
| BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |
| BC-15.4-003 | `IF !`, `IS !`, `! IN`, etc. | Replace `!` with `NOT` |
| BC-16.4-001a | `JavaConverters` | Replace with `CollectionConverters` |
| BC-16.4-001b | `.to[List]` | Replace with `.to(List)` |
| BC-16.4-001c | `TraversableOnce` | Replace with `IterableOnce` |
| BC-16.4-001d | `Traversable` | Replace with `Iterable` |
| BC-16.4-001e | `Stream.from()` | Replace with `LazyList.from()` |
| BC-16.4-001f | `.toIterator` | Replace with `.iterator` |
| BC-16.4-001g | `.view.force` | Replace with `.view.to(List)` |
| BC-16.4-001i | `'symbol` literal | Replace with `Symbol("symbol")` |

### 🔧 Category 2: ASSISTED FIX (11 patterns)
**Action: Generate an exact suggested fix snippet in the scan output. Developer reviews and decides whether to apply.**

The agent analyzes the code context and produces a copy-paste-ready fix for each finding. The developer can accept, modify, or reject each suggestion.

| ID | Pattern | Suggested Fix Snippet the Agent Generates |
|----|---------|-------------------------------------------|
| BC-SC-002 | Same temp view name used multiple times | UUID-suffixed replacement for each duplicate view name, plus reminder to update `spark.table()` references |
| BC-SC-003 | UDF referencing external variables | Factory wrapper around the detected UDF, passing the captured variable as a parameter |
| BC-SC-004 | `df.columns` / `df.schema` in loops | Cache statement before the loop: `existing_columns = set(df.columns)` |
| BC-17.3-005 | `DecimalType` without explicit precision or bare `cast("decimal")` | Explicit `DecimalType(p, s)` replacement with a suggested precision |
| BC-13.3-002 | Parquet with timestamps | Commented `spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")` |
| BC-15.4-002 | JDBC reads (useNullCalendar) | Commented `spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")` |
| BC-15.4-005 | JDBC reads (general) | Commented `spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")` |
| BC-16.4-003 | Cached data source reads | Commented `spark.conf.set("spark.sql.legacy.readFileSourceTableCacheIgnoreOptions", "true")` |
| BC-16.4-004 | `materializeSource.*none` | Replace `"none"` with `"auto"` |
| BC-16.4-006 | Auto Loader `cleanSource` | Commented config with test-first guidance |
| BC-17.3-002 | Auto Loader without explicit incremental | `.option("cloudFiles.useIncrementalListing", "auto")` insertion |

### 🟡 Category 3: MANUAL REVIEW (10 patterns)
**Action: FLAG for developer review. No fix snippet generated because the correct action depends on intent.**

| ID | Pattern | Flag Message |
|----|---------|--------------|
| BC-13.3-001 | `MERGE INTO` | **FLAG:** ANSI mode now throws CAST_OVERFLOW. Review type casting for potential overflow |
| BC-13.3-003 | `overwriteSchema` + dynamic partition | **FLAG:** Cannot combine both. Separate schema evolution from partition overwrites |
| BC-13.3-004 | MERGE/UPDATE with type mismatch | **FLAG:** ANSI store assignment policy changed. Review types for overflow |
| BC-15.4-001 | `VariantType()` in UDF | **FLAG:** May throw exception in 15.4+. Test or use StringType + JSON |
| BC-15.4-004 | `CREATE VIEW (col TYPE)` | **FLAG:** Column types in VIEW not allowed in 15.4+. Remove types, use CAST in SELECT |
| BC-15.4-006 | `CREATE VIEW` | **FLAG:** Schema binding mode changed. Review schema evolution behavior |
| BC-16.4-002 | `HashMap`/`HashSet` | **FLAG:** Iteration order changed in Scala 2.13. Don't rely on order |
| BC-16.4-001h | `collection.Seq` | **FLAG:** Now refers to immutable.Seq. Use explicit import |
| BC-SC-001 | try/except around DataFrame transforms | **FLAG:** In Spark Connect, errors appear at action time. Add `_ = df.columns` for early validation |
| BC-17.3-003 | `array()`/`map()`/`struct()` with nulls | **FLAG:** Spark Connect now preserves nulls in literal constructions instead of coercing to defaults. The new behavior is often more correct. Only add `coalesce()` if downstream logic specifically needs the old "null to default" behavior. |

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

**BC-15.4-001: VARIANT in Python UDF [REVIEW REQUIRED]**
```bash
grep -rn "VariantType" --include="*.py" /path/to/scan
```
> ⚠️ **FLAG for review** - Test on target DBR or use StringType + JSON serialization as safer alternative.

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

**BC-15.4-001: VARIANT in UDF [REVIEW REQUIRED]**
> ⚠️ **Always flag for review.** Test on target DBR or use StringType + JSON as safer alternative.
```bash
grep -rn "VariantType\s*(" --include="*.py" /path/to/scan
```

**BC-15.4-004: VIEW Column Type Definition**
```bash
grep -rn "CREATE.*VIEW.*\(.*\(INT\|STRING\|BIGINT\|DOUBLE\|NOT NULL\|DEFAULT\)" --include="*.sql" /path/to/scan
```

### Step 4: Search for ASSISTED FIX Patterns

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

### Step 5: Report Findings with THREE TIERS

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

### 🔧 ASSISTED FIX (Suggested fix provided -- developer reviews)
- BC-SC-002: Temp view name reuse found in:
  - etl.py:25: df.createOrReplaceTempView("batch")
  - etl.py:35: df2.createOrReplaceTempView("batch")  <-- DUPLICATE!

  SUGGESTED FIX (copy-paste ready):
  ┌─────────────────────────────────────────────────────
  │ import uuid
  │ # Line 25:
  │ unique_view = f"batch_{uuid.uuid4().hex[:8]}"
  │ df.createOrReplaceTempView(unique_view)
  │ # Line 35:
  │ unique_view_2 = f"batch_{uuid.uuid4().hex[:8]}"
  │ df2.createOrReplaceTempView(unique_view_2)
  └─────────────────────────────────────────────────────

- BC-SC-003: UDF captures external variable found in:
  - process.py:10: multiplier = 1.0
  - process.py:12: @udf(...) def calc(x): return x * multiplier
  - process.py:18: multiplier = 2.5  <-- Changed after UDF definition!

  SUGGESTED FIX (copy-paste ready):
  ┌─────────────────────────────────────────────────────
  │ def make_calc_udf(multiplier):
  │     @udf("double")
  │     def calc(x):
  │         return x * multiplier
  │     return calc
  │ calc = make_calc_udf(multiplier)
  └─────────────────────────────────────────────────────

- BC-17.3-002: Auto Loader without explicit incremental listing:
  - streaming.py:50: spark.readStream.format("cloudFiles")...

  SUGGESTED FIX (copy-paste ready):
  ┌─────────────────────────────────────────────────────
  │ .option("cloudFiles.useIncrementalListing", "auto")
  └─────────────────────────────────────────────────────
  ℹ️ Test first: Only add if Auto Loader is slower on DBR 17.3.

### 🟡 MANUAL REVIEW (Human judgment required)
- BC-17.3-003: array/struct/map construction at line [X] with nullable columns
  - **FLAG:** Spark Connect now preserves nulls instead of coercing to defaults.
    The new behavior is often more correct. Only add coalesce() if needed.

### Summary
- Files scanned: X
- 🔴 AUTO-FIX: Y findings (will be fixed)
- 🔧 ASSISTED FIX: Z findings (suggested fixes provided)
- 🟡 MANUAL REVIEW: W findings (developer decision required)
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
# MAGIC | 55,85 | BC-SC-002 | Temp view reuse | UUID-suffixed view names (see snippet) |
# MAGIC | 30 | BC-17.3-002 | Auto Loader | `.option("cloudFiles.useIncrementalListing", "auto")` |
# MAGIC 
# MAGIC ### 🟡 Manual Review Still Required
# MAGIC | Line | BC-ID | Issue | Action Needed |
# MAGIC |------|-------|-------|---------------|
# MAGIC | 90 | BC-17.3-003 | Nulls in array/struct | Review null behavior |
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

### Step 4: Verify Assisted Fix Items Are Addressed

Re-run the detection patterns for each Assisted Fix item and report status:

| BC-ID | What to Check | Status Values |
|-------|---------------|---------------|
| BC-SC-002 | No duplicate temp view names remain | Applied / Not Applied / N/A |
| BC-SC-003 | UDFs with external variables are wrapped in factory functions | Applied / Not Applied / N/A |
| BC-SC-004 | Schema access cached outside loops | Applied / Not Applied / N/A |
| BC-17.3-005 | No bare `cast("decimal")` without explicit precision | Applied / Not Applied / N/A |
| BC-13.3-002 | Parquet timestamp config line present if Parquet reads exist | Applied / Not Applied / N/A |
| BC-15.4-002 | JDBC config line present if JDBC reads exist | Applied / Not Applied / N/A |
| BC-15.4-005 | JDBC config line present if JDBC reads exist | Applied / Not Applied / N/A |
| BC-16.4-003 | Cache config line present if cached reads exist | Applied / Not Applied / N/A |
| BC-16.4-004 | `materializeSource=none` replaced with `auto` | Applied / Not Applied / N/A |
| BC-16.4-006 | cleanSource config reviewed | Applied / Not Applied / N/A |
| BC-17.3-002 | Auto Loader has explicit incremental listing option | Applied / Not Applied / N/A |

### Step 5: Verify Manual Review Items Are Acknowledged

Check that each manual review item has been either:
- **Addressed** by the developer
- **Acknowledged** as acceptable with documented reason

### Step 6: Generate Validation Report

```
## Validation Report

### Auto-Fix Validation
✅ No input_file_name() found
✅ No '!' syntax for NOT found  
✅ No deprecated Scala imports found

### Replacement Verification
✅ Found 3 instances of _metadata.file_name
✅ Found 5 instances of NOT syntax
✅ Found 2 instances of CollectionConverters

### Assisted Fix Validation
✅ BC-SC-002: Applied - Unique view names confirmed
✅ BC-SC-003: Applied - Factory pattern wrapping external variables
✅ BC-17.3-002: Applied - Explicit incremental listing option added
⚠️ BC-15.4-005: Not Applied - JDBC config line not yet added (awaiting test)

### Manual Review Status
✅ BC-17.3-003: [REVIEWED] - Developer confirmed null preservation is acceptable

### Summary
Status: ✅ PASSED - Ready for DBR 17.3 upgrade
Files validated: 15
All breaking changes addressed
```

---

## Quick Reference: All Breaking Changes

### 🔴 HIGH Severity - Code-Level Breaking Changes

| ID | Pattern | Fix |
|----|---------|-----|
| BC-17.3-001 | `input_file_name()` | `_metadata.file_name` |
| BC-13.3-001 | MERGE INTO type overflow | Add explicit CAST with bounds check |
| BC-16.4-001a | `JavaConverters` | `CollectionConverters` |
| BC-16.4-001c | `TraversableOnce` | `IterableOnce` |
| BC-16.4-001d | `Traversable` | `Iterable` |
| BC-16.4-002 | `HashMap`/`HashSet` ordering | Don't rely on iteration order |
| BC-SC-001 | Lazy schema analysis | Call `df.columns` for early validation |

### 🟡 MEDIUM Severity - Potential Issues

| ID | Pattern | Fix |
|----|---------|-----|
| BC-15.4-003 | `IF !`, `IS !`, `! IN` | Use `NOT` |
| BC-16.4-001b | `.to[List]` | `.to(List)` |
| BC-16.4-001e | `Stream.from()` | `LazyList.from()` |
| BC-16.4-001f | `.toIterator` | `.iterator` |
| BC-16.4-001g | `.view.force` | `.view.to(List)` |
| BC-16.4-001h | `collection.Seq` | Use explicit `immutable.Seq` |
| BC-13.3-003 | `overwriteSchema` + dynamic partition | Separate operations |
| BC-17.3-002 | Auto Loader incremental | Set option explicitly |
| BC-15.4-006 | VIEW schema binding | Review schema evolution |
| BC-16.4-003 | Data source cache | Set legacy cache option |
| BC-16.4-006 | Auto Loader cleanSource | Review cleanup behavior |

### 🟢 LOW Severity - Subtle Changes

| ID | Pattern | Fix |
|----|---------|-----|
| BC-15.4-001 | `VariantType` in UDF | Test or use StringType + JSON |
| BC-15.4-004 | VIEW column types | Remove types, CAST in SELECT |
| BC-13.3-002 | Parquet TIMESTAMP_NTZ | Set inferTimestampNTZ=false |
| BC-15.4-002 | JDBC timestamp | Set useNullCalendar=false |
| BC-15.4-005 | JDBC reads | Test timestamp handling |
| BC-16.4-004 | MERGE materializeSource=none | Remove or use "auto" |
| BC-16.4-001i | `'symbol` literal | `Symbol("symbol")` |
| BC-16.4-005 | Json4s library | Review json4s usage |
| BC-17.3-003 | Null handling in literals | Handle nulls explicitly |
| BC-17.3-004 | Decimal precision | Specify precision/scale |
| BC-14.3-001 | Thriftserver hive.aux.jars.path | Config removed |
| BC-13.3-004 | ANSI store assignment | Review type policies |

### Spark Connect Behavioral Changes (Serverless/Connect Users)

| ID | Severity | Behavior | Best Practice |
|----|----------|----------|---------------|
| BC-SC-001 | HIGH | Lazy schema analysis | Call `df.columns` to trigger early error detection |
| BC-SC-003 | LOW | UDF late binding | Use function factory to capture variables |
| BC-SC-004 | LOW | Schema access RPC | Cache `df.columns` locally in loops |

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
# MAGIC | 55,85 | BC-SC-002 | Temp view "batch" reused | UUID-suffixed names (see snippet) |
# MAGIC | 30 | BC-17.3-002 | Auto Loader | `.option("cloudFiles.useIncrementalListing", "auto")` |
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Run: `@databricks-dbr-migration fix all auto-fixable issues`
# MAGIC 2. Review assisted fix snippets above and apply as appropriate
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
