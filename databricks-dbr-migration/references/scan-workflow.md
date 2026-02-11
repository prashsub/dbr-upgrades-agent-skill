# Scan Workflow

Step-by-step instructions for scanning code for breaking change patterns. Includes file discovery, grep patterns, special detection logic, report format, and scan summary cell generation.

---

## Capability 1: SCAN - Find Breaking Changes

When user asks to scan code for breaking changes, follow these steps:

### Step 1: Identify Target Files

Find all relevant files in the specified path **including subdirectories**:

```bash
# Find Python files (recursively scans all subdirectories)
find /path/to/scan -name "*.py" -type f

# Find Jupyter/Databricks notebook files (.ipynb)
find /path/to/scan -name "*.ipynb" -type f

# Find SQL files
find /path/to/scan -name "*.sql" -type f

# Find Scala files
find /path/to/scan -name "*.scala" -type f
```

**Important: `.ipynb` (Jupyter Notebook) File Handling:**
- `.ipynb` files are JSON-structured — code lives inside `"source"` arrays within cell objects
- `grep` will still match patterns inside `.ipynb` files, but line numbers refer to the JSON file lines, not notebook cell lines
- When fixing `.ipynb` files, parse the JSON structure and modify the appropriate cell's `"source"` array
- Databricks notebooks may be exported as either `.py` (with `# MAGIC` prefixes) or `.ipynb` format — scan for **both**

**Important for Multi-File Projects:**
- The `find` command recursively searches all subdirectories (e.g., `utils/`, `src/`, etc.)
- Scan ALL Python and notebook files found, including helper modules and utility packages
- Check `import` and `from` statements to identify dependencies
- If a notebook imports from local modules (e.g., `from utils.helpers import foo`), scan those imported files too
- Look for package structures with `__init__.py` files

**Example Multi-File Structure:**
```
project/
├── main_notebook.py          ← Scan this (.py notebook format)
├── main_notebook.ipynb       ← Scan this (.ipynb notebook format)
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
grep -rn "input_file_name\s*(" --include="*.py" --include="*.ipynb" --include="*.sql" --include="*.scala" /path/to/scan
```

**BC-15.4-001: VARIANT in Python UDF [REVIEW REQUIRED]**
```bash
grep -rn "VariantType" --include="*.py" --include="*.ipynb" /path/to/scan
```
> ⚠️ **FLAG for review** - Test on target DBR or use StringType + JSON serialization as safer alternative.

**BC-16.4-001: Scala JavaConverters [DEPRECATED in 16.4]**
```bash
grep -rn "scala.collection.JavaConverters" --include="*.scala" --include="*.ipynb" /path/to/scan
```

#### MEDIUM SEVERITY PATTERNS

**BC-15.4-003: '!' syntax for NOT [DISALLOWED in 15.4]**
```bash
grep -rn "IF\s*!" --include="*.sql" --include="*.ipynb" /path/to/scan
grep -rn "IS\s*!" --include="*.sql" --include="*.ipynb" /path/to/scan
grep -rn "\s!\s*IN\b" --include="*.sql" --include="*.ipynb" /path/to/scan
```

**BC-16.4-001b: Scala .to[Collection] syntax [CHANGED in 16.4]**
```bash
grep -rn "\.to\[" --include="*.scala" --include="*.ipynb" /path/to/scan
```

### Step 3: Search for MANUAL REVIEW Patterns

**BC-15.4-001: VARIANT in UDF [REVIEW REQUIRED]**
> ⚠️ **Always flag for review.** Test on target DBR or use StringType + JSON as safer alternative.
```bash
grep -rn "VariantType\s*(" --include="*.py" --include="*.ipynb" /path/to/scan
```

**BC-15.4-004: VIEW Column Type Definition**
```bash
grep -rn "CREATE.*VIEW.*\(.*\(INT\|STRING\|BIGINT\|DOUBLE\|NOT NULL\|DEFAULT\)" --include="*.sql" --include="*.ipynb" /path/to/scan
```

### Step 4: Search for ASSISTED FIX Patterns

**BC-SC-002: Temp View Name Reuse**
```bash
grep -rn "createOrReplaceTempView\|createTempView" --include="*.py" --include="*.ipynb" --include="*.scala" /path/to/scan
# Then check if same name appears multiple times in same file
```

**BC-SC-003: UDF with External Variables**
```bash
grep -rn "@udf" --include="*.py" --include="*.ipynb" /path/to/scan
# Then check if function body references variables defined outside
```

**BC-SC-004: Schema Access in Loops**
```bash
grep -rn "\.columns\|\.schema\|\.dtypes" --include="*.py" --include="*.ipynb" /path/to/scan
# Then check if inside for/while loop
```

**BC-13.3-002: Parquet Timestamp**
```bash
grep -rn "\.parquet\|read.parquet" --include="*.py" --include="*.ipynb" /path/to/scan
```

**BC-15.4-002: JDBC**
```bash
grep -rn "\.jdbc\|read.jdbc" --include="*.py" --include="*.ipynb" /path/to/scan
```

**BC-16.4-004: MERGE materializeSource=none**
```bash
grep -rn "materializeSource.*none" --include="*.py" --include="*.ipynb" --include="*.sql" /path/to/scan
```

**BC-17.3-002: Auto Loader**
```bash
grep -rn "cloudFiles" --include="*.py" --include="*.ipynb" /path/to/scan
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

**ALWAYS add the scan results as a new markdown cell at the end of the notebook.**

Load `assets/markdown-templates/scan-summary.md` and populate EVERY variable. Here is how to fill each one:

| Variable | How to Populate |
|----------|----------------|
| `{SCAN_DATE}` | Current date/time: `YYYY-MM-DD HH:MM` |
| `{TARGET_VERSION}` | Target DBR version (e.g., `17.3`) |
| `{AUTO_FIX_COUNT}` | Count of auto-fix findings |
| `{ASSISTED_FIX_COUNT}` | Count of assisted fix findings |
| `{MANUAL_REVIEW_COUNT}` | Count of manual review findings |
| `{AUTO_FIX_ITEMS}` | One table row per auto-fix finding: `\| line \| BC-ID \| pattern \| fix \|` |
| `{ASSISTED_FIX_ITEMS}` | One table row per assisted fix finding: `\| line \| BC-ID \| issue \| suggested fix \|` |
| `{MANUAL_REVIEW_ITEMS}` | One table row per manual review finding: `\| line \| BC-ID \| issue \| action \|` |
| **`{ASSISTED_FIX_SNIPPETS}`** | **⬇️ SEE BELOW — this is the most important one** |

#### How to fill `{ASSISTED_FIX_SNIPPETS}` (REQUIRED — do not leave blank)

For EACH assisted fix finding, you MUST generate a code block with the actual fix. Follow these steps:

1. **Go back to the code** you already scanned. Re-read 5-10 lines around each assisted fix finding.
2. **For each finding**, generate a block with this structure:

```
# MAGIC #### 🔧 {BC-ID}: {what was found, using actual names}
# MAGIC
# MAGIC **Original (line {N}):**
# MAGIC ```python
# MAGIC {paste the actual original line(s) from the code}
# MAGIC ```
# MAGIC
# MAGIC **Replacement (copy-paste this):**
# MAGIC ```python
# MAGIC {the actual fixed code using real variable names, function names, view names}
# MAGIC ```
# MAGIC ⚠️ {what the developer should verify}
```

3. **Repeat** for every assisted fix finding. Concatenate all blocks into `{ASSISTED_FIX_SNIPPETS}`.

**Example — if the scan found `df_batch1.createOrReplaceTempView("batch")` on line 114 and `df_batch2.createOrReplaceTempView("batch")` on line 119:**

```
# MAGIC #### 🔧 BC-SC-002: Temp view `"batch"` reused (lines 114, 119)
# MAGIC
# MAGIC **Original (line 114):** `df_batch1.createOrReplaceTempView("batch")`
# MAGIC **Original (line 119):** `df_batch2.createOrReplaceTempView("batch")`
# MAGIC
# MAGIC **Replacement (copy-paste this):**
# MAGIC ```python
# MAGIC import uuid
# MAGIC
# MAGIC # Line 114:
# MAGIC batch_view_1 = f"batch_{uuid.uuid4().hex[:8]}"
# MAGIC df_batch1.createOrReplaceTempView(batch_view_1)
# MAGIC
# MAGIC # Line 119:
# MAGIC batch_view_2 = f"batch_{uuid.uuid4().hex[:8]}"
# MAGIC df_batch2.createOrReplaceTempView(batch_view_2)
# MAGIC ```
# MAGIC ⚠️ Also update `spark.table("batch")` and `spark.sql("SELECT ... FROM batch")` to use the new names.
```

**If you found a UDF `apply_multiplier` capturing `multiplier` at lines 141-143:**

```
# MAGIC #### 🔧 BC-SC-003: UDF `apply_multiplier` captures external variable `multiplier`
# MAGIC
# MAGIC **Original (lines 141-143):**
# MAGIC ```python
# MAGIC @udf("double")
# MAGIC def apply_multiplier(value):
# MAGIC     return value * multiplier
# MAGIC ```
# MAGIC
# MAGIC **Replacement (copy-paste this):**
# MAGIC ```python
# MAGIC def make_apply_multiplier_udf(multiplier):
# MAGIC     @udf("double")
# MAGIC     def apply_multiplier(value):
# MAGIC         return value * multiplier
# MAGIC     return apply_multiplier
# MAGIC
# MAGIC apply_multiplier = make_apply_multiplier_udf(multiplier)
# MAGIC ```
# MAGIC ⚠️ Confirm `multiplier` should be locked at 1.0 (definition time), not 2.5 (execution time).
```

**DO NOT skip this step. DO NOT leave `{ASSISTED_FIX_SNIPPETS}` blank. The whole point of "Assisted Fix" is providing the actual fix code.**
