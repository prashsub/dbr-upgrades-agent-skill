# Snippet Generation Guide

How to generate copy-paste-ready assisted fix snippets for each BC-ID finding. The agent MUST follow these instructions for every assisted fix finding.

---

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

#### BC-15.4-005 / BC-15.4-002: JDBC Timestamp Handling (Config-based with Self-Test)
1. Read the JDBC read code to identify the table/URL being read
2. Extract the actual JDBC URL and table name from the code
3. Generate a self-comparison test cell that reads with both settings and diffs the results

```
# MAGIC #### 🔧 {BC-ID}: JDBC timestamp handling at line {line}
# MAGIC
# MAGIC **Original:** `{paste the actual .format("jdbc") or .jdbc() line}`
# MAGIC
# MAGIC **Self-Test (run on target DBR to check if a fix is needed):**
# MAGIC ```python
# MAGIC # === {BC-ID} Self-Test ===
# MAGIC spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "true")
# MAGIC df_new = (spark.read.format("jdbc")
# MAGIC     .option("url", "{actual_jdbc_url}")
# MAGIC     .option("dbtable", "{actual_table}")
# MAGIC     .load()
# MAGIC )
# MAGIC
# MAGIC spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")
# MAGIC df_old = (spark.read.format("jdbc")
# MAGIC     .option("url", "{actual_jdbc_url}")
# MAGIC     .option("dbtable", "{actual_table}")
# MAGIC     .load()
# MAGIC )
# MAGIC
# MAGIC diff_count = df_new.exceptAll(df_old).count()
# MAGIC print(f"Rows that differ: {diff_count}")
# MAGIC if diff_count == 0:
# MAGIC     print("SAFE: No fix needed for this JDBC read.")
# MAGIC else:
# MAGIC     print("FIX REQUIRED: Add the config line below before this JDBC read.")
# MAGIC ```
# MAGIC
# MAGIC **If self-test shows differences, add this BEFORE the JDBC read:**
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.legacy.jdbc.useNullCalendar", "false")
# MAGIC ```
# MAGIC ℹ️ No source database access required. The self-test compares both calendar modes on the same DBR version.
```

#### BC-13.3-002 / BC-16.4-003: Other Config-based Fixes
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
# MAGIC ℹ️ Test first on target DBR and compare results. Only uncomment if behavior changed.
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

#### BC-16.4-007: DateTime Pattern Width (JDK 17)
1. Read the `to_date`/`to_timestamp`/`date_format` call to identify the column argument and the strict format pattern
2. Extract the actual column argument (e.g., `col("bill_date")`, `col("date_str")`, or a bare column name)
3. Determine the function name (`to_date`, `to_timestamp`, or `date_format`)
4. Generate a `coalesce(try_to_date)` replacement that handles mixed 2-digit/4-digit year data
5. Add a decision note for the developer to simplify if their data has only one year format

```
# MAGIC #### 🔧 BC-16.4-007: Strict datetime pattern at line {line}
# MAGIC
# MAGIC **Original:** `{paste the actual to_date/to_timestamp call}`
# MAGIC
# MAGIC **Replacement (copy-paste this — handles mixed 2-digit/4-digit years):**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import coalesce, try_to_date
# MAGIC
# MAGIC coalesce(
# MAGIC     try_to_date({actual_col_arg}, "M/d/yyyy"),   # 4-digit year first
# MAGIC     try_to_date({actual_col_arg}, "M/d/yy"),     # 2-digit year fallback
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ⚠️ **Serverless note:** `to_date` throws `CANNOT_PARSE_TIMESTAMP` on Serverless (ANSI mode).
# MAGIC `try_to_date` returns NULL instead of throwing, making it safe for `coalesce`.
# MAGIC
# MAGIC **Simplify if you know the year format:**
# MAGIC - Only 4-digit years → `to_date({actual_col_arg}, "M/d/yyyy")`
# MAGIC - Only 2-digit years → `to_date({actual_col_arg}, "M/d/yy")`
# MAGIC - Mixed or unknown → Use the `coalesce(try_to_date)` pattern above
```

**Key rules:**
- **NEVER use placeholder names** like `[view_name]` or `{udf_name}`. Use the ACTUAL names from the code.
- **ALWAYS read the surrounding code** before generating a snippet. Do not guess names.
- **ALWAYS show the original code** so the developer can see what's being replaced.
- **For BC-SC-003:** Copy the actual function body and only change the variable reference to use the factory parameter.
- **For BC-SC-002:** Use the actual DataFrame variable name from the line (e.g., `df_batch1`, not `df`).
- **If no findings for a BC-ID:** Omit that BC-ID's snippet block entirely.
