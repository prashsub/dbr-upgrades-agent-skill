# Step 2: Using Databricks Assistant for Migration

This guide shows you how to use the Databricks Assistant (agent mode) with the DBR migration skill to scan, fix, and validate your code.

## Prerequisites

- [ ] Migration skill installed (see [01-skill-setup.md](01-skill-setup.md))
- [ ] Databricks Assistant in **agent mode** enabled
- [ ] Notebook or folder with code to migrate

---

## Step 2.1: Enable Agent Mode

1. Open **Databricks Assistant** (sidebar icon or `Cmd/Ctrl + I`)
2. Click the **mode toggle** at the top
3. Select **Agent mode** (not "Chat" mode)

> **Note:** Agent mode allows the Assistant to read files, execute code, and make changes. Skills only work in agent mode.

---

## Step 2.2: Scan Your Code for Breaking Changes

### Scan a Single Notebook

Ask the Assistant:
```
Scan this notebook for breaking changes when upgrading to DBR 17.3
```

### Scan a Folder

Ask the Assistant:
```
Scan all notebooks in /Workspace/Users/me/my-project/ for DBR 17.3 compatibility issues
```

### Scan with Specific Target Version

```
Scan /Workspace/my-project for breaking changes between DBR 13.3 and DBR 16.4
```

### Expected Output

The Assistant will provide a report like:

```
## Scan Results for /Workspace/my-project/

### HIGH Severity (Must Fix Before Upgrade)
- BC-17.3-001: input_file_name() found in:
  - etl_pipeline.py:42: df.withColumn("source", input_file_name())
  - data_loader.py:87: source_file = input_file_name()

- BC-15.4-001: VARIANT in Python UDF found in:
  - transform_utils.py:23: @udf(returnType=VariantType())

### MEDIUM Severity (Should Fix)
- BC-15.4-003: '!' syntax found in:
  - setup.sql:15: CREATE TABLE IF ! EXISTS staging_table

### Summary
- Files scanned: 12
- HIGH severity: 3 findings
- MEDIUM severity: 1 finding
```

---

## Step 2.3: Understand the Findings

Ask the Assistant to explain any finding:

```
Explain why input_file_name() is a breaking change and how to fix it
```

**Expected Response:**
> The `input_file_name()` function was deprecated in DBR 13.3 and completely removed in DBR 17.3. 
> 
> **Fix:** Replace with `_metadata.file_name`:
> ```python
> # Before
> df.withColumn("source", input_file_name())
> 
> # After  
> df.select("*", "_metadata.file_name").withColumnRenamed("file_name", "source")
> ```

---

## Step 2.4: Apply Fixes

### Fix All Issues

```
Fix all the breaking changes you found in my project
```

### Fix Specific Issues

```
Only fix the input_file_name() issues, skip the others for now
```

### Preview Fixes (Dry Run)

```
Show me what changes you would make to fix the breaking changes, but don't apply them yet
```

### Expected Output

```
## Fixes Applied

### etl_pipeline.py
✅ Line 5: Removed deprecated import
✅ Line 42: Replaced input_file_name() with _metadata.file_name

### data_loader.py
✅ Line 87: Replaced input_file_name() with _metadata.file_name

### setup.sql
✅ Line 15: Changed "IF ! EXISTS" to "IF NOT EXISTS"

Total: 4 fixes applied in 3 files
```

---

## Step 2.5: Validate Fixes

After applying fixes, ask the Assistant to validate:

```
Validate that all breaking changes have been fixed in my project
```

**Expected Output:**

```
## Validation Report

### Breaking Patterns Check
✅ No input_file_name() found
✅ No VARIANT in Python UDFs found
✅ No deprecated '!' syntax found
✅ No deprecated Scala imports found

### Replacement Verification
✅ Found 2 instances of _metadata.file_name (expected replacements)
✅ Found 1 instance of IF NOT EXISTS (expected replacement)

### Summary
Status: ✅ PASSED - Ready for DBR 17.3 upgrade
Files validated: 12
All breaking changes resolved
```

---

## Step 2.6: Handle Complex Cases

### Scala 2.13 Migration (DBR 16.4+)

For Scala code, ask for detailed guidance:

```
I have Scala notebooks. Help me understand the Scala 2.13 migration for DBR 16.4
```

### Spark Connect Patterns

For serverless compute or Databricks Connect users:

```
Check my code for Spark Connect compatibility issues
```

### Auto Loader Configuration

```
Review my Auto Loader jobs for the DBR 17.3 incremental listing change
```

---

## Common Prompts Reference

| Task | Prompt |
|------|--------|
| Full scan | "Scan [path] for DBR 17.3 breaking changes" |
| Scan specific version | "Find breaking changes for upgrading from DBR 13.3 to 15.4" |
| Fix all | "Fix all breaking changes in [path]" |
| Fix specific | "Only fix BC-17.3-001 (input_file_name) issues" |
| Explain issue | "Explain breaking change BC-15.4-001" |
| Validate | "Validate my migration fixes" |
| Show legacy flags | "What configuration flags preserve legacy behavior?" |
| Migration checklist | "Generate a migration checklist for my project" |

---

## Tips for Effective Use

### 1. Scan Before Each LTS Jump

Don't jump directly from 13.3 to 17.3. Scan for each intermediate version:

```
First, scan for DBR 14.3 breaking changes
Then scan for DBR 15.4 breaking changes
Then scan for DBR 16.4 breaking changes
Finally scan for DBR 17.3 breaking changes
```

### 2. Use Dry Run First

Always preview changes before applying:

```
Show me the fixes but don't apply them yet
```

### 3. Keep a Change Log

Ask the Assistant to document changes:

```
Create a summary of all changes made for my migration documentation
```

### 4. Handle False Positives

If the Assistant flags something incorrectly:

```
This instance of input_file_name is in a comment, please ignore it
```

---

## Troubleshooting

### Assistant Can't Access Files

**Symptom:** "I don't have access to that location"

**Solution:** 
- Ensure you're in Agent mode
- Check file permissions
- Try specifying the full path: `/Workspace/Users/you/project/`

### Fixes Not Working

**Symptom:** Applied fixes but code doesn't run

**Solution:**
1. Ask the Assistant to validate the fix
2. Check for import statements that need updating
3. Review the full context around the change

### Too Many Findings

**Symptom:** Overwhelming number of results

**Solution:**
```
Show me only HIGH severity issues first
```

---

## Next Steps

After scanning and fixing your code:

→ **[03-quality-validation.md](03-quality-validation.md)**: Validate code quality and correctness

---

## References

- [Extend Assistant with Agent Skills - Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/assistant/skills)
