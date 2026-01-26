# Effective Prompts Guide for DBR Migration Agent Skill

This guide provides ready-to-use prompts for efficiently running the DBR LTS Migration Agent Skill in Databricks Assistant.

---

## 📋 Table of Contents

1. [Quick Start Prompts](#quick-start-prompts)
2. [Scanning Prompts](#scanning-prompts)
3. [Fixing Prompts](#fixing-prompts)
4. [Validation Prompts](#validation-prompts)
5. [Multi-File Project Prompts](#multi-file-project-prompts)
6. [Specific Breaking Change Prompts](#specific-breaking-change-prompts)
7. [Configuration Prompts](#configuration-prompts)
8. [Troubleshooting Prompts](#troubleshooting-prompts)

---

## Quick Start Prompts

### 🔍 Initial Assessment

```
@dbr-migration scan this notebook for breaking changes when upgrading from DBR 13.3 to DBR 17.3
```

```
@dbr-migration analyze this folder for DBR 17.3 compatibility issues
```

### 🔧 Quick Fix

```
@dbr-migration fix all breaking changes in this notebook
```

```
@dbr-migration fix the input_file_name() issues you found
```

### ✅ Validation

```
@dbr-migration validate that all breaking changes have been fixed
```

---

## Scanning Prompts

### Single File Scans

**Scan current notebook:**
```
@dbr-migration scan this notebook for all breaking changes from DBR 13.3 to 17.3
```

**Scan with specific target version:**
```
@dbr-migration check this notebook for DBR 16.4 LTS compatibility issues
```

**Scan focusing on specific categories:**
```
@dbr-migration scan this notebook for Spark Connect compatibility issues
```

```
@dbr-migration check this Scala file for Scala 2.13 breaking changes
```

### Multi-File and Folder Scans

**Scan entire folder recursively:**
```
@dbr-migration scan all Python files in the /Workspace/Users/myuser/project folder for breaking changes
```

**Scan with subdirectories:**
```
@dbr-migration scan this folder including all subdirectories (utils/, config/, src/) for DBR 17.3 compatibility
```

**Scan specific file types:**
```
@dbr-migration scan all .py and .sql files in this directory for breaking changes
```

### Detailed Scan Reports

**Request categorized findings:**
```
@dbr-migration scan this notebook and categorize findings into: auto-fix, manual review, and config changes
```

**Get severity-based report:**
```
@dbr-migration scan this project and show HIGH severity issues first
```

**Request line-by-line details:**
```
@dbr-migration scan this file and show me the exact line numbers and code snippets for each breaking change
```

---

## Fixing Prompts

### Automatic Fixes

**Fix all auto-fixable issues:**
```
@dbr-migration automatically fix all breaking changes that can be safely remediated
```

**Fix specific pattern:**
```
@dbr-migration fix all input_file_name() usages in this notebook
```

```
@dbr-migration replace all '!' syntax with 'NOT' in SQL statements
```

**Fix with backup:**
```
@dbr-migration fix all breaking changes and create a backup of the original file
```

### Targeted Fixes

**Fix only HIGH severity issues:**
```
@dbr-migration fix only the HIGH severity breaking changes, leave the rest for manual review
```

**Fix specific breaking change IDs:**
```
@dbr-migration fix BC-17.3-001 and BC-15.4-003 in this notebook
```

**Fix across multiple files:**
```
@dbr-migration fix all input_file_name() usages in this notebook and all files in the utils/ folder
```

### Manual Review Guidance

**Get fix recommendations without applying:**
```
@dbr-migration show me how to fix the Spark Connect issues but don't apply changes yet
```

**Request decision guidance:**
```
@dbr-migration explain which VARIANT UDF issues I need to fix based on my target DBR version (16.4)
```

---

## Validation Prompts

### Post-Fix Validation

**Verify fixes were applied:**
```
@dbr-migration validate that all breaking changes have been addressed
```

**Check for remaining issues:**
```
@dbr-migration scan this notebook again to confirm no breaking changes remain
```

**Validate specific fixes:**
```
@dbr-migration verify that all input_file_name() references have been replaced correctly
```

### Syntax and Quality Checks

**Check for syntax errors after fixes:**
```
@dbr-migration validate Python syntax in all files I just modified
```

**Validate imports:**
```
@dbr-migration check that all imports are still valid after the fixes
```

### Cross-File Validation

**Validate multi-file changes:**
```
@dbr-migration validate that changes in main_notebook.py are compatible with updates in utils/helpers.py
```

---

## Multi-File Project Prompts

### Project-Wide Operations

**Scan entire project:**
```
@dbr-migration scan this entire project folder (including utils/, config/, and all subdirectories) for breaking changes
```

**Fix with dependency awareness:**
```
@dbr-migration fix all breaking changes in this folder and ensure cross-file imports remain valid
```

**Generate project summary:**
```
@dbr-migration create a summary report of all breaking changes found across all files in this project
```

### Handling Dependencies

**Check import dependencies:**
```
@dbr-migration identify all files that import from utils/dbr_helpers.py and check if they're affected by changes
```

**Fix utility module and dependents:**
```
@dbr-migration fix breaking changes in utils/helpers.py and update all notebooks that import from it
```

---

## Specific Breaking Change Prompts

### BC-17.3-001: input_file_name()

```
@dbr-migration find all usages of input_file_name() and replace them with _metadata.file_name
```

```
@dbr-migration show me everywhere input_file_name() is used in this project
```

### BC-15.4-003: ! Syntax for NOT

```
@dbr-migration replace all '!' operators with 'NOT' keyword in SQL statements
```

### BC-SC-001 to BC-SC-004: Spark Connect Issues

```
@dbr-migration identify all Spark Connect compatibility issues in this notebook
```

```
@dbr-migration flag all temp view name reuse patterns that will cause issues in Spark Connect
```

```
@dbr-migration show me which UDFs reference external variables and suggest fixes
```

### Scala 2.13 Issues (BC-16.4-001a-e)

```
@dbr-migration fix all Scala 2.13 compatibility issues in this .scala file
```

```
@dbr-migration replace JavaConverters with CollectionConverters and update .to[] syntax
```

---

## Configuration Prompts

### Configuration Check

```
@dbr-migration check if I need any Spark configuration changes for DBR 17.3
```

```
@dbr-migration show me recommended configuration settings for timestamp handling
```

### Legacy Config Application

```
@dbr-migration generate Spark configuration code to maintain DBR 13.3 behavior
```

```
@dbr-migration create a cell with all recommended legacy configurations for this notebook
```

### Performance Configs

```
@dbr-migration check if Auto Loader needs configuration changes and show performance implications
```

---

## Troubleshooting Prompts

### When Fixes Don't Work

**Verify fix was applied correctly:**
```
@dbr-migration the input_file_name() fix didn't work, can you show me what changed and verify the syntax?
```

**Re-scan after manual changes:**
```
@dbr-migration I manually fixed some issues, can you re-scan and tell me what's left?
```

### Understanding Errors

**Explain error message:**
```
@dbr-migration I'm getting "AnalysisException: Undefined function: input_file_name" - what does this mean and how do I fix it?
```

**Debug failed fix:**
```
@dbr-migration the fix for BC-15.4-003 caused a syntax error, can you review and correct it?
```

### False Positives

**Report false positive:**
```
@dbr-migration you flagged line 42 as using input_file_name() but it's in a comment - can you re-scan?
```

**Verify specific pattern:**
```
@dbr-migration double-check if this code pattern is actually a breaking change or a false positive
```

---

## Advanced Prompts

### Custom Scans

**Scan for specific pattern:**
```
@dbr-migration search for all DataFrame operations that might be affected by Spark Connect lazy evaluation
```

**Find deprecated patterns:**
```
@dbr-migration find all deprecated Spark APIs in this notebook even if they're not breaking changes yet
```

### Reporting

**Generate detailed report:**
```
@dbr-migration create a detailed migration report with all findings, fixes applied, and remaining manual tasks
```

**Export findings to CSV format:**
```
@dbr-migration list all breaking changes in a table format with columns: File, Line, BC-ID, Severity, Status
```

### Batch Operations

**Process multiple notebooks:**
```
@dbr-migration scan all notebooks in the folder list: notebook1.py, notebook2.py, notebook3.py and summarize findings
```

---

## Best Practices for Prompts

### ✅ DO

1. **Be specific about target DBR version:**
   - ✅ "scan for DBR 17.3 compatibility"
   - ❌ "scan for issues"

2. **Mention file scope explicitly:**
   - ✅ "scan this notebook and all files in utils/"
   - ❌ "scan everything"

3. **Request categorized output:**
   - ✅ "categorize findings: auto-fix, manual, config"
   - ❌ "find problems"

4. **Use breaking change IDs when known:**
   - ✅ "fix BC-17.3-001"
   - ❌ "fix the file name thing"

5. **Ask for validation after fixes:**
   - ✅ "fix all issues then validate"
   - ❌ "just fix it"

### ❌ DON'T

1. **Don't use vague language:**
   - ❌ "check for stuff that might break"
   - ✅ "scan for breaking changes from DBR 13.3 to 17.3"

2. **Don't assume context:**
   - ❌ "fix it" (what is "it"?)
   - ✅ "fix the input_file_name() issues in this notebook"

3. **Don't skip validation:**
   - ❌ "fix and we're done"
   - ✅ "fix then validate no issues remain"

4. **Don't ignore multi-file dependencies:**
   - ❌ "just fix this one file"
   - ✅ "fix this file and check imported modules"

---

## Prompt Templates

### Template 1: Full Migration Workflow

```
@dbr-migration 
1. Scan this folder (including subdirectories) for all breaking changes from DBR 13.3 to 17.3
2. Categorize findings into: auto-fix, manual review, and configuration
3. Apply all automatic fixes
4. Generate a detailed report of:
   - What was fixed automatically
   - What needs manual review with specific guidance
   - What configuration changes to test
5. Validate that all auto-fixable issues are resolved
```

### Template 2: Quick Notebook Fix

```
@dbr-migration scan this notebook for DBR 17.3 breaking changes, fix all auto-fixable issues, and validate the fixes were applied correctly
```

### Template 3: Multi-File Project Scan

```
@dbr-migration 
Scan this project folder recursively:
- Include all .py files (main notebooks and utility modules)
- Check for breaking changes when upgrading to DBR 17.3
- Show a summary with file paths and breaking change counts
- Flag any cross-file dependencies that need coordinated fixes
```

### Template 4: Specific Issue Focus

```
@dbr-migration 
Focus on [input_file_name / Spark Connect / Scala 2.13] issues:
- Scan this [file/folder]
- Show all occurrences with line numbers
- Explain the fix for each occurrence
- Apply fixes if safe to do so
```

---

## Integration with Databricks Assistant

### Using @-mentions

Always prefix your prompt with the skill name:
```
@dbr-migration [your prompt here]
```

### In Databricks Notebooks

**Cell magic:**
```python
# In a notebook cell
@dbr-migration scan this notebook for breaking changes
```

**Multi-cell workflow:**
```python
# Cell 1: Scan
@dbr-migration scan for breaking changes

# Cell 2: Review findings, then fix
@dbr-migration fix all auto-fixable issues

# Cell 3: Validate
@dbr-migration validate all fixes
```

### In Databricks Workspace Files

**Right-click context menu:**
1. Right-click on notebook/file
2. Select "Ask Databricks Assistant"
3. Use prompts from this guide

---

## Prompt Response Times

Typical response times:

| Operation | Files | Expected Time |
|-----------|-------|---------------|
| Scan single notebook | 1 | 5-10 seconds |
| Scan folder (10 files) | 10 | 15-30 seconds |
| Fix breaking changes | 1-5 | 10-20 seconds |
| Validate fixes | 1-5 | 5-10 seconds |
| Full project scan | 20+ | 1-2 minutes |

---

## Troubleshooting Common Issues

### Issue: Agent doesn't find obvious breaking changes

**Try this prompt:**
```
@dbr-migration re-scan this file using the latest breaking change patterns, specifically looking for [pattern]
```

### Issue: Fix created syntax errors

**Try this prompt:**
```
@dbr-migration the fix you applied to line [X] caused a syntax error, please review and correct it
```

### Issue: Need to understand a specific change

**Try this prompt:**
```
@dbr-migration explain why [code pattern] is a breaking change and show me the correct way to write it for DBR 17.3
```

---

## Additional Resources

- [SKILL.md](../databricks-lts-migration/SKILL.md) - Complete skill documentation
- [BREAKING-CHANGES.md](../databricks-lts-migration/references/BREAKING-CHANGES.md) - All breaking changes reference
- [02-using-assistant.md](./02-using-assistant.md) - General assistant usage guide
- [BREAKING-CHANGES-EXPLAINED.md](./BREAKING-CHANGES-EXPLAINED.md) - Detailed breaking change explanations

---

## Quick Reference Card

**Scan:** `@dbr-migration scan [target] for DBR 17.3 breaking changes`

**Fix:** `@dbr-migration fix all [auto-fixable/specific BC-ID] issues`

**Validate:** `@dbr-migration validate all fixes were applied correctly`

**Report:** `@dbr-migration create detailed migration report`

**Help:** `@dbr-migration explain [breaking change ID or pattern]`

---

**Pro Tip:** Bookmark this page and keep it open when running migrations! 🔖
