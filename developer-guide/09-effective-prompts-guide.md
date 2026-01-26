# Effective Prompts Guide for DBR Migration Agent Skill

This guide provides ready-to-use prompts for efficiently running the DBR LTS Migration Agent Skill in Databricks Assistant.

---

## ⚠️ Prerequisites: Skill Installation

**IMPORTANT:** Before using any prompts in this guide, ensure the skill is properly installed.

> 🔍 **Common Issue:** The agent may initially fail to find the skill if it's checking the wrong path. Databricks Workspace installations use `/Workspace/Users/` while local installations use `/Users/`. Make sure to verify the correct path for your environment below.

### Required Installation Location

The skill installation location depends on your environment:

#### Option 1: Databricks Workspace (Recommended for Databricks)
```
/Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/
```

**Example:**
```
/Workspace/Users/prashanth.subrahmanyam@databricks.com/.assistant/skills/databricks-dbr-migration/
```

#### Option 2: Local Filesystem (for local development)
```
/Users/{your-username}/.assistant/skills/databricks-dbr-migration/
```

### Verify Skill Installation

**Step 1: Check if the skill folder exists**

**For Databricks Workspace:**
```bash
ls -la /Workspace/Users/$(whoami)/.assistant/skills/databricks-dbr-migration/
```

**For Local Filesystem:**
```bash
ls -la ~/.assistant/skills/databricks-dbr-migration/
```

**Expected output:** You should see:
- `SKILL.md` (main skill file)
- `assets/` folder
- `references/` folder  
- `scripts/` folder

**Step 2: Verify the skill is recognized**

In Databricks Assistant, type:
```
What skills do you have access to?
```

**Expected response:** The assistant should list `databricks-dbr-migration` among available skills.

### If Skill Is Not Installed

#### For Databricks Workspace:

1. **Create the skills directory (if needed):**
   ```bash
   mkdir -p /Workspace/Users/$(whoami)/.assistant/skills/
   ```

2. **Copy the skill folder:**
   ```bash
   cp -r databricks-dbr-migration /Workspace/Users/$(whoami)/.assistant/skills/
   ```

3. **Verify installation:**
   ```bash
   ls -la /Workspace/Users/$(whoami)/.assistant/skills/databricks-dbr-migration/SKILL.md
   ```

#### For Local Filesystem:

1. **Copy the skill folder:**
   ```bash
   cp -r databricks-dbr-migration ~/.assistant/skills/
   ```

2. **Verify permissions:**
   ```bash
   chmod -R 755 ~/.assistant/skills/databricks-dbr-migration/
   ```

3. **Restart Databricks Assistant** (if necessary)

> 💡 **Tip:** See [01-skill-setup.md](./01-skill-setup.md) for detailed installation instructions.

---

## 📋 Table of Contents

1. [Prerequisites: Skill Installation](#️-prerequisites-skill-installation) ⬅️ **Start here!**
2. [Quick Start Prompts](#quick-start-prompts)
3. [Scanning Prompts](#scanning-prompts)
4. [Fixing Prompts](#fixing-prompts)
5. [Validation Prompts](#validation-prompts)
6. [Multi-File Project Prompts](#multi-file-project-prompts)
7. [Specific Breaking Change Prompts](#specific-breaking-change-prompts)
8. [Configuration Prompts](#configuration-prompts)
9. [Troubleshooting Prompts](#troubleshooting-prompts)

---

## Quick Start Prompts

> ⚠️ **Before proceeding:** Ensure you've completed the [Prerequisites](#️-prerequisites-skill-installation) section above!

> 💡 **Tip:** Replace `{your-email}` with your actual Databricks email (e.g., `john.doe@company.com`)

### 🔍 Initial Assessment

**Option A: If skill is installed in ~/.assistant/skills/**
```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan this notebook for breaking changes when upgrading from DBR 13.3 to DBR 17.3
```

**Option B: If using skill directly from this repo (recommended for testing)**
```
Using the DBR migration skill at ./databricks-dbr-migration/SKILL.md, scan this notebook for breaking changes when upgrading from DBR 13.3 to DBR 17.3
```

```
Using the DBR migration skill at ./databricks-dbr-migration/SKILL.md, analyze this folder for DBR 17.3 compatibility issues
```

### 🔧 Quick Fix

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, fix all breaking changes in this notebook
```

```
Using the skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, fix the input_file_name() issues you found
```

### ✅ Validation

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, validate that all breaking changes have been fixed
```

### 📝 Shorthand (After First Use)

Once the agent has loaded the skill, you can use shorter prompts:
```
@databricks-dbr-migration scan this notebook for DBR 17.3 breaking changes
```

```
@databricks-dbr-migration fix all auto-fixable issues
```

---

## Scanning Prompts

> 💡 **First-time use:** Include the full skill path. After the skill is loaded, you can use `@databricks-dbr-migration` shorthand.

### Single File Scans

**Scan current notebook (first time - include skill path):**
```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan this notebook for all breaking changes from DBR 13.3 to 17.3
```

**Scan current notebook (after skill is loaded):**
```
@databricks-dbr-migration scan this notebook for all breaking changes from DBR 13.3 to 17.3
```

**Scan with specific target version:**
```
@databricks-dbr-migration check this notebook for DBR 16.4 LTS compatibility issues
```

**Scan focusing on specific categories:**
```
@databricks-dbr-migration scan this notebook for Spark Connect compatibility issues
```

```
@databricks-dbr-migration check this Scala file for Scala 2.13 breaking changes
```

### Multi-File and Folder Scans

**Scan entire folder recursively:**
```
@databricks-dbr-migration scan all Python files in the /Workspace/Users/myuser/project folder for breaking changes
```

**Scan with subdirectories:**
```
@databricks-dbr-migration scan this folder including all subdirectories (utils/, config/, src/) for DBR 17.3 compatibility
```

**Scan specific file types:**
```
@databricks-dbr-migration scan all .py and .sql files in this directory for breaking changes
```

### Detailed Scan Reports

**Request categorized findings:**
```
@databricks-dbr-migration scan this notebook and categorize findings into: auto-fix, manual review, and config changes
```

**Get severity-based report:**
```
@databricks-dbr-migration scan this project and show HIGH severity issues first
```

**Request line-by-line details:**
```
@databricks-dbr-migration scan this file and show me the exact line numbers and code snippets for each breaking change
```

---

## Fixing Prompts

### Automatic Fixes

**Fix all auto-fixable issues:**
```
@databricks-dbr-migration automatically fix all breaking changes that can be safely remediated
```

**Fix specific pattern:**
```
@databricks-dbr-migration fix all input_file_name() usages in this notebook
```

```
@databricks-dbr-migration replace all '!' syntax with 'NOT' in SQL statements
```

**Fix with backup:**
```
@databricks-dbr-migration fix all breaking changes and create a backup of the original file
```

### Targeted Fixes

**Fix only HIGH severity issues:**
```
@databricks-dbr-migration fix only the HIGH severity breaking changes, leave the rest for manual review
```

**Fix specific breaking change IDs:**
```
@databricks-dbr-migration fix BC-17.3-001 and BC-15.4-003 in this notebook
```

**Fix across multiple files:**
```
@databricks-dbr-migration fix all input_file_name() usages in this notebook and all files in the utils/ folder
```

### Manual Review Guidance

**Get fix recommendations without applying:**
```
@databricks-dbr-migration show me how to fix the Spark Connect issues but don't apply changes yet
```

**Request decision guidance:**
```
@databricks-dbr-migration explain which VARIANT UDF issues I need to fix based on my target DBR version (16.4)
```

---

## Validation Prompts

### Post-Fix Validation

**Verify fixes were applied:**
```
@databricks-dbr-migration validate that all breaking changes have been addressed
```

**Check for remaining issues:**
```
@databricks-dbr-migration scan this notebook again to confirm no breaking changes remain
```

**Validate specific fixes:**
```
@databricks-dbr-migration verify that all input_file_name() references have been replaced correctly
```

### Syntax and Quality Checks

**Check for syntax errors after fixes:**
```
@databricks-dbr-migration validate Python syntax in all files I just modified
```

**Validate imports:**
```
@databricks-dbr-migration check that all imports are still valid after the fixes
```

### Cross-File Validation

**Validate multi-file changes:**
```
@databricks-dbr-migration validate that changes in main_notebook.py are compatible with updates in utils/helpers.py
```

---

## Multi-File Project Prompts

### Project-Wide Operations

**Scan entire project:**
```
@databricks-dbr-migration scan this entire project folder (including utils/, config/, and all subdirectories) for breaking changes
```

**Fix with dependency awareness:**
```
@databricks-dbr-migration fix all breaking changes in this folder and ensure cross-file imports remain valid
```

**Generate project summary:**
```
@databricks-dbr-migration create a summary report of all breaking changes found across all files in this project
```

### Handling Dependencies

**Check import dependencies:**
```
@databricks-dbr-migration identify all files that import from utils/dbr_helpers.py and check if they're affected by changes
```

**Fix utility module and dependents:**
```
@databricks-dbr-migration fix breaking changes in utils/helpers.py and update all notebooks that import from it
```

---

## Specific Breaking Change Prompts

### BC-17.3-001: input_file_name()

```
@databricks-dbr-migration find all usages of input_file_name() and replace them with _metadata.file_name
```

```
@databricks-dbr-migration show me everywhere input_file_name() is used in this project
```

### BC-15.4-003: ! Syntax for NOT

```
@databricks-dbr-migration replace all '!' operators with 'NOT' keyword in SQL statements
```

### BC-SC-001 to BC-SC-004: Spark Connect Issues

```
@databricks-dbr-migration identify all Spark Connect compatibility issues in this notebook
```

```
@databricks-dbr-migration flag all temp view name reuse patterns that will cause issues in Spark Connect
```

```
@databricks-dbr-migration show me which UDFs reference external variables and suggest fixes
```

### Scala 2.13 Issues (BC-16.4-001a-e)

```
@databricks-dbr-migration fix all Scala 2.13 compatibility issues in this .scala file
```

```
@databricks-dbr-migration replace JavaConverters with CollectionConverters and update .to[] syntax
```

---

## Configuration Prompts

### Configuration Check

```
@databricks-dbr-migration check if I need any Spark configuration changes for DBR 17.3
```

```
@databricks-dbr-migration show me recommended configuration settings for timestamp handling
```

### Legacy Config Application

```
@databricks-dbr-migration generate Spark configuration code to maintain DBR 13.3 behavior
```

```
@databricks-dbr-migration create a cell with all recommended legacy configurations for this notebook
```

### Performance Configs

```
@databricks-dbr-migration check if Auto Loader needs configuration changes and show performance implications
```

---

## Troubleshooting Prompts

### When Fixes Don't Work

**Verify fix was applied correctly:**
```
@databricks-dbr-migration the input_file_name() fix didn't work, can you show me what changed and verify the syntax?
```

**Re-scan after manual changes:**
```
@databricks-dbr-migration I manually fixed some issues, can you re-scan and tell me what's left?
```

### Understanding Errors

**Explain error message:**
```
@databricks-dbr-migration I'm getting "AnalysisException: Undefined function: input_file_name" - what does this mean and how do I fix it?
```

**Debug failed fix:**
```
@databricks-dbr-migration the fix for BC-15.4-003 caused a syntax error, can you review and correct it?
```

### False Positives

**Report false positive:**
```
@databricks-dbr-migration you flagged line 42 as using input_file_name() but it's in a comment - can you re-scan?
```

**Verify specific pattern:**
```
@databricks-dbr-migration double-check if this code pattern is actually a breaking change or a false positive
```

---

## Advanced Prompts

### Custom Scans

**Scan for specific pattern:**
```
@databricks-dbr-migration search for all DataFrame operations that might be affected by Spark Connect lazy evaluation
```

**Find deprecated patterns:**
```
@databricks-dbr-migration find all deprecated Spark APIs in this notebook even if they're not breaking changes yet
```

### Reporting

**Generate detailed report:**
```
@databricks-dbr-migration create a detailed migration report with all findings, fixes applied, and remaining manual tasks
```

**Export findings to CSV format:**
```
@databricks-dbr-migration list all breaking changes in a table format with columns: File, Line, BC-ID, Severity, Status
```

### Batch Operations

**Process multiple notebooks:**
```
@databricks-dbr-migration scan all notebooks in the folder list: notebook1.py, notebook2.py, notebook3.py and summarize findings
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

> 💡 **Copy-paste ready!** Replace `{your-email}` with your Databricks email address.

### Template 1: Full Migration Workflow (First Use)

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/:

1. Scan this folder (including subdirectories) for all breaking changes from DBR 13.3 to 17.3
2. Categorize findings into: auto-fix, manual review, and configuration
3. Apply all automatic fixes
4. Generate a detailed report of:
   - What was fixed automatically
   - What needs manual review with specific guidance
   - What configuration changes to test
5. Validate that all auto-fixable issues are resolved
```

### Template 2: Quick Notebook Fix (First Use)

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan this notebook for DBR 17.3 breaking changes, fix all auto-fixable issues, and validate the fixes were applied correctly
```

### Template 3: Multi-File Project Scan

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/:

Scan this project folder recursively:
- Include all .py files (main notebooks and utility modules)
- Check for breaking changes when upgrading to DBR 17.3
- Show a summary with file paths and breaking change counts
- Flag any cross-file dependencies that need coordinated fixes
```

### Template 4: Specific Issue Focus

```
Using the skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/:

Focus on [input_file_name / Spark Connect / Scala 2.13] issues:
- Scan this [file/folder]
- Show all occurrences with line numbers
- Explain the fix for each occurrence
- Apply fixes if safe to do so
```

### Template 5: Follow-up Prompts (After Skill is Loaded)

Once the skill is loaded in your session, you can use shorter prompts:
```
@databricks-dbr-migration fix all breaking changes in this notebook
```

```
@databricks-dbr-migration validate no breaking patterns remain
```

---

## Integration with Databricks Assistant

### Verify Skill is Loaded (First Time Setup)

**Before using any prompts, verify the skill is accessible:**

```
What agent skills are currently available?
```

**Expected:** You should see `databricks-dbr-migration` in the list.

**If not listed:**

**For Databricks Workspace:**
1. Check installation: `ls -la /Workspace/Users/$(whoami)/.assistant/skills/databricks-dbr-migration/`
2. Verify SKILL.md: `cat /Workspace/Users/$(whoami)/.assistant/skills/databricks-dbr-migration/SKILL.md | head -5`
3. Restart Databricks Assistant

**For Local Filesystem:**
1. Check installation: `ls ~/.assistant/skills/databricks-dbr-migration/`
2. Verify SKILL.md: `cat ~/.assistant/skills/databricks-dbr-migration/SKILL.md | head -5`
3. Restart Databricks Assistant

### Using the Skill

#### First Time Use (Important!)

On first use, **always include the full skill path** so the agent knows where to find it:

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan this notebook for breaking changes
```

#### After Skill is Loaded

Once the agent has loaded the skill in your session, you can use the @-mention shorthand:
```
@databricks-dbr-migration [your prompt here]
```

> ⚠️ **Important:** The skill name is `databricks-dbr-migration` (with hyphens), not `dbr-migration`

**Test the skill is loaded:**
```
@databricks-dbr-migration hello, are you working?
```

**Expected response:** The agent should acknowledge and describe its capabilities (SCAN, FIX, FLAG, VALIDATE).

### In Databricks Notebooks

**First prompt (include skill path):**
```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan this notebook for breaking changes when upgrading from DBR 13.3 to DBR 17.3
```

**Multi-cell workflow (after skill is loaded):**
```python
# Cell 1: Scan (first time - include skill path)
# "Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan for breaking changes"

# Cell 2: Review findings, then fix (skill already loaded)
# "@databricks-dbr-migration fix all auto-fixable issues"

# Cell 3: Validate (skill already loaded)
# "@databricks-dbr-migration validate all fixes"
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

### Issue: Skill not found or @-mention doesn't work

**Symptoms:**
- Agent responds "I don't have access to that skill"
- @databricks-dbr-migration doesn't autocomplete
- Agent says "I can't find databricks-dbr-migration"

**Solution 1: Verify Installation Path**

**For Databricks Workspace:**
```bash
# Check if skill folder exists
ls -la /Workspace/Users/$(whoami)/.assistant/skills/databricks-dbr-migration/

# Verify SKILL.md exists
cat /Workspace/Users/$(whoami)/.assistant/skills/databricks-dbr-migration/SKILL.md | head -5
```

**For Local Filesystem:**
```bash
# Check if skill folder exists
ls -la ~/.assistant/skills/databricks-dbr-migration/

# Verify SKILL.md exists
cat ~/.assistant/skills/databricks-dbr-migration/SKILL.md | head -5
```

**Solution 2: Reinstall the Skill**

**For Databricks Workspace:**
```bash
# From repository root
mkdir -p /Workspace/Users/$(whoami)/.assistant/skills/
cp -r databricks-dbr-migration /Workspace/Users/$(whoami)/.assistant/skills/
```

**For Local Filesystem:**
```bash
# From repository root
cp -r databricks-dbr-migration ~/.assistant/skills/
chmod -R 755 ~/.assistant/skills/databricks-dbr-migration/
```

**Solution 3: Check Correct Path**

The skill MUST be in one of these locations:

✅ **Databricks Workspace (Recommended):**
```
/Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/
```

✅ **Local Filesystem:**
```
/Users/{username}/.assistant/skills/databricks-dbr-migration/
```

❌ **Incorrect paths:**
- `~/.databricks/skills/`
- `~/skills/`
- `/opt/skills/`
- `/databricks/skills/`

**Solution 4: Ask Assistant to List Skills**
```
What skills are available? Please list all installed agent skills.
```

If `databricks-dbr-migration` is not listed, reinstall using Solution 2.

---

### Issue: Agent doesn't find obvious breaking changes

**Try this prompt:**
```
@databricks-dbr-migration re-scan this file using the latest breaking change patterns, specifically looking for [pattern]
```

### Issue: Fix created syntax errors

**Try this prompt:**
```
@databricks-dbr-migration the fix you applied to line [X] caused a syntax error, please review and correct it
```

### Issue: Need to understand a specific change

**Try this prompt:**
```
@databricks-dbr-migration explain why [code pattern] is a breaking change and show me the correct way to write it for DBR 17.3
```

---

## Additional Resources

- [SKILL.md](../databricks-dbr-migration/SKILL.md) - Complete skill documentation
- [BREAKING-CHANGES.md](../databricks-dbr-migration/references/BREAKING-CHANGES.md) - All breaking changes reference
- [02-using-assistant.md](./02-using-assistant.md) - General assistant usage guide
- [BREAKING-CHANGES-EXPLAINED.md](./BREAKING-CHANGES-EXPLAINED.md) - Detailed breaking change explanations

---

## Quick Reference Card

### First Use (Include Skill Path)

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, [your request]
```

### After Skill is Loaded (Shorthand)

| Action | Prompt |
|--------|--------|
| **Scan** | `@databricks-dbr-migration scan [target] for DBR 17.3 breaking changes` |
| **Fix** | `@databricks-dbr-migration fix all [auto-fixable/specific BC-ID] issues` |
| **Validate** | `@databricks-dbr-migration validate all fixes were applied correctly` |
| **Report** | `@databricks-dbr-migration create detailed migration report` |
| **Help** | `@databricks-dbr-migration explain [breaking change ID or pattern]` |

### Example First-Time Prompt (Copy-Paste Ready)

```
Using the DBR migration skill at /Workspace/Users/prashanth.subrahmanyam@databricks.com/.assistant/skills/databricks-dbr-migration/, scan this notebook for breaking changes when upgrading from DBR 13.3 to DBR 17.3, categorize them into auto-fix, manual review, and config changes, then fix all auto-fixable issues
```

---

**Pro Tip:** Bookmark this page and keep it open when running migrations! 🔖
