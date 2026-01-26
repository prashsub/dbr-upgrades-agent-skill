# Effective Prompts Guide for DBR Migration

Simple guide for using the DBR Migration Agent Skill.

---

## Your Workflow

```
1. PROFILER     → Get list of jobs with potential issues
2. OPEN JOB     → Open each flagged job in Databricks
3. RUN AGENT    → Agent scans, fixes, and flags items for review
4. REVIEW       → Review agent's work, implement suggestions
5. TEST & LOG   → Test on new DBR, log any issues to tracker
```

---

## Prerequisites

### Skill Installation

The skill must be installed at:
```
/Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/
```

**Verify installation:**
```bash
ls /Workspace/Users/$(whoami)/.assistant/skills/databricks-dbr-migration/SKILL.md
```

If not installed, copy from the repo:
```bash
cp -r databricks-dbr-migration /Workspace/Users/$(whoami)/.assistant/skills/
```

---

## Main Prompt (Use This)

**Copy-paste this prompt for each job:**

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan and fix this notebook for DBR 17.3 compatibility
```

**What the agent will do:**
1. Scan for all breaking changes
2. Apply automatic fixes where possible
3. Flag items that need your review
4. Add a summary cell to the notebook

---

## What the Agent Does

### Auto-Fixes (Agent Applies)

The agent automatically fixes these patterns:

| Pattern | Fix |
|---------|-----|
| `input_file_name()` | → `_metadata.file_name` |
| `IF !`, `IS !`, `! IN` | → `NOT` syntax |
| `JavaConverters` (Scala) | → `CollectionConverters` |
| `.to[List]` (Scala) | → `.to(List)` |

**Your job:** Review the changes, verify syntax is correct.

### Manual Review Flags (Agent Flags, You Decide)

The agent flags these for your review:

| Pattern | Why Flagged |
|---------|-------------|
| Temp view name reuse | May cause issues in Spark Connect |
| UDF with external variables | Variables captured at execution time |
| VIEW column type definitions | Not allowed in DBR 15.4+ |

**Your job:** Read the flag, decide if change is needed, implement if yes.

### Config Suggestions (Agent Flags, You Test)

The agent flags these for testing:

| Pattern | Suggested Config |
|---------|------------------|
| Auto Loader | `cloudFiles.useIncrementalListing` |
| Parquet timestamps | `spark.sql.parquet.inferTimestampNTZ.enabled` |
| JDBC reads | `spark.sql.legacy.jdbc.useNullCalendar` |

**Your job:** Test on new DBR first. Only add config if results differ.

---

## Review Checklist

After running the agent, review:

- [ ] **Auto-fixes:** Look correct? Syntax valid?
- [ ] **Manual review items:** Need changes? Implemented?
- [ ] **Config suggestions:** Tested? Config needed?

---

## Testing & Logging Issues

### Test the Job

Run the job on DBR 17.3 (or target version) and check:
- Job completes successfully
- Output is correct
- Performance is acceptable

### Log Issues to Central Tracker

**If any issues found, log to the central tracker:**

| Issue Type | Examples |
|------------|----------|
| **Error** | Job fails, runtime exception, syntax error |
| **Data Quality** | Output differs, wrong results |
| **Performance** | Job runs slower (>10% degradation) |
| **Regression** | Behavior changed unexpectedly |

**Tracker fields to fill:**
- Job Name
- Issue Type (Error / Data Quality / Performance / Regression)
- Description
- Error Message (if applicable)
- Severity (Critical / High / Medium / Low)

---

## Additional Prompts

### Scan Only (Don't Fix Yet)

```
Using the DBR migration skill, scan this notebook for DBR 17.3 breaking changes but don't apply fixes yet
```

### Fix Specific Pattern

```
Using the DBR migration skill, fix all input_file_name() usages in this notebook
```

### Validate After Changes

```
Using the DBR migration skill, validate that all breaking changes have been addressed
```

### Explain a Breaking Change

```
Using the DBR migration skill, explain BC-17.3-001 and show me how to fix it
```

### Scan Entire Folder

```
Using the DBR migration skill, scan all files in this folder for DBR 17.3 breaking changes
```

---

## Troubleshooting

### Agent Not Finding Skill

**Solution:** Include the full path in your prompt:
```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, ...
```

### Agent Missed Something

**Solution:** Re-run with specific focus:
```
Using the DBR migration skill, specifically check for [pattern] in this notebook
```

### Fix Caused Syntax Error

**Solution:** Ask agent to review:
```
Using the DBR migration skill, the fix on line X caused a syntax error, please review and correct
```

---

## Quick Reference

| Step | Action | Prompt |
|------|--------|--------|
| **Scan & Fix** | Run agent on job | `Using the DBR migration skill at [path], scan and fix this notebook for DBR 17.3 compatibility` |
| **Review** | Check agent's work | Look at auto-fixes, manual flags, config suggestions |
| **Test** | Run on new DBR | Execute job, verify output |
| **Log Issues** | Report problems | Add to central tracker if any issues found |

---

## Summary

1. Get job list from profiler
2. Open each job, run the agent
3. Review agent's auto-fixes and suggestions
4. Implement any needed changes
5. Test on new DBR
6. Log any issues to central tracker

**Keep it simple:** Agent does the scanning and fixing. You review and test. Problems go to the tracker.
