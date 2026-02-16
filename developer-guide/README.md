# DBR Migration Guide

Upgrade from DBR 13.3 LTS to 17.3 LTS using the Databricks Assistant Agent Skill.

---

## Workflow

```
1. PROFILER     → Get list of jobs with potential breaking changes
2. OPEN JOB     → Open each flagged job in Databricks
3. RUN AGENT    → Agent scans, fixes, and flags items for review
4. REVIEW       → Review agent's work, implement suggestions
5. TEST & LOG   → Test on new DBR, log any issues to tracker
```

---

## Step 1: Run Profiler

Run the [workspace-profiler.py](../workspace-profiler/workspace-profiler.py) notebook to get a list of jobs with potential breaking changes.

```python
# In Databricks, run the profiler notebook
# Configure migration path in CONFIG section:
#   "source_dbr_version": "13.3"  # Your CURRENT DBR version (patterns <= this are skipped)
#   "target_dbr_version": "17.3"  # Your TARGET DBR version
# Output: List of jobs/notebooks with potential issues
```

**Migration Path Filtering:** The profiler automatically skips patterns that don't apply to your migration. For example, if upgrading from 13.3 → 17.3, BC-13.3-* patterns are skipped because they're already working on your current version.

**Output:** Delta table + CSV with jobs that need review.

---

## Step 2: Install Skill (One-Time Setup)

Copy the migration skill to your workspace:

```bash
# Skill location
/Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/
```

**Verify installation:**
```
ls /Workspace/Users/$(whoami)/.assistant/skills/databricks-dbr-migration/SKILL.md
```

If not installed, copy from the repo:
```bash
mkdir -p /Workspace/Users/$(whoami)/.assistant/skills/
cp -r databricks-dbr-migration /Workspace/Users/$(whoami)/.assistant/skills/
```

---

## Step 3: Run Agent on Each Job

Open each flagged job and use this prompt:

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan and fix this notebook for DBR 17.3 compatibility
```

### What the Agent Does

| Action | Description |
|--------|-------------|
| **Auto-fixes** | Applies safe fixes (input_file_name, ! syntax, Scala changes) |
| **Flags manual review** | Items you need to decide on (temp view patterns, UDF patterns) |
| **Flags config items** | Settings to test (Auto Loader, timestamps) |

---

## Step 4: Review & Implement

Review everything the agent did:

### Auto-Fixes (Agent Applied)
- Check the code changes look correct
- Verify syntax is valid

### Manual Review Items (Agent Flagged)
- Read the explanation
- Decide if change is needed
- Implement if yes

### Config Suggestions (Agent Flagged)
- Note suggested configs
- Test on new DBR to see if needed
- Add config only if behavior differs

---

## Step 5: Test & Log Issues

### Test the Job
Run the job on the new DBR version:
- Job completes successfully?
- Output is correct?
- Performance is acceptable?

### Log Issues to Tracker

**If ANY issues found, log to the central tracker:**

| Issue Type | When to Log |
|------------|-------------|
| **Error** | Job fails, syntax error, runtime exception |
| **Data Quality** | Output differs, wrong results |
| **Performance** | Job runs >10% slower |
| **Regression** | Behavior changed unexpectedly |

---

## Central Issue Tracker

> **📍 Tracker Link:** `<<ADD_TRACKER_LINK_HERE>>`

### Tracker Fields

| Field | Example |
|-------|---------|
| Issue ID | ISS-001 |
| Date | 2026-01-26 |
| POD | Your team |
| Job Name | daily_etl_pipeline |
| Issue Type | Error / Data Quality / Performance / Regression |
| Description | Job fails with AnalysisException |
| Error Message | Column 'x' not found |
| Severity | Critical / High / Medium / Low |
| Status | New / Investigating / Resolved |
| Resolution | How it was fixed |

### Severity Guidelines

| Severity | Criteria |
|----------|----------|
| Critical | Production-blocking, data corruption |
| High | Major functionality broken, >50% slower |
| Medium | Functionality impacted, 10-50% slower |
| Low | Minor issue, workaround available |

---

## Sign-Off Process

### When All Jobs Pass

1. **Verify completion:**
   - All flagged jobs tested
   - All issues resolved or logged
   - Performance acceptable

2. **POD Lead signs off** in tracker

3. **Send confirmation email** to BAU team

### Pod Lead Validation

For comprehensive validation before production deployment, Pod Leads should use:

- **[Pod Lead Validation One-Pager](POD-LEAD-VALIDATION-ONE-PAGER.md)** - Quick checklist for tracking validation status
- **[Pod Lead Validation Checklist](POD-LEAD-VALIDATION-CHECKLIST.md)** - Detailed guide with instructions for each validation step

These documents cover:
- P1 vs P2 validation requirements
- Row count and business metrics validation
- At-scale UAT testing (P1 workloads)
- Performance regression testing
- Downstream report validation
- Final sign-off procedures

---

## Common Prompts

| Task | Prompt |
|------|--------|
| Scan & Fix | `Using the DBR migration skill at [path], scan and fix this notebook for DBR 17.3 compatibility` |
| Scan Only | `Using the DBR migration skill, scan this notebook for DBR 17.3 breaking changes` |
| Fix Specific | `Using the DBR migration skill, fix all input_file_name() usages` |
| Validate | `Using the DBR migration skill, validate that all breaking changes have been fixed` |
| Explain | `Using the DBR migration skill, explain BC-17.3-001` |

---

## What the Agent Fixes

### Auto-Fix Patterns (7)

| Pattern | Fix |
|---------|-----|
| `input_file_name()` | → `_metadata.file_name` |
| `IF !`, `IS !`, `! IN` | → `NOT` syntax |
| `JavaConverters` (Scala) | → `CollectionConverters` |
| `.to[List]` (Scala) | → `.to(List)` |
| `TraversableOnce` | → `IterableOnce` |
| `Traversable` | → `Iterable` |
| `Stream.from()` | → `LazyList.from()` |

### Manual Review Patterns (6)

| Pattern | Why Flagged |
|---------|-------------|
| `VariantType()` in UDF | Check target version (OK in 16.4+) |
| VIEW column types | Not allowed in 15.4+ |
| Temp view reuse | Spark Connect compatibility |
| UDF external variables | Variable capture timing |
| Schema access in loops | Performance in Spark Connect |
| Try/except around transforms | Error timing in Spark Connect |

### Config Suggestions (4)

| Pattern | Config to Test |
|---------|----------------|
| Auto Loader | `cloudFiles.useIncrementalListing` |
| Parquet timestamps | `spark.sql.parquet.inferTimestampNTZ.enabled` |
| JDBC reads | `spark.sql.legacy.jdbc.useNullCalendar` |
| materializeSource | Remove `none` option |

---

## Troubleshooting

### Agent Not Finding Skill
Include the full path in your prompt:
```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, ...
```

### Agent Missed Something
Re-run with specific focus:
```
Using the DBR migration skill, specifically check for [pattern] in this notebook
```

### Fix Caused Syntax Error
Ask agent to review:
```
The fix on line X caused a syntax error, please review and correct
```

---

## References

- **[Workspace Profiler README](../workspace-profiler/README.md)** - Detailed profiler configuration and usage guide
- **[workspace-profiler.py](../workspace-profiler/workspace-profiler.py)** - Profiler script
- **[BREAKING-CHANGES-EXPLAINED.md](BREAKING-CHANGES-EXPLAINED.md)** - Technical details on each breaking change
- **[SKILL.md](../databricks-dbr-migration/SKILL.md)** - Agent skill definition

### External Links
- [Databricks Runtime Release Notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/)
- [Spark Connect vs Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)
- [Agent Skills Documentation](https://learn.microsoft.com/en-us/azure/databricks/assistant/skills)

---

## Support

| Type | Contact |
|------|---------|
| Testing Questions | Your team's support channel |
| Technical Issues | BAU/Platform team |
| Escalation | POD lead or BAU manager |

---

*Last Updated: January 2026*
