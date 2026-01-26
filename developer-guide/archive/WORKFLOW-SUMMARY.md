# DBR Migration Workflow

## Simple 5-Step Process

```
┌─────────────────────────────────────────────────────────────────────┐
│  1. PROFILER                                                        │
│     Run workspace profiler → Get list of jobs with potential issues │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  2. OPEN JOB                                                        │
│     Open each flagged job/notebook in Databricks                    │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  3. RUN AGENT                                                       │
│     Agent scans, applies auto-fixes, flags items for your review    │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  4. REVIEW & IMPLEMENT                                              │
│     Review agent's changes and suggestions → Implement as needed    │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  5. TEST & LOG ISSUES                                               │
│     Test on new DBR → Log any issues to Central Tracker             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Step 1: Get Jobs from Profiler

Run the workspace profiler to identify jobs with potential breaking changes.

**Output:** List of jobs/notebooks that need review

| Job Name | Potential Issues | Priority |
|----------|------------------|----------|
| daily_etl.py | input_file_name(), Auto Loader | High |
| customer_pipeline.py | Temp view reuse | Medium |
| report_gen.sql | ! syntax | Low |

---

## Step 2: Open Each Job

Open the flagged job/notebook in Databricks workspace.

---

## Step 3: Run the Agent

**Prompt:**
```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan and fix this notebook for DBR 17.3 compatibility
```

**What the agent does:**
- **Auto-fixes** code issues (e.g., `input_file_name()` → `_metadata.file_name`)
- **Flags manual review items** with explanations (e.g., temp view reuse patterns)
- **Flags config items** that may need testing (e.g., Auto Loader settings)

---

## Step 4: Review & Implement

Review everything the agent did and suggested:

### Auto-Fixes (Agent Applied)
- Review the code changes
- Verify syntax is correct
- Accept or adjust as needed

### Manual Review Items (Agent Flagged)
- Read the agent's explanation
- Decide if change is needed for your use case
- Implement the change if needed

### Config Items (Agent Flagged)
- Note the suggested configs
- Test on new DBR to see if config is needed
- Add config only if behavior differs

---

## Step 5: Test & Log Issues

### Test the Job
Run the job on the new DBR version and verify:
- Job completes successfully
- Output is correct
- Performance is acceptable

### If Issues Found → Log to Central Tracker

**Log ANY issues to the central tracker:**

| Issue Type | What to Log |
|------------|-------------|
| **Error** | Job fails, syntax error, runtime exception |
| **Data/Quality** | Output differs, wrong results, data mismatch |
| **Performance** | Job runs slower than expected |
| **Regression** | Behavior changed unexpectedly |

**Central Tracker Fields:**

| Field | Example |
|-------|---------|
| Job Name | daily_etl.py |
| Issue Type | Performance / Error / Data Quality / Regression |
| Description | Job 40% slower after migration |
| Error Message | (if applicable) |
| Severity | Critical / High / Medium / Low |
| Status | New / In Progress / Resolved |
| Notes | Needs investigation - possible Auto Loader config |

---

## Quick Reference

### Agent Prompt (Copy-Paste)

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, scan and fix this notebook for DBR 17.3 compatibility
```

### What to Review

| Agent Output | Your Action |
|--------------|-------------|
| ✅ Auto-fixed code | Review the changes |
| 🟡 Manual review flag | Read, decide, implement if needed |
| ⚙️ Config suggestion | Test first, add if needed |

### When to Log Issues

| Situation | Action |
|-----------|--------|
| Job runs successfully | ✅ Done - no logging needed |
| Job fails | ❌ Log to tracker |
| Output is wrong | ❌ Log to tracker |
| Performance degraded | ❌ Log to tracker |
| Unexpected behavior | ❌ Log to tracker |

---

## Central Issue Tracker

> **📍 Tracker Link:** `<<CENTRAL_TRACKER_LINK>>`

All issues go to ONE tracker for triage and investigation.

| Field | Description |
|-------|-------------|
| **Issue ID** | Auto-generated (ISS-001) |
| **Date** | When reported |
| **POD** | Your team |
| **Job Name** | Name of the job |
| **Issue Type** | Error / Data Quality / Performance / Regression |
| **Description** | What happened |
| **Severity** | Critical / High / Medium / Low |
| **Status** | New / Investigating / Resolved |
| **Assigned To** | Who is investigating |
| **Resolution** | How it was fixed |

---

## Summary

1. **Profiler** → Gives you the list of jobs to check
2. **Open job** → Go to each flagged job
3. **Run agent** → Agent fixes what it can, flags the rest
4. **Review & implement** → You review and make final decisions
5. **Test & log** → Test on new DBR, log any issues to central tracker

**That's it.** Simple workflow, one tracker for all issues.
