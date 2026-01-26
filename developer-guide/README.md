# Databricks Runtime Upgrade Guide for Developers

This guide provides step-by-step instructions for developers to upgrade Databricks Runtime (DBR) from 13.3 LTS to 17.3 LTS using the **Databricks Assistant Agent Skills** feature.

---

## 📋 Quick Links

### For POD Teams (Start Here)

| Document | Description |
|----------|-------------|
| **[07-testing-validation-signoff-guide.md](07-testing-validation-signoff-guide.md)** | ⭐ **Main Guide** - Step-by-step testing & sign-off instructions |
| **[BREAKING-CHANGES-EXPLAINED.md](BREAKING-CHANGES-EXPLAINED.md)** | 📖 **Learn the changes** - Every breaking change explained + **Developer Action Guides** |
| [06-output-validation-criteria.md](06-output-validation-criteria.md) | Criteria for which jobs need output comparison |
| [08-tracker-templates.md](08-tracker-templates.md) | Templates for Issue, Performance, and Sign-Off trackers |

> 💡 **New!** `BREAKING-CHANGES-EXPLAINED.md` now includes **step-by-step guides** for:
> - Manual Review items (what to check, decision matrices, fix patterns)
> - Configuration Settings (when to add, where to add, how to test)

### Technical Deep-Dives

| Document | Description |
|----------|-------------|
| [01-skill-setup.md](01-skill-setup.md) | Install the DBR migration skill in your workspace |
| [02-using-assistant.md](02-using-assistant.md) | Use Databricks Assistant to scan and fix breaking changes |
| **[09-effective-prompts-guide.md](09-effective-prompts-guide.md)** | 🎯 **Ready-to-use prompts** for efficiently running the agent skill |
| [03-quality-validation.md](03-quality-validation.md) | Validate code quality and correctness after migration |
| [04-performance-testing.md](04-performance-testing.md) | Test for performance regressions |
| [05-rollout-checklist.md](05-rollout-checklist.md) | Complete checklist for production rollout |

### 🔍 Account-Level Profiler (Run First!)

Before starting individual notebook fixes, run the workspace profiler to get a complete picture:

| Script | Description |
|--------|-------------|
| **[workspace-profiler.py](workspace-profiler.py)** | Databricks notebook that scans **all jobs and workspace notebooks** for breaking changes |

**Output includes:**
- Delta table with all findings
- CSV export for Excel analysis
- HTML report with severity breakdown
- Clickable links to notebooks and jobs
- Duplicate temp view detection

---

## 🚀 Quick Start for POD Teams

```
Step 1: Review your jobs in "DBR Upgrade-Jobs list_16.4.xlsx"
        └─ Filter: Serverless (Y/N) = "N" to get your job list

Step 2: For each job, run on DBR 16.4 in UAT
        └─ Document: Success/Failure, Duration

Step 3: If output validation required (see criteria doc)
        └─ Compare output: 13.3 vs 16.4
        └─ Screenshot successful validation

Step 4: Log any issues
        └─ Failures → Issue Tracker
        └─ Performance slowdown → Performance Tracker

Step 5: Complete testing → POD Lead signs off
        └─ Update Sign-Off Tracker
        └─ Send confirmation email to BAU
```

---

## 📊 Trackers

| Tracker | Purpose | Status |
|---------|---------|--------|
| Issue Tracker | Log job failures and errors | *(Create using [template](08-tracker-templates.md#issue-tracker))* |
| Performance Tracker | Log performance regressions | *(Create using [template](08-tracker-templates.md#performance-tracker))* |
| Sign-Off Tracker | Track POD sign-off status | *(Create using [template](08-tracker-templates.md#sign-off-tracker))* |

---

## 📅 Timeline

| Phase | Dates | Responsible |
|-------|-------|-------------|
| Testing & Validation | *TBD - Coordinate with BAU* | POD Teams |
| Sign-Off Collection | *TBD - After testing complete* | POD Leads |
| Production Migration (16.4) | *TBD - After all sign-offs* | BAU/DevOps |
| Migration to 17.3 LTS | *TBD - Post 16.4 stabilization* | BAU (no POD effort) |

> **Note:** Actual dates will be communicated by your BAU team. Contact them for the current migration schedule.

---

## 📌 Key Information

### What PODs Need to Do

1. **Test all non-serverless jobs** on DBR 16.4 in UAT
2. **Validate output** for jobs that meet the [criteria](06-output-validation-criteria.md)
3. **Log issues** in the trackers
4. **Sign off** when complete

### What BAU/DevOps Will Do

1. Migrate all signed-off jobs to production on 16.4
2. Migrate all 16.4 jobs to 17.3 LTS (**no POD effort required**)
3. Provide support during testing phase

---

## 📖 Migration Path

```
DBR 13.3 LTS → DBR 16.4 LTS → DBR 17.3 LTS
   Spark 3.4.1    Spark 3.5.2    Spark 4.0.0
                     ↑               ↑
              POD Testing      BAU Handles
                Required        (No POD effort)
```

**Critical Changes in 16.4:**
- Scala 2.13 migration (requires Scala code changes)
- Data source cache behavior change
- Collection API changes

**Critical Changes in 17.3 (BAU will handle):**
- `input_file_name()` removed → `_metadata.file_name`
- Auto Loader incremental listing default changed
- Spark 4.0 API changes

---

## 🆘 Support

| Type | Contact |
|------|---------|
| Testing Questions | *Your team's designated support channel* |
| Technical Issues | *BAU/Platform team distribution list* |
| Escalation | *Your POD lead or BAU manager* |

> **Note:** Update the contact information above with your organization's actual support channels.

---

## 📚 References

- [Extend Assistant with Agent Skills - Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/assistant/skills)
- [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)
- [Databricks Runtime Release Notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/)

---

*Last Updated: January 2026*  
*Owner: Platform/BAU Team*
