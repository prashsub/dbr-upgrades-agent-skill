# Central Issue Tracker Template

One tracker for all DBR migration issues.

---

## Central Issue Tracker

> **📍 Create tracker and add link here:** `<<CENTRAL_TRACKER_LINK>>`

### Purpose

Log ALL issues discovered during DBR migration testing:
- Errors and failures
- Data quality issues
- Performance regressions
- Unexpected behavior

### Template (Excel/SharePoint)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| Issue ID | Auto-number | Unique identifier | ISS-001 |
| Date Reported | Date | When logged | 2026-01-23 |
| POD | Dropdown | Team that owns job | POD-A |
| Job Name | Text | Name of job/notebook | daily_etl_pipeline |
| Job ID | Text | Databricks job ID | 123456 |
| **Issue Type** | Dropdown | Category | Error / Data Quality / Performance / Regression |
| Description | Long Text | What happened | Job fails with AnalysisException |
| Error Message | Long Text | Full error (if applicable) | AnalysisException: Column 'x' not found |
| Severity | Dropdown | Impact level | Critical / High / Medium / Low |
| Status | Dropdown | Current state | New / Investigating / Resolved / Won't Fix |
| Assigned To | Person | Who is investigating | John Doe |
| Root Cause | Text | What caused it | Breaking change BC-17.3-001 |
| Resolution | Long Text | How it was fixed | Replaced input_file_name() with _metadata |
| Resolution Date | Date | When resolved | 2026-01-24 |
| Notes | Long Text | Additional info | - |

---

### Issue Types

| Type | Description | Examples |
|------|-------------|----------|
| **Error** | Job fails to complete | Runtime exception, syntax error, import failure |
| **Data Quality** | Output is incorrect | Wrong values, missing data, schema mismatch |
| **Performance** | Job runs slower | >10% slower execution time |
| **Regression** | Behavior changed | Different results than before migration |

---

### Severity Guidelines

| Severity | Criteria |
|----------|----------|
| **Critical** | Production-blocking, data corruption risk |
| **High** | Major functionality broken, significant performance impact (>50% slower) |
| **Medium** | Functionality impacted, moderate performance impact (10-50% slower) |
| **Low** | Minor issue, workaround available |

---

### Sample Data

| Issue ID | POD | Job Name | Issue Type | Severity | Status |
|----------|-----|----------|------------|----------|--------|
| ISS-001 | POD-A | daily_sales_agg | Error | High | Resolved |
| ISS-002 | POD-B | customer_etl | Performance | Medium | Investigating |
| ISS-003 | POD-A | report_generator | Data Quality | Critical | New |
| ISS-004 | POD-C | ml_features | Regression | High | Investigating |

---

### Views to Create

1. **All Open Issues** - Status != Resolved
2. **By POD** - Group by POD
3. **By Severity** - Filter Critical and High
4. **By Issue Type** - Group by Type
5. **My Issues** - Assigned To = Current User

---

## Sign-Off Tracker (Optional)

> **📍 Tracker Link:** `<<SIGNOFF_TRACKER_LINK>>`

Track testing completion by POD.

| Column | Description |
|--------|-------------|
| POD | Team name |
| POD Lead | Lead responsible for sign-off |
| Total Jobs | Jobs assigned to POD |
| Jobs Tested | Completed testing |
| Open Issues | Unresolved issues |
| Sign-Off Status | Not Started / In Progress / Signed Off |
| Sign-Off Date | When signed off |

---

## Quick Links

| Item | Link |
|------|------|
| Central Issue Tracker | `<<CENTRAL_TRACKER_LINK>>` |
| Sign-Off Tracker | `<<SIGNOFF_TRACKER_LINK>>` |
| Jobs List | `<<JOBS_LIST_LINK>>` |
| Workflow Guide | [WORKFLOW-SUMMARY.md](WORKFLOW-SUMMARY.md) |
| Prompts Guide | [09-effective-prompts-guide.md](09-effective-prompts-guide.md) |

---

*Last Updated: January 2026*
