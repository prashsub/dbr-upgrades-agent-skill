# DBR 16.4 Upgrade Tracker Templates

This document contains templates for the three trackers needed for the DBR upgrade process.

---

## Tracker 1: Issue Tracker

> **📍 Create this tracker and add link here:** `<<ISSUE_TRACKER_LINK>>`

### Purpose
Track any job failures, errors, or issues discovered during DBR 16.4 testing.

### Template (Excel/SharePoint List)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| Issue ID | Auto-number | Unique identifier | ISS-001 |
| Date Reported | Date | When issue was logged | 2026-01-23 |
| POD | Dropdown | Team that owns the job | POD-A |
| Job Name | Text | Name of the affected job | daily_etl_pipeline |
| Job ID | Text | Databricks job ID | 123456 |
| Error Type | Dropdown | Category of error | Runtime Error / Syntax Error / Data Error / Timeout / Other |
| Error Message | Long Text | Full error message | AnalysisException: Column 'x' not found |
| Stack Trace Link | URL | Link to full stack trace | <<LINK>> |
| Severity | Dropdown | Impact level | Critical / High / Medium / Low |
| Status | Dropdown | Current state | New / In Progress / Resolved / Won't Fix |
| Assigned To | Person | Who is working on it | John Doe |
| Resolution | Long Text | How it was fixed | Updated import statement |
| Resolution Date | Date | When it was resolved | 2026-01-24 |
| Related to Breaking Change | Dropdown | Which breaking change | BC-15.4-001 / BC-16.4-001 / None |
| Notes | Long Text | Additional information | - |

### Sample Data

| Issue ID | POD | Job Name | Error Type | Severity | Status |
|----------|-----|----------|------------|----------|--------|
| ISS-001 | POD-A | daily_sales_agg | Runtime Error | High | Resolved |
| ISS-002 | POD-B | customer_etl | Syntax Error | Medium | In Progress |
| ISS-003 | POD-A | report_generator | Data Error | Critical | New |

### Views to Create

1. **All Open Issues** - Status != Resolved
2. **By POD** - Group by POD column
3. **By Severity** - Filter Critical and High
4. **My Issues** - Assigned To = Current User

---

## Tracker 2: Performance Issue Tracker

> **📍 Create this tracker and add link here:** `<<PERF_TRACKER_LINK>>`

### Purpose
Track jobs with performance degradation (slower execution) on DBR 16.4 compared to 13.3.

### Template (Excel/SharePoint List)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| Perf ID | Auto-number | Unique identifier | PERF-001 |
| Date Reported | Date | When logged | 2026-01-23 |
| POD | Dropdown | Team that owns job | POD-A |
| Job Name | Text | Name of the job | hourly_aggregation |
| Job ID | Text | Databricks job ID | 789012 |
| Duration 13.3 (min) | Number | Run time on 13.3 | 15 |
| Duration 16.4 (min) | Number | Run time on 16.4 | 22 |
| Difference (min) | Calculated | 16.4 - 13.3 | 7 |
| Difference (%) | Calculated | ((16.4-13.3)/13.3)*100 | 46.7% |
| Severity | Dropdown | Based on threshold | Critical / High / Medium / Low |
| Job Complexity | Dropdown | Type of workload | Simple / Moderate / Complex |
| Root Cause | Text | If identified | Join strategy changed |
| Mitigation Applied | Text | Config or code change | Added shuffle hint |
| Duration After Fix (min) | Number | If mitigated | 17 |
| Status | Dropdown | Current state | New / Investigating / Mitigated / Accepted / Escalated |
| Databricks Ticket | Text | If escalated | SR-12345 |
| Notes | Long Text | Additional info | - |

### Severity Classification (Auto-calculate)

```
IF Difference > 30 min OR Difference% > 50% THEN "Critical"
ELSE IF Difference > 15 min OR Difference% > 25% THEN "High"
ELSE IF Difference > 5 min OR Difference% > 10% THEN "Medium"
ELSE "Low"
```

### Sample Data

| Perf ID | POD | Job Name | Duration 13.3 | Duration 16.4 | Diff % | Severity | Status |
|---------|-----|----------|---------------|---------------|--------|----------|--------|
| PERF-001 | POD-A | daily_etl | 45 | 52 | 15.6% | Medium | Investigating |
| PERF-002 | POD-C | ml_features | 120 | 180 | 50.0% | Critical | Escalated |
| PERF-003 | POD-B | report_gen | 10 | 11 | 10.0% | Low | Accepted |

### Views to Create

1. **Critical Issues** - Severity = Critical
2. **Escalated to Databricks** - Status = Escalated
3. **By POD** - Group by POD
4. **Needs Attention** - Severity = Critical OR High, Status = New

---

## Tracker 3: Sign-Off Tracker

> **📍 Create this tracker and add link here:** `<<SIGNOFF_TRACKER_LINK>>`

### Purpose
Track testing completion and sign-off status by POD Lead.

### Template (Excel/SharePoint List)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| POD | Text | Team name | POD-A |
| POD Lead | Person | Lead responsible for sign-off | Jane Smith |
| Total Jobs | Number | Total jobs assigned to POD | 25 |
| Serverless Jobs | Number | Jobs that are serverless (excluded) | 5 |
| Jobs to Test | Calculated | Total - Serverless | 20 |
| Jobs Requiring Output Validation | Number | Per criteria doc | 8 |
| Jobs Tested | Number | Completed testing | 18 |
| Jobs Validated (Output) | Number | Output comparison done | 7 |
| Jobs Passed | Number | Successful tests | 18 |
| Jobs Failed | Number | With issues | 2 |
| Open Issues | Number | Unresolved issues | 1 |
| Performance Issues Logged | Number | Count of perf issues | 3 |
| Testing Complete | Yes/No | All jobs tested? | Yes |
| Validation Complete | Yes/No | All required validations done? | Yes |
| Sign-Off Status | Dropdown | Current state | Not Started / In Progress / Ready for Sign-Off / Signed Off |
| Sign-Off Date | Date | When signed off | 2026-01-25 |
| Sign-Off Email Sent | Yes/No | Confirmation email sent | Yes |
| Notes | Long Text | Any blockers or comments | 1 job pending vendor fix |

### Sample Data

| POD | POD Lead | Jobs to Test | Jobs Tested | Sign-Off Status | Sign-Off Date |
|-----|----------|--------------|-------------|-----------------|---------------|
| POD-A | Jane Smith | 20 | 20 | Signed Off | 2026-01-25 |
| POD-B | John Doe | 35 | 28 | In Progress | - |
| POD-C | Alice Wong | 15 | 15 | Ready for Sign-Off | - |
| POD-D | Bob Chen | 10 | 0 | Not Started | - |

### Progress Dashboard View

Create a summary view showing:

```
+------------------+--------------------+--------------------+
|    POD Status    |   Testing Progress |   Sign-Off Status  |
+------------------+--------------------+--------------------+
| POD-A            | ██████████ 100%    | ✅ Signed Off      |
| POD-B            | ████████░░  80%    | 🟡 In Progress     |
| POD-C            | ██████████ 100%    | 🟢 Ready           |
| POD-D            | ░░░░░░░░░░   0%    | ⚪ Not Started     |
+------------------+--------------------+--------------------+
| TOTAL            | 72/80 (90%)        | 1/4 Complete       |
+------------------+--------------------+--------------------+
```

---

## Jobs List Excel Cleanup Instructions

### For: `DBR Upgrade-Jobs list_16.4.xlsx`

#### Column Updates Required

1. **Serverless (Y/N)** - Ensure consistent values:
   - Use "Y" for Yes (Serverless)
   - Use "N" for No (Not Serverless)
   - Remove any blank cells - all jobs must have a value
   - Remove variations like "Yes", "NO", "n/a"

2. **Add New Column: "Output Validation Required"**
   - Values: "Y" or "N"
   - Based on [validation criteria document](06-output-validation-criteria.md)

3. **Add New Column: "Errors/Comments"**
   - For any issues discovered during review
   - Example: "Import error - needs fix", "Missing config"

4. **Add New Column: "Testing Status"**
   - Values: Not Started / In Progress / Completed / Failed
   - Track progress

#### Data Validation Rules

```
Column: Serverless (Y/N)
- Data validation: List = Y, N
- No blanks allowed

Column: Priority
- Data validation: List = P0, P1, P2, P3
- No blanks allowed

Column: POD
- Data validation: List = POD-A, POD-B, POD-C, ...
- No blanks allowed
```

#### Cleanup Checklist

- [ ] All Serverless (Y/N) values are "Y" or "N" only
- [ ] No blank cells in required columns
- [ ] POD names are consistent (no typos)
- [ ] Priority values are consistent (P0, P1, etc.)
- [ ] New columns added (Output Validation Required, Errors/Comments, Testing Status)
- [ ] Filter works correctly on all columns

---

## Quick Links Summary

| Tracker | Purpose | Link |
|---------|---------|------|
| Issue Tracker | Log job failures and errors | `<<ISSUE_TRACKER_LINK>>` |
| Performance Tracker | Log performance regressions | `<<PERF_TRACKER_LINK>>` |
| Sign-Off Tracker | Track POD testing and sign-off | `<<SIGNOFF_TRACKER_LINK>>` |
| Jobs List Excel | Master list of jobs | `<<EXCEL_LINK>>` |
| Validation Criteria Doc | Which jobs need output validation | `<<CRITERIA_DOC_LINK>>` |
| Testing Guide | Step-by-step instructions | `<<TESTING_GUIDE_LINK>>` |

---

*Last Updated: <<DATE>>*  
*Owner: <<BAU_TEAM>>*
