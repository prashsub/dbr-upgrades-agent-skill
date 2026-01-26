# Output Validation Criteria: DBR 13.3 → 16.4

> **📢 Post this document on Teams Channel and add link here:** `<<TEAMS_LINK>>`

## Purpose

This document defines **which jobs require output comparison validation** between DBR 13.3 and 16.4. Following these criteria prevents unnecessary time, effort, and compute costs by focusing validation efforts on jobs that are actually impacted by breaking changes.

---

## Quick Decision Guide

```
┌─────────────────────────────────────────────────────────────┐
│                  Does my job need output validation?        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │ Is the job Serverless?        │
              └───────────────────────────────┘
                      │               │
                     YES              NO
                      │               │
                      ▼               ▼
              ┌──────────────┐  ┌──────────────────────────┐
              │ NO OUTPUT    │  │ Check criteria below     │
              │ VALIDATION   │  │ for output validation    │
              │ REQUIRED     │  │ requirement              │
              └──────────────┘  └──────────────────────────┘
```

---

## Jobs That REQUIRE Output Validation

A job **MUST** have output compared between 13.3 and 16.4 if it meets **ANY** of the following criteria:

### Criterion 1: Uses Breaking Change Patterns

| Pattern | Description | Risk |
|---------|-------------|------|
| `input_file_name()` | Function deprecated, may behave differently | HIGH |
| MERGE INTO / UPDATE with type casting | Overflow behavior changed (NULL → ERROR) | HIGH |
| Python UDF with complex types | Type handling may differ | MEDIUM |
| Parquet timestamp columns | TIMESTAMP vs TIMESTAMP_NTZ inference | MEDIUM |
| Delta schema evolution | Schema overwrite + dynamic partition blocked | MEDIUM |

**How to Check:**
```bash
# Run this against your job notebooks/scripts
grep -rn "input_file_name\|MERGE INTO\|VariantType" /path/to/job/code
```

### Criterion 2: Financial/Regulatory Data

Jobs that produce data used for:
- [ ] Financial reporting (P&L, Balance Sheet, etc.)
- [ ] Regulatory submissions (SOX, Basel, etc.)
- [ ] Audit trails
- [ ] Customer billing
- [ ] Risk calculations

### Criterion 3: Downstream Dependencies

Jobs where output is:
- [ ] Consumed by 3+ downstream jobs
- [ ] Fed to external systems (APIs, data shares)
- [ ] Used by dashboards/reports with SLAs
- [ ] Source for ML model training

### Criterion 4: Complex Transformations

Jobs that include:
- [ ] Complex window functions
- [ ] Multiple JOINs (5+ tables)
- [ ] Custom aggregations (UDAFs)
- [ ] Recursive CTEs or graph operations
- [ ] Schema inference from external sources

### Criterion 5: Priority Classification

Jobs classified as:
- [ ] **P0** (Critical) - ALWAYS validate output
- [ ] **P1** (High) - Validate if any other criterion met

---

## Jobs That DO NOT Require Output Validation

A job **DOES NOT** need output comparison if:

| Condition | Reason |
|-----------|--------|
| Serverless Job | Serverless already runs on newer runtime, no change |
| Simple SELECT queries only | No transformation logic to validate |
| Job only moves data (COPY INTO) | Byte-for-byte copy, no transformation |
| Job only runs DDL (CREATE TABLE) | Schema operations, no data output |
| Test/Dev jobs (non-production) | Not used for business decisions |
| Already validated in previous upgrade cycle | No changes since last validation |

---

## Validation Scope by Job Type

### ETL/Data Pipeline Jobs

| Validation | Required? |
|------------|-----------|
| Row count comparison | ✅ Yes |
| Schema comparison | ✅ Yes |
| Sample data hash comparison | ✅ Yes |
| Full data comparison | Only if P0 |

### Aggregation/Reporting Jobs

| Validation | Required? |
|------------|-----------|
| Row count comparison | ✅ Yes |
| Numeric precision check | ✅ Yes |
| Null count comparison | ✅ Yes |
| Business metric comparison | ✅ Yes |

### ML Feature Jobs

| Validation | Required? |
|------------|-----------|
| Feature value distribution | ✅ Yes |
| Null/missing value counts | ✅ Yes |
| Data type consistency | ✅ Yes |

---

## Jobs List for Output Validation

Based on the criteria above, the following jobs from `DBR Upgrade-Jobs list_16.4.xlsx` require output validation:

> **📋 Filter the Excel file:**
> 1. Filter `Serverless (Y/N)` = "N"
> 2. Filter `Priority` = "P0" or "P1"
> 3. Cross-reference with criteria above

### Summary by POD

| POD | Total Jobs | Require Output Validation | Run Only |
|-----|------------|---------------------------|----------|
| POD-A | XX | XX | XX |
| POD-B | XX | XX | XX |
| POD-C | XX | XX | XX |
| **Total** | **204** | **XX** | **XX** |

---

## How to Perform Output Validation

### Step 1: Capture 13.3 Output

```python
# Run job on 13.3 cluster and save output
df_13_3 = spark.table("production.my_table")
df_13_3.write.mode("overwrite").parquet("/tmp/validation/job_name_13_3")
```

### Step 2: Capture 16.4 Output

```python
# Run job on 16.4 cluster and save output
df_16_4 = spark.table("production.my_table")
df_16_4.write.mode("overwrite").parquet("/tmp/validation/job_name_16_4")
```

### Step 3: Compare

```python
# Use comparison utility
from validation_utils import validate_output

result = validate_output(
    source_df=spark.read.parquet("/tmp/validation/job_name_13_3"),
    target_df=spark.read.parquet("/tmp/validation/job_name_16_4"),
    key_cols=["id", "date"],
    name="My Job Output"
)

# Screenshot the result for sign-off
```

---

## Exception Process

If you believe your job meets the criteria but should be exempt from output validation:

1. Document the reason
2. Get POD Lead approval
3. Notify BAU team at `<<EMAIL_DL>>`
4. Add exception note to tracker

---

## Questions?

Contact BAU/DevOps team: `<<EMAIL_DL>>`

---

*Last Updated: <<DATE>>*
*Owner: <<BAU_TEAM>>*
