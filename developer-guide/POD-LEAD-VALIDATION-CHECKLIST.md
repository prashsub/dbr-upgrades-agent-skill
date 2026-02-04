# Pod Lead Validation Checklist: DBR Migration Sign-Off Guide

> **Version:** 1.0  
> **Target Migration:** DBR 13.3 LTS → 17.3 LTS  
> **Last Updated:** February 2026

---

## Quick Start

📋 **For quick reference:** Use the **[One-Pager Checklist](POD-LEAD-VALIDATION-ONE-PAGER.md)** to track your validation progress.

📖 **This document** provides detailed instructions on how to complete each validation step.

---

## Purpose

This document provides Pod Leads with comprehensive guidance to validate that DBR migration was performed correctly for all workflows under their ownership. It covers:

- Code migration correctness
- Data quality validation (row counts, business metrics)
- Downstream report accuracy
- Performance regression testing
- UAT requirements for P1 workloads

---

## Quick Reference: Validation Requirements by Priority

| Priority | Code Review | Row Count Check | Business Metrics | Downstream Reports | At-Scale UAT | Performance Test |
|----------|-------------|-----------------|------------------|-------------------|--------------|------------------|
| **P1 (Critical)** | ✅ Required | ✅ Required | ✅ Required | ✅ Required | ✅ Required | ✅ Required |
| **P2 (Standard)** | ✅ Required | ✅ Required | ⚪ Optional | ⚪ Optional | ❌ Not Required | ⚪ Optional |

---

## Section 1: Pre-Validation Setup

### 1.1 Prerequisites Checklist

Before starting validation, ensure the following are in place:

- [ ] Access to Job Tracker spreadsheet: `<<JOB_TRACKER_LINK>>`
- [ ] Access to Sign-Off Tracker: `<<SIGNOFF_TRACKER_LINK>>`
- [ ] Access to Issue Tracker: `<<ISSUE_TRACKER_LINK>>`
- [ ] Access to Performance Tracker: `<<PERF_TRACKER_LINK>>`
- [ ] UAT environment configured with DBR 17.3 LTS clusters
- [ ] Baseline metrics from DBR 13.3 runs documented
- [ ] List of all workflows owned by your POD

### 1.2 Identify Your Workflows

1. Open the Job Tracker spreadsheet
2. Filter by your POD name
3. Note the priority classification for each workflow:
   - **P1 (Critical):** Production-critical, revenue-impacting, or regulatory workflows
   - **P2 (Standard):** Supporting workflows with lower business impact
4. Exclude Serverless jobs (already on compatible runtime)

---

## Section 2: Code Migration Validation

### 2.1 Migration Completion Verification (All Priorities)

Confirm that the developer has completed the code migration and the workflow runs successfully:

- [ ] **Migration completed** - Developer has applied all required code changes
- [ ] **Job executes successfully** - Workflow runs to completion on DBR 17.3 without errors
- [ ] **No critical warnings** - Execution logs show no blocking warnings or deprecations
- [ ] **Cluster configured correctly** - Job uses DBR 17.3 LTS cluster with compatible libraries
- [ ] **Peer review completed** - A second team member has reviewed the changes

### 2.2 Code Migration Sign-Off

| Workflow | Developer | Migration Complete | Runs Successfully | Peer Reviewed | Sign-Off |
|----------|-----------|-------------------|-------------------|---------------|----------|
| ________ | ________ | [ ] | [ ] | [ ] | [ ] |
| ________ | ________ | [ ] | [ ] | [ ] | [ ] |
| ________ | ________ | [ ] | [ ] | [ ] | [ ] |

---

## Section 3: Data Quality Validation

### 3.1 Row Count Validation (All Priorities)

**Required for:** P1 and P2 workflows

Confirm that output data volumes match between DBR versions:

| Workflow | DBR 13.3 Row Count | DBR 17.3 Row Count | Match? | Validated By |
|----------|-------------------|-------------------|--------|--------------|
| ________ | ________ | ________ | [ ] | ________ |
| ________ | ________ | ________ | [ ] | ________ |
| ________ | ________ | ________ | [ ] | ________ |

**Acceptance Criteria:**
- Row counts must match exactly (0% variance)
- Any variance must be investigated and documented before sign-off

> **Note:** See Appendix A for SQL templates to capture row counts.

### 3.2 Business Metrics Validation (P1 Only)

**Required for:** P1 workflows only

Confirm that key business metrics match between DBR versions:

| Workflow | Metric | DBR 13.3 Value | DBR 17.3 Value | Variance | Acceptable? |
|----------|--------|---------------|---------------|----------|-------------|
| ________ | ________ | ________ | ________ | ______% | [ ] |
| ________ | ________ | ________ | ________ | ______% | [ ] |
| ________ | ________ | ________ | ________ | ______% | [ ] |

**Acceptance Criteria:**
| Metric Type | Acceptable Variance |
|-------------|---------------------|
| Financial totals (Revenue, Cost, Profit) | Exact match required |
| Counts (Customers, Transactions) | Exact match required |
| Averages and percentages | ≤0.01% variance |

- [ ] Business stakeholder reviewed and approved metrics
- [ ] Evidence saved to tracker

### 3.3 Schema Validation (All Priorities)

Confirm output schema consistency:

- [ ] Output columns match between DBR 13.3 and 17.3
- [ ] Data types are consistent
- [ ] No unexpected columns added or removed

---

## Section 4: Downstream Report Validation (P1 Only)

### 4.1 Downstream Dependencies

List all downstream consumers of P1 workflow outputs and confirm they are unaffected:

| Workflow | Downstream Consumer | Type | Owner Notified | Validated | Sign-Off |
|----------|---------------------|------|----------------|-----------|----------|
| ________ | ________ | Dashboard | [ ] | [ ] | [ ] |
| ________ | ________ | Data Pipeline | [ ] | [ ] | [ ] |
| ________ | ________ | External API | [ ] | [ ] | [ ] |
| ________ | ________ | ML Model | [ ] | [ ] | [ ] |

### 4.2 Report Validation Criteria

For each downstream report/dashboard, confirm:

- [ ] Report loads without errors
- [ ] Visualizations display correctly
- [ ] Key metrics match expected values
- [ ] Report owner has reviewed and approved

---

## Section 5: At-Scale UAT Testing (P1 Only)

**Critical:** P1 workloads must complete at-scale UAT testing before production deployment approval.

### 5.1 UAT Requirements

| Requirement | Criteria |
|-------------|----------|
| **Data Volume** | ≥80% of production data volume |
| **Environment** | Production-equivalent cluster specifications |
| **Execution** | Complete end-to-end workflow without errors |
| **Duration** | Within acceptable range of production timing |

### 5.2 UAT Validation

| Workflow | Data Scale (%) | Completed Successfully | Duration Acceptable | UAT Passed |
|----------|---------------|------------------------|---------------------|------------|
| ________ | ______% | [ ] | [ ] | [ ] |
| ________ | ______% | [ ] | [ ] | [ ] |
| ________ | ______% | [ ] | [ ] | [ ] |

### 5.3 UAT Sign-Off (P1)

For each P1 workflow, obtain sign-off from:

| Workflow | Technical Lead | Business Stakeholder | QA (if applicable) |
|----------|---------------|---------------------|-------------------|
| ________ | ________ | ________ | ________ |
| ________ | ________ | ________ | ________ |

---

## Section 6: Performance Regression Testing

### 6.1 Performance Thresholds

Compare job duration between DBR 13.3 and DBR 17.3:

| Status | Threshold | Action Required | Sign-Off Authority |
|--------|-----------|-----------------|-------------------|
| **Acceptable** | ≤10% slower | Document, proceed | Pod Lead |
| **Warning** | 10-25% slower | Document justification, proceed with caution | Pod Lead + Tech Lead |
| **Concern** | 25-50% slower | Root cause analysis required | Tech Lead + Platform Team |
| **Blocking** | >50% slower | Must resolve before deployment | Platform Team required |

**Calculation:** `Change % = ((DBR 17.3 Duration - DBR 13.3 Duration) / DBR 13.3 Duration) × 100`

### 6.2 Performance Validation

| Workflow | DBR 13.3 Duration | DBR 17.3 Duration | Change % | Status | Approved |
|----------|-------------------|-------------------|----------|--------|----------|
| ________ | ________ min | ________ min | ______% | ________ | [ ] |
| ________ | ________ min | ________ min | ______% | ________ | [ ] |
| ________ | ________ min | ________ min | ______% | ________ | [ ] |

### 6.3 Regression Investigation (If Needed)

For workflows with WARNING or higher status, escalate to technical team for investigation:

| Common Cause | Escalate To |
|--------------|-------------|
| Query plan changes | Platform Team |
| Shuffle/partition issues | Platform Team |
| Auto Loader behavior | Platform Team |
| Photon/caching changes | Platform Team |

---

## Section 7: P2 Workload Simplified Validation

For P2 (Standard priority) workloads, validation is limited to:

| Workflow | Migration Complete | Runs Successfully | Row Counts Match | Sign-Off |
|----------|-------------------|-------------------|------------------|----------|
| ________ | [ ] | [ ] | [ ] | [ ] |
| ________ | [ ] | [ ] | [ ] | [ ] |
| ________ | [ ] | [ ] | [ ] | [ ] |

**Optional (recommended for long-running jobs):**
- [ ] Performance compared
- [ ] Business metrics spot-checked

---

## Section 8: Final Sign-Off Process

### 8.1 Sign-Off Authority Matrix

| Validation Type | P1 Sign-Off Authority | P2 Sign-Off Authority |
|-----------------|----------------------|----------------------|
| Code Migration | Pod Lead + Tech Lead | Pod Lead |
| Data Quality | Pod Lead + Business Stakeholder | Pod Lead |
| Performance | Pod Lead + Platform Team (if WARNING+) | Pod Lead |
| UAT Completion | Pod Lead + Tech Lead + Business | N/A |
| Production Deployment | Pod Lead + All above | Pod Lead |

### 8.2 Pre-Production Deployment Checklist

#### P1 Workloads - Complete All Items

- [ ] Code migration verified and signed off
- [ ] Row count validation passed
- [ ] Business metrics validation passed
- [ ] Schema validation passed
- [ ] Downstream reports validated
- [ ] At-scale UAT testing completed
- [ ] UAT sign-off obtained
- [ ] Performance regression within acceptable limits
- [ ] All issues logged to tracker resolved
- [ ] Business stakeholder approval obtained

#### P2 Workloads - Complete Required Items

- [ ] Code migration verified and signed off
- [ ] Row count validation passed
- [ ] Job executes successfully on DBR 17.3
- [ ] Any issues logged to tracker resolved

### 8.3 Final Sign-Off Form

```
═══════════════════════════════════════════════════════════════
           DBR MIGRATION VALIDATION SIGN-OFF FORM
═══════════════════════════════════════════════════════════════

POD Name: _________________________
Sign-Off Date: _________________________

WORKFLOW SUMMARY
────────────────
Total Workflows Owned: _______
P1 Workflows: _______
P2 Workflows: _______
Serverless (Excluded): _______

VALIDATION STATUS
─────────────────
P1 Workflows:
  ☐ All code migrations verified
  ☐ All row count validations passed
  ☐ All business metrics validated
  ☐ All downstream reports verified
  ☐ All at-scale UAT tests completed
  ☐ All performance tests within thresholds

P2 Workflows:
  ☐ All code migrations verified
  ☐ All row count validations passed

ISSUES
──────
Open Issues: _______
  (List critical open issues if any)
  _________________________________________________
  _________________________________________________

SIGN-OFF ATTESTATION
────────────────────
I confirm that all workflows owned by this POD have undergone
the required validation as per this checklist and are ready
for production deployment on DBR 17.3 LTS.

Pod Lead: _________________________
Signature: _________________________
Date: _________________________

Tech Lead (P1 only): _________________________
Signature: _________________________
Date: _________________________

Business Stakeholder (P1 only): _________________________
Signature: _________________________
Date: _________________________

═══════════════════════════════════════════════════════════════
```

### 8.4 Post-Sign-Off Communication

After completing sign-off, send confirmation to the BAU/Platform team:

**To:** BAU/DevOps Team `<<DL>>`  
**CC:** Platform Engineering, Tech Leads  
**Subject:** DBR Migration Sign-Off Complete - [POD Name]

```
Team,

This email confirms that [POD Name] has completed all validation 
activities for the DBR 13.3 → 17.3 LTS migration.

Summary:
─────────
Total Workflows Validated: XX
  - P1 Workflows: XX (all UAT tested)
  - P2 Workflows: XX

Validation Completed:
  ✅ Code migration verification
  ✅ Row count validation
  ✅ Business metrics validation (P1)
  ✅ Downstream report validation (P1)
  ✅ At-scale UAT testing (P1)
  ✅ Performance regression testing

Open Issues: XX (none blocking)

Sign-Off Tracker Updated: Yes
Attachments: Sign-off form, validation screenshots

Please proceed with production migration scheduling for our workflows.

Best regards,
[Pod Lead Name]
[Date]
```

---

## Appendix A: Validation SQL Templates

### A.1 Comprehensive Row Count Comparison

```sql
-- Template for detailed row count comparison
WITH baseline AS (
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT {primary_key}) as unique_keys,
        SUM(CASE WHEN {primary_key} IS NULL THEN 1 ELSE 0 END) as null_keys,
        COUNT(DISTINCT {partition_column}) as partitions
    FROM {schema}.{table}_baseline
),
target AS (
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT {primary_key}) as unique_keys,
        SUM(CASE WHEN {primary_key} IS NULL THEN 1 ELSE 0 END) as null_keys,
        COUNT(DISTINCT {partition_column}) as partitions
    FROM {schema}.{table}_target
)
SELECT 
    'Baseline (13.3)' as version,
    b.total_rows,
    b.unique_keys,
    b.null_keys,
    b.partitions
FROM baseline b
UNION ALL
SELECT 
    'Target (17.3)' as version,
    t.total_rows,
    t.unique_keys,
    t.null_keys,
    t.partitions
FROM target t;
```

### A.2 Data Hash Comparison

```sql
-- Compare data using hash for exact match verification
WITH baseline_hash AS (
    SELECT 
        {primary_key},
        hash(concat_ws('|', *)) as row_hash
    FROM {schema}.{table}_baseline
),
target_hash AS (
    SELECT 
        {primary_key},
        hash(concat_ws('|', *)) as row_hash
    FROM {schema}.{table}_target
)
SELECT 
    COALESCE(b.{primary_key}, t.{primary_key}) as key,
    CASE 
        WHEN b.row_hash IS NULL THEN 'Missing in baseline'
        WHEN t.row_hash IS NULL THEN 'Missing in target'
        WHEN b.row_hash != t.row_hash THEN 'Hash mismatch'
        ELSE 'Match'
    END as comparison_result
FROM baseline_hash b
FULL OUTER JOIN target_hash t ON b.{primary_key} = t.{primary_key}
WHERE b.row_hash IS NULL 
   OR t.row_hash IS NULL 
   OR b.row_hash != t.row_hash;
```

---

## Appendix B: Common Issues and Resolutions

| Issue | Symptom | Resolution |
|-------|---------|------------|
| Row count mismatch | Target has fewer/more rows | Check for filter changes, null handling, deduplication logic |
| Business metric variance | Aggregations differ slightly | Check for floating-point precision, rounding changes |
| Schema differences | New/missing columns | Verify schema evolution settings, check for dynamic columns |
| Performance regression | >25% slower | Review query plans, check shuffle settings, verify Photon config |
| Downstream report errors | Dashboard shows errors | Check for type changes, null handling, column renames |

---

## Appendix C: Contact Information

| Role | Contact | When to Reach |
|------|---------|---------------|
| Platform Team | `<<PLATFORM_DL>>` | Performance issues, cluster config |
| BAU/DevOps | `<<BAU_DL>>` | Deployment questions, scheduling |
| Technical Support | `<<SUPPORT_CHANNEL>>` | Breaking change questions |
| Escalation | `<<ESCALATION_CONTACT>>` | Blocking issues, deadline concerns |

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | February 2026 | Platform Team | Initial version |

---

*This document is maintained by the Platform Engineering team. For questions or suggestions, contact `<<PLATFORM_DL>>`.*
