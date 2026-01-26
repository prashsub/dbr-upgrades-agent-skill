# DBR 16.4 Upgrade: Testing, Validation & Sign-Off Guide

> **📢 Post this document on Teams Channel and add link here:** `<<TEAMS_LINK>>`

## Overview

This document provides **step-by-step instructions** for POD teams to test, validate, and sign-off on the DBR 16.4 upgrade for their jobs.

| Milestone | Target Date | Owner |
|-----------|-------------|-------|
| Testing Complete | <<DATE>> | POD Teams |
| Validation Complete | <<DATE>> | POD Teams |
| Sign-Off Deadline | <<DATE>> | POD Leads |
| Production Migration | <<DATE>> | BAU/DevOps |

---

## Prerequisites

Before starting, ensure you have:

- [ ] Access to UAT environment
- [ ] Copy of `DBR Upgrade-Jobs list_16.4.xlsx`
- [ ] Access to Issue Tracker: `<<ISSUE_TRACKER_LINK>>`
- [ ] Access to Performance Tracker: `<<PERF_TRACKER_LINK>>`
- [ ] Access to Sign-Off Tracker: `<<SIGNOFF_TRACKER_LINK>>`
- [ ] Reviewed the Risk Checklist: `<<RISK_CHECKLIST_LINK>>`

---

## Step-by-Step Instructions

### Step 1: Prepare Your Job List

**Action:** Identify which jobs belong to your POD and determine validation requirements.

1. Open `DBR Upgrade-Jobs list_16.4.xlsx`
2. Filter by your POD name
3. Note the `Serverless (Y/N)` column:
   - **"Y" (Serverless):** Skip - no action required
   - **"N" (Not Serverless):** Continue to Step 2
4. Review `Output Validation Required` column (see [criteria document](06-output-validation-criteria.md))

**Output:** List of jobs you need to test

---

### Step 2: Test Jobs in UAT Environment

**Action:** Execute all your non-serverless jobs on DBR 16.4 in UAT.

#### 2a. Verify Job Configuration

```
Before running, confirm:
✅ Job cluster is set to DBR 16.4 LTS
✅ All library dependencies are compatible
✅ Spark configurations are applied
```

#### 2b. Execute the Job

1. Navigate to your job in Workflows
2. Click **"Run Now"** or trigger via your normal process
3. Monitor the job until completion

#### 2c. Document the Result

| Field | What to Record |
|-------|----------------|
| Job Name | From Excel list |
| Run ID | From Databricks UI |
| Status | Success / Failed |
| Duration (16.4) | From job run |
| Duration (13.3) | From historical runs |
| Notes | Any observations |

---

### Step 3: Validate Output (If Required)

**Action:** For jobs that meet the [output validation criteria](06-output-validation-criteria.md), compare output between 13.3 and 16.4.

#### 3a. Check If Validation Required

```
Your job REQUIRES output validation if:
□ Listed in "Output Validation Required" column = "Y"
□ OR meets any criteria in the validation criteria document
```

#### 3b. If Output Validation IS Required

1. **Capture 13.3 Baseline** (if not already done)
   ```sql
   -- Save output from 13.3 run
   CREATE TABLE validation.{job_name}_13_3 AS
   SELECT * FROM {output_table}
   ```

2. **Capture 16.4 Output**
   ```sql
   -- Save output from 16.4 run  
   CREATE TABLE validation.{job_name}_16_4 AS
   SELECT * FROM {output_table}
   ```

3. **Run Comparison**
   ```sql
   -- Quick validation query
   SELECT 
     '13.3' as version, COUNT(*) as row_count FROM validation.{job_name}_13_3
   UNION ALL
   SELECT 
     '16.4' as version, COUNT(*) as row_count FROM validation.{job_name}_16_4
   ```

4. **Take Screenshot** of successful validation
5. **Upload Screenshot** to Sign-Off Tracker

#### 3c. If Output Validation IS NOT Required

1. Confirm job ran successfully (Step 2)
2. Note "Run-only validation" in tracker
3. Proceed to Step 4

---

### Step 4: Handle Job Failures

**Action:** If any job fails, follow this resolution process.

#### 4a. Log the Failure

1. Go to Issue Tracker: `<<ISSUE_TRACKER_LINK>>`
2. Create new entry with:
   - Job Name
   - Error Message
   - Stack Trace (screenshot)
   - POD Name
   - Your contact info

#### 4b. Review Risk Checklist

1. Open Risk Checklist: `<<RISK_CHECKLIST_LINK>>`
2. Check if your error matches any known issues
3. Apply recommended fix if available

#### 4c. Get Assistance (If Needed)

If you cannot resolve the issue:
- **Slack:** `<<SUPPORT_CHANNEL>>`
- **Email:** `<<SUPPORT_DL>>`
- **Escalation:** `<<ESCALATION_CONTACT>>`

#### 4d. Re-run After Fix

1. Apply the fix to your job
2. Re-run the job (Step 2)
3. Validate output if required (Step 3)
4. Update Issue Tracker with resolution

---

### Step 5: Compare Performance

**Action:** For ALL jobs, compare run duration between 13.3 and 16.4.

#### 5a. Calculate Duration Difference

```
Duration Difference = Duration_16.4 - Duration_13.3
Percentage Change = ((Duration_16.4 - Duration_13.3) / Duration_13.3) * 100
```

#### 5b. Report Significant Variance

**Threshold:** Report if duration increased by more than **<<XX>> minutes** OR **<<XX>>%**

If threshold exceeded:
1. Go to Performance Tracker: `<<PERF_TRACKER_LINK>>`
2. Log the following:
   - Job Name
   - Duration on 13.3
   - Duration on 16.4
   - Difference (minutes and %)
   - Job complexity notes

#### 5c. Performance Issue Classification

| Severity | Criteria | Action |
|----------|----------|--------|
| Critical | >50% slower OR >30 min increase | Immediate escalation |
| High | 25-50% slower OR 15-30 min increase | Report, await analysis |
| Medium | 10-25% slower OR 5-15 min increase | Report, monitor |
| Low | <10% slower OR <5 min increase | Note only |

---

### Step 6: Complete Sign-Off

**Action:** After ALL jobs pass testing and validation, provide formal sign-off.

#### 6a. Verify Completion

Confirm for your POD:
- [ ] All non-serverless jobs tested
- [ ] All required output validations completed
- [ ] All failures resolved and re-tested
- [ ] Performance variances logged
- [ ] Screenshots uploaded to tracker

#### 6b. POD Lead Sign-Off

1. POD Lead reviews all job results
2. POD Lead signs off in Sign-Off Tracker: `<<SIGNOFF_TRACKER_LINK>>`

#### 6c. Send Confirmation Email

**To:** BAU/DevOps `<<DL>>`  
**CC:** `<<CC_LIST>>`  
**Subject:** DBR 16.4 Upgrade Sign-Off - [POD Name]

```
Hi Team,

This email confirms that [POD Name] has completed testing and validation 
for the DBR 16.4 upgrade.

Summary:
- Total Jobs Tested: XX
- Jobs with Output Validation: XX  
- Performance Issues Logged: XX
- Open Issues: XX (if any, list them)

Sign-Off Tracker Updated: Yes
POD Lead: [Name]
Date: [Date]

Please proceed with production migration for our jobs.

Thanks,
[Your Name]
```

---

## Summary Checklist

### For Each Job

- [ ] Job runs successfully on 16.4
- [ ] Output validated (if required)
- [ ] Performance compared and logged
- [ ] Issues resolved (if any)

### For POD Sign-Off

- [ ] All jobs completed
- [ ] Sign-Off Tracker updated
- [ ] Confirmation email sent

---

## Important Links

| Resource | Link |
|----------|------|
| Jobs List Excel | `<<EXCEL_LINK>>` |
| Output Validation Criteria | `<<CRITERIA_DOC_LINK>>` |
| Risk Checklist | `<<RISK_CHECKLIST_LINK>>` |
| Issue Tracker | `<<ISSUE_TRACKER_LINK>>` |
| Performance Tracker | `<<PERF_TRACKER_LINK>>` |
| Sign-Off Tracker | `<<SIGNOFF_TRACKER_LINK>>` |
| Support Channel | `<<SUPPORT_CHANNEL>>` |

---

## Timeline

| Phase | Dates | Responsible |
|-------|-------|-------------|
| Testing & Validation | <<START>> - <<END>> | POD Teams |
| Sign-Off Collection | <<START>> - <<END>> | POD Leads |
| Production Migration | <<DATE>> | BAU/DevOps |

---

## FAQ

**Q: What if I can't complete testing by the deadline?**
A: Contact BAU team immediately at `<<DL>>` to discuss extension.

**Q: What if a job has intermittent failures?**
A: Run the job 3 times. If 2+ runs succeed, log the failure and proceed with sign-off noting the intermittent behavior.

**Q: Do I need to test serverless jobs?**
A: No. Serverless jobs already run on newer runtime and are excluded from this upgrade cycle.

**Q: What happens after 16.4 sign-off?**
A: All jobs on 16.4 LTS will be migrated to 17.3 LTS by BAU team **with no additional effort from PODs**.

---

## Support Contacts

| Role | Contact |
|------|---------|
| BAU/DevOps Lead | `<<NAME>> - <<EMAIL>>` |
| Technical Support | `<<SUPPORT_DL>>` |
| Escalation | `<<ESCALATION_CONTACT>>` |

---

*Last Updated: <<DATE>>*  
*Owner: <<BAU_TEAM>>*
