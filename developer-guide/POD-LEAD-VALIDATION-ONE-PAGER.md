# DBR Migration Validation Checklist - One-Pager

**POD:** _________________________ | **Pod Lead:** _________________________ | **Date:** _________________________

> **📖 Detailed Instructions:** See `POD-LEAD-VALIDATION-CHECKLIST.md` for complete guidance

---

## Quick Reference: Validation by Priority

| Item | P1 (Critical) | P2 (Standard) |
|------|--------------|---------------|
| Code Migration | ✅ Required | ✅ Required |
| Row Count Validation | ✅ Required | ✅ Required |
| Business Metrics | ✅ Required | ⚪ Optional |
| Downstream Reports | ✅ Required | ❌ Not Required |
| At-Scale UAT | ✅ Required | ❌ Not Required |
| Performance Testing | ✅ Required | ⚪ Optional (recommended for long-running jobs) |

**Performance Thresholds:** OK ≤10% | WARN 10-25% | CONCERN 25-50% | BLOCK >50% slower

---

## Workflow Summary

| Category | Count |
|----------|-------|
| Total Workflows Owned | _____ |
| P1 (Critical) Workflows | _____ |
| P2 (Standard) Workflows | _____ |
| Serverless (Excluded) | _____ |

---

## P1 Workflows Validation

| Workflow Name | Code ✓ | Row Count ✓ | Biz Metrics ✓ | Reports ✓ | UAT ✓ | Performance (13.3→17.3) | Perf Status | Sign-Off |
|---------------|--------|-------------|---------------|-----------|-------|------------------------|-------------|----------|
| _____________ | [ ] | [ ] | [ ] | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | [ ] | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | [ ] | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | [ ] | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | [ ] | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |

**Legend:** Code = Migration Complete | Row Count = Counts Match | Biz Metrics = Business Metrics Match | Reports = Downstream Reports OK | UAT = At-Scale UAT Passed  
**Perf Status:** OK = ≤10% | WARN = 10-25% | CONCERN = 25-50% | BLOCK = >50%

---

## P2 Workflows Validation

| Workflow Name | Code ✓ | Row Count ✓ | Performance (optional) | Perf Status | Sign-Off |
|---------------|--------|-------------|------------------------|-------------|----------|
| _____________ | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |
| _____________ | [ ] | [ ] | ___min → ___min (___%) | _______ | ________ |

**Note:** Performance testing is optional for P2, but recommended for long-running workflows

---

## Issues & Exceptions

| Issue # | Workflow | Description | Status | Resolution |
|---------|----------|-------------|--------|------------|
| _______ | ________ | ______________________ | ________ | ______________ |
| _______ | ________ | ______________________ | ________ | ______________ |
| _______ | ________ | ______________________ | ________ | ______________ |

---

## Final Sign-Off

### P1 Workflows (Complete All)

- [ ] All P1 workflows migrated and validated
- [ ] All row count validations passed (0% variance)
- [ ] All business metrics validated (≤0.01% variance)
- [ ] All downstream reports verified
- [ ] All at-scale UAT tests completed (≥80% production volume)
- [ ] All performance checks completed (no BLOCK status)
- [ ] Any WARN/CONCERN performance issues escalated and approved
- [ ] Business stakeholder approval obtained

### P2 Workflows (Complete All)

- [ ] All P2 workflows migrated and validated
- [ ] All row count validations passed (0% variance)
- [ ] All workflows run successfully on DBR 17.3
- [ ] Performance spot-checked for long-running workflows (if applicable)

### Issues

- [ ] All blocking issues resolved
- [ ] Non-blocking issues documented in tracker: `<<TRACKER_LINK>>`

---

## Approvals

| Role | Name | Signature | Date |
|------|------|-----------|------|
| **Pod Lead** | _____________ | _____________ | _______ |
| **Tech Lead** (P1) | _____________ | _____________ | _______ |
| **Business Stakeholder** (P1) | _____________ | _____________ | _______ |

---

## Post Sign-Off

- [ ] Sign-off tracker updated: `<<SIGNOFF_TRACKER_LINK>>`
- [ ] Evidence uploaded (screenshots, comparison results)
- [ ] Confirmation email sent to BAU/DevOps: `<<BAU_DL>>`

---

**Next Steps:** BAU/DevOps will schedule production deployment for validated workflows.

**Support:** Platform Team `<<PLATFORM_DL>>` | Technical Support `<<SUPPORT_CHANNEL>>`

---

*Version 1.0 | February 2026 | Detailed guide: `POD-LEAD-VALIDATION-CHECKLIST.md`*
