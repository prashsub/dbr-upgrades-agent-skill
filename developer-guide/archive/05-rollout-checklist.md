# Step 5: Production Rollout Checklist

This comprehensive checklist guides you through the production rollout of your DBR upgrade.

## Prerequisites

- [ ] Quality validation passed (see [03-quality-validation.md](03-quality-validation.md))
- [ ] Performance testing completed (see [04-performance-testing.md](04-performance-testing.md))
- [ ] Stakeholder approval obtained
- [ ] Rollback plan documented

---

## Phase 1: Pre-Rollout Preparation

### 1.1 Documentation Review

- [ ] **Breaking changes documented**
  - All breaking changes identified and fixed
  - Migration notes for team members
  - Known issues documented

- [ ] **Runbooks updated**
  - Troubleshooting guides reflect new DBR
  - Monitoring alerts updated
  - On-call documentation current

- [ ] **Change management**
  - Change request submitted (if required)
  - Change window scheduled
  - Stakeholders notified

### 1.2 Infrastructure Preparation

- [ ] **Cluster configurations**
  - New cluster policies for DBR 17.3 created
  - Instance pools updated (if used)
  - Spot instance configurations verified

- [ ] **Library compatibility**
  - All cluster libraries tested on target DBR
  - PyPI packages verified
  - Maven coordinates updated for Scala 2.13 (if applicable)

- [ ] **Unity Catalog / Metastore**
  - Catalog permissions verified
  - External locations accessible
  - Service credentials valid

### 1.3 Rollback Preparation

- [ ] **Table snapshots created**
  ```sql
  -- Create snapshots of critical tables
  CREATE TABLE backup.critical_table_snapshot 
  AS SELECT * FROM production.critical_table;
  
  -- Or use Delta time travel
  -- Tables can be restored to timestamp before migration
  ```

- [ ] **Cluster templates saved**
  - Current cluster configurations exported
  - Job configurations backed up
  - Workflow definitions exported

- [ ] **Rollback procedure tested**
  - Verified ability to switch back to 13.3 LTS
  - Tested data restoration procedure
  - Documented rollback steps

---

## Phase 2: Staged Rollout

### Stage 1: Development Environment

**Timeline:** Day 1-2

- [ ] Upgrade dev/sandbox clusters to 17.3 LTS
- [ ] Run automated test suite
- [ ] Developers validate individual workloads
- [ ] Address any unexpected issues

**Go/No-Go Criteria:**
- All automated tests pass
- No critical bugs discovered
- Dev team sign-off obtained

### Stage 2: Test/QA Environment

**Timeline:** Day 3-5

- [ ] Upgrade test clusters to 17.3 LTS
- [ ] Run integration tests
- [ ] Execute end-to-end test scenarios
- [ ] QA team validation

**Go/No-Go Criteria:**
- Integration tests pass
- E2E tests pass
- QA sign-off obtained

### Stage 3: Pre-Production / Staging

**Timeline:** Day 6-7

- [ ] Upgrade staging clusters to 17.3 LTS
- [ ] Run production-like workloads
- [ ] Performance comparison with production baseline
- [ ] Load testing (if applicable)

**Go/No-Go Criteria:**
- Performance within acceptable range
- No data quality issues
- Staging sign-off obtained

### Stage 4: Production (Canary)

**Timeline:** Day 8-10

- [ ] Upgrade single production workflow to 17.3 LTS
- [ ] Monitor for 24-48 hours
- [ ] Verify data quality
- [ ] Monitor error rates and latency

**Go/No-Go Criteria:**
- No increase in error rates
- Latency within SLA
- Data quality validated

### Stage 5: Production (Full)

**Timeline:** Day 11+

- [ ] Upgrade remaining production clusters
- [ ] Update job cluster configurations
- [ ] Update interactive cluster policies
- [ ] Notify all users of upgrade

---

## Phase 3: Job and Workflow Updates

### 3.1 Batch Jobs

For each batch job:

- [ ] **Update cluster configuration**
  ```json
  {
    "spark_version": "17.3.x-scala2.13",
    "node_type_id": "...",
    "num_workers": ...
  }
  ```

- [ ] **Test job execution**
  - Trigger manual run
  - Verify output
  - Check logs for warnings

- [ ] **Update schedule** (if timing changed)

### 3.2 Streaming Jobs

For each streaming job:

- [ ] **Plan for checkpoint migration**
  ```python
  # If checkpoint format changed, may need to restart from scratch
  # Document the checkpoint location
  checkpoint_location = "/path/to/checkpoint"
  ```

- [ ] **Update Auto Loader configurations**
  ```python
  # Explicitly set incremental listing if needed
  .option("cloudFiles.useIncrementalListing", "auto")
  ```

- [ ] **Test streaming pipeline**
  - Start with clean checkpoint (test env)
  - Verify data processing
  - Monitor lag metrics

### 3.3 Interactive Clusters

- [ ] **Update cluster policies**
  - Set default DBR to 17.3 LTS
  - Update allowed DBR versions
  - Communicate to users

- [ ] **Update shared clusters**
  - Upgrade during low-usage window
  - Notify active users
  - Monitor usage after upgrade

---

## Phase 4: Post-Rollout Validation

### 4.1 Immediate Validation (Day 0)

- [ ] All jobs started successfully
- [ ] No increase in job failures
- [ ] Data pipelines producing output
- [ ] Interactive queries working

### 4.2 Short-Term Validation (Day 1-3)

- [ ] **Data Quality Checks**
  ```sql
  -- Row count comparison
  SELECT 
    'Today' as period,
    COUNT(*) as row_count
  FROM production.daily_table
  WHERE date = current_date()
  
  UNION ALL
  
  SELECT 
    'Yesterday' as period,
    COUNT(*) as row_count  
  FROM production.daily_table
  WHERE date = current_date() - 1
  ```

- [ ] **Performance Monitoring**
  - Query latency within expected range
  - Job duration within expected range
  - Resource utilization normal

- [ ] **Error Rate Monitoring**
  - No increase in job failures
  - No new error types in logs
  - User-reported issues addressed

### 4.3 Long-Term Validation (Week 1-2)

- [ ] **Trend Analysis**
  - Week-over-week job performance
  - Data quality metrics stable
  - Cost metrics (if tracking)

- [ ] **User Feedback**
  - Collect user feedback
  - Address any reported issues
  - Document lessons learned

---

## Phase 5: Cleanup and Documentation

### 5.1 Cleanup Tasks

- [ ] **Remove old cluster configurations**
  - Delete 13.3 LTS cluster templates
  - Update policies to disallow old DBR
  - Remove deprecated configurations

- [ ] **Clean up migration artifacts**
  - Remove backup tables (after retention period)
  - Archive migration logs
  - Clean up test data

- [ ] **Update dependencies**
  - Remove compatibility shims if added
  - Update to recommended library versions
  - Clean up deprecated code

### 5.2 Documentation Updates

- [ ] **Update team documentation**
  - System architecture docs
  - Onboarding guides
  - Troubleshooting runbooks

- [ ] **Record lessons learned**
  - What went well
  - What could be improved
  - Recommendations for future upgrades

- [ ] **Archive migration records**
  - Keep test results
  - Save comparison reports
  - Document configuration changes

---

## Rollback Procedure

### When to Rollback

Trigger rollback if:
- Critical job failures that can't be quickly resolved
- Data corruption detected
- Unacceptable performance degradation
- Security vulnerability discovered

### Rollback Steps

1. **Immediate Actions**
   ```
   - Stop affected jobs
   - Notify stakeholders
   - Assess scope of impact
   ```

2. **Cluster Rollback**
   ```
   - Update cluster configuration to DBR 13.3 LTS
   - Restart affected clusters
   - Verify cluster health
   ```

3. **Job Rollback**
   ```
   - Revert job cluster configurations
   - Restart failed jobs
   - Verify job execution
   ```

4. **Data Recovery (if needed)**
   ```sql
   -- Restore from Delta time travel
   RESTORE TABLE production.affected_table 
   TO TIMESTAMP AS OF '2024-01-01 00:00:00';
   
   -- Or restore from backup
   INSERT OVERWRITE TABLE production.affected_table
   SELECT * FROM backup.affected_table_snapshot;
   ```

5. **Post-Rollback**
   ```
   - Document the issue
   - Analyze root cause
   - Plan remediation
   - Schedule retry
   ```

---

## Final Checklist Summary

### Pre-Rollout
- [ ] Breaking changes fixed and tested
- [ ] Quality validation passed
- [ ] Performance testing completed
- [ ] Rollback plan documented
- [ ] Stakeholders notified

### During Rollout
- [ ] Staged deployment followed
- [ ] Each stage validated before proceeding
- [ ] Issues addressed as discovered
- [ ] Communication maintained

### Post-Rollout
- [ ] All jobs running successfully
- [ ] Data quality verified
- [ ] Performance acceptable
- [ ] Documentation updated
- [ ] Cleanup completed

---

## Sign-Off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Developer | | | |
| Tech Lead | | | |
| QA | | | |
| DevOps/Platform | | | |
| Product Owner | | | |

---

## Support Contacts

| Role | Contact | Escalation |
|------|---------|------------|
| Platform Engineering | platform@company.com | Slack: #platform-support |
| On-Call | See PagerDuty | Escalate after 15 min |
| Databricks Support | support.databricks.com | Premium support ticket |

---

## References

- [Databricks Runtime Release Notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/)
- [Extend Assistant with Agent Skills](https://learn.microsoft.com/en-us/azure/databricks/assistant/skills)
- [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)
- [Delta Lake Time Travel](https://docs.databricks.com/en/delta/history.html)
