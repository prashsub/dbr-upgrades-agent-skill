# Workspace Profiler

The Workspace Profiler scans your Databricks workspace to identify notebooks that may need updates for DBR migration. It detects breaking change patterns and outputs results to a Delta table for analysis.

---

## Quick Start

1. Import `workspace-profiler.py` into your Databricks workspace
2. Configure the widgets (see Configuration below)
3. Run all cells
4. Review results in the output Delta table

---

## Scan Modes

The profiler supports three main scanning configurations:

### Mode 1: Jobs Only (Minimal)
```
scan_jobs = True
scan_workspace = False
jobs_only_mode = False  (doesn't matter)
```

**What it does:**
- Scans notebook code directly from job task definitions
- Only notebooks explicitly configured in job tasks
- Misses notebooks called via `%run` (dependencies)
- **Fastest option**

**Use case:** Quick check of what's in your job definitions

---

### Mode 2: Jobs + Dependencies (Recommended)
```
scan_jobs = True
jobs_only_mode = True
include_nested_notebooks = True
```

**What it does:**
1. Collects notebook paths from job definitions
2. Parses `%run` statements to find dependencies
3. Scans job notebooks directly from job tasks
4. Scans dependency notebooks
5. **Workspace walk is automatically skipped** (not needed - all job notebooks already scanned)

**What you get:**
- All notebooks used by jobs (direct + dependencies)
- Skips standalone notebooks not connected to any job
- Fast - no filesystem traversal needed

**Use case:** Focus on production code that actually runs

**Note:** `scan_workspace` is ignored when `jobs_only_mode=True` because the workspace scan would be redundant.

---

### Mode 3: Full Workspace (Everything)
```
scan_jobs = True
scan_workspace = True
jobs_only_mode = False
```

**What it does:**
1. Scans all job notebooks
2. Scans **ALL** notebooks in workspace_paths

**What you get:**
- Every notebook in your workspace (minus exclude_paths)
- Includes abandoned/test/draft notebooks
- Most comprehensive, but most noise

**Use case:** Full audit before migration

---

### Visual Summary

```
                          scan_jobs=True         scan_jobs=True         scan_jobs=True
                          jobs_only_mode=False   jobs_only_mode=True    scan_workspace=True
                          scan_workspace=False   (any scan_workspace)   jobs_only_mode=False
                          ─────────────────────  ─────────────────────  ─────────────────────
Job task notebooks        ✅ Scanned             ✅ Scanned             ✅ Scanned
%run dependencies         ❌ Missed              ✅ Scanned             ✅ Scanned  
Standalone notebooks      ❌ Skipped             ❌ Skipped             ✅ Scanned
Workspace walk            ❌ Skipped             ❌ Auto-skipped        ✅ Runs (slow)
                          ─────────────────────  ─────────────────────  ─────────────────────
Scope                     Narrowest              Balanced (fast)        Widest (slow)
```

**Note:** When `jobs_only_mode=True`, the workspace filesystem walk is automatically skipped because all job-related notebooks are already scanned directly. This makes jobs-only mode much faster.

---

## Configuration Reference

### Output Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `output_catalog` | `main` | Unity Catalog for results |
| `output_schema` | `dbr_migration` | Schema for results |
| `output_table` | `scan_results` | Table name for results |
| `truncate_on_scan` | `True` | Clear table before each scan |
| `export_csv` | `True` | Export results to CSV |
| `csv_path` | `/Volumes/.../scan_results.csv` | CSV export location |

### Scan Scope

| Parameter | Default | Description |
|-----------|---------|-------------|
| `scan_jobs` | `True` | Scan notebooks from job definitions |
| `scan_workspace` | `True` | Scan workspace filesystem |
| `workspace_paths` | `/` | Paths to scan (comma-separated) |
| `exclude_paths` | `/Repos,/Shared/Archive` | Paths to skip |

### Migration Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_dbr_version` | `13.3` | Your current DBR version |
| `target_dbr_version` | `17.3` | Target DBR version |

Patterns already working on your source version are automatically skipped.

### Job Filtering

| Parameter | Default | Description |
|-----------|---------|-------------|
| `filter_jobs_by_activity` | `True` | Only scan recently-run jobs |
| `job_activity_days` | `365` | Days of activity to consider |
| `jobs_only_mode` | `True` | Only scan job-related notebooks |
| `include_nested_notebooks` | `True` | Resolve `%run` dependencies |
| `max_nested_depth` | `10` | Max depth for dependency resolution |

### Testing/Limits

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_jobs` | (empty) | Limit jobs to scan (for testing) |
| `max_notebooks` | (empty) | Limit notebooks per path |
| `dry_run` | `False` | Count items without scanning |
| `verbose` | `True` | Print detailed progress |

### Checkpointing

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_checkpointing` | `True` | Enable resume capability |
| `checkpoint_table` | `scan_checkpoint` | Checkpoint table name |
| `checkpoint_batch_size` | `50` | Items per batch before persisting |

---

## Checkpointing & Resume

The profiler uses a **batched checkpoint system** to prevent data loss during long scans.

### How It Works

1. Scan items and buffer results in memory
2. When buffer reaches `checkpoint_batch_size` (e.g., 50):
   - Write all buffered **results** to Delta results table
   - Write all buffered **checkpoints** to Delta checkpoint table
   - Clear buffers and continue
3. On crash/interruption: only lose items in the current batch (max 49)

### Atomicity Guarantee

Checkpoints are **only written AFTER results are persisted**. This ensures:
- If a checkpoint exists, its results are guaranteed to be in Delta
- On resume, items are either fully processed or will be re-scanned
- No data loss scenario where checkpoint says "done" but results are missing

### Resuming a Failed Scan

To resume a failed scan:

1. Find the `SCAN_ID` from the previous run (printed at start)
2. Set the `scan_id` widget to that value (or modify the code)
3. Run again - already-scanned items will be skipped

```python
# The profiler will print:
# 📊 Resuming scan - found 1500 existing results for scan_id: 20260204_143022
# 📊 Found 1500 existing checkpoints for scan_id: 20260204_143022
```

---

## Output Schema

Results are written to the configured Delta table with this schema:

| Column | Type | Description |
|--------|------|-------------|
| `notebook_path` | STRING | Full path to the notebook |
| `source_type` | STRING | JOB, WORKSPACE, or JOB_DEPENDENCY |
| `job_id` | STRING | Job ID (if from job scan) |
| `job_name` | STRING | Job name (if from job scan) |
| `task_key` | STRING | Task key within job |
| `breaking_change_id` | STRING | Pattern ID (e.g., BC-17.3-001) |
| `breaking_change_name` | STRING | Pattern name |
| `severity` | STRING | HIGH, MEDIUM, LOW, or OK |
| `introduced_in` | STRING | DBR version pattern was introduced |
| `line_number` | INT | Line number of match |
| `line_content` | STRING | Content of matching line |
| `description` | STRING | What the pattern detects |
| `remediation` | STRING | How to fix |
| `scan_id` | STRING | Unique scan identifier |
| `target_dbr_version` | STRING | Target DBR version |

---

## System Tables Integration

The profiler queries `system.lakeflow.job_run_timeline` to:
- Filter to only recently-active jobs
- Skip jobs that haven't run in N days

**Requirements:**
- Unity Catalog enabled
- Access to system tables
- Workspace must have job run history

If system tables are unavailable, the profiler falls back to scanning all jobs.

---

## Performance Tips

### For Large Workspaces (10,000+ notebooks)

1. **Start with jobs-only mode:**
   ```
   scan_jobs = True
   scan_workspace = True
   jobs_only_mode = True
   ```

2. **Use activity filtering:**
   ```
   filter_jobs_by_activity = True
   job_activity_days = 90  # Last 3 months
   ```

3. **Increase batch size for fewer writes:**
   ```
   checkpoint_batch_size = 100
   ```

4. **Exclude irrelevant paths:**
   ```
   exclude_paths = /Repos,/Shared/Archive,/Users/*/Trash
   ```

### For Testing

1. **Limit scope:**
   ```
   max_jobs = 10
   max_notebooks = 100
   ```

2. **Dry run first:**
   ```
   dry_run = True
   ```

---

## Troubleshooting

### "Jobs with recent runs" > "Total jobs in workspace"

The system table query wasn't filtering by workspace. This was fixed by adding:
```sql
WHERE workspace_id = {current_workspace_id}
```

### Scan takes too long

- Reduce `job_activity_days` to scan fewer jobs
- Enable `jobs_only_mode` to skip standalone notebooks
- Add paths to `exclude_paths`
- Use `max_jobs` / `max_notebooks` limits

### Results table is empty

1. Check `dry_run` is `False`
2. Verify the source/target version range includes applicable patterns
3. Check `verbose` output for errors
4. Ensure notebooks have content (empty notebooks are skipped)

### Checkpoint resume not working

1. Ensure `enable_checkpointing = True`
2. Use the same `SCAN_ID` as the failed run
3. Don't change `truncate_on_scan` to `True` when resuming

---

## Example Queries

### Find notebooks with critical issues
```sql
SELECT DISTINCT notebook_path, breaking_change_id, breaking_change_name
FROM main.dbr_migration.scan_results
WHERE severity = 'HIGH'
  AND scan_id = 'latest_scan_id'
ORDER BY notebook_path
```

### Summary by severity
```sql
SELECT severity, COUNT(*) as count
FROM main.dbr_migration.scan_results
WHERE scan_id = 'latest_scan_id'
GROUP BY severity
ORDER BY 
  CASE severity 
    WHEN 'HIGH' THEN 1 
    WHEN 'MEDIUM' THEN 2 
    WHEN 'LOW' THEN 3 
    ELSE 4 
  END
```

### Notebooks with most issues
```sql
SELECT notebook_path, COUNT(*) as issue_count
FROM main.dbr_migration.scan_results
WHERE scan_id = 'latest_scan_id'
  AND severity != 'OK'
GROUP BY notebook_path
ORDER BY issue_count DESC
LIMIT 20
```

### Issues by breaking change pattern
```sql
SELECT breaking_change_id, breaking_change_name, COUNT(*) as occurrences
FROM main.dbr_migration.scan_results
WHERE scan_id = 'latest_scan_id'
  AND severity != 'OK'
GROUP BY breaking_change_id, breaking_change_name
ORDER BY occurrences DESC
```

---

## Related Documentation

- [Developer Guide README](README.md) - Overall migration workflow
- [Breaking Changes Explained](BREAKING-CHANGES-EXPLAINED.md) - Technical details on each pattern
- [Pod Lead Validation Checklist](POD-LEAD-VALIDATION-CHECKLIST.md) - Validation procedures

---

*Last Updated: February 2026*
