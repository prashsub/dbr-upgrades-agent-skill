# Databricks notebook source
# MAGIC %md
# MAGIC # DBR Migration - Workspace Breaking Changes Profiler
# MAGIC 
# MAGIC This notebook scans **notebook tasks** in jobs and workspace notebooks for breaking changes
# MAGIC between DBR 13.3 LTS and 17.3 LTS.
# MAGIC 
# MAGIC ## Scope & Limitations
# MAGIC - **Scans:** Notebook tasks in jobs, workspace notebooks
# MAGIC - **Does NOT scan:** Python wheel tasks, JAR tasks, SQL tasks, spark_python_task, spark_submit_task, DLT pipelines, dbt tasks, or files in Repos/Git
# MAGIC - **Detection method:** Regex pattern matching (static analysis only)
# MAGIC - **Not all breaking changes are detectable** - some are runtime behavioral changes (e.g., file modification detection in 13.3)
# MAGIC 
# MAGIC ## Output
# MAGIC - **Delta Table**: `{catalog}.{schema}.dbr_migration_scan_results`
# MAGIC - **CSV Export**: Optional export to a specified path (always overwrites)
# MAGIC - **Re-runs**: Set `truncate_on_scan=True` (default) to replace results, or `False` to append history
# MAGIC 
# MAGIC ## Migration Path Filtering
# MAGIC - Set `source_dbr_version` to your CURRENT DBR version (e.g., "13.3")
# MAGIC - Only patterns introduced AFTER your source version will be flagged
# MAGIC - Example: For 13.3 → 17.3 migration, BC-13.3-xxx patterns are skipped (already working)
# MAGIC 
# MAGIC ## Clean Notebook Tracking
# MAGIC - Set `track_clean_notebooks=True` (default) to include notebooks with no issues
# MAGIC - Clean notebooks appear with `severity="OK"` and `breaking_change_id="CLEAN"`
# MAGIC - Helps track which notebooks are ready for upgrade vs which need review
# MAGIC 
# MAGIC ## Usage
# MAGIC 1. Configure the parameters below
# MAGIC 2. Run all cells
# MAGIC 3. Review results in the output table
# MAGIC 4. **Important:** Manual review still required for behavioral changes not detectable by pattern matching

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC ### Interactive Widget Configuration
# MAGIC 
# MAGIC All configuration parameters are available as **interactive widgets** at the top of this notebook.
# MAGIC Simply modify the widget values in the UI - no code changes required!
# MAGIC 
# MAGIC > **Widget Reference:** [Databricks Widgets Documentation](https://learn.microsoft.com/en-us/azure/databricks/notebooks/widgets)
# MAGIC 
# MAGIC ### Key Configuration Options
# MAGIC 
# MAGIC | Widget | Description | Default |
# MAGIC |--------|-------------|---------|
# MAGIC | **Filter Jobs by Activity** | Only scan jobs that have run recently | True |
# MAGIC | **Job Activity Days** | How many days to look back for job runs | 365 |
# MAGIC | **Jobs-Only Mode** | Skip standalone notebooks not used by jobs | False |
# MAGIC | **Include Nested Notebooks** | Follow `%run` and `dbutils.notebook.run()` calls | True |
# MAGIC | **Source/Target DBR Version** | Define migration path for pattern filtering | 13.3 → 17.3 |
# MAGIC | **Max Jobs/Notebooks** | Limit scan for testing (empty = scan all) | empty |
# MAGIC | **Dry Run** | Count files without scanning content | False |
# MAGIC 
# MAGIC ### Common Configuration Scenarios
# MAGIC 
# MAGIC **1. Full Production Scan (Recommended):**
# MAGIC - Filter Jobs by Activity: `True`
# MAGIC - Job Activity Days: `365`
# MAGIC - Jobs-Only Mode: `True`
# MAGIC - Include Nested Notebooks: `True`
# MAGIC 
# MAGIC **2. Quick Test Run:**
# MAGIC - Max Jobs: `5`
# MAGIC - Max Notebooks: `10`
# MAGIC - Dry Run: `False`
# MAGIC 
# MAGIC **3. Scan Specific Folder Only:**
# MAGIC - Scan Jobs: `False`
# MAGIC - Workspace Paths: `/Users/your.name/project`
# MAGIC 
# MAGIC **4. Count Files Without Scanning:**
# MAGIC - Dry Run: `True`
# MAGIC 
# MAGIC ### System Table Filtering
# MAGIC 
# MAGIC Job activity filtering uses `system.lakeflow.job_run_timeline` to identify jobs that have actually run.
# MAGIC > **Reference:** [Jobs System Tables](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/jobs)
# MAGIC 
# MAGIC ### Nested Notebook Resolution
# MAGIC 
# MAGIC When **Include Nested Notebooks** is enabled, the profiler follows:
# MAGIC - `%run ./path/to/notebook` (inline execution)
# MAGIC - `dbutils.notebook.run("path", timeout, args)` (ephemeral job)
# MAGIC 
# MAGIC > **Reference:** [Orchestrate notebooks and modularize code](https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebook-workflows)
# MAGIC 
# MAGIC ### Checkpointing (Resume Failed Scans)
# MAGIC 
# MAGIC Checkpointing is enabled by default. To resume a failed scan:
# MAGIC ```python
# MAGIC # Find the scan_id from the failed run's output, then set it before running:
# MAGIC SCAN_ID = "20260125_143022"  # Replace with actual scan_id from failed run
# MAGIC ```
# MAGIC 
# MAGIC To clear checkpoints and start fresh:
# MAGIC ```python
# MAGIC clear_checkpoint()  # Clears all
# MAGIC # or
# MAGIC clear_checkpoint("20260125_143022")  # Clears specific scan
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive Configuration (Widgets)
# MAGIC 
# MAGIC All configuration parameters are available as **interactive widgets** at the top of this notebook.
# MAGIC You can modify them directly in the UI without editing code.
# MAGIC 
# MAGIC **Widget Reference:** [Databricks Widgets Documentation](https://learn.microsoft.com/en-us/azure/databricks/notebooks/widgets)

# COMMAND ----------

# ============================================================
# CREATE CONFIGURATION WIDGETS
# ============================================================
# These widgets appear at the top of the notebook for easy configuration.
# All parameters can be changed interactively without editing code.

# --- Helper functions for widget value parsing ---
def _parse_bool(value: str) -> bool:
    """Parse string to boolean."""
    return value.lower() in ('true', 'yes', '1', 'on')

def _parse_int_or_none(value: str) -> int:
    """Parse string to int, return None if empty or 'None'."""
    if not value or value.lower() == 'none' or value.strip() == '':
        return None
    return int(value)

def _parse_list(value: str) -> list:
    """Parse comma-separated string to list."""
    if not value or value.strip() == '':
        return []
    return [item.strip() for item in value.split(',') if item.strip()]

# --- Output Settings ---
dbutils.widgets.text("output_catalog", "main", "1. Output Catalog")
dbutils.widgets.text("output_schema", "dbr_migration", "2. Output Schema")
dbutils.widgets.text("output_table", "scan_results", "3. Output Table")
dbutils.widgets.dropdown("truncate_on_scan", "True", ["True", "False"], "4. Truncate Before Scan")

# --- CSV Export ---
dbutils.widgets.dropdown("export_csv", "True", ["True", "False"], "5. Export to CSV")
dbutils.widgets.text("csv_path", "/Volumes/main/dbr_migration/exports/scan_results.csv", "6. CSV Export Path")

# --- Scan Scope ---
dbutils.widgets.dropdown("scan_jobs", "True", ["True", "False"], "7. Scan Jobs")
dbutils.widgets.dropdown("scan_workspace", "True", ["True", "False"], "8. Scan Workspace")
dbutils.widgets.text("workspace_paths", "/", "9. Workspace Paths (comma-separated)")

# --- Filtering ---
dbutils.widgets.text("exclude_paths", "/Repos,/Shared/Archive", "10. Exclude Paths (comma-separated)")

# --- Migration Path ---
dbutils.widgets.dropdown("source_dbr_version", "13.3", ["11.3", "12.2", "13.3", "14.3", "15.4", "16.4"], "11. Source DBR Version")
dbutils.widgets.dropdown("target_dbr_version", "17.3", ["14.3", "15.4", "16.4", "17.3"], "12. Target DBR Version")

# --- Clean Notebook Tracking ---
dbutils.widgets.dropdown("track_clean_notebooks", "True", ["True", "False"], "13. Track Clean Notebooks")

# --- Job Filtering (System Tables) ---
dbutils.widgets.dropdown("filter_jobs_by_activity", "True", ["True", "False"], "14. Filter Jobs by Activity")
dbutils.widgets.text("job_activity_days", "365", "15. Job Activity Days")

# --- Jobs-Only Mode ---
dbutils.widgets.dropdown("jobs_only_mode", "False", ["True", "False"], "16. Jobs-Only Mode")

# --- Nested Notebook Resolution ---
dbutils.widgets.dropdown("include_nested_notebooks", "True", ["True", "False"], "17. Include Nested Notebooks")
dbutils.widgets.text("max_nested_depth", "10", "18. Max Nested Depth")

# --- Testing/Development Limits ---
dbutils.widgets.text("max_jobs", "", "19. Max Jobs (empty=all)")
dbutils.widgets.text("max_notebooks", "", "20. Max Notebooks (empty=all)")
dbutils.widgets.dropdown("dry_run", "False", ["True", "False"], "21. Dry Run")
dbutils.widgets.dropdown("verbose", "True", ["True", "False"], "22. Verbose Output")

# --- Checkpointing ---
dbutils.widgets.dropdown("enable_checkpointing", "True", ["True", "False"], "23. Enable Checkpointing")
dbutils.widgets.text("checkpoint_table", "scan_checkpoint", "24. Checkpoint Table")

# COMMAND ----------

# ============================================================
# BUILD CONFIG FROM WIDGETS
# ============================================================
# Read all widget values and construct the CONFIG dictionary.
# This allows interactive configuration through the widget UI.

CONFIG = {
    # Output settings
    "output_catalog": dbutils.widgets.get("output_catalog"),
    "output_schema": dbutils.widgets.get("output_schema"),
    "output_table": dbutils.widgets.get("output_table"),
    "truncate_on_scan": _parse_bool(dbutils.widgets.get("truncate_on_scan")),
    
    # Optional CSV export
    "export_csv": _parse_bool(dbutils.widgets.get("export_csv")),
    "csv_path": dbutils.widgets.get("csv_path"),
    
    # Scan scope
    "scan_jobs": _parse_bool(dbutils.widgets.get("scan_jobs")),
    "scan_workspace": _parse_bool(dbutils.widgets.get("scan_workspace")),
    "workspace_paths": _parse_list(dbutils.widgets.get("workspace_paths")) or ["/"],
    
    # Filtering
    "exclude_paths": _parse_list(dbutils.widgets.get("exclude_paths")),
    "file_extensions": [".py", ".sql", ".scala"],  # Not configurable via widget (rarely changed)
    
    # Migration path
    "source_dbr_version": dbutils.widgets.get("source_dbr_version"),
    "target_dbr_version": dbutils.widgets.get("target_dbr_version"),
    
    # Track clean notebooks
    "track_clean_notebooks": _parse_bool(dbutils.widgets.get("track_clean_notebooks")),
    
    # Job filtering (System Tables)
    "filter_jobs_by_activity": _parse_bool(dbutils.widgets.get("filter_jobs_by_activity")),
    "job_activity_days": int(dbutils.widgets.get("job_activity_days") or "365"),
    
    # Jobs-only mode
    "jobs_only_mode": _parse_bool(dbutils.widgets.get("jobs_only_mode")),
    
    # Nested notebook resolution
    "include_nested_notebooks": _parse_bool(dbutils.widgets.get("include_nested_notebooks")),
    "max_nested_depth": int(dbutils.widgets.get("max_nested_depth") or "10"),
    
    # Testing/Development limits
    "max_jobs": _parse_int_or_none(dbutils.widgets.get("max_jobs")),
    "max_notebooks": _parse_int_or_none(dbutils.widgets.get("max_notebooks")),
    "dry_run": _parse_bool(dbutils.widgets.get("dry_run")),
    "verbose": _parse_bool(dbutils.widgets.get("verbose")),
    
    # Checkpointing
    "enable_checkpointing": _parse_bool(dbutils.widgets.get("enable_checkpointing")),
    "checkpoint_table": dbutils.widgets.get("checkpoint_table"),
    "checkpoint_batch_size": 10,  # Not configurable via widget (internal setting)
}

# Display current configuration
print("=" * 60)
print("CURRENT CONFIGURATION (from widgets)")
print("=" * 60)
for key, value in CONFIG.items():
    print(f"  {key}: {value}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reset Widgets to Defaults (Optional)
# MAGIC 
# MAGIC Uncomment and run the cell below to reset all widgets to their default values.

# COMMAND ----------

# # UNCOMMENT TO RESET ALL WIDGETS TO DEFAULTS
# # This removes all widgets and re-runs the notebook to recreate them with default values
# 
# dbutils.widgets.removeAll()
# print("All widgets removed. Re-run the notebook to recreate widgets with default values.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Breaking Change Patterns

# COMMAND ----------

import re
import os
from typing import List, Dict, Tuple, Optional, Set
from dataclasses import dataclass, asdict
from datetime import datetime

def parse_version(version_str: str) -> Tuple[int, int]:
    """Parse version string like '13.3' or '17.3' into tuple (major, minor)."""
    parts = version_str.split(".")
    return (int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)

def version_greater_than(v1: str, v2: str) -> bool:
    """Return True if v1 > v2."""
    return parse_version(v1) > parse_version(v2)

def filter_patterns_for_migration(patterns: list, source_version: str, target_version: str) -> list:
    """
    Filter patterns to only include those relevant for the migration path.
    
    A pattern is relevant if its introduced_in version is:
    - Greater than the source version (already working on source)
    - Less than or equal to the target version
    
    Example: For 13.3 → 17.3 migration:
    - BC-13.3-001 (introduced_in="13.3") is SKIPPED (already working on 13.3)
    - BC-14.3-001 (introduced_in="14.3") is INCLUDED
    - BC-17.3-001 (introduced_in="17.3") is INCLUDED
    """
    source = parse_version(source_version)
    target = parse_version(target_version)
    
    filtered = []
    for p in patterns:
        introduced = parse_version(p.introduced_in)
        # Include if introduced AFTER source and AT OR BEFORE target
        if introduced > source and introduced <= target:
            filtered.append(p)
    
    return filtered

@dataclass
class BreakingChangePattern:
    """Definition of a breaking change pattern."""
    id: str
    name: str
    severity: str  # HIGH, MEDIUM, LOW
    introduced_in: str
    pattern: str
    file_types: List[str]
    description: str
    remediation: str

# All breaking change patterns to scan for
BREAKING_PATTERNS = [
    # ============================================================
    # HIGH SEVERITY - Will cause immediate failures
    # ============================================================
    BreakingChangePattern(
        id="BC-17.3-001",
        name="input_file_name() Removed",
        severity="HIGH",
        introduced_in="17.3",
        pattern=r"\binput_file_name\s*\(",
        file_types=[".py", ".sql", ".scala"],
        description="input_file_name() function is removed in DBR 17.3",
        remediation="Replace with _metadata.file_name"
    ),
    BreakingChangePattern(
        id="BC-16.4-001a",
        name="Scala JavaConverters Import",
        severity="HIGH",
        introduced_in="16.4",
        pattern=r"import\s+scala\.collection\.JavaConverters",
        file_types=[".scala"],
        description="JavaConverters is deprecated in Scala 2.13",
        remediation="Use 'import scala.jdk.CollectionConverters._' instead"
    ),
    BreakingChangePattern(
        id="BC-16.4-001c",
        name="Scala TraversableOnce",
        severity="HIGH",
        introduced_in="16.4",
        pattern=r"\bTraversableOnce\b",
        file_types=[".scala"],
        description="TraversableOnce is renamed to IterableOnce in Scala 2.13",
        remediation="Replace TraversableOnce with IterableOnce"
    ),
    BreakingChangePattern(
        id="BC-16.4-001d",
        name="Scala Traversable",
        severity="HIGH",
        introduced_in="16.4",
        pattern=r"\bTraversable\b(?!Once)",
        file_types=[".scala"],
        description="Traversable is renamed to Iterable in Scala 2.13",
        remediation="Replace Traversable with Iterable"
    ),
    BreakingChangePattern(
        id="BC-16.4-002",
        name="Scala HashMap/HashSet Ordering",
        severity="HIGH",
        introduced_in="16.4",
        pattern=r"\b(HashMap|HashSet)\s*[\[\(]",
        file_types=[".scala"],
        description="[Review] HashMap/HashSet iteration order changed in Scala 2.13",
        remediation="Don't rely on iteration order; use LinkedHashMap/ListMap or explicit sorting"
    ),
    BreakingChangePattern(
        id="BC-13.3-001",
        name="MERGE INTO Type Casting (Review)",
        severity="HIGH",
        introduced_in="13.3",
        pattern=r"\bMERGE\s+INTO\b",
        file_types=[".py", ".sql", ".scala"],
        description="[Review] MERGE/UPDATE now follows ANSI casting - overflow throws error instead of NULL",
        remediation="Ensure source/target column types match or use explicit CAST with overflow handling"
    ),
    BreakingChangePattern(
        id="BC-SC-001",
        name="Spark Connect Lazy Analysis (Review)",
        severity="HIGH",
        introduced_in="13.3",
        pattern=r"except\s+.*(?:AnalysisException|SparkException|IllegalArgumentException)",
        file_types=[".py", ".scala"],
        description="[Review] Spark Connect defers schema analysis - these exceptions may not be caught until action time",
        remediation="Trigger eager analysis with df.columns or df.schema after transformations if error handling is needed"
    ),
    
    # ============================================================
    # MEDIUM SEVERITY - May cause failures or incorrect results
    # ============================================================
    BreakingChangePattern(
        id="BC-15.4-003",
        name="'!' Syntax for NOT",
        severity="MEDIUM",
        introduced_in="15.4",
        pattern=r"(IF|IS)\s*!(?!\s*=)",
        file_types=[".sql"],
        description="Using '!' instead of 'NOT' outside boolean expressions is disallowed",
        remediation="Replace '!' with 'NOT' (e.g., IF NOT EXISTS, IS NOT NULL)"
    ),
    BreakingChangePattern(
        id="BC-15.4-003b",
        name="'!' Syntax for NOT IN/BETWEEN/LIKE",
        severity="MEDIUM",
        introduced_in="15.4",
        pattern=r"\s!\s*(IN|BETWEEN|LIKE|EXISTS)\b",
        file_types=[".sql"],
        description="Using '!' instead of 'NOT' for IN/BETWEEN/LIKE/EXISTS is disallowed",
        remediation="Replace with 'NOT IN', 'NOT BETWEEN', 'NOT LIKE', 'NOT EXISTS'"
    ),
    BreakingChangePattern(
        id="BC-16.4-001b",
        name="Scala .to[Collection] Syntax",
        severity="MEDIUM",
        introduced_in="16.4",
        pattern=r"\.to\s*\[\s*(List|Set|Vector|Seq|Array)\s*\]",
        file_types=[".scala"],
        description=".to[Collection] syntax changed in Scala 2.13",
        remediation="Use .to(Collection) syntax instead (e.g., .to(List))"
    ),
    BreakingChangePattern(
        id="BC-16.4-001e",
        name="Scala Stream (Lazy)",
        severity="MEDIUM",
        introduced_in="16.4",
        pattern=r"\bStream\s*\.\s*(from|continually|iterate|empty|cons)",
        file_types=[".scala"],
        description="Stream is replaced by LazyList in Scala 2.13",
        remediation="Replace Stream with LazyList"
    ),
    BreakingChangePattern(
        id="BC-16.4-001f",
        name="Scala .toIterator Deprecated",
        severity="MEDIUM",
        introduced_in="16.4",
        pattern=r"\.toIterator\b",
        file_types=[".scala"],
        description=".toIterator is deprecated in Scala 2.13",
        remediation="Use .iterator instead of .toIterator"
    ),
    BreakingChangePattern(
        id="BC-16.4-001g",
        name="Scala .view.force Deprecated",
        severity="MEDIUM",
        introduced_in="16.4",
        pattern=r"\.view\s*\.\s*force\b",
        file_types=[".scala"],
        description=".view.force is deprecated in Scala 2.13",
        remediation="Use .view.to(List) or .view.toList instead"
    ),
    BreakingChangePattern(
        id="BC-16.4-001h",
        name="Scala collection.Seq Changed",
        severity="MEDIUM",
        introduced_in="16.4",
        pattern=r"\bcollection\.Seq\b(?!\.)",
        file_types=[".scala"],
        description="collection.Seq now refers to immutable.Seq in Scala 2.13",
        remediation="Use collection.immutable.Seq or collection.mutable.Seq explicitly"
    ),
    BreakingChangePattern(
        id="BC-13.3-003",
        name="overwriteSchema with Dynamic Partition",
        severity="MEDIUM",
        introduced_in="13.3",
        pattern=r"overwriteSchema.*true",
        file_types=[".py", ".scala"],
        description="[Review] overwriteSchema=true found - verify it's not combined with partitionOverwriteMode='dynamic' (fails in 13.3+)",
        remediation="If also using dynamic partition overwrite, separate into distinct operations"
    ),
    BreakingChangePattern(
        id="BC-17.3-002",
        name="Auto Loader Incremental Listing",
        severity="MEDIUM",
        introduced_in="17.3",
        pattern=r"cloudFiles\.useIncrementalListing",
        file_types=[".py", ".scala", ".sql"],
        description="Auto Loader incremental listing default changed to false",
        remediation="Explicitly set cloudFiles.useIncrementalListing if needed"
    ),
    BreakingChangePattern(
        id="BC-17.3-002b",
        name="Auto Loader Default Behavior (Review)",
        severity="MEDIUM",
        introduced_in="17.3",
        pattern=r"\.format\s*\(\s*[\"']cloudFiles[\"']\s*\)",
        file_types=[".py", ".scala"],
        description="[Review] Auto Loader now does full directory listings by default (not incremental)",
        remediation="Add .option('cloudFiles.useIncrementalListing', 'auto') to preserve old behavior"
    ),
    BreakingChangePattern(
        id="BC-16.4-006",
        name="Auto Loader cleanSource Behavior",
        severity="MEDIUM",
        introduced_in="16.4",
        pattern=r"cloudFiles\.cleanSource",
        file_types=[".py", ".scala", ".sql"],
        description="[Review] cloudFiles.cleanSource behavior changed in 16.4",
        remediation="Review cleanSource settings; behavior for file cleanup may differ"
    ),
    BreakingChangePattern(
        id="BC-15.4-006",
        name="View Schema Binding Mode",
        severity="MEDIUM",
        introduced_in="15.4",
        pattern=r"CREATE\s+(OR\s+REPLACE\s+)?VIEW\b",
        file_types=[".sql"],
        description="[Review] View schema binding default changed from BINDING to schema compensation",
        remediation="Verify view definitions handle underlying table schema changes correctly"
    ),
    BreakingChangePattern(
        id="BC-16.4-003",
        name="Data Source Cache Options Setting",
        severity="MEDIUM",
        introduced_in="16.4",
        pattern=r"spark\.sql\.legacy\.readFileSourceTableCacheIgnoreOptions",
        file_types=[".py", ".scala", ".sql"],
        description="Table reads now respect options for all cached plans",
        remediation="Set spark.sql.legacy.readFileSourceTableCacheIgnoreOptions=true to restore old behavior"
    ),
    # NOTE: BC-SC-002 removed - was flagging ALL temp views which caused over-reporting.
    # Duplicate temp view detection (BC-SC-002-DUP) in scan_duplicate_temp_views() handles
    # the actual risk of reusing the same view name. See Spark Connect guidance:
    # https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic
    
    # ============================================================
    # LOW SEVERITY - Informational or subtle behavior changes
    # ============================================================
    BreakingChangePattern(
        id="BC-13.3-002",
        name="Parquet Timestamp NTZ Setting",
        severity="LOW",
        introduced_in="13.3",
        pattern=r"spark\.sql\.parquet\.inferTimestampNTZ",
        file_types=[".py", ".scala", ".sql"],
        description="Parquet timestamp inference behavior changed",
        remediation="Set spark.sql.parquet.inferTimestampNTZ.enabled explicitly"
    ),
    BreakingChangePattern(
        id="BC-13.3-002b",
        name="Parquet Read (TIMESTAMP_NTZ Review)",
        severity="LOW",
        introduced_in="13.3",
        pattern=r"\.parquet\s*\(|\.format\s*\(\s*[\"']parquet[\"']\s*\)",
        file_types=[".py", ".scala"],
        description="[Review] Parquet reads may infer TIMESTAMP_NTZ differently in 13.3+",
        remediation="Set spark.sql.parquet.inferTimestampNTZ.enabled=false or use explicit schema"
    ),
    BreakingChangePattern(
        id="BC-13.3-004",
        name="ANSI Store Assignment Policy",
        severity="LOW",
        introduced_in="13.3",
        pattern=r"spark\.sql\.storeAssignmentPolicy",
        file_types=[".py", ".scala", ".sql"],
        description="[Review] storeAssignmentPolicy default is ANSI - overflow throws error",
        remediation="Ensure MERGE/UPDATE operations handle type casting explicitly"
    ),
    BreakingChangePattern(
        id="BC-15.4-001",
        name="VARIANT Type in Python UDF",
        severity="MEDIUM",
        introduced_in="15.4",
        pattern=r"VariantType\s*\(",
        file_types=[".py"],
        description="[Review] VARIANT type in Python UDF/UDAF/UDTF may throw exception in DBR 15.4+",
        remediation="Use STRING type with JSON serialization, or Scala UDFs for VARIANT handling"
    ),
    BreakingChangePattern(
        id="BC-15.4-002",
        name="JDBC useNullCalendar Setting",
        severity="LOW",
        introduced_in="15.4",
        pattern=r"spark\.sql\.legacy\.jdbc\.useNullCalendar",
        file_types=[".py", ".scala", ".sql"],
        description="JDBC useNullCalendar default changed to true",
        remediation="Set spark.sql.legacy.jdbc.useNullCalendar explicitly if needed"
    ),
    BreakingChangePattern(
        id="BC-15.4-004",
        name="View Column Type Definition",
        severity="LOW",
        introduced_in="15.4",
        pattern=r"CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+\w+\s*\([^)]*\b(INT|STRING|BIGINT|DOUBLE|BOOLEAN|NOT\s+NULL|DEFAULT)\b",
        file_types=[".sql"],
        description="Column type definitions in CREATE VIEW are disallowed",
        remediation="Remove column type specifications from CREATE VIEW"
    ),
    BreakingChangePattern(
        id="BC-14.3-001",
        name="Thriftserver hive.aux.jars.path Removed",
        severity="LOW",
        introduced_in="14.3",
        pattern=r"hive\.aux\.jars\.path|hive\.server2\.global\.init\.file\.location",
        file_types=[".py", ".scala", ".sql"],
        description="Hive auxiliary JARs and global init file configs removed",
        remediation="Use cluster init scripts or Unity Catalog volumes for JARs"
    ),
    BreakingChangePattern(
        id="BC-16.4-001i",
        name="Scala Symbol Literal Deprecated",
        severity="LOW",
        introduced_in="16.4",
        pattern=r"'[a-zA-Z_][a-zA-Z0-9_]*(?![a-zA-Z0-9_'])",
        file_types=[".scala"],
        description="Symbol literals ('symbol) are deprecated in Scala 2.13",
        remediation="Use Symbol(\"symbol\") constructor instead"
    ),
    BreakingChangePattern(
        id="BC-16.4-004",
        name="MERGE materializeSource=none",
        severity="LOW",
        introduced_in="16.4",
        pattern=r"merge\.materializeSource.*none",
        file_types=[".py", ".scala", ".sql"],
        description="merge.materializeSource=none is no longer allowed",
        remediation="Remove merge.materializeSource=none configuration"
    ),
    BreakingChangePattern(
        id="BC-16.4-005",
        name="Json4s Library Usage (Review)",
        severity="LOW",
        introduced_in="16.4",
        pattern=r"import\s+org\.json4s",
        file_types=[".scala"],
        description="[Review] Json4s downgraded from 4.0.7 to 3.7.0-M11 for Scala 2.13",
        remediation="Review Json4s API usage for compatibility with 3.7.x"
    ),
    BreakingChangePattern(
        id="BC-17.3-003",
        name="Spark Connect Literal Handling (Review)",
        severity="LOW",
        introduced_in="17.3",
        pattern=r"\b(array|map|struct)\s*\(",
        file_types=[".py", ".scala"],
        description="[Review] Spark Connect 17.3: null values preserved, decimal precision changed to (38,18)",
        remediation="Handle nulls explicitly with coalesce(); specify decimal precision if needed"
    ),
    BreakingChangePattern(
        id="BC-17.3-005",
        name="Spark Connect Decimal Precision",
        severity="LOW",
        introduced_in="17.3",
        pattern=r"DecimalType\s*\(|\.cast\s*\(\s*[\"']decimal",
        file_types=[".py", ".scala"],
        description="[Review] Spark Connect: decimal precision in array/map literals defaults to (38,18)",
        remediation="Specify explicit precision/scale if plan comparison or exact precision required"
    ),
    BreakingChangePattern(
        id="BC-SC-003",
        name="UDF Definition (Review)",
        severity="LOW",
        introduced_in="14.3",
        pattern=r"@udf\s*\(",
        file_types=[".py"],
        description="[Review] Spark Connect: UDFs serialize at execution time",
        remediation="Check if external variables are captured correctly; use function factory pattern"
    ),
    BreakingChangePattern(
        id="BC-SC-004",
        name="Schema Access (Review)",
        severity="LOW",
        introduced_in="13.3",
        pattern=r"\.(columns|schema|dtypes)\b",
        file_types=[".py"],
        description="[Review] Spark Connect: schema access triggers RPC",
        remediation="Cache df.columns/df.schema if accessed multiple times"
    ),
    BreakingChangePattern(
        id="BC-15.4-005",
        name="JDBC Read (Review)",
        severity="LOW",
        introduced_in="15.4",
        pattern=r"\.jdbc\s*\(|\.format\s*\(\s*[\"']jdbc[\"']\s*\)",
        file_types=[".py", ".scala"],
        description="[Review] JDBC timestamp handling changed - useNullCalendar default now true",
        remediation="Test timestamp values from JDBC sources; set useNullCalendar=false if issues"
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpointing Functions

# COMMAND ----------

def get_checkpoint_table_name() -> str:
    """Get the full checkpoint table name."""
    return f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['checkpoint_table']}"

def initialize_checkpoint_table():
    """Create the checkpoint table if it doesn't exist."""
    if not CONFIG.get("enable_checkpointing", False):
        return
    
    checkpoint_table = get_checkpoint_table_name()
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {checkpoint_table} (
            scan_id STRING,
            item_type STRING,
            item_id STRING,
            item_path STRING,
            status STRING,
            scanned_at TIMESTAMP,
            error_message STRING
        )
        USING DELTA
    """)
    print(f"✅ Checkpoint table ready: {checkpoint_table}")

def get_completed_items(scan_id: str, item_type: str) -> set:
    """Get set of already scanned items from checkpoint."""
    if not CONFIG.get("enable_checkpointing", False):
        return set()
    
    checkpoint_table = get_checkpoint_table_name()
    
    try:
        df = spark.sql(f"""
            SELECT item_id 
            FROM {checkpoint_table}
            WHERE scan_id = '{scan_id}' 
              AND item_type = '{item_type}'
              AND status = 'completed'
        """)
        return set(row.item_id for row in df.collect())
    except:
        return set()

def save_checkpoint(scan_id: str, item_type: str, item_id: str, item_path: str, status: str = "completed", error: str = None):
    """Save a checkpoint for a scanned item."""
    if not CONFIG.get("enable_checkpointing", False):
        return
    
    checkpoint_table = get_checkpoint_table_name()
    
    # Use SQL INSERT for simplicity
    error_escaped = error.replace("'", "''") if error else ""
    path_escaped = item_path.replace("'", "''") if item_path else ""
    
    spark.sql(f"""
        INSERT INTO {checkpoint_table}
        VALUES (
            '{scan_id}',
            '{item_type}',
            '{item_id}',
            '{path_escaped}',
            '{status}',
            current_timestamp(),
            '{error_escaped}'
        )
    """)

def save_checkpoints_batch(scan_id: str, items: list):
    """Save multiple checkpoints at once (more efficient)."""
    if not CONFIG.get("enable_checkpointing", False) or not items:
        return
    
    checkpoint_table = get_checkpoint_table_name()
    
    # Create DataFrame from items
    from pyspark.sql.functions import current_timestamp, lit
    
    df = spark.createDataFrame(items)
    df = df.withColumn("scan_id", lit(scan_id)) \
           .withColumn("scanned_at", current_timestamp())
    
    df.write.format("delta").mode("append").saveAsTable(checkpoint_table)

def get_scan_progress(scan_id: str) -> dict:
    """Get progress summary for a scan."""
    if not CONFIG.get("enable_checkpointing", False):
        return {}
    
    checkpoint_table = get_checkpoint_table_name()
    
    try:
        df = spark.sql(f"""
            SELECT 
                item_type,
                status,
                COUNT(*) as count
            FROM {checkpoint_table}
            WHERE scan_id = '{scan_id}'
            GROUP BY item_type, status
        """)
        return {f"{row.item_type}_{row.status}": row['count'] for row in df.collect()}
    except:
        return {}

def clear_checkpoint(scan_id: str = None):
    """Clear checkpoints for a scan (or all if scan_id is None)."""
    if not CONFIG.get("enable_checkpointing", False):
        return
    
    checkpoint_table = get_checkpoint_table_name()
    
    if scan_id:
        spark.sql(f"DELETE FROM {checkpoint_table} WHERE scan_id = '{scan_id}'")
        print(f"🗑️ Cleared checkpoints for scan: {scan_id}")
    else:
        spark.sql(f"DELETE FROM {checkpoint_table}")
        print(f"🗑️ Cleared all checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job System Table Functions
# MAGIC 
# MAGIC Query `system.lakeflow.job_run_timeline` to find jobs that have been executed recently.
# MAGIC This enables filtering to only scan active/relevant jobs.

# COMMAND ----------

def get_active_job_ids_from_system_tables(days: int = 365) -> set:
    """
    Query system.lakeflow.job_run_timeline to get job IDs that have been executed
    within the specified number of days.
    
    Args:
        days: Number of days to look back for job activity (default: 365)
    
    Returns:
        Set of (workspace_id, job_id) tuples for active jobs
    
    Note: Uses system.lakeflow.job_run_timeline which tracks job runs and metadata.
    Reference: https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/jobs
    """
    try:
        # Query job_run_timeline for jobs with recent activity
        # We use period_start_time to filter for jobs that have actually run
        query = f"""
        SELECT DISTINCT
            workspace_id,
            job_id
        FROM system.lakeflow.job_run_timeline
        WHERE 
            period_start_time >= CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
            AND run_type = 'JOB_RUN'  -- Only standard job executions, not SUBMIT_RUN or WORKFLOW_RUN
        """
        
        df = spark.sql(query)
        active_jobs = set()
        
        for row in df.collect():
            # Store as string for consistent comparison
            active_jobs.add(str(row.job_id))
        
        return active_jobs
        
    except Exception as e:
        print(f"⚠️ Warning: Could not query system.lakeflow.job_run_timeline: {e}")
        print("   Falling back to scanning all jobs (no activity filter applied)")
        return None  # Return None to indicate fallback to scanning all


def get_active_jobs_with_metadata(days: int = 365) -> dict:
    """
    Query system tables to get active jobs with their metadata.
    
    Args:
        days: Number of days to look back for job activity
    
    Returns:
        Dictionary mapping job_id to job metadata including:
        - last_run_time: Most recent run timestamp
        - run_count: Number of runs in the period
        - result_states: Set of result states from runs
    """
    try:
        query = f"""
        WITH job_activity AS (
            SELECT
                job_id,
                MAX(period_end_time) as last_run_time,
                COUNT(DISTINCT run_id) as run_count,
                COLLECT_SET(result_state) as result_states
            FROM system.lakeflow.job_run_timeline
            WHERE 
                period_start_time >= CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
                AND run_type = 'JOB_RUN'
            GROUP BY job_id
        )
        SELECT * FROM job_activity
        """
        
        df = spark.sql(query)
        jobs_metadata = {}
        
        for row in df.collect():
            jobs_metadata[str(row.job_id)] = {
                "last_run_time": row.last_run_time,
                "run_count": row.run_count,
                "result_states": list(row.result_states) if row.result_states else []
            }
        
        return jobs_metadata
        
    except Exception as e:
        print(f"⚠️ Warning: Could not query job metadata from system tables: {e}")
        return {}


def print_job_activity_summary(active_jobs: set, total_jobs: int, days: int):
    """Print a summary of job activity filtering."""
    if active_jobs is None:
        print(f"  ℹ️ System tables not available - scanning all {total_jobs} jobs")
    else:
        filtered_count = len(active_jobs)
        skipped_count = total_jobs - filtered_count
        print(f"  📊 Job Activity Filter (last {days} days):")
        print(f"     - Total jobs in workspace: {total_jobs}")
        print(f"     - Jobs with recent runs:   {filtered_count}")
        print(f"     - Jobs skipped (inactive): {skipped_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Dependency Resolution
# MAGIC 
# MAGIC Parse notebooks to find nested notebook calls and resolve the full dependency tree.
# MAGIC 
# MAGIC **Supported patterns:**
# MAGIC - `%run ./path/to/notebook` - Inline notebook execution (functions/variables become available)
# MAGIC - `%run /absolute/path/notebook` - Absolute path %run
# MAGIC - `dbutils.notebook.run("path", timeout)` - Starts ephemeral job (Python)
# MAGIC - `dbutils.notebook.run("path", 60, Map(...))` - With arguments (Scala)
# MAGIC 
# MAGIC **Reference:** [Orchestrate notebooks and modularize code](https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebook-workflows)

# COMMAND ----------

def extract_run_references(content: str, current_notebook_path: str) -> List[str]:
    """
    Extract notebook paths referenced via %run magic commands.
    
    The %run command includes another notebook within the current notebook,
    making all functions and variables from the called notebook available.
    
    Handles various %run formats:
    - %run ./relative_notebook
    - %run ../parent_folder/notebook
    - %run /absolute/path/notebook
    - %run "/path/with spaces/notebook"
    - %run $variable (skipped - can't resolve dynamically)
    
    Note: %run must be in a cell by itself and runs the entire notebook inline.
    Reference: https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebook-workflows#run
    
    Args:
        content: Notebook content as string
        current_notebook_path: Path of the current notebook (for resolving relative paths)
    
    Returns:
        List of resolved notebook paths
    """
    references = []
    
    # Pattern to match %run commands
    # Handles: %run path, %run "path", %run 'path'
    run_pattern = re.compile(
        r'%run\s+'
        r'(?:'
        r'"([^"]+)"'           # Double-quoted path
        r'|'
        r"'([^']+)'"           # Single-quoted path
        r'|'
        r'(\S+)'               # Unquoted path (no spaces)
        r')',
        re.IGNORECASE
    )
    
    for match in run_pattern.finditer(content):
        # Get the matched path (from whichever group matched)
        path = match.group(1) or match.group(2) or match.group(3)
        
        if not path:
            continue
        
        # Skip variable references (can't resolve dynamically)
        if path.startswith('$') or '{' in path:
            continue
        
        # Skip if accidentally matched something else
        if 'dbutils' in path.lower():
            continue
        
        resolved_path = _resolve_notebook_path(path, current_notebook_path)
        if resolved_path and resolved_path not in references:
            references.append(resolved_path)
    
    return references


def extract_dbutils_notebook_run_references(content: str, current_notebook_path: str) -> List[str]:
    """
    Extract notebook paths from dbutils.notebook.run() calls.
    
    Handles (Python):
    - dbutils.notebook.run("path", timeout)
    - dbutils.notebook.run("path", 60, {"argument": "data"})
    - dbutils.notebook.run('/path/to/notebook', timeout)
    
    Handles (Scala):
    - dbutils.notebook.run("path", 60)
    - dbutils.notebook.run("path", 60, Map("argument" -> "data"))
    
    Reference: https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebook-workflows#notebook-run
    
    Note: Variable paths (e.g., dbutils.notebook.run(notebook_path, ...)) cannot be resolved.
    
    Args:
        content: Notebook content as string
        current_notebook_path: Path of the current notebook (for resolving relative paths)
    
    Returns:
        List of resolved notebook paths
    """
    references = []
    
    # Pattern for dbutils.notebook.run with string literal path
    # Handles both Python and Scala syntax
    # Examples:
    #   dbutils.notebook.run("notebook-name", 60, {"argument": "data"})
    #   dbutils.notebook.run("notebook-name", 60, Map("argument" -> "data"))
    run_patterns = [
        # Standard pattern: dbutils.notebook.run("path", ...)
        re.compile(
            r'dbutils\.notebook\.run\s*\(\s*'
            r'(?:'
            r'"([^"]+)"'           # Double-quoted path
            r'|'
            r"'([^']+)'"           # Single-quoted path
            r')',
            re.IGNORECASE
        ),
        # Pattern with f-string prefix (to detect and skip): f"..." or f'...'
        re.compile(
            r'dbutils\.notebook\.run\s*\(\s*f["\']',
            re.IGNORECASE
        ),
        # Pattern for triple-quoted strings
        re.compile(
            r'dbutils\.notebook\.run\s*\(\s*'
            r'(?:'
            r'"""([^"]+)"""'       # Triple double-quoted path
            r'|'
            r"'''([^']+)'''"       # Triple single-quoted path
            r')',
            re.IGNORECASE
        ),
    ]
    
    # Main pattern for extracting paths
    main_pattern = run_patterns[0]
    triple_quote_pattern = run_patterns[2]
    
    # First, try standard quotes
    for match in main_pattern.finditer(content):
        path = match.group(1) or match.group(2)
        
        if not path:
            continue
        
        # Skip if it looks like a variable, f-string, or concatenation
        if '{' in path or path.startswith('$') or '+' in path:
            continue
        
        # Skip if path contains escape sequences that look like variables
        if '\\' in path and any(c in path for c in ['n', 't', 'r']):
            # Might be legitimate escape, keep it
            pass
        
        resolved_path = _resolve_notebook_path(path, current_notebook_path)
        if resolved_path and resolved_path not in references:
            references.append(resolved_path)
    
    # Also try triple-quoted strings (less common but valid)
    for match in triple_quote_pattern.finditer(content):
        path = match.group(1) or match.group(2)
        
        if not path:
            continue
        
        if '{' in path or path.startswith('$'):
            continue
        
        resolved_path = _resolve_notebook_path(path, current_notebook_path)
        if resolved_path and resolved_path not in references:
            references.append(resolved_path)
    
    return references


def _resolve_notebook_path(path: str, current_notebook_path: str) -> Optional[str]:
    """
    Resolve a notebook path (relative or absolute) to an absolute workspace path.
    
    Args:
        path: The notebook path (may be relative)
        current_notebook_path: The path of the current notebook (for resolving relative paths)
    
    Returns:
        Resolved absolute path, or None if path cannot be resolved
    """
    if not path or not path.strip():
        return None
    
    path = path.strip()
    
    # Skip if it looks like a variable or f-string interpolation
    if '{' in path or path.startswith('$'):
        return None
    
    # Resolve relative paths
    if path.startswith('./') or path.startswith('../'):
        current_dir = os.path.dirname(current_notebook_path)
        resolved_path = os.path.normpath(os.path.join(current_dir, path))
    elif not path.startswith('/'):
        # Relative path without ./ prefix - treat as same directory
        current_dir = os.path.dirname(current_notebook_path)
        resolved_path = os.path.normpath(os.path.join(current_dir, path))
    else:
        # Absolute path
        resolved_path = path
    
    # Normalize path separators for workspace paths
    resolved_path = resolved_path.replace('\\', '/')
    
    return resolved_path


def resolve_notebook_dependencies(
    notebook_path: str, 
    visited: set = None,
    depth: int = 0,
    max_depth: int = 10,
    verbose: bool = False
) -> set:
    """
    Recursively resolve all notebook dependencies via %run and dbutils.notebook.run.
    
    Args:
        notebook_path: Path to the notebook to analyze
        visited: Set of already visited notebooks (prevents cycles)
        depth: Current recursion depth
        max_depth: Maximum recursion depth to prevent infinite loops
        verbose: Print debug information
    
    Returns:
        Set of all notebook paths that are dependencies (including the original)
    """
    if visited is None:
        visited = set()
    
    # Prevent cycles and infinite recursion
    if notebook_path in visited or depth > max_depth:
        return visited
    
    visited.add(notebook_path)
    
    try:
        content, file_type = export_notebook(notebook_path)
        
        if not content:
            return visited
        
        # Extract %run references
        run_refs = extract_run_references(content, notebook_path)
        
        # Extract dbutils.notebook.run references
        dbutils_refs = extract_dbutils_notebook_run_references(content, notebook_path)
        
        all_refs = list(set(run_refs + dbutils_refs))
        
        if verbose and all_refs:
            indent = "  " * depth
            print(f"{indent}📎 {notebook_path} references {len(all_refs)} notebook(s)")
        
        # Recursively resolve dependencies
        for ref_path in all_refs:
            if ref_path not in visited:
                resolve_notebook_dependencies(
                    ref_path, 
                    visited, 
                    depth + 1, 
                    max_depth,
                    verbose
                )
    
    except Exception as e:
        if verbose:
            print(f"  ⚠️ Could not resolve dependencies for {notebook_path}: {e}")
    
    return visited


def collect_job_notebook_dependencies(job_notebooks: List[str], verbose: bool = False) -> set:
    """
    Collect all notebooks that are directly or indirectly referenced by job tasks.
    
    Args:
        job_notebooks: List of notebook paths from job tasks
        verbose: Print progress information
    
    Returns:
        Set of all notebook paths including dependencies
    """
    all_notebooks = set()
    max_depth = CONFIG.get("max_nested_depth", 10)
    
    if verbose:
        print(f"  🔍 Resolving notebook dependencies (max depth: {max_depth})...")
    
    for notebook_path in job_notebooks:
        dependencies = resolve_notebook_dependencies(
            notebook_path,
            visited=set(),
            max_depth=max_depth,
            verbose=verbose
        )
        all_notebooks.update(dependencies)
    
    if verbose:
        direct_count = len(job_notebooks)
        total_count = len(all_notebooks)
        nested_count = total_count - direct_count
        print(f"  📊 Notebook dependency summary:")
        print(f"     - Direct job notebooks:    {direct_count}")
        print(f"     - Nested dependencies:     {nested_count}")
        print(f"     - Total notebooks to scan: {total_count}")
    
    return all_notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scanning Functions

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, ExportFormat
from databricks.sdk.service.jobs import JobSettings
import base64

# Initialize workspace client
w = WorkspaceClient()

def get_workspace_url() -> str:
    """Get the current workspace URL for generating links."""
    # Get from spark conf or context
    try:
        return spark.conf.get("spark.databricks.workspaceUrl")
    except:
        # Fallback: construct from notebook context
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        return ctx.browserHostName().get()

WORKSPACE_URL = f"https://{get_workspace_url()}"

def get_notebook_link(path: str) -> str:
    """Generate a clickable link to a notebook."""
    encoded_path = path.replace("/", "%2F").replace(" ", "%20")
    return f"{WORKSPACE_URL}/#workspace{encoded_path}"

def get_job_link(job_id: int) -> str:
    """Generate a clickable link to a job."""
    return f"{WORKSPACE_URL}/#job/{job_id}"

# COMMAND ----------

def scan_content_for_patterns(
    content: str, 
    file_type: str,
    patterns: List[BreakingChangePattern]
) -> List[Dict]:
    """
    Scan content for breaking change patterns.
    
    Returns list of findings with line numbers.
    """
    findings = []
    lines = content.split('\n')
    
    for pattern in patterns:
        # Check if pattern applies to this file type
        if file_type not in pattern.file_types:
            continue
        
        regex = re.compile(pattern.pattern, re.IGNORECASE)
        
        for line_num, line in enumerate(lines, 1):
            if regex.search(line):
                findings.append({
                    "breaking_change_id": pattern.id,
                    "breaking_change_name": pattern.name,
                    "severity": pattern.severity,
                    "introduced_in": pattern.introduced_in,
                    "line_number": line_num,
                    "line_content": line.strip()[:200],
                    "description": pattern.description,
                    "remediation": pattern.remediation,
                })
    
    return findings

# COMMAND ----------

def scan_duplicate_temp_views(content: str, file_type: str) -> List[Dict]:
    """
    Scan for temp view names that are reused multiple times.
    """
    if file_type not in ['.py', '.scala']:
        return []
    
    findings = []
    lines = content.split('\n')
    view_usages = {}
    
    skip_keywords = ['uuid', 'hex', 'random', 'unique', 'timestamp', 'datetime', 'now']
    
    temp_view_pattern = re.compile(
        r'(createOrReplaceTempView|createTempView|createGlobalTempView)\s*\(\s*'
        r'(?:'
        r'["\']([^"\']+)["\']'
        r'|'
        r'([a-zA-Z_][a-zA-Z0-9_]*)'
        r')\s*\)',
        re.IGNORECASE
    )
    
    for line_num, line in enumerate(lines, 1):
        if any(skip in line.lower() for skip in skip_keywords):
            continue
        if re.search(r'(createOrReplaceTempView|createTempView|createGlobalTempView)\s*\(\s*f["\']', line):
            continue
        
        for match in temp_view_pattern.finditer(line):
            view_name = match.group(2) or match.group(3)
            if not view_name or view_name in ['df', 'spark', 'self', 'result', 'data']:
                continue
            
            if view_name not in view_usages:
                view_usages[view_name] = []
            view_usages[view_name].append((line_num, line.strip()[:150]))
    
    for view_name, usages in view_usages.items():
        if len(usages) > 1:
            first_line = usages[0][0]
            for line_num, line_content in usages[1:]:
                findings.append({
                    "breaking_change_id": "BC-SC-002-DUP",
                    "breaking_change_name": f"Temp View '{view_name}' Reused",
                    "severity": "MEDIUM",
                    "introduced_in": "13.3",
                    "line_number": line_num,
                    "line_content": line_content,
                    "description": f"Temp view '{view_name}' reused (first on line {first_line}). Spark Connect uses name lookup.",
                    "remediation": f"Use unique view names: f\"{view_name}_{{uuid.uuid4()}}\"",
                })
    
    return findings

# COMMAND ----------

def get_file_type(path: str, language: str = None) -> str:
    """Determine file type from path or language."""
    if path.endswith('.py') or language == 'PYTHON':
        return '.py'
    elif path.endswith('.scala') or language == 'SCALA':
        return '.scala'
    elif path.endswith('.sql') or language == 'SQL':
        return '.sql'
    else:
        return '.unknown'

def export_notebook(path: str) -> Tuple[str, str]:
    """
    Export a notebook's content.
    
    Returns: (content, file_type)
    """
    try:
        # Get notebook info first
        obj = w.workspace.get_status(path)
        language = obj.language.value if obj.language else None
        
        # Export as source
        response = w.workspace.export(path=path, format=ExportFormat.SOURCE)
        content = base64.b64decode(response.content).decode('utf-8')
        
        file_type = get_file_type(path, language)
        return content, file_type
    except Exception as e:
        print(f"Warning: Could not export {path}: {e}")
        return None, None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Workflows (Jobs)

# COMMAND ----------

def scan_all_jobs(
    scan_id: str, 
    max_jobs: int = None, 
    dry_run: bool = False, 
    verbose: bool = True,
    filter_by_activity: bool = False,
    activity_days: int = 365,
    collect_notebooks_only: bool = False
) -> Tuple[List[Dict], List[str]]:
    """
    Scan all jobs in the workspace for breaking changes.
    
    Args:
        scan_id: Unique identifier for this scan (for checkpointing)
        max_jobs: Maximum number of jobs to scan (None for all)
        dry_run: If True, just count jobs without scanning content
        verbose: Print detailed progress
        filter_by_activity: If True, only scan jobs that have run within activity_days
        activity_days: Number of days to look back for job activity (default: 365)
        collect_notebooks_only: If True, only collect notebook paths without scanning
    
    Returns:
        Tuple of (scan_results, job_notebook_paths)
        - scan_results: List of finding dictionaries
        - job_notebook_paths: List of notebook paths from job tasks (for dependency resolution)
    """
    results = []
    job_notebook_paths = []  # Track all notebooks referenced by jobs
    checkpoint_batch = []
    
    print("Fetching jobs...")
    jobs = list(w.jobs.list())
    total_jobs = len(jobs)
    
    # Filter jobs by activity using system tables
    active_job_ids = None
    if filter_by_activity:
        print(f"  Querying system.lakeflow.job_run_timeline for jobs active in last {activity_days} days...")
        active_job_ids = get_active_job_ids_from_system_tables(activity_days)
        print_job_activity_summary(active_job_ids, total_jobs, activity_days)
        
        if active_job_ids is not None:
            # Filter jobs list to only include active jobs
            jobs = [j for j in jobs if str(j.job_id) in active_job_ids]
            print(f"  Filtered to {len(jobs)} active jobs")
    
    if max_jobs:
        jobs = jobs[:max_jobs]
        print(f"Found {total_jobs} jobs, limiting to {max_jobs} for testing")
    else:
        print(f"Found {len(jobs)} jobs to scan")
    
    if dry_run:
        print(f"[DRY RUN] Would scan {len(jobs)} jobs")
        return [], []
    
    # Get already completed jobs from checkpoint
    completed_jobs = get_completed_items(scan_id, "job")
    if completed_jobs:
        print(f"  ⏩ Resuming: {len(completed_jobs)} jobs already scanned")
    
    skipped = 0
    for idx, job in enumerate(jobs, 1):
        job_id_str = str(job.job_id)
        
        # Skip if already scanned (checkpoint) - but still collect notebook paths if in collect_only mode
        if job_id_str in completed_jobs and not collect_notebooks_only:
            skipped += 1
            continue
        
        try:
            # Get full job details
            job_details = w.jobs.get(job.job_id)
            job_name = job_details.settings.name if job_details.settings else f"Job {job.job_id}"
            
            if verbose and not collect_notebooks_only:
                resumed_info = f" (resumed, skipped {skipped})" if skipped > 0 and idx == skipped + 1 else ""
                print(f"  [{idx}/{len(jobs)}] Scanning job: {job_name[:50]}...{resumed_info}")
            elif verbose and collect_notebooks_only and idx == 1:
                print(f"  Collecting notebook paths from {len(jobs)} jobs...")
            
            # Get tasks
            tasks = job_details.settings.tasks if job_details.settings and job_details.settings.tasks else []
            
            for task in tasks:
                notebook_path = None
                
                # Check for notebook task
                if task.notebook_task:
                    notebook_path = task.notebook_task.notebook_path
                
                # Check for python file task
                elif task.spark_python_task:
                    # Can't easily scan python files in DBFS/Volumes
                    continue
                
                if notebook_path:
                    # Always collect notebook paths for dependency resolution
                    if notebook_path not in job_notebook_paths:
                        job_notebook_paths.append(notebook_path)
                    
                    # Skip actual scanning if we're only collecting notebook paths
                    if collect_notebooks_only:
                        continue
                    
                    content, file_type = export_notebook(notebook_path)
                    if content:
                        # Scan for patterns (filtered for migration path)
                        findings = scan_content_for_patterns(content, file_type, APPLICABLE_PATTERNS)
                        
                        # Scan for duplicate temp views
                        findings.extend(scan_duplicate_temp_views(content, file_type))
                        
                        if findings:
                            for finding in findings:
                                results.append({
                                    "scan_timestamp": datetime.now().isoformat(),
                                    "source_type": "JOB",
                                    "job_id": job.job_id,
                                    "job_name": job_name,
                                    "job_link": get_job_link(job.job_id),
                                    "task_name": task.task_key,
                                    "notebook_path": notebook_path,
                                    "notebook_link": get_notebook_link(notebook_path),
                                    **finding
                                })
                            if verbose:
                                print(f"    → Task '{task.task_key}': {len(findings)} findings")
                        elif CONFIG.get("track_clean_notebooks", True):
                            # Track clean notebooks with no issues
                            results.append({
                                "scan_timestamp": datetime.now().isoformat(),
                                "source_type": "JOB",
                                "job_id": job.job_id,
                                "job_name": job_name,
                                "job_link": get_job_link(job.job_id),
                                "task_name": task.task_key,
                                "notebook_path": notebook_path,
                                "notebook_link": get_notebook_link(notebook_path),
                                "breaking_change_id": "CLEAN",
                                "breaking_change_name": "No Issues Found",
                                "severity": "OK",
                                "introduced_in": None,
                                "line_number": None,
                                "line_content": None,
                                "description": "No breaking change patterns detected",
                                "remediation": "Ready for upgrade"
                            })
                            if verbose:
                                print(f"    → Task '{task.task_key}': ✅ No issues")
            
            # Save checkpoint for this job (skip if only collecting notebooks)
            if not collect_notebooks_only:
                save_checkpoint(scan_id, "job", job_id_str, job_name, "completed")
            
        except Exception as e:
            print(f"Warning: Could not scan job {job.job_id}: {e}")
            # Save failed checkpoint (skip if only collecting notebooks)
            if not collect_notebooks_only:
                save_checkpoint(scan_id, "job", job_id_str, str(job.job_id), "failed", str(e))
    
    if verbose and collect_notebooks_only:
        print(f"  Found {len(job_notebook_paths)} unique notebooks from job tasks")
    
    return results, job_notebook_paths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Job-Related Notebooks (Including Dependencies)

# COMMAND ----------

def scan_job_notebooks_with_dependencies(
    scan_id: str,
    job_notebooks: List[str],
    include_nested: bool = True,
    max_nested_depth: int = 10,
    dry_run: bool = False,
    verbose: bool = True
) -> List[Dict]:
    """
    Scan notebooks that are part of jobs, including nested dependencies via %run.
    
    This function:
    1. Takes the list of notebook paths from job tasks
    2. Resolves all %run and dbutils.notebook.run dependencies
    3. Scans all notebooks (direct and nested) for breaking changes
    
    Args:
        scan_id: Unique identifier for this scan (for checkpointing)
        job_notebooks: List of notebook paths from job tasks
        include_nested: If True, include notebooks referenced via %run
        max_nested_depth: Maximum depth for dependency resolution
        dry_run: If True, just count notebooks without scanning
        verbose: Print detailed progress
    
    Returns:
        List of finding dictionaries
    """
    results = []
    
    print(f"Processing {len(job_notebooks)} direct job notebooks...")
    
    # Resolve dependencies if enabled
    if include_nested:
        all_notebooks = collect_job_notebook_dependencies(job_notebooks, verbose)
        
        # Separate direct and nested for reporting
        direct_notebooks = set(job_notebooks)
        nested_notebooks = all_notebooks - direct_notebooks
        
        if verbose:
            print(f"  Total notebooks to scan: {len(all_notebooks)}")
            print(f"    - Direct job notebooks: {len(direct_notebooks)}")
            print(f"    - Nested dependencies:  {len(nested_notebooks)}")
    else:
        all_notebooks = set(job_notebooks)
    
    if dry_run:
        print(f"[DRY RUN] Would scan {len(all_notebooks)} job-related notebooks")
        return []
    
    # Get already completed notebooks from checkpoint
    completed_notebooks = get_completed_items(scan_id, "job_notebook")
    if completed_notebooks:
        print(f"  ⏩ Resuming: {len(completed_notebooks)} notebooks already scanned")
    
    # Convert to list for indexing
    notebooks_to_scan = list(all_notebooks - completed_notebooks)
    total_to_scan = len(notebooks_to_scan)
    
    for idx, notebook_path in enumerate(notebooks_to_scan, 1):
        if verbose:
            is_nested = notebook_path not in job_notebooks
            nested_indicator = " (nested)" if is_nested else ""
            print(f"  [{idx}/{total_to_scan}] Scanning{nested_indicator}: {notebook_path[:60]}...")
        
        try:
            content, file_type = export_notebook(notebook_path)
            
            if not content:
                continue
            
            # Determine source type (direct job notebook or nested dependency)
            is_direct = notebook_path in job_notebooks
            source_type = "JOB" if is_direct else "JOB_DEPENDENCY"
            
            # Scan for patterns
            findings = scan_content_for_patterns(content, file_type, APPLICABLE_PATTERNS)
            findings.extend(scan_duplicate_temp_views(content, file_type))
            
            if findings:
                for finding in findings:
                    results.append({
                        "scan_timestamp": datetime.now().isoformat(),
                        "source_type": source_type,
                        "job_id": None,  # Could be multiple jobs referencing this
                        "job_name": None,
                        "job_link": None,
                        "task_name": None,
                        "notebook_path": notebook_path,
                        "notebook_link": get_notebook_link(notebook_path),
                        **finding
                    })
                if verbose:
                    print(f"    → {len(findings)} findings")
            elif CONFIG.get("track_clean_notebooks", True):
                results.append({
                    "scan_timestamp": datetime.now().isoformat(),
                    "source_type": source_type,
                    "job_id": None,
                    "job_name": None,
                    "job_link": None,
                    "task_name": None,
                    "notebook_path": notebook_path,
                    "notebook_link": get_notebook_link(notebook_path),
                    "breaking_change_id": "CLEAN",
                    "breaking_change_name": "No Issues Found",
                    "severity": "OK",
                    "introduced_in": None,
                    "line_number": None,
                    "line_content": None,
                    "description": "No breaking change patterns detected",
                    "remediation": "Ready for upgrade"
                })
                if verbose:
                    print(f"    → ✅ No issues")
            
            # Save checkpoint
            save_checkpoint(scan_id, "job_notebook", notebook_path, notebook_path, "completed")
            
        except Exception as e:
            print(f"Warning: Could not scan notebook {notebook_path}: {e}")
            save_checkpoint(scan_id, "job_notebook", notebook_path, notebook_path, "failed", str(e))
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Workspace Notebooks

# COMMAND ----------

def scan_workspace_path(
    scan_id: str,
    root_path: str, 
    exclude_paths: List[str],
    max_notebooks: int = None,
    dry_run: bool = False,
    verbose: bool = True,
    filter_to_notebooks: set = None
) -> List[Dict]:
    """
    Recursively scan a workspace path for notebooks with breaking changes.
    
    Args:
        scan_id: Unique identifier for this scan (for checkpointing)
        root_path: Workspace path to scan
        exclude_paths: Paths to exclude from scanning
        max_notebooks: Maximum notebooks to scan (None for all)
        dry_run: If True, just count notebooks without scanning content
        verbose: Print detailed progress
        filter_to_notebooks: If provided, only scan notebooks in this set (for jobs_only_mode)
    """
    results = []
    notebooks_scanned = [0]  # Use list to allow modification in nested function
    notebooks_found = [0]
    notebooks_filtered_out = [0]  # Track notebooks skipped due to filter
    
    # Get already completed notebooks from checkpoint
    completed_notebooks = get_completed_items(scan_id, "notebook")
    if completed_notebooks:
        print(f"  ⏩ Resuming: {len(completed_notebooks)} notebooks already scanned")
    
    if filter_to_notebooks is not None:
        print(f"  🔍 Filtering: Only scanning {len(filter_to_notebooks)} job-related notebooks")
    
    def should_exclude(path: str) -> bool:
        return any(path.startswith(excl) for excl in exclude_paths)
    
    def should_filter_out(path: str) -> bool:
        """Check if notebook should be filtered out (not in job-related set)."""
        if filter_to_notebooks is None:
            return False
        return path not in filter_to_notebooks
    
    def scan_directory(path: str):
        # Check if we've hit the limit
        if max_notebooks and notebooks_scanned[0] >= max_notebooks:
            return
            
        try:
            objects = w.workspace.list(path)
            for obj in objects:
                # Check limit again inside loop
                if max_notebooks and notebooks_scanned[0] >= max_notebooks:
                    return
                    
                if should_exclude(obj.path):
                    continue
                
                if obj.object_type == ObjectType.DIRECTORY:
                    scan_directory(obj.path)
                    
                elif obj.object_type == ObjectType.NOTEBOOK:
                    notebooks_found[0] += 1
                    
                    # Skip if not in filter set (jobs_only_mode)
                    if should_filter_out(obj.path):
                        notebooks_filtered_out[0] += 1
                        continue
                    
                    # Skip if already scanned (checkpoint)
                    if obj.path in completed_notebooks:
                        continue
                    
                    if dry_run:
                        if verbose:
                            print(f"  [DRY RUN] Would scan: {obj.path}")
                        continue
                    
                    notebooks_scanned[0] += 1
                    
                    if verbose:
                        limit_info = f"/{max_notebooks}" if max_notebooks else ""
                        print(f"  [{notebooks_scanned[0]}{limit_info}] Scanning: {obj.path[:60]}...")
                    
                    try:
                        content, file_type = export_notebook(obj.path)
                        if content:
                            findings = scan_content_for_patterns(content, file_type, APPLICABLE_PATTERNS)
                            findings.extend(scan_duplicate_temp_views(content, file_type))
                            
                            if findings:
                                for finding in findings:
                                    results.append({
                                        "scan_timestamp": datetime.now().isoformat(),
                                        "source_type": "WORKSPACE",
                                        "job_id": None,
                                        "job_name": None,
                                        "job_link": None,
                                        "task_name": None,
                                        "notebook_path": obj.path,
                                        "notebook_link": get_notebook_link(obj.path),
                                        **finding
                                    })
                                if verbose:
                                    print(f"    → {len(findings)} findings")
                            elif CONFIG.get("track_clean_notebooks", True):
                                # Track clean notebooks with no issues
                                results.append({
                                    "scan_timestamp": datetime.now().isoformat(),
                                    "source_type": "WORKSPACE",
                                    "job_id": None,
                                    "job_name": None,
                                    "job_link": None,
                                    "task_name": None,
                                    "notebook_path": obj.path,
                                    "notebook_link": get_notebook_link(obj.path),
                                    "breaking_change_id": "CLEAN",
                                    "breaking_change_name": "No Issues Found",
                                    "severity": "OK",
                                    "introduced_in": None,
                                    "line_number": None,
                                    "line_content": None,
                                    "description": "No breaking change patterns detected",
                                    "remediation": "Ready for upgrade"
                                })
                                if verbose:
                                    print(f"    → ✅ No issues")
                        
                        # Save checkpoint for this notebook
                        save_checkpoint(scan_id, "notebook", obj.path, obj.path, "completed")
                        
                    except Exception as nb_error:
                        print(f"Warning: Could not scan notebook {obj.path}: {nb_error}")
                        save_checkpoint(scan_id, "notebook", obj.path, obj.path, "failed", str(nb_error))
                            
        except Exception as e:
            print(f"Warning: Could not scan {path}: {e}")
    
    print(f"Scanning workspace path: {root_path}")
    if max_notebooks:
        print(f"  (Limited to {max_notebooks} notebooks for testing)")
    
    scan_directory(root_path)
    
    if dry_run:
        print(f"[DRY RUN] Found {notebooks_found[0]} notebooks in {root_path}")
        if filter_to_notebooks is not None:
            print(f"[DRY RUN] Would filter to {len(filter_to_notebooks)} job-related notebooks")
    
    # Print filtering stats if applicable
    if filter_to_notebooks is not None and notebooks_filtered_out[0] > 0:
        print(f"  📊 Filtering summary:")
        print(f"     - Total notebooks found:      {notebooks_found[0]}")
        print(f"     - Filtered out (standalone):  {notebooks_filtered_out[0]}")
        print(f"     - Scanned (job-related):      {notebooks_scanned[0]}")
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Scan

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Define schema for results
RESULT_SCHEMA = StructType([
    StructField("scan_timestamp", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("job_id", LongType(), True),
    StructField("job_name", StringType(), True),
    StructField("job_link", StringType(), True),
    StructField("task_name", StringType(), True),
    StructField("notebook_path", StringType(), True),
    StructField("notebook_link", StringType(), True),
    StructField("breaking_change_id", StringType(), True),
    StructField("breaking_change_name", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("introduced_in", StringType(), True),
    StructField("line_number", IntegerType(), True),
    StructField("line_content", StringType(), True),
    StructField("description", StringType(), True),
    StructField("remediation", StringType(), True),
])

# COMMAND ----------

# Generate unique scan ID (or reuse existing for resume)
import uuid
SCAN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

# Filter patterns for the migration path (source → target)
source_version = CONFIG.get("source_dbr_version", "13.3")
target_version = CONFIG.get("target_dbr_version", "17.3")

APPLICABLE_PATTERNS = filter_patterns_for_migration(BREAKING_PATTERNS, source_version, target_version)

print(f"Migration path: DBR {source_version} → {target_version}")
print(f"Patterns applicable: {len(APPLICABLE_PATTERNS)} of {len(BREAKING_PATTERNS)} total")
print(f"Skipped patterns (already working on {source_version}): {len(BREAKING_PATTERNS) - len(APPLICABLE_PATTERNS)}")

# Run the scan
all_results = []
job_notebook_paths = []  # Track notebooks from jobs for dependency resolution

print("=" * 60)
print("DBR MIGRATION - WORKSPACE PROFILER")
print("=" * 60)
print(f"Migration Path: DBR {CONFIG['source_dbr_version']} → {CONFIG['target_dbr_version']}")
print(f"Scan ID: {SCAN_ID}")
print(f"Scan started: {datetime.now().isoformat()}")

# Show mode configuration
print()
print("📋 SCAN CONFIGURATION:")
if CONFIG.get("jobs_only_mode"):
    print("   - Mode: JOBS ONLY (no standalone notebooks)")
else:
    print("   - Mode: Full workspace scan")

if CONFIG.get("filter_jobs_by_activity"):
    print(f"   - Job activity filter: Last {CONFIG.get('job_activity_days', 365)} days")
else:
    print("   - Job activity filter: DISABLED (scanning all jobs)")

if CONFIG.get("include_nested_notebooks"):
    print(f"   - Nested notebooks (%run): ENABLED (max depth: {CONFIG.get('max_nested_depth', 10)})")
else:
    print("   - Nested notebooks (%run): DISABLED")

# Show limits if set
if CONFIG.get("max_jobs") or CONFIG.get("max_notebooks") or CONFIG.get("dry_run"):
    print()
    print("⚠️  TESTING MODE:")
    if CONFIG.get("dry_run"):
        print("   - DRY RUN enabled (no actual scanning)")
    if CONFIG.get("max_jobs"):
        print(f"   - Max jobs: {CONFIG['max_jobs']}")
    if CONFIG.get("max_notebooks"):
        print(f"   - Max notebooks per path: {CONFIG['max_notebooks']}")

# Initialize checkpointing
if CONFIG.get("enable_checkpointing"):
    print()
    print("📍 CHECKPOINTING ENABLED:")
    print(f"   - Checkpoint table: {get_checkpoint_table_name()}")
    print(f"   - Scan ID: {SCAN_ID}")
    print("   - To resume a failed scan, set SCAN_ID to the previous scan's ID")
    initialize_checkpoint_table()
    
    # Show resume info if there's existing progress
    progress = get_scan_progress(SCAN_ID)
    if progress:
        print(f"   - Existing progress found: {progress}")
print()

# Collect job notebooks first if in jobs_only_mode (for filtering workspace scan)
job_related_notebooks = None
if CONFIG.get("jobs_only_mode") or CONFIG.get("include_nested_notebooks"):
    print("COLLECTING JOB NOTEBOOKS...")
    print("-" * 40)
    _, job_notebook_paths = scan_all_jobs(
        scan_id=SCAN_ID,
        max_jobs=CONFIG.get("max_jobs"),
        dry_run=False,  # Need to collect notebooks even in dry_run
        verbose=CONFIG.get("verbose", True),
        filter_by_activity=CONFIG.get("filter_jobs_by_activity", False),
        activity_days=CONFIG.get("job_activity_days", 365),
        collect_notebooks_only=True  # Just collect paths, don't scan yet
    )
    
    # Resolve dependencies if enabled
    if CONFIG.get("include_nested_notebooks") and job_notebook_paths:
        print()
        print("RESOLVING NOTEBOOK DEPENDENCIES...")
        print("-" * 40)
        job_related_notebooks = collect_job_notebook_dependencies(
            job_notebook_paths, 
            verbose=CONFIG.get("verbose", True)
        )
    else:
        job_related_notebooks = set(job_notebook_paths)
    print()

# Scan jobs
if CONFIG["scan_jobs"]:
    print("SCANNING JOBS...")
    print("-" * 40)
    job_results, _ = scan_all_jobs(
        scan_id=SCAN_ID,
        max_jobs=CONFIG.get("max_jobs"),
        dry_run=CONFIG.get("dry_run", False),
        verbose=CONFIG.get("verbose", True),
        filter_by_activity=CONFIG.get("filter_jobs_by_activity", False),
        activity_days=CONFIG.get("job_activity_days", 365),
        collect_notebooks_only=False  # Actually scan this time
    )
    all_results.extend(job_results)
    print(f"Jobs scan complete: {len(job_results)} findings")
    print()

# Scan job-related notebooks with dependencies (if enabled)
if CONFIG.get("include_nested_notebooks") and job_related_notebooks:
    print("SCANNING JOB-RELATED NOTEBOOKS (with dependencies)...")
    print("-" * 40)
    # Get notebooks that weren't already scanned as direct job tasks
    # (those are scanned with job context in scan_all_jobs)
    already_scanned_in_jobs = set(job_notebook_paths)
    nested_notebooks = job_related_notebooks - already_scanned_in_jobs
    
    if nested_notebooks:
        nested_results = scan_job_notebooks_with_dependencies(
            scan_id=SCAN_ID,
            job_notebooks=list(nested_notebooks),
            include_nested=False,  # Already resolved, no need to recurse
            dry_run=CONFIG.get("dry_run", False),
            verbose=CONFIG.get("verbose", True)
        )
        all_results.extend(nested_results)
        print(f"Nested notebooks scan complete: {len(nested_results)} findings")
    else:
        print("  No additional nested notebooks to scan")
    print()

# Scan workspace
if CONFIG["scan_workspace"]:
    # Determine filter based on jobs_only_mode
    notebook_filter = None
    if CONFIG.get("jobs_only_mode"):
        notebook_filter = job_related_notebooks
        print("SCANNING WORKSPACE NOTEBOOKS (job-related only)...")
    else:
        print("SCANNING WORKSPACE NOTEBOOKS...")
    print("-" * 40)
    
    for path in CONFIG["workspace_paths"]:
        workspace_results = scan_workspace_path(
            scan_id=SCAN_ID,
            root_path=path, 
            exclude_paths=CONFIG["exclude_paths"],
            max_notebooks=CONFIG.get("max_notebooks"),
            dry_run=CONFIG.get("dry_run", False),
            verbose=CONFIG.get("verbose", True),
            filter_to_notebooks=notebook_filter
        )
        all_results.extend(workspace_results)
    
    workspace_finding_count = len([r for r in all_results if r.get('source_type') in ('WORKSPACE', 'JOB_DEPENDENCY')])
    print(f"Workspace scan complete: {workspace_finding_count} findings")
    print()

# Calculate summary stats
clean_count = len([r for r in all_results if r.get("severity") == "OK"])
issue_count = len([r for r in all_results if r.get("severity") != "OK"])
total_notebooks = len(set(r["notebook_path"] for r in all_results))
clean_notebooks = len(set(r["notebook_path"] for r in all_results if r.get("severity") == "OK"))
notebooks_with_issues = total_notebooks - clean_notebooks

print("=" * 60)
print("SCAN SUMMARY")
print("=" * 60)
print(f"Total notebooks scanned: {total_notebooks}")
print(f"  ✅ Clean (no issues):  {clean_notebooks}")
print(f"  ⚠️  With issues:        {notebooks_with_issues}")
print()
print(f"Total records: {len(all_results)}")
print(f"  - Issue findings: {issue_count}")
print(f"  - Clean records:  {clean_count}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# Convert to DataFrame
if all_results:
    results_df = spark.createDataFrame(all_results, schema=RESULT_SCHEMA)
else:
    results_df = spark.createDataFrame([], schema=RESULT_SCHEMA)

# Show summary
print("FINDINGS BY SEVERITY:")
results_df.groupBy("severity").count().orderBy("severity").show()

print("FINDINGS BY BREAKING CHANGE:")
results_df.groupBy("breaking_change_id", "breaking_change_name").count().orderBy("count", ascending=False).show(50, truncate=False)

print("TOP NOTEBOOKS BY FINDINGS:")
results_df.groupBy("notebook_path").count().orderBy("count", ascending=False).show(20, truncate=False)

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONFIG['output_catalog']}.{CONFIG['output_schema']}")

# Save to Delta table
output_table = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_table']}"
print(f"Saving results to: {output_table}")

# Add scan metadata
from pyspark.sql.functions import lit, current_timestamp

results_with_metadata = results_df \
    .withColumn("scan_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .withColumn("target_dbr_version", lit(CONFIG["target_dbr_version"]))

# Truncate table before writing if configured (avoids duplicates on re-runs)
if CONFIG.get("truncate_on_scan", False):
    try:
        spark.sql(f"TRUNCATE TABLE {output_table}")
        print(f"🗑️ Truncated existing table: {output_table}")
    except Exception as e:
        # Table might not exist yet, that's OK
        print(f"ℹ️ Table {output_table} will be created (truncate skipped: {e})")

# Write to Delta
write_mode = "overwrite" if CONFIG.get("truncate_on_scan", False) else "append"
results_with_metadata.write \
    .format("delta") \
    .mode(write_mode) \
    .option("mergeSchema", "true") \
    .saveAsTable(output_table)

print(f"✅ Saved {results_df.count()} findings to {output_table}")

# COMMAND ----------

# Optional: Export to CSV
if CONFIG["export_csv"]:
    print(f"Exporting to CSV: {CONFIG['csv_path']}")
    
    results_df.coalesce(1).write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save(CONFIG["csv_path"])
    
    print(f"✅ CSV exported to {CONFIG['csv_path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Results

# COMMAND ----------

# View latest scan results using CONFIG values
output_table = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_table']}"

latest_results_df = spark.sql(f"""
SELECT 
  severity,
  breaking_change_id,
  breaking_change_name,
  notebook_path,
  notebook_link,
  job_name,
  line_number,
  remediation
FROM {output_table}
WHERE scan_id = (SELECT MAX(scan_id) FROM {output_table})
ORDER BY 
  CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
  breaking_change_id,
  notebook_path
""")

display(latest_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Dashboard Query
# MAGIC 
# MAGIC Use this SQL in a Databricks SQL Dashboard:

# COMMAND ----------

# Dashboard Query: Breaking Changes Summary using CONFIG values
output_table = f"{CONFIG['output_catalog']}.{CONFIG['output_schema']}.{CONFIG['output_table']}"

summary_df = spark.sql(f"""
WITH latest_scan AS (
  SELECT MAX(scan_id) as scan_id 
  FROM {output_table}
)
SELECT 
  severity,
  breaking_change_id,
  breaking_change_name,
  COUNT(*) as occurrence_count,
  COUNT(DISTINCT notebook_path) as affected_notebooks,
  COUNT(DISTINCT job_id) as affected_jobs,
  FIRST(remediation) as remediation
FROM {output_table}
WHERE scan_id = (SELECT scan_id FROM latest_scan)
GROUP BY severity, breaking_change_id, breaking_change_name
ORDER BY 
  CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
  occurrence_count DESC
""")

display(summary_df)

# Print the raw SQL for use in Databricks SQL Dashboard
print("=" * 60)
print("SQL for Databricks SQL Dashboard (copy/paste):")
print("=" * 60)
print(f"""
WITH latest_scan AS (
  SELECT MAX(scan_id) as scan_id 
  FROM {output_table}
)
SELECT 
  severity,
  breaking_change_id,
  breaking_change_name,
  COUNT(*) as occurrence_count,
  COUNT(DISTINCT notebook_path) as affected_notebooks,
  COUNT(DISTINCT job_id) as affected_jobs,
  FIRST(remediation) as remediation
FROM {output_table}
WHERE scan_id = (SELECT scan_id FROM latest_scan)
GROUP BY severity, breaking_change_id, breaking_change_name
ORDER BY 
  CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
  occurrence_count DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Detailed Report

# COMMAND ----------

# Generate a detailed HTML report
def generate_html_report(df) -> str:
    """Generate an HTML report from the scan results."""
    
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>DBR Migration Scan Report</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; }
            h1 { color: #1b3a57; }
            h2 { color: #2c5282; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px; }
            table { border-collapse: collapse; width: 100%; margin: 20px 0; }
            th, td { border: 1px solid #e2e8f0; padding: 12px; text-align: left; }
            th { background-color: #f7fafc; }
            .severity-HIGH { background-color: #fed7d7; color: #c53030; font-weight: bold; }
            .severity-MEDIUM { background-color: #fefcbf; color: #975a16; }
            .severity-LOW { background-color: #c6f6d5; color: #276749; }
            a { color: #3182ce; text-decoration: none; }
            a:hover { text-decoration: underline; }
            .summary { display: flex; gap: 20px; margin: 20px 0; }
            .summary-card { padding: 20px; border-radius: 8px; flex: 1; }
            .card-high { background: #fed7d7; }
            .card-medium { background: #fefcbf; }
            .card-low { background: #c6f6d5; }
            .card-number { font-size: 36px; font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>🔍 DBR Migration Scan Report</h1>
        <p>Generated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</p>
    """
    
    # Summary cards
    counts = df.groupBy("severity").count().collect()
    count_dict = {row["severity"]: row["count"] for row in counts}
    
    # Count clean notebooks
    clean_count = count_dict.get('OK', 0)
    
    html += f"""
        <div class="summary">
            <div class="summary-card card-high">
                <div class="card-number">{count_dict.get('HIGH', 0)}</div>
                <div>HIGH Severity</div>
            </div>
            <div class="summary-card card-medium">
                <div class="card-number">{count_dict.get('MEDIUM', 0)}</div>
                <div>MEDIUM Severity</div>
            </div>
            <div class="summary-card card-low">
                <div class="card-number">{count_dict.get('LOW', 0)}</div>
                <div>LOW Severity</div>
            </div>
            <div class="summary-card" style="background: linear-gradient(135deg, #28a745, #20c997);">
                <div class="card-number">{clean_count}</div>
                <div>✅ Clean (No Issues)</div>
            </div>
        </div>
    """
    
    # Filter for issues (exclude clean notebooks)
    issues_df = df.filter("severity != 'OK'")
    clean_df = df.filter("severity = 'OK'")
    
    # Detailed findings table (issues only)
    html += """
        <h2>⚠️ Notebooks With Issues</h2>
        <table>
            <tr>
                <th>Severity</th>
                <th>Breaking Change</th>
                <th>Notebook</th>
                <th>Line</th>
                <th>Remediation</th>
            </tr>
    """
    
    for row in issues_df.orderBy("severity", "notebook_path").collect():
        html += f"""
            <tr>
                <td class="severity-{row['severity']}">{row['severity']}</td>
                <td>{row['breaking_change_id']}: {row['breaking_change_name']}</td>
                <td><a href="{row['notebook_link']}" target="_blank">{row['notebook_path']}</a></td>
                <td>{row['line_number']}</td>
                <td>{row['remediation']}</td>
            </tr>
        """
    
    html += """
        </table>
    """
    
    # Clean notebooks section
    if clean_count > 0:
        html += """
        <h2>✅ Clean Notebooks (Ready for Upgrade)</h2>
        <table>
            <tr>
                <th>Notebook</th>
                <th>Status</th>
            </tr>
        """
        
        for row in clean_df.select("notebook_path", "notebook_link").distinct().orderBy("notebook_path").collect():
            html += f"""
            <tr>
                <td><a href="{row['notebook_link']}" target="_blank">{row['notebook_path']}</a></td>
                <td style="color: #28a745; font-weight: bold;">✅ No Issues Found</td>
            </tr>
            """
        
        html += """
        </table>
        """
    
    html += """
    </body>
    </html>
    """
    
    return html

# Generate and save HTML report
if all_results:
    html_report = generate_html_report(results_df)
    
    # Use CONFIG csv_path to derive HTML path
    if CONFIG.get("export_csv") and CONFIG.get("csv_path"):
        html_path = CONFIG["csv_path"].replace(".csv", ".html")
    else:
        # Default path using CONFIG catalog/schema
        html_path = f"/Volumes/{CONFIG['output_catalog']}/{CONFIG['output_schema']}/exports/scan_report.html"
    
    # Save to Unity Catalog Volume (no path conversion needed for /Volumes/ paths)
    dbutils.fs.put(html_path, html_report, overwrite=True)
    print(f"✅ HTML report saved to: {html_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Review HIGH severity findings first** - These will cause immediate failures
# MAGIC 2. **Review MEDIUM severity findings** - These may cause issues in some scenarios
# MAGIC 3. **Review LOW severity findings** - These are informational/best practices
# MAGIC 
# MAGIC Use the DBR Migration Agent Skill in Databricks Assistant to:
# MAGIC - Scan individual notebooks
# MAGIC - Apply automatic fixes
# MAGIC - Validate changes
# MAGIC 
# MAGIC See: [DBR Migration Agent Skill](https://learn.microsoft.com/en-us/azure/databricks/assistant/skills)
