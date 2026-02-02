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
# MAGIC ## Usage
# MAGIC 1. Configure the parameters below
# MAGIC 2. Run all cells
# MAGIC 3. Review results in the output table
# MAGIC 4. **Important:** Manual review still required for behavioral changes not detectable by pattern matching

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC ### Quick Testing Examples
# MAGIC 
# MAGIC **Test with just 5 jobs and 10 notebooks:**
# MAGIC ```python
# MAGIC CONFIG["max_jobs"] = 5
# MAGIC CONFIG["max_notebooks"] = 10
# MAGIC ```
# MAGIC 
# MAGIC **Dry run to count files without scanning:**
# MAGIC ```python
# MAGIC CONFIG["dry_run"] = True
# MAGIC ```
# MAGIC 
# MAGIC **Scan specific folder only:**
# MAGIC ```python
# MAGIC CONFIG["scan_jobs"] = False
# MAGIC CONFIG["workspace_paths"] = ["/Users/your.name/project"]
# MAGIC CONFIG["max_notebooks"] = 20
# MAGIC ```
# MAGIC 
# MAGIC ### Checkpointing (Resume Failed Scans)
# MAGIC 
# MAGIC **Enable checkpointing (default):**
# MAGIC ```python
# MAGIC CONFIG["enable_checkpointing"] = True
# MAGIC CONFIG["checkpoint_table"] = "scan_checkpoint"  # Table in same catalog.schema
# MAGIC ```
# MAGIC 
# MAGIC **Resume a failed scan:**
# MAGIC ```python
# MAGIC # Find the scan_id from the failed run's output, then set it before running:
# MAGIC SCAN_ID = "20260125_143022"  # Replace with actual scan_id from failed run
# MAGIC ```
# MAGIC 
# MAGIC **Clear checkpoints and start fresh:**
# MAGIC ```python
# MAGIC clear_checkpoint()  # Clears all
# MAGIC # or
# MAGIC clear_checkpoint("20260125_143022")  # Clears specific scan
# MAGIC ```

# COMMAND ----------

# Configuration - Modify these as needed
CONFIG = {
    # Output settings
    "output_catalog": "main",           # Unity Catalog name
    "output_schema": "dbr_migration",   # Schema name
    "output_table": "scan_results",     # Table name
    "truncate_on_scan": True,           # Truncate table before each scan (avoids duplicates on re-runs)
    
    # Optional CSV export
    "export_csv": True,
    "csv_path": "/Volumes/main/dbr_migration/exports/scan_results.csv",
    
    # Scan scope
    "scan_jobs": True,                  # Scan job notebooks
    "scan_workspace": True,             # Scan workspace notebooks
    "workspace_paths": ["/"],           # Paths to scan (use ["/"] for entire workspace)
    
    # Filtering
    "exclude_paths": [
        "/Repos",                       # Exclude Repos (usually version controlled)
        "/Shared/Archive",              # Exclude archived notebooks
    ],
    "file_extensions": [".py", ".sql", ".scala"],
    
    # Target DBR version
    "target_dbr_version": "17.3",
    
    # ============================================================
    # TESTING/DEVELOPMENT LIMITS (set to None for full scan)
    # ============================================================
    "max_jobs": None,                   # Max jobs to scan (e.g., 5 for testing, None for all)
    "max_notebooks": None,              # Max notebooks to scan per workspace path (e.g., 10 for testing)
    "dry_run": False,                   # If True, just count files without scanning content
    "verbose": True,                    # Print detailed progress
    
    # ============================================================
    # CHECKPOINTING (for resumable scans)
    # ============================================================
    "enable_checkpointing": True,       # Enable checkpoint to resume failed scans
    "checkpoint_table": "scan_checkpoint",  # Table name for checkpoints (in same catalog.schema)
    "checkpoint_batch_size": 10,        # Save checkpoint every N items scanned
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Breaking Change Patterns

# COMMAND ----------

import re
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

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

def scan_all_jobs(scan_id: str, max_jobs: int = None, dry_run: bool = False, verbose: bool = True) -> List[Dict]:
    """
    Scan all jobs in the workspace for breaking changes.
    
    Args:
        scan_id: Unique identifier for this scan (for checkpointing)
        max_jobs: Maximum number of jobs to scan (None for all)
        dry_run: If True, just count jobs without scanning content
        verbose: Print detailed progress
    """
    results = []
    checkpoint_batch = []
    
    print("Fetching jobs...")
    jobs = list(w.jobs.list())
    total_jobs = len(jobs)
    
    if max_jobs:
        jobs = jobs[:max_jobs]
        print(f"Found {total_jobs} jobs, limiting to {max_jobs} for testing")
    else:
        print(f"Found {total_jobs} jobs")
    
    if dry_run:
        print(f"[DRY RUN] Would scan {len(jobs)} jobs")
        return []
    
    # Get already completed jobs from checkpoint
    completed_jobs = get_completed_items(scan_id, "job")
    if completed_jobs:
        print(f"  ⏩ Resuming: {len(completed_jobs)} jobs already scanned")
    
    skipped = 0
    for idx, job in enumerate(jobs, 1):
        job_id_str = str(job.job_id)
        
        # Skip if already scanned (checkpoint)
        if job_id_str in completed_jobs:
            skipped += 1
            continue
        
        try:
            # Get full job details
            job_details = w.jobs.get(job.job_id)
            job_name = job_details.settings.name if job_details.settings else f"Job {job.job_id}"
            
            if verbose:
                resumed_info = f" (resumed, skipped {skipped})" if skipped > 0 and idx == skipped + 1 else ""
                print(f"  [{idx}/{len(jobs)}] Scanning job: {job_name[:50]}...{resumed_info}")
            
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
                    content, file_type = export_notebook(notebook_path)
                    if content:
                        # Scan for patterns
                        findings = scan_content_for_patterns(content, file_type, BREAKING_PATTERNS)
                        
                        # Scan for duplicate temp views
                        findings.extend(scan_duplicate_temp_views(content, file_type))
                        
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
                        
                        if findings and verbose:
                            print(f"    → Task '{task.task_key}': {len(findings)} findings")
            
            # Save checkpoint for this job
            save_checkpoint(scan_id, "job", job_id_str, job_name, "completed")
            
        except Exception as e:
            print(f"Warning: Could not scan job {job.job_id}: {e}")
            # Save failed checkpoint
            save_checkpoint(scan_id, "job", job_id_str, str(job.job_id), "failed", str(e))
    
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
    verbose: bool = True
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
    """
    results = []
    notebooks_scanned = [0]  # Use list to allow modification in nested function
    notebooks_found = [0]
    
    # Get already completed notebooks from checkpoint
    completed_notebooks = get_completed_items(scan_id, "notebook")
    if completed_notebooks:
        print(f"  ⏩ Resuming: {len(completed_notebooks)} notebooks already scanned")
    
    def should_exclude(path: str) -> bool:
        return any(path.startswith(excl) for excl in exclude_paths)
    
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
                            findings = scan_content_for_patterns(content, file_type, BREAKING_PATTERNS)
                            findings.extend(scan_duplicate_temp_views(content, file_type))
                            
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
                            
                            if findings and verbose:
                                print(f"    → {len(findings)} findings")
                        
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

# Run the scan
all_results = []

print("=" * 60)
print("DBR MIGRATION - WORKSPACE PROFILER")
print("=" * 60)
print(f"Target DBR Version: {CONFIG['target_dbr_version']}")
print(f"Scan ID: {SCAN_ID}")
print(f"Scan started: {datetime.now().isoformat()}")

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

# Scan jobs
if CONFIG["scan_jobs"]:
    print("SCANNING JOBS...")
    print("-" * 40)
    job_results = scan_all_jobs(
        scan_id=SCAN_ID,
        max_jobs=CONFIG.get("max_jobs"),
        dry_run=CONFIG.get("dry_run", False),
        verbose=CONFIG.get("verbose", True)
    )
    all_results.extend(job_results)
    print(f"Jobs scan complete: {len(job_results)} findings")
    print()

# Scan workspace
if CONFIG["scan_workspace"]:
    print("SCANNING WORKSPACE NOTEBOOKS...")
    print("-" * 40)
    for path in CONFIG["workspace_paths"]:
        workspace_results = scan_workspace_path(
            scan_id=SCAN_ID,
            root_path=path, 
            exclude_paths=CONFIG["exclude_paths"],
            max_notebooks=CONFIG.get("max_notebooks"),
            dry_run=CONFIG.get("dry_run", False),
            verbose=CONFIG.get("verbose", True)
        )
        all_results.extend(workspace_results)
    print(f"Workspace scan complete: {len([r for r in all_results if r['source_type'] == 'WORKSPACE'])} findings")
    print()

print("=" * 60)
print(f"TOTAL FINDINGS: {len(all_results)}")
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
        </div>
    """
    
    # Detailed findings table
    html += """
        <h2>Detailed Findings</h2>
        <table>
            <tr>
                <th>Severity</th>
                <th>Breaking Change</th>
                <th>Notebook</th>
                <th>Line</th>
                <th>Remediation</th>
            </tr>
    """
    
    for row in df.orderBy("severity", "notebook_path").collect():
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
