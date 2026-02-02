#!/usr/bin/env python3
"""
Databricks LTS Migration - Breaking Changes Scanner

Scans a codebase for patterns affected by breaking changes between
Databricks Runtime LTS versions.

Usage:
    python scan-breaking-changes.py /path/to/codebase [--source-version 13.3] [--target-version 17.3]

Migration Path Filtering:
    --source-version: Your CURRENT DBR version (patterns <= this are skipped)
    --target-version: Your TARGET DBR version (patterns > this are skipped)
    
    Example: For 13.3 → 17.3 migration:
    - BC-13.3-001 is SKIPPED (already working on 13.3)
    - BC-14.3-001 through BC-17.3-xxx are INCLUDED

Output:
    - Console report of findings
    - Optional JSON report file
"""

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class Finding:
    """A single breaking change finding."""
    breaking_change_id: str
    severity: str
    file_path: str
    line_number: int
    line_content: str
    description: str
    remediation: str
    introduced_in: str


@dataclass
class ScanResult:
    """Complete scan results."""
    scan_date: str
    target_version: str
    files_scanned: int
    findings: List[Finding] = field(default_factory=list)
    summary: Dict[str, int] = field(default_factory=dict)


# Breaking change patterns to scan for
PATTERNS = [
    {
        "id": "BC-17.3-001",
        "name": "input_file_name() Removed",
        "severity": "HIGH",
        "introduced_in": "17.3",
        "pattern": r"\binput_file_name\s*\(",
        "file_types": [".py", ".sql", ".scala"],
        "description": "input_file_name() function is removed in DBR 17.3",
        "remediation": "Replace with _metadata.file_name"
    },
    # NOTE: BC-SC-002 (Temp View Reuse) is handled by scan_duplicate_temp_views()
    # which provides smarter detection of actual reuse, not just presence.
    # Spark Connect behavioral patterns (warnings for manual review)
    # NOTE: These patterns flag potential issues but may have false positives.
    # Manual review is required to determine if the code is actually affected.
    {
        "id": "BC-SC-003",
        "name": "UDF Definition - Manual Review",
        "severity": "LOW",
        "introduced_in": "14.3",
        "pattern": r"@udf\s*\(",
        "file_types": [".py"],
        "description": "[MANUAL REVIEW] Spark Connect: UDFs serialize at execution time, not definition. Check if external variables are captured correctly",
        "remediation": "If UDF captures external variables, use function factory pattern or ensure variables are stable at execution time"
    },
    {
        "id": "BC-SC-004",
        "name": "Schema Access - Manual Review",
        "severity": "LOW",
        "introduced_in": "13.3",
        "pattern": r"\.(columns|schema|dtypes)\b",
        "file_types": [".py"],
        "description": "[MANUAL REVIEW] Spark Connect: schema/columns access triggers RPC. Cache if used repeatedly",
        "remediation": "Cache df.columns/df.schema in a local variable if accessed multiple times"
    },
    {
        "id": "BC-15.4-003",
        "name": "'!' Syntax for NOT",
        "severity": "MEDIUM",
        "introduced_in": "15.4",
        "pattern": r"(IF|IS)\s*!(?!\s*=)",  # Match IF ! or IS ! but not !=
        "file_types": [".sql"],
        "description": "Using '!' instead of 'NOT' outside boolean expressions is disallowed",
        "remediation": "Replace '!' with 'NOT' (e.g., IF NOT EXISTS, IS NOT NULL)"
    },
    {
        "id": "BC-15.4-003b",
        "name": "'!' Syntax for NOT IN/BETWEEN/LIKE",
        "severity": "MEDIUM", 
        "introduced_in": "15.4",
        "pattern": r"\s!\s*(IN|BETWEEN|LIKE|EXISTS)\b",
        "file_types": [".sql"],
        "description": "Using '!' instead of 'NOT' for IN/BETWEEN/LIKE/EXISTS is disallowed",
        "remediation": "Replace with 'NOT IN', 'NOT BETWEEN', 'NOT LIKE', 'NOT EXISTS'"
    },
    {
        "id": "BC-15.4-001",
        "name": "VARIANT Type in Python UDF",
        "severity": "MEDIUM",
        "introduced_in": "15.4",
        "pattern": r"VariantType\s*\(",
        "file_types": [".py"],
        "description": "[REVIEW] VARIANT type in Python UDFs may throw exception in 15.4+. Test on target DBR version.",
        "remediation": "Test on target DBR, or use STRING type with JSON parsing for safer cross-version compatibility"
    },
    {
        "id": "BC-16.4-001a",
        "name": "Scala JavaConverters Import",
        "severity": "HIGH",
        "introduced_in": "16.4",
        "pattern": r"import\s+scala\.collection\.JavaConverters",
        "file_types": [".scala"],
        "description": "JavaConverters is deprecated in Scala 2.13",
        "remediation": "Use 'import scala.jdk.CollectionConverters._' instead"
    },
    {
        "id": "BC-16.4-001b",
        "name": "Scala .to[Collection] Syntax",
        "severity": "MEDIUM",
        "introduced_in": "16.4", 
        "pattern": r"\.to\s*\[\s*(List|Set|Vector|Seq|Array)\s*\]",
        "file_types": [".scala"],
        "description": ".to[Collection] syntax changed in Scala 2.13",
        "remediation": "Use .to(Collection) syntax instead (e.g., .to(List))"
    },
    {
        "id": "BC-16.4-001c",
        "name": "Scala TraversableOnce",
        "severity": "HIGH",
        "introduced_in": "16.4",
        "pattern": r"\bTraversableOnce\b",
        "file_types": [".scala"],
        "description": "TraversableOnce is renamed to IterableOnce in Scala 2.13",
        "remediation": "Replace TraversableOnce with IterableOnce"
    },
    {
        "id": "BC-16.4-001d", 
        "name": "Scala Traversable",
        "severity": "HIGH",
        "introduced_in": "16.4",
        "pattern": r"\bTraversable\b(?!Once)",
        "file_types": [".scala"],
        "description": "Traversable is renamed to Iterable in Scala 2.13",
        "remediation": "Replace Traversable with Iterable"
    },
    {
        "id": "BC-16.4-001e",
        "name": "Scala Stream (Lazy)",
        "severity": "MEDIUM",
        "introduced_in": "16.4",
        "pattern": r"\bStream\s*\.\s*(from|continually|iterate|empty|cons)",
        "file_types": [".scala"],
        "description": "Stream is replaced by LazyList in Scala 2.13",
        "remediation": "Replace Stream with LazyList"
    },
    {
        "id": "BC-16.4-001f",
        "name": "Scala .toIterator Deprecated",
        "severity": "MEDIUM",
        "introduced_in": "16.4",
        "pattern": r"\.toIterator\b",
        "file_types": [".scala"],
        "description": ".toIterator is deprecated in Scala 2.13",
        "remediation": "Replace .toIterator with .iterator"
    },
    {
        "id": "BC-16.4-001g",
        "name": "Scala .view.force Deprecated",
        "severity": "MEDIUM",
        "introduced_in": "16.4",
        "pattern": r"\.view\s*\.\s*force\b",
        "file_types": [".scala"],
        "description": ".view.force is deprecated in Scala 2.13",
        "remediation": "Replace .view.force with .view.to(List) or .view.toList"
    },
    {
        "id": "BC-16.4-001h",
        "name": "Scala collection.Seq",
        "severity": "MEDIUM",
        "introduced_in": "16.4",
        "pattern": r"\bcollection\.Seq\b",
        "file_types": [".scala"],
        "description": "[REVIEW] collection.Seq now refers to immutable.Seq in Scala 2.13",
        "remediation": "Use explicit immutable.Seq or mutable.Seq import"
    },
    {
        "id": "BC-16.4-001i",
        "name": "Scala Symbol Literal",
        "severity": "LOW",
        "introduced_in": "16.4",
        "pattern": r"'[a-zA-Z_][a-zA-Z0-9_]*\b",
        "file_types": [".scala"],
        "description": "Symbol literals ('symbol) are deprecated in Scala 2.13",
        "remediation": "Replace 'symbol with Symbol(\"symbol\")"
    },
    {
        "id": "BC-16.4-002",
        "name": "Scala HashMap/HashSet Ordering",
        "severity": "HIGH",
        "introduced_in": "16.4",
        "pattern": r"\b(HashMap|HashSet)\s*[\[\(]",
        "file_types": [".scala"],
        "description": "[REVIEW] HashMap/HashSet iteration order changed in Scala 2.13. Don't rely on order.",
        "remediation": "If order matters, use explicit sorting or ListMap/LinkedHashSet"
    },
    {
        "id": "BC-13.3-001",
        "name": "MERGE INTO Type Casting",
        "severity": "HIGH",
        "introduced_in": "13.3",
        "pattern": r"\bMERGE\s+INTO\b",
        "file_types": [".py", ".sql", ".scala"],
        "description": "[REVIEW] ANSI mode throws CAST_OVERFLOW for type mismatches. Review type casting.",
        "remediation": "Add explicit bounds checking for type conversions that may overflow"
    },
    {
        "id": "BC-17.3-002",
        "name": "Auto Loader Usage",
        "severity": "MEDIUM",
        "introduced_in": "17.3",
        "pattern": r"format\s*\(\s*[\"']cloudFiles[\"']\s*\)",
        "file_types": [".py", ".scala"],
        "description": "[REVIEW] Auto Loader incremental listing default changed from 'auto' to 'false' in 17.3. Check performance.",
        "remediation": "Test performance; if slower, add .option('cloudFiles.useIncrementalListing', 'auto')"
    },
    {
        "id": "BC-13.3-002",
        "name": "Parquet Timestamp NTZ Setting",
        "severity": "LOW",
        "introduced_in": "13.3",
        "pattern": r"spark\.sql\.parquet\.inferTimestampNTZ",
        "file_types": [".py", ".scala", ".sql", ".conf"],
        "description": "Parquet timestamp inference behavior changed",
        "remediation": "Set spark.sql.parquet.inferTimestampNTZ.enabled explicitly"
    },
    {
        "id": "BC-15.4-002",
        "name": "JDBC useNullCalendar Setting",
        "severity": "LOW",
        "introduced_in": "15.4",
        "pattern": r"spark\.sql\.legacy\.jdbc\.useNullCalendar",
        "file_types": [".py", ".scala", ".sql", ".conf"],
        "description": "JDBC useNullCalendar default changed to true",
        "remediation": "Set spark.sql.legacy.jdbc.useNullCalendar explicitly if needed"
    },
    {
        "id": "BC-15.4-004",
        "name": "View Column Type Definition",
        "severity": "LOW",
        "introduced_in": "15.4",
        "pattern": r"CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+\w+\s*\([^)]*\b(INT|STRING|BIGINT|DOUBLE|BOOLEAN|NOT\s+NULL|DEFAULT)\b",
        "file_types": [".sql"],
        "description": "Column type definitions in CREATE VIEW are disallowed",
        "remediation": "Remove column type specifications from CREATE VIEW"
    },
    {
        "id": "BC-16.4-004",
        "name": "MERGE materializeSource=none",
        "severity": "LOW",
        "introduced_in": "16.4",
        "pattern": r"merge\.materializeSource.*none",
        "file_types": [".py", ".scala", ".sql", ".conf"],
        "description": "merge.materializeSource=none is no longer allowed",
        "remediation": "Remove merge.materializeSource=none configuration"
    },
    # Additional patterns from profiler
    {
        "id": "BC-13.3-003",
        "name": "overwriteSchema with Dynamic Partition",
        "severity": "MEDIUM",
        "introduced_in": "13.3",
        "pattern": r"overwriteSchema.*true",
        "file_types": [".py", ".scala"],
        "description": "[Review] overwriteSchema=true found - verify not combined with partitionOverwriteMode='dynamic'",
        "remediation": "If using dynamic partition overwrite, separate into distinct operations"
    },
    {
        "id": "BC-17.3-003",
        "name": "Spark Connect Literal Handling",
        "severity": "LOW",
        "introduced_in": "17.3",
        "pattern": r"\b(array|map|struct)\s*\(",
        "file_types": [".py", ".scala"],
        "description": "[Review] Spark Connect 17.3: null values preserved, decimal precision changed to (38,18)",
        "remediation": "Handle nulls explicitly with coalesce(); specify decimal precision if needed"
    },
    {
        "id": "BC-17.3-005",
        "name": "Spark Connect Decimal Precision",
        "severity": "LOW",
        "introduced_in": "17.3",
        "pattern": r"DecimalType\s*\(|\.cast\s*\(\s*[\"']decimal",
        "file_types": [".py", ".scala"],
        "description": "[Review] Spark Connect: decimal precision in array/map literals defaults to (38,18)",
        "remediation": "Specify explicit precision/scale if plan comparison or exact precision required"
    },
    {
        "id": "BC-15.4-006",
        "name": "View Schema Binding Mode",
        "severity": "MEDIUM",
        "introduced_in": "15.4",
        "pattern": r"CREATE\s+(OR\s+REPLACE\s+)?VIEW\b",
        "file_types": [".sql"],
        "description": "[Review] View schema binding default changed from BINDING to schema compensation",
        "remediation": "Verify view definitions handle underlying table schema changes correctly"
    },
    {
        "id": "BC-16.4-003",
        "name": "Data Source Cache Options",
        "severity": "MEDIUM",
        "introduced_in": "16.4",
        "pattern": r"spark\.sql\.legacy\.readFileSourceTableCacheIgnoreOptions",
        "file_types": [".py", ".scala", ".sql", ".conf"],
        "description": "Table reads now respect options for all cached plans",
        "remediation": "Set spark.sql.legacy.readFileSourceTableCacheIgnoreOptions=true to restore old behavior"
    },
    {
        "id": "BC-13.3-004",
        "name": "ANSI Store Assignment Policy",
        "severity": "LOW",
        "introduced_in": "13.3",
        "pattern": r"spark\.sql\.storeAssignmentPolicy",
        "file_types": [".py", ".scala", ".sql", ".conf"],
        "description": "[Review] storeAssignmentPolicy default is ANSI - overflow throws error",
        "remediation": "Ensure MERGE/UPDATE operations handle type casting explicitly"
    },
    {
        "id": "BC-14.3-001",
        "name": "Thriftserver Config Removed",
        "severity": "LOW",
        "introduced_in": "14.3",
        "pattern": r"hive\.aux\.jars\.path|hive\.server2\.global\.init\.file\.location",
        "file_types": [".py", ".scala", ".sql", ".conf"],
        "description": "Hive auxiliary JARs and global init file configs removed",
        "remediation": "Use cluster init scripts or Unity Catalog volumes for JARs"
    },
    {
        "id": "BC-16.4-005",
        "name": "Json4s Library Usage",
        "severity": "LOW",
        "introduced_in": "16.4",
        "pattern": r"import\s+org\.json4s",
        "file_types": [".scala"],
        "description": "[Review] Json4s downgraded from 4.0.7 to 3.7.0-M11 for Scala 2.13",
        "remediation": "Review Json4s API usage for compatibility with 3.7.x"
    },
    {
        "id": "BC-16.4-006",
        "name": "Auto Loader cleanSource Behavior",
        "severity": "MEDIUM",
        "introduced_in": "16.4",
        "pattern": r"cloudFiles\.cleanSource",
        "file_types": [".py", ".scala", ".sql"],
        "description": "[Review] cloudFiles.cleanSource behavior changed in 16.4",
        "remediation": "Review cleanSource settings; behavior for file cleanup may differ"
    },
    {
        "id": "BC-15.4-005",
        "name": "JDBC Read Timestamp Handling",
        "severity": "LOW",
        "introduced_in": "15.4",
        "pattern": r"\.jdbc\s*\(|\.format\s*\(\s*[\"']jdbc[\"']\s*\)",
        "file_types": [".py", ".scala"],
        "description": "[Review] JDBC timestamp handling changed - useNullCalendar default now true",
        "remediation": "Test timestamp values from JDBC sources; set useNullCalendar=false if issues"
    },
]


def get_version_number(version_str: str) -> tuple:
    """Convert version string to tuple for comparison."""
    parts = version_str.split(".")
    return tuple(int(p) for p in parts)


def scan_duplicate_temp_views(file_path: Path, lines: List[str], file_ext: str) -> List[Finding]:
    """
    Scan for temp view names that are reused multiple times in the same file.
    
    This is a Spark Connect concern: temp views use name lookup (not embedded plans),
    so reusing the same name in concurrent sessions can cause conflicts.
    
    Detects:
    - Literal strings: createOrReplaceTempView("my_view") used multiple times
    - Variable names: createOrReplaceTempView(view_name) used multiple times
    
    Does NOT flag:
    - F-strings: createOrReplaceTempView(f"view_{var}") - assumed dynamic
    - Lines containing uuid, hex, random, unique, timestamp keywords
    
    LIMITATION: Variable tracking is line-based only. If a variable is assigned
    a UUID on line 1 but used on lines 2 and 3, lines 2 and 3 will still be
    flagged because we can't track that the variable contains a unique value.
    Manual review is required to dismiss such false positives.
    """
    if file_ext not in ['.py', '.scala']:
        return []
    
    findings = []
    
    # Track view names: {name: [(line_num, line_content), ...]}
    view_usages: Dict[str, List[tuple]] = {}
    
    # Keywords that indicate dynamic/unique view names - skip these
    skip_keywords = ['uuid', 'hex', 'random', 'unique', 'timestamp', 'datetime', 'now']
    
    # Pattern to extract view name from temp view creation
    # Captures: createOrReplaceTempView("name"), createTempView('name'), etc.
    temp_view_pattern = re.compile(
        r'(createOrReplaceTempView|createTempView|createGlobalTempView)\s*\(\s*'
        r'(?:'
        r'["\']([^"\']+)["\']'  # Literal string: "name" or 'name'
        r'|'
        r'([a-zA-Z_][a-zA-Z0-9_]*)'  # Variable name
        r')\s*\)',
        re.IGNORECASE
    )
    
    for line_num, line in enumerate(lines, 1):
        # Skip lines that contain dynamic keywords (uuid, random, etc.)
        if any(skip in line.lower() for skip in skip_keywords):
            continue
        
        # Skip f-strings entirely (they likely have dynamic content)
        if re.search(r'(createOrReplaceTempView|createTempView|createGlobalTempView)\s*\(\s*f["\']', line):
            continue
        
        # Find all temp view creations on this line
        for match in temp_view_pattern.finditer(line):
            # Group 2 is literal string, Group 3 is variable name
            view_name = match.group(2) or match.group(3)
            
            if not view_name:
                continue
            
            # Skip common non-view variables
            if view_name in ['df', 'spark', 'self', 'result', 'data']:
                continue
            
            if view_name not in view_usages:
                view_usages[view_name] = []
            view_usages[view_name].append((line_num, line.strip()[:150]))
    
    # Flag any view name used more than once
    for view_name, usages in view_usages.items():
        if len(usages) > 1:
            # Create a finding for each occurrence after the first
            first_line = usages[0][0]
            for line_num, line_content in usages[1:]:
                findings.append(Finding(
                    breaking_change_id="BC-SC-002",
                    severity="MEDIUM",  # Elevated from LOW since we detected actual reuse
                    file_path=str(file_path),
                    line_number=line_num,
                    line_content=line_content,
                    description=f"Temp view '{view_name}' reused (first defined on line {first_line}). "
                                f"Spark Connect uses name lookup, not embedded plans - this can cause conflicts in concurrent sessions.",
                    remediation=f"Use unique view names with UUID: f\"{view_name}_{{uuid.uuid4()}}\"",
                    introduced_in="13.3"
                ))
    
    return findings


def should_check_pattern(pattern: dict, source_version: Optional[str], target_version: str) -> bool:
    """
    Check if pattern applies to the migration path.
    
    A pattern is relevant if its introduced_in version is:
    - Greater than the source version (already working on source)
    - Less than or equal to the target version
    
    Args:
        pattern: The breaking change pattern dict
        source_version: Current DBR version (patterns <= this are skipped). None means include all.
        target_version: Target DBR version (patterns > this are skipped)
    """
    introduced = get_version_number(pattern["introduced_in"])
    target = get_version_number(target_version)
    
    # Must be at or before target
    if introduced > target:
        return False
    
    # If source specified, must be AFTER source (already working on source)
    if source_version:
        source = get_version_number(source_version)
        if introduced <= source:
            return False
    
    return True


def scan_file(file_path: Path, patterns: List[dict]) -> List[Finding]:
    """Scan a single file for breaking change patterns."""
    findings = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Warning: Could not read {file_path}: {e}", file=sys.stderr)
        return findings
    
    file_ext = file_path.suffix.lower()
    
    # Standard pattern matching
    for pattern in patterns:
        if file_ext not in pattern["file_types"]:
            continue
            
        regex = re.compile(pattern["pattern"], re.IGNORECASE)
        
        for line_num, line in enumerate(lines, 1):
            if regex.search(line):
                findings.append(Finding(
                    breaking_change_id=pattern["id"],
                    severity=pattern["severity"],
                    file_path=str(file_path),
                    line_number=line_num,
                    line_content=line.strip()[:200],  # Truncate long lines
                    description=pattern["description"],
                    remediation=pattern["remediation"],
                    introduced_in=pattern["introduced_in"]
                ))
    
    # Special check: Duplicate temp view names (BC-SC-002)
    # This requires analyzing the whole file to detect reuse
    temp_view_findings = scan_duplicate_temp_views(file_path, lines, file_ext)
    findings.extend(temp_view_findings)
    
    return findings


def scan_directory(
    root_path: Path,
    target_version: str,
    source_version: Optional[str] = None,
    exclude_dirs: Optional[List[str]] = None
) -> ScanResult:
    """Scan directory tree for breaking changes.
    
    Args:
        root_path: Directory to scan
        target_version: Target DBR version
        source_version: Current DBR version (patterns <= this are skipped)
        exclude_dirs: Directories to exclude from scan
    """
    
    if exclude_dirs is None:
        exclude_dirs = ['.git', '__pycache__', 'node_modules', '.venv', 'venv', 'target', '.idea']
    
    # Filter patterns for migration path (source → target)
    applicable_patterns = [
        p for p in PATTERNS 
        if should_check_pattern(p, source_version, target_version)
    ]
    
    # Log filtering info
    total_patterns = len(PATTERNS)
    applicable_count = len(applicable_patterns)
    skipped_count = total_patterns - applicable_count
    
    if source_version:
        print(f"Migration path: DBR {source_version} → {target_version}")
    else:
        print(f"Target version: DBR {target_version}")
    print(f"Patterns applicable: {applicable_count} of {total_patterns}")
    if skipped_count > 0:
        print(f"Patterns skipped: {skipped_count}")
    print()
    
    result = ScanResult(
        scan_date=datetime.now().isoformat(),
        target_version=target_version,
        files_scanned=0
    )
    
    file_extensions = {'.py', '.sql', '.scala', '.conf'}
    
    for root, dirs, files in os.walk(root_path):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        
        for filename in files:
            file_path = Path(root) / filename
            
            if file_path.suffix.lower() not in file_extensions:
                continue
                
            result.files_scanned += 1
            findings = scan_file(file_path, applicable_patterns)
            result.findings.extend(findings)
    
    # Generate summary
    result.summary = {
        "total_findings": len(result.findings),
        "by_severity": {},
        "by_breaking_change": {},
        "by_version": {}
    }
    
    for finding in result.findings:
        # By severity
        sev = finding.severity
        result.summary["by_severity"][sev] = result.summary["by_severity"].get(sev, 0) + 1
        
        # By breaking change ID
        bc_id = finding.breaking_change_id
        result.summary["by_breaking_change"][bc_id] = result.summary["by_breaking_change"].get(bc_id, 0) + 1
        
        # By version introduced
        ver = finding.introduced_in
        result.summary["by_version"][ver] = result.summary["by_version"].get(ver, 0) + 1
    
    return result


def print_report(result: ScanResult):
    """Print human-readable report to console."""
    print("=" * 70)
    print("DATABRICKS LTS MIGRATION - BREAKING CHANGES SCAN REPORT")
    print("=" * 70)
    print(f"Scan Date: {result.scan_date}")
    print(f"Target Version: DBR {result.target_version} LTS")
    print(f"Files Scanned: {result.files_scanned}")
    print(f"Total Findings: {result.summary['total_findings']}")
    print()
    
    if not result.findings:
        print("✅ No breaking change patterns detected!")
        return
    
    # Summary by severity
    print("FINDINGS BY SEVERITY:")
    print("-" * 40)
    for severity in ["HIGH", "MEDIUM", "LOW"]:
        count = result.summary["by_severity"].get(severity, 0)
        if count:
            emoji = "🔴" if severity == "HIGH" else ("🟡" if severity == "MEDIUM" else "🟢")
            print(f"  {emoji} {severity}: {count}")
    print()
    
    # Summary by version
    print("FINDINGS BY VERSION INTRODUCED:")
    print("-" * 40)
    for version in sorted(result.summary["by_version"].keys(), key=get_version_number):
        count = result.summary["by_version"][version]
        print(f"  DBR {version}: {count} findings")
    print()
    
    # Detailed findings grouped by breaking change
    print("DETAILED FINDINGS:")
    print("-" * 40)
    
    grouped = {}
    for finding in result.findings:
        bc_id = finding.breaking_change_id
        if bc_id not in grouped:
            grouped[bc_id] = []
        grouped[bc_id].append(finding)
    
    for bc_id, findings in sorted(grouped.items()):
        sample = findings[0]
        print(f"\n[{bc_id}] {sample.description}")
        print(f"  Severity: {sample.severity} | Introduced: DBR {sample.introduced_in}")
        print(f"  Remediation: {sample.remediation}")
        print(f"  Occurrences: {len(findings)}")
        
        # Show first 5 occurrences
        for f in findings[:5]:
            print(f"    - {f.file_path}:{f.line_number}")
            print(f"      {f.line_content[:80]}...")
        
        if len(findings) > 5:
            print(f"    ... and {len(findings) - 5} more")
    
    print()
    print("=" * 70)
    print("Scan complete. Review findings and apply remediations before upgrading.")
    print("=" * 70)


def save_json_report(result: ScanResult, output_path: Path):
    """Save results as JSON file."""
    # Convert to dict for JSON serialization
    data = {
        "scan_date": result.scan_date,
        "target_version": result.target_version,
        "files_scanned": result.files_scanned,
        "summary": result.summary,
        "findings": [asdict(f) for f in result.findings]
    }
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\nJSON report saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Scan codebase for Databricks LTS breaking changes"
    )
    parser.add_argument(
        "path",
        help="Path to codebase directory to scan"
    )
    parser.add_argument(
        "--source-version", "-s",
        default=None,
        help="Source/current DBR version (patterns <= this are skipped). Example: 13.3"
    )
    parser.add_argument(
        "--target-version", "-t",
        default="17.3",
        help="Target DBR version (default: 17.3)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output JSON report file path"
    )
    parser.add_argument(
        "--exclude", "-e",
        nargs="*",
        default=[],
        help="Additional directories to exclude"
    )
    
    args = parser.parse_args()
    
    root_path = Path(args.path)
    if not root_path.exists():
        print(f"Error: Path does not exist: {root_path}", file=sys.stderr)
        sys.exit(1)
    
    migration_info = f"DBR {args.source_version} → {args.target_version}" if args.source_version else f"target DBR {args.target_version}"
    print(f"Scanning {root_path} for breaking changes ({migration_info})...")
    print()
    
    exclude_dirs = ['.git', '__pycache__', 'node_modules', '.venv', 'venv', 'target', '.idea']
    exclude_dirs.extend(args.exclude)
    
    result = scan_directory(root_path, args.target_version, args.source_version, exclude_dirs)
    
    print_report(result)
    
    if args.output:
        save_json_report(result, Path(args.output))
    
    # Exit with error code if HIGH severity findings
    high_count = result.summary.get("by_severity", {}).get("HIGH", 0)
    if high_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
