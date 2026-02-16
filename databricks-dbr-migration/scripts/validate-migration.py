#!/usr/bin/env python3
"""
Databricks LTS Migration - Validation Script

Validates that breaking changes have been properly fixed in a codebase.
Run this after applying fixes to ensure migration readiness.

Usage:
    python validate-migration.py /path/to/codebase [--target-version 17.3]

Exit codes:
    0 - Validation passed (ready for upgrade)
    1 - Validation failed (breaking patterns found)
    2 - Validation warnings (replacements may be incomplete)
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime


@dataclass
class ValidationCheck:
    """A validation check result."""
    check_name: str
    check_type: str  # "breaking_pattern" or "expected_replacement"
    passed: bool
    file_path: str = ""
    line_number: int = 0
    line_content: str = ""
    message: str = ""


# Breaking patterns that should NOT exist after migration
BREAKING_PATTERNS = [
    {
        "id": "BC-17.3-001",
        "name": "input_file_name() function",
        "pattern": r"\binput_file_name\s*\(",
        "file_types": [".py", ".sql", ".scala"],
        "severity": "HIGH",
        "message": "input_file_name() is removed in DBR 17.3"
    },
    {
        "id": "BC-15.4-001",
        "name": "VARIANT in Python UDF",
        "pattern": r"VariantType\s*\(",
        "file_types": [".py"],
        "severity": "MEDIUM",
        "message": "VariantType in Python UDFs may fail - test on target DBR or use StringType + JSON"
    },
    {
        "id": "BC-15.4-003",
        "name": "'!' syntax (IF/IS)",
        "pattern": r"(IF|IS)\s*!(?!\s*=)",
        "file_types": [".sql"],
        "severity": "MEDIUM",
        "message": "Use 'NOT' instead of '!' (IF NOT EXISTS, IS NOT NULL)"
    },
    {
        "id": "BC-15.4-003b",
        "name": "'!' syntax (IN/BETWEEN/LIKE)",
        "pattern": r"\s!\s*(IN|BETWEEN|LIKE|EXISTS)\b",
        "file_types": [".sql"],
        "severity": "MEDIUM",
        "message": "Use 'NOT IN', 'NOT BETWEEN', 'NOT LIKE', 'NOT EXISTS'"
    },
    {
        "id": "BC-16.4-001a",
        "name": "Scala JavaConverters import",
        "pattern": r"import\s+scala\.collection\.JavaConverters",
        "file_types": [".scala"],
        "severity": "HIGH",
        "message": "Use scala.jdk.CollectionConverters instead"
    },
    {
        "id": "BC-16.4-001b",
        "name": "Scala .to[Collection] syntax",
        "pattern": r"\.to\s*\[\s*(List|Set|Vector|Seq|Array)\s*\]",
        "file_types": [".scala"],
        "severity": "MEDIUM",
        "message": "Use .to(Collection) syntax instead"
    },
    {
        "id": "BC-16.4-001c",
        "name": "Scala TraversableOnce",
        "pattern": r"\bTraversableOnce\b",
        "file_types": [".scala"],
        "severity": "HIGH",
        "message": "TraversableOnce renamed to IterableOnce in Scala 2.13"
    },
    {
        "id": "BC-16.4-001d",
        "name": "Scala Traversable",
        "pattern": r"\bTraversable\b(?!Once)",
        "file_types": [".scala"],
        "severity": "HIGH",
        "message": "Traversable renamed to Iterable in Scala 2.13"
    },
    {
        "id": "BC-16.4-001e",
        "name": "Scala Stream",
        "pattern": r"\bStream\s*\.\s*(from|continually|iterate)",
        "file_types": [".scala"],
        "severity": "MEDIUM",
        "message": "Stream is deprecated - use LazyList in Scala 2.13"
    },
    {
        "id": "BC-16.4-001f",
        "name": "Scala .toIterator",
        "pattern": r"\.toIterator\b",
        "file_types": [".scala"],
        "severity": "MEDIUM",
        "message": ".toIterator is deprecated - use .iterator"
    },
    {
        "id": "BC-16.4-001g",
        "name": "Scala .view.force",
        "pattern": r"\.view\s*\.\s*force\b",
        "file_types": [".scala"],
        "severity": "MEDIUM",
        "message": ".view.force is deprecated - use .view.to(List)"
    },
    {
        "id": "BC-16.4-001i",
        "name": "Scala Symbol literal",
        "pattern": r"'[a-zA-Z_][a-zA-Z0-9_]*\b",
        "file_types": [".scala"],
        "severity": "LOW",
        "message": "Symbol literals deprecated - use Symbol(\"name\")"
    },
    {
        "id": "BC-16.4-007",
        "name": "Strict DateTime Pattern Width (JDK 17)",
        "pattern": r"(?:to_date|to_timestamp|date_format)\s*\(.*[\"'](MM[/\-.]|dd[/\-.]|[/\-.]yy[\"'])",
        "file_types": [".py", ".sql", ".scala"],
        "severity": "MEDIUM",
        "message": "JDK 17 strictly enforces datetime pattern width. MM/dd/yy fails on variable-width input (NULL on clusters, throws on Serverless). Use coalesce(try_to_date(col, 'M/d/yyyy'), try_to_date(col, 'M/d/yy')) for mixed-format data"
    },
]

# Expected replacements that SHOULD exist after migration
EXPECTED_REPLACEMENTS = [
    {
        "id": "REPL-001",
        "name": "_metadata.file_name replacement",
        "pattern": r"_metadata\.file_name|_metadata\[.file_name.\]|\[\"_metadata\"\]\[\"file_name\"\]",
        "file_types": [".py", ".sql", ".scala"],
        "requires": "BC-17.3-001",  # Only check if BC-17.3-001 was found
        "message": "Expected _metadata.file_name as replacement for input_file_name()"
    },
    {
        "id": "REPL-002",
        "name": "CollectionConverters import",
        "pattern": r"import\s+scala\.jdk\.CollectionConverters",
        "file_types": [".scala"],
        "requires": "BC-16.4-001a",
        "message": "Expected scala.jdk.CollectionConverters import"
    },
    {
        "id": "REPL-003",
        "name": "NOT syntax",
        "pattern": r"\b(IF\s+NOT\s+EXISTS|IS\s+NOT\s+NULL|NOT\s+IN|NOT\s+BETWEEN|NOT\s+LIKE|NOT\s+EXISTS)\b",
        "file_types": [".sql"],
        "requires_any": ["BC-15.4-003", "BC-15.4-003b"],
        "message": "Expected NOT syntax replacements"
    },
]


def scan_file_for_pattern(file_path: Path, pattern: str) -> List[Tuple[int, str]]:
    """Scan a file for a regex pattern, return list of (line_num, line_content)."""
    matches = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line in enumerate(f, 1):
                if re.search(pattern, line, re.IGNORECASE):
                    matches.append((line_num, line.strip()[:150]))
    except Exception as e:
        print(f"Warning: Could not read {file_path}: {e}", file=sys.stderr)
    return matches


def validate_codebase(
    root_path: Path,
    target_version: str = "17.3",
    exclude_dirs: List[str] = None
) -> Tuple[List[ValidationCheck], Dict]:
    """
    Validate a codebase for breaking changes and expected replacements.
    
    Returns:
        Tuple of (list of validation checks, summary dict)
    """
    if exclude_dirs is None:
        exclude_dirs = ['.git', '__pycache__', 'node_modules', '.venv', 'venv', 'target', '.idea']
    
    checks = []
    found_breaking = set()
    files_scanned = 0
    
    file_extensions = {'.py', '.sql', '.scala'}
    
    # Collect all files
    all_files = []
    for root, dirs, files in os.walk(root_path):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for filename in files:
            file_path = Path(root) / filename
            if file_path.suffix.lower() in file_extensions:
                all_files.append(file_path)
    
    files_scanned = len(all_files)
    
    # Check for breaking patterns (should NOT exist)
    print("Checking for breaking patterns...")
    for bp in BREAKING_PATTERNS:
        pattern = re.compile(bp["pattern"], re.IGNORECASE)
        
        for file_path in all_files:
            if file_path.suffix.lower() not in bp["file_types"]:
                continue
            
            matches = scan_file_for_pattern(file_path, bp["pattern"])
            
            for line_num, line_content in matches:
                found_breaking.add(bp["id"])
                checks.append(ValidationCheck(
                    check_name=f"{bp['id']}: {bp['name']}",
                    check_type="breaking_pattern",
                    passed=False,
                    file_path=str(file_path),
                    line_number=line_num,
                    line_content=line_content,
                    message=bp["message"]
                ))
    
    # Add passed checks for breaking patterns not found
    checked_patterns = set()
    for bp in BREAKING_PATTERNS:
        if bp["id"] not in found_breaking and bp["id"] not in checked_patterns:
            checks.append(ValidationCheck(
                check_name=f"{bp['id']}: {bp['name']}",
                check_type="breaking_pattern",
                passed=True,
                message=f"No instances of {bp['name']} found"
            ))
            checked_patterns.add(bp["id"])
    
    # Check for expected replacements (should exist if breaking pattern was found)
    print("Checking for expected replacements...")
    for repl in EXPECTED_REPLACEMENTS:
        # Only check if the related breaking change was found somewhere
        # Support both "requires" (single) and "requires_any" (list)
        if repl.get("requires") and repl["requires"] not in found_breaking:
            continue
        if repl.get("requires_any"):
            if not any(req in found_breaking for req in repl["requires_any"]):
                continue
        
        found_replacement = False
        for file_path in all_files:
            if file_path.suffix.lower() not in repl["file_types"]:
                continue
            
            matches = scan_file_for_pattern(file_path, repl["pattern"])
            if matches:
                found_replacement = True
                for line_num, line_content in matches:
                    checks.append(ValidationCheck(
                        check_name=f"{repl['id']}: {repl['name']}",
                        check_type="expected_replacement",
                        passed=True,
                        file_path=str(file_path),
                        line_number=line_num,
                        line_content=line_content,
                        message=repl["message"]
                    ))
        
        requires_met = (
            repl.get("requires") in found_breaking or
            (repl.get("requires_any") and any(req in found_breaking for req in repl["requires_any"]))
        )
        if not found_replacement and requires_met:
            checks.append(ValidationCheck(
                check_name=f"{repl['id']}: {repl['name']}",
                check_type="expected_replacement",
                passed=False,
                message=f"WARNING: {repl['message']} not found, but related breaking change exists"
            ))
    
    # Generate summary
    breaking_failures = [c for c in checks if c.check_type == "breaking_pattern" and not c.passed]
    replacement_warnings = [c for c in checks if c.check_type == "expected_replacement" and not c.passed]
    
    summary = {
        "files_scanned": files_scanned,
        "breaking_patterns_found": len(breaking_failures),
        "replacement_warnings": len(replacement_warnings),
        "high_severity": len([c for c in breaking_failures if "HIGH" in c.message or c.check_name.startswith("BC-17") or c.check_name.startswith("BC-15.4-001") or c.check_name.startswith("BC-16.4-001")]),
        "passed": len(breaking_failures) == 0,
        "target_version": target_version
    }
    
    return checks, summary


def print_validation_report(checks: List[ValidationCheck], summary: Dict):
    """Print a human-readable validation report."""
    print()
    print("=" * 70)
    print("DATABRICKS LTS MIGRATION - VALIDATION REPORT")
    print("=" * 70)
    print(f"Target Version: DBR {summary['target_version']} LTS")
    print(f"Files Scanned: {summary['files_scanned']}")
    print()
    
    # Breaking patterns section
    print("BREAKING PATTERNS CHECK")
    print("-" * 40)
    
    breaking_checks = [c for c in checks if c.check_type == "breaking_pattern"]
    passed_breaking = [c for c in breaking_checks if c.passed]
    failed_breaking = [c for c in breaking_checks if not c.passed]
    
    # Show passed checks
    for check in passed_breaking:
        print(f"✅ {check.check_name}")
    
    # Show failed checks
    if failed_breaking:
        print()
        print("❌ FAILURES (must fix before upgrade):")
        
        # Group by check name
        grouped = {}
        for check in failed_breaking:
            if check.check_name not in grouped:
                grouped[check.check_name] = []
            grouped[check.check_name].append(check)
        
        for check_name, checks_list in grouped.items():
            print(f"\n  {check_name}")
            print(f"  {checks_list[0].message}")
            for c in checks_list[:5]:
                print(f"    - {c.file_path}:{c.line_number}")
                print(f"      {c.line_content[:70]}...")
            if len(checks_list) > 5:
                print(f"    ... and {len(checks_list) - 5} more")
    
    print()
    
    # Expected replacements section
    print("REPLACEMENT VERIFICATION")
    print("-" * 40)
    
    replacement_checks = [c for c in checks if c.check_type == "expected_replacement"]
    passed_repl = [c for c in replacement_checks if c.passed]
    failed_repl = [c for c in replacement_checks if not c.passed]
    
    if passed_repl:
        # Group and count
        repl_counts = {}
        for c in passed_repl:
            if c.check_name not in repl_counts:
                repl_counts[c.check_name] = 0
            repl_counts[c.check_name] += 1
        
        for check_name, count in repl_counts.items():
            print(f"✅ {check_name}: {count} instances found")
    
    if failed_repl:
        print()
        for check in failed_repl:
            print(f"⚠️  {check.check_name}")
            print(f"   {check.message}")
    
    if not replacement_checks:
        print("ℹ️  No replacement verification needed")
    
    print()
    
    # Summary
    print("=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    
    if summary['passed']:
        print()
        print("✅ VALIDATION PASSED")
        print()
        print(f"Your codebase is ready for upgrade to DBR {summary['target_version']} LTS!")
        print("No breaking patterns were found.")
        if summary['replacement_warnings'] > 0:
            print(f"Note: {summary['replacement_warnings']} replacement warning(s) - review recommended")
    else:
        print()
        print("❌ VALIDATION FAILED")
        print()
        print(f"Found {summary['breaking_patterns_found']} breaking pattern(s)")
        print(f"  - HIGH severity: {summary['high_severity']}")
        print()
        print("Please fix the issues above before upgrading.")
        print("Run the scan-breaking-changes.py script for detailed remediation guidance.")
    
    print()
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Validate codebase for Databricks LTS migration readiness"
    )
    parser.add_argument(
        "path",
        help="Path to codebase directory to validate"
    )
    parser.add_argument(
        "--target-version", "-t",
        default="17.3",
        help="Target DBR version (default: 17.3)"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Only output pass/fail status"
    )
    parser.add_argument(
        "--json", "-j",
        help="Output results as JSON to specified file"
    )
    
    args = parser.parse_args()
    
    root_path = Path(args.path)
    if not root_path.exists():
        print(f"Error: Path does not exist: {root_path}", file=sys.stderr)
        sys.exit(2)
    
    if not args.quiet:
        print(f"Validating {root_path} for DBR {args.target_version} migration...")
    
    checks, summary = validate_codebase(root_path, args.target_version)
    
    if not args.quiet:
        print_validation_report(checks, summary)
    
    if args.json:
        import json
        output = {
            "validation_date": datetime.now().isoformat(),
            "target_version": args.target_version,
            "path": str(root_path),
            "summary": summary,
            "checks": [
                {
                    "name": c.check_name,
                    "type": c.check_type,
                    "passed": c.passed,
                    "file": c.file_path,
                    "line": c.line_number,
                    "content": c.line_content,
                    "message": c.message
                }
                for c in checks
            ]
        }
        with open(args.json, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"\nJSON report saved to: {args.json}")
    
    # Exit code
    if summary['passed']:
        if args.quiet:
            print("PASSED")
        sys.exit(0)
    else:
        if args.quiet:
            print("FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
