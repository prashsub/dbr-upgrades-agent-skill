#!/usr/bin/env python3
"""
Databricks LTS Migration - Automatic Fix Script

Automatically applies fixes for breaking changes in a codebase.
Run scan-breaking-changes.py first to identify issues.

Usage:
    python apply-fixes.py /path/to/codebase [--dry-run] [--fix BC-17.3-001,BC-15.4-003]

Options:
    --dry-run       Show what would be changed without modifying files
    --fix           Only apply specific fixes (comma-separated IDs)
    --backup        Create .bak files before modifying (default: true)
    --no-backup     Don't create backup files

Exit codes:
    0 - Fixes applied successfully
    1 - Error during fix application
    2 - No fixes needed

IMPORTANT: This script provides best-effort fixes. Manual review is always recommended.
"""

import argparse
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
from datetime import datetime


@dataclass
class Fix:
    """A single fix to apply."""
    file_path: str
    line_number: int
    original: str
    replacement: str
    fix_id: str
    description: str


@dataclass
class FixResult:
    """Result of applying fixes to a file."""
    file_path: str
    fixes_applied: List[Fix] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    backup_path: Optional[str] = None


# ============================================================================
# UTILITY FUNCTIONS FOR SAFE FIXES
# ============================================================================

def fix_python_input_file_name_import(content: str) -> Tuple[str, List[Dict], List[str]]:
    """
    Safely remove input_file_name from Python imports.
    Handles all edge cases:
    - Single import: removes entire line
    - Multiple imports: removes only input_file_name, cleans commas
    - Multiline imports with parentheses: from x import (a, b, c)
    - Ensures `col` is imported if replacement uses it
    
    Returns:
        Tuple of (modified_content, applied_fixes, warnings)
    """
    applied = []
    warnings = []
    
    # Check if col is already imported (single line or multiline)
    col_already_imported = bool(re.search(
        r'from\s+pyspark\.sql\.functions\s+import\s+[^;]*\bcol\b',
        content, re.MULTILINE | re.DOTALL
    ))
    
    # Check if code uses input_file_name() calls outside of strings that need col()
    # We need col() for DataFrame API calls, not for SQL strings
    needs_col_import = bool(re.search(
        r'(?<!["\'])\binput_file_name\s*\(\s*\)(?!["\'])',
        content
    ))
    
    # Handle multiline parenthesized imports:
    # from pyspark.sql.functions import (
    #     col,
    #     input_file_name,
    #     lit
    # )
    multiline_import_pattern = re.compile(
        r'(from\s+pyspark\.sql\.functions\s+import\s*\()([^)]+)(\))',
        re.MULTILINE | re.DOTALL
    )
    
    def fix_multiline_import(match):
        prefix = match.group(1)
        imports_str = match.group(2)
        suffix = match.group(3)
        
        # Split by comma and newline, preserving structure
        imports = [imp.strip() for imp in re.split(r'[,\n]', imports_str) if imp.strip()]
        
        # Remove input_file_name
        new_imports = [imp for imp in imports if imp != 'input_file_name']
        
        # Add col if needed
        if needs_col_import and not col_already_imported and 'col' not in new_imports:
            new_imports.insert(0, 'col')
        
        if not new_imports:
            return ''  # Remove entire import
        
        # Reconstruct with nice formatting
        return f"{prefix}\n    " + ",\n    ".join(new_imports) + f"\n{suffix}"
    
    new_content = multiline_import_pattern.sub(fix_multiline_import, content)
    if new_content != content:
        applied.append({
            "fix_id": "BC-17.3-001",
            "description": "Fixed multiline import (removed input_file_name)",
            "count": 1,
            "pattern": "multiline import"
        })
    content = new_content
    
    # Handle single-line imports
    lines = content.split('\n')
    modified_lines = []
    
    import_pattern = re.compile(
        r'^(\s*)(from\s+pyspark\.sql\.functions\s+import\s+)([^(].*)$'
    )
    
    for line in lines:
        match = import_pattern.match(line)
        if match and 'input_file_name' in line:
            indent = match.group(1)
            import_prefix = match.group(2)
            imports_str = match.group(3)
            
            # Handle backslash continuation
            imports_str = imports_str.rstrip('\\').strip()
            
            # Parse imports
            imports = [imp.strip() for imp in imports_str.split(',') if imp.strip()]
            
            # Remove input_file_name
            new_imports = [imp for imp in imports if imp != 'input_file_name']
            
            # Add col if needed and not present
            if needs_col_import and not col_already_imported:
                if 'col' not in new_imports:
                    new_imports.insert(0, 'col')
                    col_already_imported = True
            
            if new_imports:
                new_line = f"{indent}{import_prefix}{', '.join(new_imports)}"
                if new_line != line:
                    applied.append({
                        "fix_id": "BC-17.3-001",
                        "description": "Removed input_file_name from import",
                        "count": 1,
                        "pattern": "input_file_name import"
                    })
                modified_lines.append(new_line)
            else:
                applied.append({
                    "fix_id": "BC-17.3-001",
                    "description": "Removed empty import line",
                    "count": 1,
                    "pattern": "empty import"
                })
                continue
        else:
            modified_lines.append(line)
    
    return '\n'.join(modified_lines), applied, warnings


def fix_python_input_file_name_usage(content: str) -> Tuple[str, List[Dict], List[str]]:
    """
    Safely replace input_file_name() usage in Python.
    
    IMPORTANT: Handles SQL strings differently from DataFrame API calls:
    - DataFrame API: input_file_name() -> col("_metadata.file_name")
    - SQL strings: input_file_name() -> _metadata.file_name (no col())
    
    This function uses a two-pass approach:
    1. First, replace all occurrences inside ANY string (triple-quoted, single, double, f-strings)
       with _metadata.file_name (correct for SQL)
    2. Then, for DataFrame API usage OUTSIDE strings, replace with col("_metadata.file_name")
    
    Returns:
        Tuple of (modified_content, applied_fixes, warnings)
    """
    applied = []
    warnings = []
    
    # =========================================================================
    # Step 1: Replace input_file_name() inside ALL string literals
    # This handles: """, ''', ", ', and f-strings
    # Inside strings, we always use _metadata.file_name (no col())
    # =========================================================================
    
    sql_fix_count = 0
    
    # Process all string types in order of precedence
    # Note: Use non-greedy matching .*? for triple-quoted, character classes for single-quoted
    string_patterns = [
        # Triple-quoted strings (must come first - they're longer)
        (r'(""")(.*?)(""")', re.DOTALL),
        (r"(''')(.*?)(''')", re.DOTALL),
        # f-strings with triple quotes
        (r'(f""")(.*?)(""")', re.DOTALL),
        (r"(f''')(.*?)(''')", re.DOTALL),
        # Regular f-strings (double and single quote)
        (r'(f")((?:[^"\\]|\\.)*)(")', 0),
        (r"(f')((?:[^'\\]|\\.)*)(')", 0),
        # Regular strings (double and single quote)
        (r'(")((?:[^"\\]|\\.)*)(")', 0),
        (r"(')((?:[^'\\]|\\.)*)(')", 0),
    ]
    
    for pattern_str, flags in string_patterns:
        if flags:
            pattern = re.compile(pattern_str, flags)
        else:
            pattern = re.compile(pattern_str)
        
        def string_replacer(match):
            nonlocal sql_fix_count
            prefix = match.group(1)
            string_content = match.group(2)
            suffix = match.group(3)
            
            # Replace input_file_name() with _metadata.file_name inside string
            new_content, count = re.subn(
                r'\binput_file_name\s*\(\s*\)',
                '_metadata.file_name',
                string_content
            )
            if count > 0:
                sql_fix_count += count
            return prefix + new_content + suffix
        
        content = pattern.sub(string_replacer, content)
    
    if sql_fix_count > 0:
        applied.append({
            "fix_id": "BC-17.3-001",
            "description": "Replaced input_file_name() with _metadata.file_name inside strings",
            "count": sql_fix_count,
            "pattern": "string literal"
        })
    
    # =========================================================================
    # Step 2: Fix DataFrame API calls (OUTSIDE of strings)
    # These need col("_metadata.file_name")
    # At this point, all string contents have been processed, so any remaining
    # input_file_name() calls are DataFrame API calls
    # =========================================================================
    
    # Pattern: .withColumn("col_name", input_file_name())
    withcol_pattern = re.compile(
        r'\.withColumn\s*\(\s*(["\'])([^"\']+)\1\s*,\s*input_file_name\s*\(\s*\)\s*\)'
    )
    
    def withcol_replacer(match):
        quote = match.group(1)
        col_name = match.group(2)
        return f'.withColumn({quote}{col_name}{quote}, col("_metadata.file_name"))'
    
    new_content, count1 = withcol_pattern.subn(withcol_replacer, content)
    if count1:
        applied.append({
            "fix_id": "BC-17.3-001",
            "description": "Replaced .withColumn(..., input_file_name()) with col('_metadata.file_name')",
            "count": count1,
            "pattern": "withColumn"
        })
    content = new_content
    
    # Pattern: remaining input_file_name() calls (in select, etc.)
    # Since we already processed all strings, any remaining calls are safe to replace
    remaining_pattern = re.compile(r'\binput_file_name\s*\(\s*\)')
    new_content, count2 = remaining_pattern.subn('col("_metadata.file_name")', content)
    
    if count2:
        applied.append({
            "fix_id": "BC-17.3-001",
            "description": "Replaced input_file_name() with col('_metadata.file_name') in DataFrame API",
            "count": count2,
            "pattern": "DataFrame API"
        })
        warnings.append(
            "⚠️  Ensure 'col' is imported from pyspark.sql.functions"
        )
    content = new_content
    
    return content, applied, warnings


def fix_scala_input_file_name(content: str) -> Tuple[str, List[Dict], List[str]]:
    """
    Safely fix input_file_name in Scala code.
    
    IMPORTANT: Handles SQL strings differently from DataFrame API calls:
    - DataFrame API: input_file_name() -> col("_metadata.file_name")
    - SQL strings: input_file_name() -> _metadata.file_name (no col())
    
    Also ensures col is imported when needed.
    
    Returns:
        Tuple of (modified_content, applied_fixes, warnings)
    """
    applied = []
    warnings = []
    
    # Check if there are actual usages before removing import
    has_usage = bool(re.search(r'\binput_file_name\s*\(\s*\)', content))
    
    # Check if col is already imported
    has_col_import = bool(re.search(
        r'import\s+org\.apache\.spark\.sql\.functions\.(col|_|\{[^}]*col[^}]*\})',
        content
    ))
    has_wildcard_import = bool(re.search(
        r'import\s+org\.apache\.spark\.sql\.functions\._',
        content
    ))
    
    # =========================================================================
    # Step 1: Fix ALL string literals FIRST
    # Inside strings, replace with _metadata.file_name (not col())
    # This handles SQL strings, interpolated strings, etc.
    # =========================================================================
    
    string_patterns = [
        # Triple-quoted strings (must come first)
        (r'(""")(.*?)(""")', re.DOTALL),
        # Interpolated strings: s"...", f"..."
        (r'(s")((?:[^"\\]|\\.)*)(")', 0),
        (r'(f")((?:[^"\\]|\\.)*)(")', 0),
        # Raw interpolated: raw"..."
        (r'(raw")((?:[^"\\]|\\.)*)(")', 0),
        # Regular double-quoted strings
        (r'(")((?:[^"\\]|\\.)*)(")', 0),
    ]
    
    sql_fix_count = 0
    for pattern_str, flags in string_patterns:
        pattern = re.compile(pattern_str, flags) if flags else re.compile(pattern_str)
        
        def string_replacer(match):
            nonlocal sql_fix_count
            prefix = match.group(1)
            string_content = match.group(2)
            suffix = match.group(3)
            
            # Replace input_file_name() with _metadata.file_name inside string
            new_content, count = re.subn(
                r'\binput_file_name\s*\(\s*\)',
                '_metadata.file_name',
                string_content
            )
            if count > 0:
                sql_fix_count += count
            return prefix + new_content + suffix
        
        content = pattern.sub(string_replacer, content)
    
    if sql_fix_count > 0:
        applied.append({
            "fix_id": "BC-17.3-001",
            "description": "Replaced input_file_name() with _metadata.file_name inside strings",
            "count": sql_fix_count,
            "pattern": "string literal"
        })
    
    # =========================================================================
    # Step 2: Fix imports
    # =========================================================================
    
    # Comment out specific input_file_name import
    import_pattern = re.compile(
        r'import\s+org\.apache\.spark\.sql\.functions\.input_file_name'
    )
    if import_pattern.search(content):
        content = import_pattern.sub(
            '// import org.apache.spark.sql.functions.input_file_name  // Removed in DBR 17.3',
            content
        )
        applied.append({
            "fix_id": "BC-17.3-001",
            "description": "Commented out input_file_name import",
            "count": 1,
            "pattern": "Scala import"
        })
    
    # Warn about wildcard import
    if has_wildcard_import and has_usage:
        warnings.append(
            "⚠️  Using wildcard import - input_file_name() no longer available in DBR 17.3"
        )
    
    # =========================================================================
    # Step 3: Add col import if needed and not present
    # =========================================================================
    
    needs_col_for_api = bool(re.search(
        r'(?<!["\'`])\binput_file_name\s*\(\s*\)(?!["\'])',
        content
    ))
    
    if needs_col_for_api and not has_col_import and not has_wildcard_import:
        # Add col import after the last import statement (or at the top if no imports)
        import_lines = list(re.finditer(r'^import\s+.*$', content, re.MULTILINE))
        col_import_line = 'import org.apache.spark.sql.functions.col  // Added for _metadata.file_name'
        
        if import_lines:
            # Add after last import
            last_import = import_lines[-1]
            insert_pos = last_import.end()
            content = (
                content[:insert_pos] + 
                '\n' + col_import_line +
                content[insert_pos:]
            )
        else:
            # No imports found - add at the beginning of the file
            # But after any package declaration
            package_match = re.search(r'^package\s+.*$', content, re.MULTILINE)
            if package_match:
                insert_pos = package_match.end()
                content = (
                    content[:insert_pos] + 
                    '\n\n' + col_import_line +
                    content[insert_pos:]
                )
            else:
                # No package declaration either - add at very top
                content = col_import_line + '\n\n' + content
        
        applied.append({
            "fix_id": "BC-17.3-001",
            "description": "Added col import for _metadata.file_name replacement",
            "count": 1,
            "pattern": "col import"
        })
        has_col_import = True
    
    # =========================================================================
    # Step 4: Fix DataFrame API calls (outside of strings)
    # =========================================================================
    
    # Pattern: .withColumn("name", input_file_name())
    withcol_pattern = re.compile(
        r'\.withColumn\s*\(\s*"([^"]+)"\s*,\s*input_file_name\s*\(\s*\)\s*\)'
    )
    new_content, count1 = withcol_pattern.subn(
        r'.withColumn("\1", col("_metadata.file_name"))',
        content
    )
    if count1:
        applied.append({
            "fix_id": "BC-17.3-001",
            "description": "Replaced .withColumn(..., input_file_name()) in Scala",
            "count": count1,
            "pattern": "withColumn"
        })
    content = new_content
    
    # Pattern: remaining input_file_name() calls (in select, etc.)
    # Since we already processed all strings above, any remaining calls are safe to replace
    remaining_pattern = re.compile(r'\binput_file_name\s*\(\s*\)')
    new_content, api_fix_count = remaining_pattern.subn('col("_metadata.file_name")', content)
    
    if api_fix_count > 0:
        applied.append({
            "fix_id": "BC-17.3-001",
            "description": "Replaced input_file_name() with col('_metadata.file_name') in DataFrame API",
            "count": api_fix_count,
            "pattern": "DataFrame API"
        })
    content = new_content
    
    return content, applied, warnings


def fix_sql_not_syntax(content: str) -> Tuple[str, List[Dict], List[str]]:
    """
    Fix '!' syntax for NOT in SQL files.
    Replaces all variants: IF ! EXISTS, IS ! NULL, ! IN, ! BETWEEN, ! LIKE, ! EXISTS
    
    Returns:
        Tuple of (modified_content, applied_fixes, warnings)
    """
    applied = []
    warnings = []
    
    patterns = [
        (r'IF\s*!\s*EXISTS', 'IF NOT EXISTS', "IF ! EXISTS → IF NOT EXISTS"),
        (r'IS\s*!\s*NULL', 'IS NOT NULL', "IS ! NULL → IS NOT NULL"),
        (r'(\s)!\s*IN\b', r'\1NOT IN', "! IN → NOT IN"),
        (r'(\s)!\s*BETWEEN\b', r'\1NOT BETWEEN', "! BETWEEN → NOT BETWEEN"),
        (r'(\s)!\s*LIKE\b', r'\1NOT LIKE', "! LIKE → NOT LIKE"),
        (r'(\s)!\s*EXISTS\b', r'\1NOT EXISTS', "! EXISTS → NOT EXISTS"),
    ]
    
    for find, replace, desc in patterns:
        pattern = re.compile(find, re.IGNORECASE)
        matches = pattern.findall(content)
        if matches:
            content = pattern.sub(replace, content)
            applied.append({
                "fix_id": "BC-15.4-003",
                "description": desc,
                "count": len(matches),
                "pattern": find
            })
    
    return content, applied, warnings


def fix_scala_213_collections(content: str) -> Tuple[str, List[Dict], List[str]]:
    """
    Fix Scala 2.13 collection changes.
    Handles:
    - JavaConverters → CollectionConverters
    - .to[List] → .to(List)
    - Traversable → Iterable
    - Stream → LazyList
    - .toIterator → .iterator
    - .view.force → .view.to(List)
    - 'symbol → Symbol("symbol")
    
    Returns:
        Tuple of (modified_content, applied_fixes, warnings)
    """
    applied = []
    warnings = []
    
    # Pattern 1: JavaConverters import
    jc_pattern = re.compile(r'import\s+scala\.collection\.JavaConverters\._')
    new_content, count1 = jc_pattern.subn('import scala.jdk.CollectionConverters._', content)
    if count1:
        applied.append({
            "fix_id": "BC-16.4-001a",
            "description": "Replaced JavaConverters with CollectionConverters (Scala 2.13)",
            "count": count1,
            "pattern": "JavaConverters"
        })
    content = new_content
    
    # Pattern 2: .to[Collection] syntax
    collection_types = ['List', 'Set', 'Vector', 'Seq', 'Array', 'Map', 'IndexedSeq', 'Buffer']
    for coll_type in collection_types:
        pattern = re.compile(rf'\.to\s*\[\s*{coll_type}\s*\]')
        new_content, count = pattern.subn(f'.to({coll_type})', content)
        if count:
            applied.append({
                "fix_id": "BC-16.4-001b",
                "description": f"Replaced .to[{coll_type}] with .to({coll_type}) (Scala 2.13)",
                "count": count,
                "pattern": f".to[{coll_type}]"
            })
        content = new_content
    
    # Pattern 3: TraversableOnce → IterableOnce
    trav_once_pattern = re.compile(r'\bTraversableOnce\b')
    new_content, count3 = trav_once_pattern.subn('IterableOnce', content)
    if count3:
        applied.append({
            "fix_id": "BC-16.4-001c",
            "description": "Replaced TraversableOnce with IterableOnce (Scala 2.13)",
            "count": count3,
            "pattern": "TraversableOnce"
        })
    content = new_content
    
    # Pattern 4: Traversable → Iterable (but not TraversableOnce)
    trav_pattern = re.compile(r'\bTraversable\b(?!Once)')
    new_content, count4 = trav_pattern.subn('Iterable', content)
    if count4:
        applied.append({
            "fix_id": "BC-16.4-001d",
            "description": "Replaced Traversable with Iterable (Scala 2.13)",
            "count": count4,
            "pattern": "Traversable"
        })
    content = new_content
    
    # Pattern 5: Stream.from/continually/iterate → LazyList
    stream_pattern = re.compile(r'\bStream\s*\.\s*(from|continually|iterate)')
    new_content, count5 = stream_pattern.subn(r'LazyList.\1', content)
    if count5:
        applied.append({
            "fix_id": "BC-16.4-001e",
            "description": "Replaced Stream with LazyList (Scala 2.13)",
            "count": count5,
            "pattern": "Stream"
        })
    content = new_content
    
    # Pattern 6: .toIterator → .iterator
    toiter_pattern = re.compile(r'\.toIterator\b')
    new_content, count6 = toiter_pattern.subn('.iterator', content)
    if count6:
        applied.append({
            "fix_id": "BC-16.4-001f",
            "description": "Replaced .toIterator with .iterator (Scala 2.13)",
            "count": count6,
            "pattern": ".toIterator"
        })
    content = new_content
    
    # Pattern 7: .view.force → .view.to(List)
    # Handle various patterns: .view.map(...).force, .view.filter(...).force, etc.
    viewforce_pattern = re.compile(r'(\.view(?:\s*\.[^.]+)*)\s*\.\s*force\b')
    new_content, count7 = viewforce_pattern.subn(r'\1.to(List)', content)
    if count7:
        applied.append({
            "fix_id": "BC-16.4-001g",
            "description": "Replaced .view.force with .view.to(List) (Scala 2.13)",
            "count": count7,
            "pattern": ".view.force"
        })
    content = new_content
    
    # Pattern 8: 'symbol → Symbol("symbol")
    symbol_pattern = re.compile(r"'([a-zA-Z_][a-zA-Z0-9_]*)\b")
    new_content, count8 = symbol_pattern.subn(r'Symbol("\1")', content)
    if count8:
        applied.append({
            "fix_id": "BC-16.4-001i",
            "description": "Replaced symbol literals with Symbol() (Scala 2.13)",
            "count": count8,
            "pattern": "'symbol"
        })
    content = new_content
    
    # Warnings for manual review cases
    if re.search(r'\.toStream\b', content):
        warnings.append(
            "⚠️  .toStream found - consider using .to(LazyList) for lazy evaluation in Scala 2.13"
        )
    
    if re.search(r'\bStream\[', content):
        warnings.append(
            "⚠️  Stream type found - Stream is deprecated in Scala 2.13, consider LazyList"
        )
    
    if re.search(r'\b(HashMap|HashSet)\s*[\[\(]', content):
        warnings.append(
            "⚠️  HashMap/HashSet found - iteration order changed in Scala 2.13. Don't rely on order."
        )
    
    if re.search(r'\bcollection\.Seq\b', content):
        warnings.append(
            "⚠️  collection.Seq found - now refers to immutable.Seq in Scala 2.13. Use explicit import."
        )
    
    return content, applied, warnings


# ============================================================================
# MAIN FIX APPLICATION LOGIC
# ============================================================================

def should_apply_fix(fix_ids: Optional[List[str]], fix_id: str, sub_ids: List[str] = None) -> bool:
    """
    Check if a fix should be applied based on the requested fix_ids.
    
    Args:
        fix_ids: List of requested fix IDs (or None for all)
        fix_id: The main fix ID (e.g., 'BC-16.4-001')
        sub_ids: Optional list of sub-IDs (e.g., ['BC-16.4-001a', 'BC-16.4-001b'])
    
    Returns:
        True if the fix should be applied
    """
    if fix_ids is None:
        return True
    
    # Check main fix ID
    if fix_id in fix_ids:
        return True
    
    # Check sub-IDs
    if sub_ids:
        for sub_id in sub_ids:
            if sub_id in fix_ids:
                return True
    
    return False


def apply_fixes_to_content(
    content: str,
    file_type: str,
    fix_ids: Optional[List[str]] = None
) -> Tuple[str, List[Dict], List[str]]:
    """
    Apply fixes to file content using safe, specialized fix functions.
    
    Returns:
        Tuple of (modified_content, list of applied fixes, list of warnings)
    """
    all_applied = []
    all_warnings = []
    
    # Scala 2.13 sub-fix IDs
    scala_213_sub_ids = [
        'BC-16.4-001a', 'BC-16.4-001b', 'BC-16.4-001c', 'BC-16.4-001d',
        'BC-16.4-001e', 'BC-16.4-001f', 'BC-16.4-001g', 'BC-16.4-001i'
    ]
    
    if file_type == '.py':
        # BC-17.3-001: input_file_name removal
        if should_apply_fix(fix_ids, 'BC-17.3-001'):
            content, applied, warnings = fix_python_input_file_name_import(content)
            all_applied.extend(applied)
            all_warnings.extend(warnings)
            
            content, applied, warnings = fix_python_input_file_name_usage(content)
            all_applied.extend(applied)
            all_warnings.extend(warnings)
    
    elif file_type == '.scala':
        # BC-17.3-001: input_file_name removal
        if should_apply_fix(fix_ids, 'BC-17.3-001'):
            content, applied, warnings = fix_scala_input_file_name(content)
            all_applied.extend(applied)
            all_warnings.extend(warnings)
        
        # BC-16.4-001: Scala 2.13 collections (accepts BC-16.4-001 or any sub-ID)
        if should_apply_fix(fix_ids, 'BC-16.4-001', scala_213_sub_ids):
            content, applied, warnings = fix_scala_213_collections(content)
            all_applied.extend(applied)
            all_warnings.extend(warnings)
    
    elif file_type == '.sql':
        # BC-17.3-001: input_file_name in SQL
        if should_apply_fix(fix_ids, 'BC-17.3-001'):
            pattern = re.compile(r'\binput_file_name\s*\(\s*\)', re.IGNORECASE)
            new_content, count = pattern.subn('_metadata.file_name', content)
            if count:
                all_applied.append({
                    "fix_id": "BC-17.3-001",
                    "description": "Replaced input_file_name() with _metadata.file_name in SQL",
                    "count": count,
                    "pattern": "input_file_name()"
                })
            content = new_content
        
        # BC-15.4-003: ! syntax for NOT (accepts BC-15.4-003 or BC-15.4-003b)
        if should_apply_fix(fix_ids, 'BC-15.4-003', ['BC-15.4-003b']):
            content, applied, warnings = fix_sql_not_syntax(content)
            all_applied.extend(applied)
            all_warnings.extend(warnings)
    
    return content, all_applied, all_warnings


def process_file(
    file_path: Path,
    fix_ids: Optional[List[str]] = None,
    dry_run: bool = False,
    create_backup: bool = True
) -> FixResult:
    """Process a single file and apply fixes."""
    result = FixResult(file_path=str(file_path))
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
    except Exception as e:
        result.errors.append(f"Could not read file: {e}")
        return result
    
    file_type = file_path.suffix.lower()
    modified_content, applied, warnings = apply_fixes_to_content(
        original_content, file_type, fix_ids
    )
    
    result.warnings = warnings
    
    if not applied:
        return result
    
    # Record the fixes
    for fix_info in applied:
        result.fixes_applied.append(Fix(
            file_path=str(file_path),
            line_number=0,
            original=fix_info["pattern"][:50],
            replacement=fix_info["description"],
            fix_id=fix_info["fix_id"],
            description=f"{fix_info['description']} ({fix_info['count']} occurrences)"
        ))
    
    if dry_run:
        return result
    
    # Create backup
    if create_backup:
        backup_path = str(file_path) + ".bak"
        try:
            shutil.copy2(file_path, backup_path)
            result.backup_path = backup_path
        except Exception as e:
            result.errors.append(f"Could not create backup: {e}")
            return result
    
    # Write modified content
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(modified_content)
    except Exception as e:
        result.errors.append(f"Could not write file: {e}")
        # Restore from backup if write failed
        if result.backup_path:
            shutil.copy2(result.backup_path, file_path)
    
    return result


def process_directory(
    root_path: Path,
    fix_ids: Optional[List[str]] = None,
    dry_run: bool = False,
    create_backup: bool = True,
    exclude_dirs: Optional[List[str]] = None
) -> List[FixResult]:
    """Process all files in a directory."""
    if exclude_dirs is None:
        exclude_dirs = ['.git', '__pycache__', 'node_modules', '.venv', 'venv', 'target', '.idea']
    
    results = []
    file_extensions = {'.py', '.sql', '.scala'}
    
    for root, dirs, files in os.walk(root_path):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        
        for filename in files:
            file_path = Path(root) / filename
            
            if file_path.suffix.lower() not in file_extensions:
                continue
            
            result = process_file(file_path, fix_ids, dry_run, create_backup)
            if result.fixes_applied or result.errors or result.warnings:
                results.append(result)
    
    return results


def print_report(results: List[FixResult], dry_run: bool):
    """Print a report of applied fixes."""
    print()
    print("=" * 70)
    if dry_run:
        print("DATABRICKS LTS MIGRATION - FIX PREVIEW (DRY RUN)")
    else:
        print("DATABRICKS LTS MIGRATION - FIX REPORT")
    print("=" * 70)
    print()
    
    if not results:
        print("No fixes needed - codebase appears clean!")
        return
    
    total_fixes = 0
    files_modified = 0
    all_warnings = []
    
    for result in results:
        if result.fixes_applied or result.warnings:
            files_modified += 1
            print(f"📄 {result.file_path}")
            
            if result.backup_path and not dry_run:
                print(f"   Backup: {result.backup_path}")
            
            for fix in result.fixes_applied:
                total_fixes += 1
                action = "Would apply" if dry_run else "Applied"
                print(f"   ✅ [{fix.fix_id}] {action}: {fix.description}")
            
            for warning in result.warnings:
                print(f"   {warning}")
                all_warnings.append(f"{result.file_path}: {warning}")
            
            print()
        
        for error in result.errors:
            print(f"❌ {result.file_path}: {error}")
    
    print("-" * 70)
    if dry_run:
        print(f"DRY RUN SUMMARY: Would apply {total_fixes} fix(es) to {files_modified} file(s)")
        print("\nRun without --dry-run to apply these fixes.")
    else:
        print(f"SUMMARY: Applied {total_fixes} fix(es) to {files_modified} file(s)")
        print("\nRun validate-migration.py to verify all fixes were applied correctly.")
    
    if all_warnings:
        print()
        print("⚠️  MANUAL REVIEW REQUIRED:")
        print("-" * 70)
        for warning in all_warnings:
            print(f"   • {warning}")
    
    print()
    print("=" * 70)
    print("IMPORTANT: Always perform manual code review after applying fixes.")
    print("Some patterns may require human judgment for correct remediation.")
    print("=" * 70)


# Available fix definitions for documentation
FIX_DEFINITIONS = {
    "BC-17.3-001": {
        "name": "input_file_name() Removed",
        "file_types": [".py", ".sql", ".scala"],
        "description": "Replaces input_file_name() with _metadata.file_name"
    },
    "BC-15.4-003": {
        "name": "'!' Syntax for NOT",
        "file_types": [".sql"],
        "description": "Replaces ! with NOT in SQL (IF ! EXISTS → IF NOT EXISTS)"
    },
    "BC-16.4-001": {
        "name": "Scala 2.13 Collection Changes (all)",
        "file_types": [".scala"],
        "description": "Fixes JavaConverters, .to[List], Traversable, Stream, .toIterator, .view.force, Symbol literals"
    },
    "BC-16.4-001a": {
        "name": "Scala JavaConverters",
        "file_types": [".scala"],
        "description": "Replaces JavaConverters with CollectionConverters"
    },
    "BC-16.4-001b": {
        "name": "Scala .to[Collection]",
        "file_types": [".scala"],
        "description": "Replaces .to[List] with .to(List)"
    },
    "BC-16.4-001c": {
        "name": "Scala TraversableOnce",
        "file_types": [".scala"],
        "description": "Replaces TraversableOnce with IterableOnce"
    },
    "BC-16.4-001d": {
        "name": "Scala Traversable",
        "file_types": [".scala"],
        "description": "Replaces Traversable with Iterable"
    },
    "BC-16.4-001e": {
        "name": "Scala Stream",
        "file_types": [".scala"],
        "description": "Replaces Stream with LazyList"
    },
    "BC-16.4-001f": {
        "name": "Scala .toIterator",
        "file_types": [".scala"],
        "description": "Replaces .toIterator with .iterator"
    },
    "BC-16.4-001g": {
        "name": "Scala .view.force",
        "file_types": [".scala"],
        "description": "Replaces .view.force with .view.to(List)"
    },
    "BC-16.4-001i": {
        "name": "Scala Symbol Literals",
        "file_types": [".scala"],
        "description": "Replaces 'symbol with Symbol(\"symbol\")"
    },
}


def main():
    parser = argparse.ArgumentParser(
        description="Automatically apply fixes for Databricks LTS breaking changes",
        epilog="IMPORTANT: This script provides best-effort fixes. Manual review is always recommended."
    )
    parser.add_argument(
        "path",
        help="Path to codebase directory to fix"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Show what would be changed without modifying files"
    )
    parser.add_argument(
        "--fix", "-f",
        help="Only apply specific fixes (comma-separated IDs, e.g., BC-17.3-001,BC-16.4-001a)"
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        default=True,
        help="Create .bak files before modifying (default: true)"
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Don't create backup files"
    )
    parser.add_argument(
        "--list-fixes",
        action="store_true",
        help="List available fix IDs and exit"
    )
    
    args = parser.parse_args()
    
    if args.list_fixes:
        print("Available fix IDs:")
        print("-" * 50)
        for fix_id, fix_def in FIX_DEFINITIONS.items():
            print(f"  {fix_id}: {fix_def['name']}")
            print(f"    File types: {', '.join(fix_def['file_types'])}")
            print(f"    {fix_def['description']}")
            print()
        print("-" * 50)
        print("NOTE: These are best-effort fixes. Manual review recommended.")
        sys.exit(0)
    
    root_path = Path(args.path)
    if not root_path.exists():
        print(f"Error: Path does not exist: {root_path}", file=sys.stderr)
        sys.exit(1)
    
    fix_ids = None
    if args.fix:
        fix_ids = [f.strip() for f in args.fix.split(",")]
        # Validate fix IDs
        for fid in fix_ids:
            if fid not in FIX_DEFINITIONS:
                print(f"Error: Unknown fix ID: {fid}", file=sys.stderr)
                print(f"Available: {', '.join(FIX_DEFINITIONS.keys())}")
                sys.exit(1)
    
    create_backup = args.backup and not args.no_backup
    
    print(f"Processing {root_path}...")
    if args.dry_run:
        print("(DRY RUN - no files will be modified)")
    
    results = process_directory(
        root_path,
        fix_ids=fix_ids,
        dry_run=args.dry_run,
        create_backup=create_backup
    )
    
    print_report(results, args.dry_run)
    
    # Exit codes
    if not results:
        sys.exit(2)  # No fixes needed
    
    has_errors = any(r.errors for r in results)
    if has_errors:
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
