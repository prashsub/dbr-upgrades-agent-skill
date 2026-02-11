---
name: databricks-dbr-migration
description: Find, fix, and validate breaking changes when upgrading Databricks Runtime between LTS versions (13.3 to 17.3). Use this skill when users ask to scan code for DBR compatibility issues, automatically fix breaking changes, validate migrations, or upgrade Databricks workflows. Covers Spark 3.4 to 4.0, Scala 2.12 to 2.13, Delta Lake, Auto Loader, Python UDFs, and SQL syntax. Triggers on "scan", "fix", "validate", "breaking changes", "DBR upgrade", "migration", "compatibility", "DBR 17.3", "DBR 16.4", "DBR 15.4".
license: Apache-2.0
compatibility: Requires file system access. Works with Databricks notebooks (.py and .ipynb), Python, SQL, and Scala files.
metadata:
  author: Databricks Solution Architect
  version: "5.0.0"
  domain: platform-migration
  last-updated: "2026-02-10"
allowed-tools: Read Write Bash(grep:*) Bash(find:*) Bash(python:*)
---

# Databricks LTS Migration Agent

This skill enables agents to **find**, **fix**, and **validate** breaking changes when upgrading Databricks Runtime from 13.3 LTS to 17.3 LTS.

## Agent Capabilities

1. **SCAN** - Find breaking changes in code (32 patterns in scanner script, plus special detection for temp view reuse)
2. **FIX** - Apply automatic remediations (10 Scala/SQL patterns via apply-fixes.py)
3. **FLAG** - Explicitly flag items requiring manual review or configuration testing
4. **VALIDATE** - Verify fixes are correct (12 critical patterns checked)
5. **SUMMARIZE** - Add a summary markdown cell to the notebook

> **Note:** The workspace profiler (`developer-guide/workspace-profiler.py`) has the most comprehensive coverage with 35 patterns. The agent scanner implements 32 regex-based patterns plus special detection for BC-SC-002 (temp view reuse).

---

## CRITICAL: Add Summary as Markdown Cell

**After scanning, fixing, or validating, ALWAYS add a summary as a NEW MARKDOWN CELL at the end of the notebook.**

### How to Add Summaries

1. **Load the appropriate template** from `assets/markdown-templates/`
2. **Replace all template variables** with actual findings (counts, line numbers, BC-IDs)
3. **Add as a new cell** at the end of the notebook

### Available Templates

| Action | Template File | When to Use |
|--------|---------------|-------------|
| **SCAN** | `assets/markdown-templates/scan-summary.md` | After scanning code for breaking changes |
| **FIX** | `assets/markdown-templates/fix-summary.md` | After applying automatic fixes |
| **VALIDATE** | `assets/markdown-templates/validation-report.md` | After validating fixes were applied correctly |

### Template Variables

**All templates use variables like:**
- `{SCAN_DATE}` / `{FIX_DATE}` / `{VALIDATION_DATE}` -> Current date/time (YYYY-MM-DD HH:MM)
- `{TARGET_VERSION}` -> Target DBR version (e.g., "17.3")
- `{AUTO_FIX_COUNT}` -> Number of auto-fixable issues
- `{ASSISTED_FIX_COUNT}` -> Number of assisted fix items (snippet provided, developer decides)
- `{MANUAL_REVIEW_COUNT}` -> Number of manual review items
- `{AUTO_FIX_ITEMS}` / `{APPLIED_FIXES}` -> Table rows with findings/fixes
- `{ASSISTED_FIX_ITEMS}` -> Table rows for assisted fix items
- `{ASSISTED_FIX_SNIPPETS}` -> Code snippet blocks for each assisted fix
- `{ASSISTED_FIX_VALIDATION}` -> Validation status for each assisted fix
- `{MANUAL_REVIEW_ITEMS}` -> Table rows for manual review items

**Example table row format:**
```
| 42 | BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |
```

> See `assets/markdown-templates/README.md` for complete template documentation.

### CRITICAL: How to Generate `{ASSISTED_FIX_SNIPPETS}`

**This is the most important variable.** The agent MUST read the actual code around each finding and generate a real, copy-paste-ready replacement — NOT a generic example.

**Workflow for EACH Assisted Fix finding:**
1. **READ** the code: Read 5-10 lines around the detected pattern to understand context
2. **EXTRACT** real names: Pull the actual variable names, function names, view names, UDF return types, column names, etc. from the code
3. **GENERATE** the fix: Write the replacement code using those exact extracted names — the developer should be able to copy-paste it directly
4. **SHOW** before -> after: Include the original line(s) as a comment, then the fixed version
5. **ADD** a review note: Explain what to verify before applying

**REQUIRED: For the per-BC-ID template blocks and key rules, you MUST read [references/snippet-generation-guide.md](references/snippet-generation-guide.md).** Do NOT generate snippets without reading this file first. It contains the exact template format for each BC-ID (BC-SC-002, BC-SC-003, BC-SC-004, BC-17.3-005, BC-17.3-002, BC-15.4-005/BC-15.4-002, BC-13.3-002/BC-16.4-003, BC-16.4-004).

---

## CRITICAL: Migration Path Awareness

**IMPORTANT:** When scanning, consider both the SOURCE (current) and TARGET DBR versions. Only flag patterns that are relevant to the migration path.

### Source Version Filtering

If the user is migrating FROM a specific version, skip patterns that were already addressed:

| Migration Path | Patterns to Flag |
|----------------|------------------|
| **13.3 -> 17.3** | BC-14.3-*, BC-15.4-*, BC-16.4-*, BC-17.3-* (skip BC-13.3-*) |
| **14.3 -> 17.3** | BC-15.4-*, BC-16.4-*, BC-17.3-* |
| **15.4 -> 17.3** | BC-16.4-*, BC-17.3-* |
| **Any -> 16.4** | All patterns up to BC-16.4-* |

**Example:** User says "I'm upgrading from 13.3 to 17.3" - skip BC-13.3-* patterns because they're already working on 13.3.

### Target Version Filtering

| Target Version | Patterns to Flag |
|----------------|------------------|
| **17.3** | All patterns up to 17.3 |
| **16.4** | All except BC-17.3-* patterns |
| **15.4** | All except BC-16.4-* and BC-17.3-* patterns |

**BC-15.4-001 Guidance (VariantType in UDF):**
- Flag as MEDIUM severity regardless of version
- Recommend testing on target DBR or using StringType + JSON as safer alternative
- Do NOT claim it's "fixed" in later versions - behavior may vary

---

## CRITICAL: Three Tiers of Findings

When scanning code, categorize ALL findings into these three categories and handle them appropriately:

### Category 1: AUTO-FIX (10 patterns)
**Action: Automatically apply the fix. No developer review needed.**

| ID | Pattern | Fix |
|----|---------|-----|
| BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |
| BC-15.4-003 | `IF !`, `IS !`, `! IN`, etc. | Replace `!` with `NOT` |
| BC-16.4-001a | `JavaConverters` | Replace with `CollectionConverters` |
| BC-16.4-001b | `.to[List]` | Replace with `.to(List)` |
| BC-16.4-001c | `TraversableOnce` | Replace with `IterableOnce` |
| BC-16.4-001d | `Traversable` | Replace with `Iterable` |
| BC-16.4-001e | `Stream.from()` | Replace with `LazyList.from()` |
| BC-16.4-001f | `.toIterator` | Replace with `.iterator` |
| BC-16.4-001g | `.view.force` | Replace with `.view.to(List)` |
| BC-16.4-001i | `'symbol` literal | Replace with `Symbol("symbol")` |

### Category 2: ASSISTED FIX (11 patterns)
**Action: Generate an exact suggested fix snippet in the scan output. Developer reviews and decides whether to apply.**

The agent analyzes the code context and produces a copy-paste-ready fix for each finding. The developer can accept, modify, or reject each suggestion.

| ID | Pattern | Suggested Fix Snippet the Agent Generates |
|----|---------|-------------------------------------------|
| BC-SC-002 | Same temp view name used multiple times | UUID-suffixed replacement for each duplicate view name, plus reminder to update `spark.table()` references |
| BC-SC-003 | UDF referencing external variables | Factory wrapper around the detected UDF, passing the captured variable as a parameter |
| BC-SC-004 | `df.columns` / `df.schema` in loops | Cache statement before the loop: `existing_columns = set(df.columns)` |
| BC-17.3-005 | `DecimalType` without explicit precision or bare `cast("decimal")` | Explicit `DecimalType(p, s)` replacement with a suggested precision |
| BC-13.3-002 | Parquet with timestamps | Commented `spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")` |
| BC-15.4-002 | JDBC reads (useNullCalendar) | Self-comparison test (read with `useNullCalendar=true` vs `false`, diff with `exceptAll`) + conditional config fix |
| BC-15.4-005 | JDBC reads (general) | Self-comparison test (read with `useNullCalendar=true` vs `false`, diff with `exceptAll`) + conditional config fix |
| BC-16.4-003 | Cached data source reads | Commented `spark.conf.set("spark.sql.legacy.readFileSourceTableCacheIgnoreOptions", "true")` |
| BC-16.4-004 | `materializeSource.*none` | Replace `"none"` with `"auto"` |
| BC-16.4-006 | Auto Loader `cleanSource` | Commented config with test-first guidance |
| BC-17.3-002 | Auto Loader without explicit incremental | `.option("cloudFiles.useIncrementalListing", "auto")` insertion |

### Category 3: MANUAL REVIEW (10 patterns)
**Action: FLAG for developer review. No fix snippet generated because the correct action depends on intent.**

| ID | Pattern | Flag Message |
|----|---------|--------------|
| BC-13.3-001 | `MERGE INTO` | **FLAG:** ANSI mode now throws CAST_OVERFLOW. Review type casting for potential overflow |
| BC-13.3-003 | `overwriteSchema` + dynamic partition | **FLAG:** Cannot combine both. Separate schema evolution from partition overwrites |
| BC-13.3-004 | MERGE/UPDATE with type mismatch | **FLAG:** ANSI store assignment policy changed. Review types for overflow |
| BC-15.4-001 | `VariantType()` in UDF | **FLAG:** May throw exception in 15.4+. Test or use StringType + JSON |
| BC-15.4-004 | `CREATE VIEW (col TYPE)` | **FLAG:** Column types in VIEW not allowed in 15.4+. Remove types, use CAST in SELECT |
| BC-15.4-006 | `CREATE VIEW` | **FLAG:** Schema binding mode changed. Review schema evolution behavior |
| BC-16.4-002 | `HashMap`/`HashSet` | **FLAG:** Iteration order changed in Scala 2.13. Don't rely on order |
| BC-16.4-001h | `collection.Seq` | **FLAG:** Now refers to immutable.Seq. Use explicit import |
| BC-SC-001 | try/except around DataFrame transforms | **FLAG:** In Spark Connect, errors appear at action time. Add `_ = df.columns` for early validation |
| BC-17.3-003 | `array()`/`map()`/`struct()` with nulls | **FLAG:** Spark Connect now preserves nulls in literal constructions instead of coercing to defaults. Only add `coalesce()` if downstream logic specifically needs the old "null to default" behavior. |

---

## Capability 1: SCAN - Find Breaking Changes

**REQUIRED: Before scanning, you MUST read and follow [references/scan-workflow.md](references/scan-workflow.md).** Do NOT attempt to scan without reading this file first. It contains the grep patterns for all severity levels, special detection logic for BC-SC-002/BC-SC-003/BC-SC-004, report format, and scan summary cell generation instructions.

**For each assisted fix finding, you MUST read and follow [references/snippet-generation-guide.md](references/snippet-generation-guide.md).** Do NOT generate generic snippets. The guide contains the exact template format for each BC-ID using the actual variable names, function names, and line numbers from the code.

---

## Capability 2: FIX - Apply Automatic Remediations

**REQUIRED: Before fixing, you MUST read and follow [references/fix-workflow.md](references/fix-workflow.md).** It contains the markdown cell template, multi-file fix strategy, and code transformations for all 10 auto-fix patterns (BC-17.3-001, BC-15.4-003, BC-15.4-001, BC-16.4-001a-i).

**For assisted fix snippets in the fix summary, you MUST also read [references/snippet-generation-guide.md](references/snippet-generation-guide.md).**

---

## Capability 3: VALIDATE - Verify Fixes

**REQUIRED: Before validating, you MUST read and follow [references/validate-workflow.md](references/validate-workflow.md).** It contains syntax validation, replacement verification, assisted fix status checks (including JDBC self-test verification), manual review acknowledgment checks, and the validation report format.

---

## Quick Reference: All Breaking Changes

### HIGH Severity - Code-Level Breaking Changes

| ID | Pattern | Fix |
|----|---------|-----|
| BC-17.3-001 | `input_file_name()` | `_metadata.file_name` |
| BC-13.3-001 | MERGE INTO type overflow | Add explicit CAST with bounds check |
| BC-16.4-001a | `JavaConverters` | `CollectionConverters` |
| BC-16.4-001c | `TraversableOnce` | `IterableOnce` |
| BC-16.4-001d | `Traversable` | `Iterable` |
| BC-16.4-002 | `HashMap`/`HashSet` ordering | Don't rely on iteration order |
| BC-SC-001 | Lazy schema analysis | Call `df.columns` for early validation |

### MEDIUM Severity - Potential Issues

| ID | Pattern | Fix |
|----|---------|-----|
| BC-15.4-003 | `IF !`, `IS !`, `! IN` | Use `NOT` |
| BC-16.4-001b | `.to[List]` | `.to(List)` |
| BC-16.4-001e | `Stream.from()` | `LazyList.from()` |
| BC-16.4-001f | `.toIterator` | `.iterator` |
| BC-16.4-001g | `.view.force` | `.view.to(List)` |
| BC-16.4-001h | `collection.Seq` | Use explicit `immutable.Seq` |
| BC-13.3-003 | `overwriteSchema` + dynamic partition | Separate operations |
| BC-17.3-002 | Auto Loader incremental | Set option explicitly |
| BC-15.4-006 | VIEW schema binding | Review schema evolution |
| BC-16.4-003 | Data source cache | Set legacy cache option |
| BC-16.4-006 | Auto Loader cleanSource | Review cleanup behavior |

### LOW Severity - Subtle Changes

| ID | Pattern | Fix |
|----|---------|-----|
| BC-15.4-001 | `VariantType` in UDF | Test or use StringType + JSON |
| BC-15.4-004 | VIEW column types | Remove types, CAST in SELECT |
| BC-13.3-002 | Parquet TIMESTAMP_NTZ | Set inferTimestampNTZ=false |
| BC-15.4-002 | JDBC timestamp | Run self-test; set useNullCalendar=false if diff > 0 |
| BC-15.4-005 | JDBC reads | Run self-test; set useNullCalendar=false if diff > 0 |
| BC-16.4-004 | MERGE materializeSource=none | Remove or use "auto" |
| BC-16.4-001i | `'symbol` literal | `Symbol("symbol")` |
| BC-16.4-005 | Json4s library | Review json4s usage |
| BC-17.3-003 | Null handling in literals | Handle nulls explicitly |
| BC-17.3-004 | Decimal precision | Specify precision/scale |
| BC-14.3-001 | Thriftserver hive.aux.jars.path | Config removed |
| BC-13.3-004 | ANSI store assignment | Review type policies |

### Spark Connect Behavioral Changes (Serverless/Connect Users)

| ID | Severity | Behavior | Best Practice |
|----|----------|----------|---------------|
| BC-SC-001 | HIGH | Lazy schema analysis | Call `df.columns` to trigger early error detection |
| BC-SC-003 | LOW | UDF late binding | Use function factory to capture variables |
| BC-SC-004 | LOW | Schema access RPC | Cache `df.columns` locally in loops |

Source: [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)

---

## Example Agent Workflow

For a complete end-to-end example of how to handle a scan-and-fix request, see [references/example-workflow.md](references/example-workflow.md).

---

## Reference Files

### Workflow References (Agent Instructions)

- [references/snippet-generation-guide.md](references/snippet-generation-guide.md) - **CRITICAL:** Per-BC-ID template blocks for generating copy-paste-ready fix snippets
- [references/scan-workflow.md](references/scan-workflow.md) - Step-by-step scan instructions with grep patterns and report format
- [references/fix-workflow.md](references/fix-workflow.md) - Fix instructions with code transformations and markdown cell template
- [references/validate-workflow.md](references/validate-workflow.md) - Validation steps and report format
- [references/example-workflow.md](references/example-workflow.md) - Complete end-to-end agent workflow example

### Knowledge References

- [references/QUICK-REFERENCE.md](references/QUICK-REFERENCE.md) - Quick lookup of all breaking changes
- [references/BREAKING-CHANGES.md](references/BREAKING-CHANGES.md) - Complete breaking changes with code examples
- [references/SPARK-CONNECT-GUIDE.md](references/SPARK-CONNECT-GUIDE.md) - Spark Connect vs Classic behavioral differences
- [references/MIGRATION-CHECKLIST.md](references/MIGRATION-CHECKLIST.md) - Full migration checklist
- [references/SCALA-213-GUIDE.md](references/SCALA-213-GUIDE.md) - Scala 2.13 migration details

### Scripts and Assets

- [scripts/scan-breaking-changes.py](scripts/scan-breaking-changes.py) - Automated scanner
- [scripts/apply-fixes.py](scripts/apply-fixes.py) - Automatic fix application
- [scripts/validate-migration.py](scripts/validate-migration.py) - Validation script
- [assets/fix-patterns.json](assets/fix-patterns.json) - Machine-readable fix patterns

## Additional Resources

- [Databricks Release Notes](https://docs.databricks.com/release-notes/runtime/supported.html) - Official documentation
- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Compare Spark Connect to Spark Classic](https://learn.microsoft.com/en-us/azure/databricks/spark/connect-vs-classic)
- [DBR 17.3 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/17.3lts.html)
- [DBR 16.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/16.4lts.html)
- [DBR 15.4 LTS Release Notes](https://docs.databricks.com/en/release-notes/runtime/15.4lts.html)

---

*Generic DBR migration skill | Last Updated: February 2026*
