# DBR Migration Training & Demo Notebook

This folder contains comprehensive training and demo materials for DBR migration from 13.3 LTS to 17.3 LTS.

## Files

| File | Purpose | Lines |
|------|---------|-------|
| `dbr_migration_demo_notebook.py` | **Training + Demo** - Comprehensive guide with all breaking changes | ~1300 |
| `dbr_migration_test_notebook.py` | **Test Notebook** - Compact notebook with all 17 breaking changes for quick Agent testing | ~350 |
| `dbr_migration_demo_notebook_FIXED.py` | **Reference** - Shows all issues remediated | ~800 |

## Quick Start: Which Notebook to Use?

| Use Case | Notebook |
|----------|----------|
| **Testing the Agent Skill quickly** | `dbr_migration_test_notebook.py` |
| **Training developers on breaking changes** | `dbr_migration_demo_notebook.py` |
| **Seeing the fixed code** | `dbr_migration_demo_notebook_FIXED.py` |

## What's in the Training Notebook?

The demo notebook now serves **dual purposes**:

### 📖 1. Training Document

Each breaking change includes:
- **📖 What Changed** - Clear explanation of the change
- **🔍 Detection Pattern** - Regex pattern used by the scanner
- **📋 Decision Matrix** - When to fix vs. skip
- **❌ Problem Code** - Runnable examples that break
- **✅ Fixed Code** - Correct implementation

### 🔧 2. Demo Notebook

Contains intentional breaking patterns that the Agent Skill can:
1. **SCAN** - Detect all issues
2. **FIX** - Auto-remediate where possible
3. **VALIDATE** - Verify corrections

## Breaking Changes Covered (17 Total)

### 🔴 Auto-Fix (7 patterns)

| ID | Pattern | Fix |
|----|---------|-----|
| BC-17.3-001 | `input_file_name()` | `_metadata.file_name` |
| BC-15.4-003 | `IF ! EXISTS`, `IS ! NULL` | Use `NOT` |
| BC-16.4-001a | `JavaConverters` | `CollectionConverters` |
| BC-16.4-001b | `.to[List]` | `.to(List)` |
| BC-16.4-001c | `TraversableOnce` | `IterableOnce` |
| BC-16.4-001d | `Traversable` | `Iterable` |
| BC-16.4-001e | `Stream.from()` | `LazyList.from()` |

### 🟡 Manual Review (6 patterns)

| ID | Pattern | Decision Needed |
|----|---------|-----------------|
| BC-15.4-001 | `VariantType()` in UDF | Skip if 16.4+ |
| BC-15.4-004 | `CREATE VIEW (col TYPE)` | Remove types |
| BC-SC-001 | Lazy schema analysis | Add validation |
| BC-SC-002 | Temp view name reuse | Add UUID |
| BC-SC-003 | UDF late binding | Function factory |
| BC-SC-004 | Schema access in loops | Cache outside |

### ⚙️ Config Settings (4 patterns)

| ID | Setting | When to Add |
|----|---------|-------------|
| BC-13.3-002 | `parquet.inferTimestampNTZ` | Timestamps wrong |
| BC-15.4-002 | `jdbc.useNullCalendar` | JDBC timestamps wrong |
| BC-16.4-004 | `merge.materializeSource` | Using `"none"` |
| BC-17.3-002 | `cloudFiles.useIncrementalListing` | Slow Auto Loader |

## How to Use

### For Training

1. **Open the notebook** in Databricks
2. **Read through each section** - explanations are inline
3. **Run the code** (on DBR 13.3) to see it work
4. **Run on DBR 17.3** to see failures
5. **Use the decision matrices** to understand when to apply fixes

### For Demo

1. **Upload** `dbr_migration_demo_notebook.py` to Databricks
2. **Open Databricks Assistant** (agent mode)
3. **Run these commands:**

```
# Step 1: Scan
Scan this notebook for breaking changes when upgrading to DBR 17.3

# Step 2: Fix
Fix all the auto-fixable breaking changes

# Step 3: Review
Show me the manual review items and help me decide on each one

# Step 4: Validate
Validate that all breaking changes have been addressed
```

### For Side-by-Side Comparison

Open both notebooks:
- `dbr_migration_demo_notebook.py` - BEFORE (with issues)
- `dbr_migration_demo_notebook_FIXED.py` - AFTER (remediated)

Walk through each section showing the transformation.

## Notebook Structure

```
📓 dbr_migration_demo_notebook.py
│
├── 📊 Overview & Quick Reference Table
│   └── All 17 breaking changes at a glance
│
├── 🔧 Setup
│   └── NYC Taxi dataset loading
│
├── 🔴 SECTION 1: Auto-Fixable Code Changes
│   ├── BC-17.3-001: input_file_name()
│   │   ├── 📖 What Changed (with version table)
│   │   ├── 🔍 Detection Pattern (regex)
│   │   ├── ❌ Problem Code (3 patterns)
│   │   └── ✅ Correct Code
│   │
│   └── BC-15.4-003: ! Syntax for NOT
│       ├── 📖 What Changed (with pattern table)
│       ├── 🔍 Detection Patterns (2 regex)
│       ├── ❌ Problem SQL (5 patterns)
│       └── ✅ Correct SQL
│
├── 🟡 SECTION 2: Manual Review Items
│   ├── BC-15.4-001: VARIANT in UDF
│   │   ├── 📖 What Changed
│   │   ├── 📋 Decision Matrix
│   │   ├── ❌ Problem Code
│   │   └── ✅ Fix (for 15.4 only)
│   │
│   ├── BC-15.4-004: VIEW Column Types
│   ├── BC-SC-001: Lazy Schema Analysis
│   ├── BC-SC-002: Temp View Name Reuse
│   ├── BC-SC-003: UDF Late Binding
│   └── BC-SC-004: Schema Access in Loops
│
├── ⚙️ SECTION 3: Configuration Changes
│   ├── BC-13.3-002: Parquet Timestamp NTZ
│   ├── BC-15.4-002: JDBC useNullCalendar
│   ├── BC-16.4-004: MERGE materializeSource
│   └── BC-17.3-002: Auto Loader Incremental
│
├── 🔷 SECTION 4: Scala 2.13 Changes
│   ├── BC-16.4-001a: JavaConverters
│   ├── BC-16.4-001b: .to[Collection]
│   ├── BC-16.4-001c: TraversableOnce
│   ├── BC-16.4-001d: Traversable
│   └── BC-16.4-001e: Stream → LazyList
│
├── 📊 Complete Summary
│   └── Quick reference tables
│
└── 🎯 Demo Instructions
    └── Step-by-step Agent Skill usage
```

## Dataset

Uses NYC Taxi dataset (pre-loaded in Databricks):
```
/databricks-datasets/nyctaxi/tables/nyctaxi_yellow
```

## Expected Duration

| Use Case | Duration |
|----------|----------|
| Quick overview | 20-30 min |
| Full training | 60-90 min |
| Demo only | 15-20 min |
| Hands-on workshop | 2-3 hours |

## Troubleshooting

### Notebook won't run on DBR 13.3

Some patterns (like `VariantType`) were introduced after 13.3. Comment out those cells.

### NYC Taxi data not found

Use this fallback:
```python
taxi_df = spark.range(1000).withColumn("fare_amount", lit(10.0))
```

### Spark Connect patterns don't show different behavior

These only differ on **Serverless** or **Databricks Connect**. On classic clusters, both patterns work identically.

### Config changes don't cause visible issues

Config changes are behavioral - they don't "fail" but may produce different results. Focus on explaining the concepts.

### Scala examples don't run

Scala examples are shown as Python strings. For hands-on Scala training, create a separate Scala notebook.

---

*Last Updated: January 2026*
