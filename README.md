# Databricks Runtime LTS Migration Agent Skill

A comprehensive agent skill for upgrading Databricks Runtime from **13.3 LTS** to **17.3 LTS**, with automated breaking change detection, fixes, and validation.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![DBR](https://img.shields.io/badge/DBR-13.3%20→%2017.3-orange.svg)]()
[![Spark](https://img.shields.io/badge/Spark-3.4%20→%204.0-red.svg)]()

---

## 🎯 Overview

This agent skill helps you automatically detect and fix breaking changes when upgrading Databricks Runtime between LTS versions. It supports:

- **Apache Spark 3.4 → 4.0** migration
- **Scala 2.12 → 2.13** compatibility
- **Delta Lake** breaking changes
- **Auto Loader** schema evolution
- **Python UDFs** and **SQL syntax** updates
- **Spark Connect** compatibility

## ✨ Key Features

### 🔍 **Automated Scanning**
- Scan Python (`.py`), SQL (`.sql`), and Scala (`.scala`) files
- Detect 50+ breaking change patterns across 8 categories
- Severity-based classification (HIGH, MEDIUM, LOW)
- Line-level precision with context

### 🔧 **Automatic Fixes**
- Auto-remediation for common breaking changes
- Safe transformations with backup creation
- Detailed fix reports with before/after comparisons
- Pattern-based fixes using proven solutions

### ✅ **Validation & Testing**
- Syntax validation for fixed code
- Performance regression detection
- Output comparison tools
- Job-level sign-off tracking

---

## 📁 Repository Structure

```
.
├── databricks-dbr-migration/     # Agent skill definition (v4.0.0)
│   ├── SKILL.md                  # Main skill configuration
│   ├── assets/
│   │   ├── fix-patterns.json           # Automated fix patterns
│   │   ├── version-matrix.json         # DBR version compatibility
│   │   ├── migration-template.sql
│   │   └── markdown-templates/         # Summary templates
│   │       ├── scan-summary.md
│   │       ├── fix-summary.md
│   │       ├── validation-report.md
│   │       └── README.md
│   ├── references/               # Breaking changes documentation
│   │   ├── BREAKING-CHANGES.md   # Complete reference guide
│   │   ├── MIGRATION-CHECKLIST.md
│   │   ├── QUICK-REFERENCE.md
│   │   ├── SCALA-213-GUIDE.md
│   │   └── SPARK-CONNECT-GUIDE.md
│   └── scripts/                  # Python automation tools
│       ├── scan-breaking-changes.py
│       ├── apply-fixes.py
│       └── validate-migration.py
│
├── demo/                         # Example notebooks
│   ├── dbr_migration_demo_notebook.py        # Before
│   ├── dbr_migration_demo_notebook_FIXED.py  # After
│   ├── dbr_migration_test_notebook.py        # Multi-file test
│   └── utils/                                # Test utilities
│       ├── dbr_test_helpers.py
│       └── __init__.py
│
├── workspace-profiler/            # 🔍 Account-level scanner
│   ├── README.md                 # Profiler configuration & usage guide
│   └── workspace-profiler.py     # The profiler script
│
├── developer-guide/              # ⭐ Start here!
│   ├── README.md                 # Complete workflow guide
│   ├── BREAKING-CHANGES-EXPLAINED.md  # Detailed explanations
│   └── archive/                       # Detailed technical guides
│       ├── 01-skill-setup.md
│       ├── 02-using-assistant.md
│       ├── 03-quality-validation.md
│       ├── 04-performance-testing.md
│       ├── 05-rollout-checklist.md
│       ├── 06-output-validation-criteria.md
│       ├── 07-testing-validation-signoff-guide.md
│       ├── 08-tracker-templates.md
│       ├── 09-effective-prompts-guide.md
│       ├── VALIDATION-REPORT.md
│       ├── SCRIPTS-VALIDATION-REPORT.md
│       └── OPUS-REVIEW.md
│
└── README.md                     # This file
```

---

## 🚀 Quick Start

### 1️⃣ Install the Skill (One-Time Setup)

```bash
# Copy skill to your Databricks workspace
mkdir -p /Workspace/Users/$(whoami)/.assistant/skills/
cp -r databricks-dbr-migration /Workspace/Users/$(whoami)/.assistant/skills/

# Verify installation
ls /Workspace/Users/$(whoami)/.assistant/skills/databricks-dbr-migration/SKILL.md
```

### 2️⃣ Use the Agent (Main Workflow)

**First-time use** (include full skill path):

```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, 
scan and fix this notebook for DBR 17.3 compatibility
```

**After skill is loaded** (shorthand works):

```
@databricks-dbr-migration scan this notebook for DBR 17.3 breaking changes
```

```
@databricks-dbr-migration fix all auto-fixable issues
```

```
@databricks-dbr-migration validate all fixes were applied correctly
```

> 💡 **Pro Tip:** See [Developer Guide](developer-guide/README.md) for the complete workflow!

### Using Python Scripts

**1️⃣ Scan for Breaking Changes**

Use the Python script to scan your code:

```bash
# Full scan (all patterns)
python databricks-dbr-migration/scripts/scan-breaking-changes.py \
  --path /path/to/your/code \
  --output scan-results.json

# Migration path filtering (e.g., 13.3 → 17.3, skips BC-13.3-* patterns)
python databricks-dbr-migration/scripts/scan-breaking-changes.py \
  --path /path/to/your/code \
  --source-version 13.3 \
  --target-version 17.3 \
  --output scan-results.json
```

Or use the workspace profiler to scan all jobs and notebooks:

```python
# Run in Databricks notebook
# Configure migration path in CONFIG:
#   "source_dbr_version": "13.3"  # Current version (patterns <= this are skipped)
#   "target_dbr_version": "17.3"  # Target version
%run ./workspace-profiler/workspace-profiler.py
```

### 2️⃣ **Apply Automated Fixes**

```bash
python databricks-dbr-migration/scripts/apply-fixes.py \
  --input scan-results.json \
  --backup-dir ./backups
```

### 3️⃣ **Validate Changes**

```bash
python databricks-dbr-migration/scripts/validate-migration.py \
  --path /path/to/fixed/code \
  --report validation-report.html
```

---

## 📖 Documentation

### ⭐ Start Here

| Document | Description |
|----------|-------------|
| **[Developer Guide](developer-guide/README.md)** | 🚀 **Main workflow** - Profiler → Agent → Review → Test |
| [Breaking Changes Explained](developer-guide/BREAKING-CHANGES-EXPLAINED.md) | 📖 Every breaking change with examples |
| [Workspace Profiler](workspace-profiler/) | 🔍 Scan all jobs/notebooks in your workspace |

### Skill Reference

| Document | Description |
|----------|-------------|
| [Breaking Changes Reference](databricks-dbr-migration/references/BREAKING-CHANGES.md) | Complete list of all breaking changes |
| [Migration Checklist](databricks-dbr-migration/references/MIGRATION-CHECKLIST.md) | Step-by-step migration checklist |
| [Quick Reference](databricks-dbr-migration/references/QUICK-REFERENCE.md) | Quick lookup for common issues |
| [Scala 2.13 Guide](databricks-dbr-migration/references/SCALA-213-GUIDE.md) | Scala 2.12 → 2.13 migration |
| [Spark Connect Guide](databricks-dbr-migration/references/SPARK-CONNECT-GUIDE.md) | Spark Connect compatibility |

### Detailed Technical Guides (Archive)

For deep dives into specific topics, see [developer-guide/archive/](developer-guide/archive/):

| Document | Description |
|----------|-------------|
| [Skill Setup](developer-guide/archive/01-skill-setup.md) | Detailed installation instructions |
| [Using Assistant](developer-guide/archive/02-using-assistant.md) | Advanced assistant usage |
| [Effective Prompts Guide](developer-guide/archive/09-effective-prompts-guide.md) | 50+ prompt variations |
| [Quality Validation](developer-guide/archive/03-quality-validation.md) | Code quality validation framework |
| [Performance Testing](developer-guide/archive/04-performance-testing.md) | Performance regression testing |
| [Testing & Sign-Off Guide](developer-guide/archive/07-testing-validation-signoff-guide.md) | Complete testing framework |
| [Validation Reports](developer-guide/archive/) | VALIDATION-REPORT.md, SCRIPTS-VALIDATION-REPORT.md, OPUS-REVIEW.md |

---

## 🔍 Breaking Change Categories

| Category | Count | Examples |
|----------|-------|----------|
| **Delta Lake** | 12 | MERGE INTO syntax, column mapping, identity columns |
| **SQL** | 15 | CREATE TABLE, ANSI mode, implicit casting |
| **Auto Loader** | 8 | Schema hints, rescued data, inference |
| **Python** | 10 | UDF parameters, DataFrame API, deprecated methods |
| **Spark Core** | 12 | RDD operations, partition handling, configurations |
| **Scala** | 8 | Scala 2.13 compatibility, deprecated APIs |
| **Performance** | 6 | Broadcast joins, shuffle, caching |
| **Configuration** | 5 | Spark configs, cluster settings |

**Total: 76 breaking changes detected and documented**

---

## 💡 Example Usage

### Complete Workflow (Copy-Paste Ready)

**Step 1: Run Profiler**
```python
# In Databricks notebook
%run ./workspace-profiler/workspace-profiler.py
# Output: List of jobs/notebooks with potential breaking changes
```

**Step 2: Open Flagged Job & Run Agent**
```
Using the DBR migration skill at /Workspace/Users/{your-email}/.assistant/skills/databricks-dbr-migration/, 
scan and fix this notebook for DBR 17.3 compatibility
```

**Step 3: Review Agent's Work**
- Check auto-fixes applied
- Review manual review items flagged
- Note config suggestions to test

**Step 4: Test on DBR 17.3**
- Run job on new cluster
- Verify output correctness
- Log any issues to tracker

### Additional Prompts (After Skill Loaded)

```
@databricks-dbr-migration scan this entire folder for breaking changes
```

```
@databricks-dbr-migration fix only the input_file_name() issues
```

```
@databricks-dbr-migration validate no breaking patterns remain
```

---

## 🏆 Success Metrics

Based on validation testing:

- ✅ **100%** detection rate for known breaking changes
- ✅ **95%** automatic fix success rate
- ✅ **Zero** false negatives on critical issues
- ✅ **<5%** false positive rate
- ✅ **Validated** on 50+ real-world notebooks

See [VALIDATION-REPORT.md](developer-guide/archive/VALIDATION-REPORT.md) for detailed results.

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-pattern`)
3. Add your breaking change pattern to `databricks-dbr-migration/assets/fix-patterns.json`
4. Update documentation in `databricks-dbr-migration/references/`
5. Submit a pull request

---

## 📄 License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

---

## 🆘 Support & Issues

- **Documentation**: Start with [developer-guide/README.md](developer-guide/README.md)
- **Issues**: Open a GitHub issue for bug reports or feature requests
- **Questions**: Check the [Breaking Changes Explained](developer-guide/BREAKING-CHANGES-EXPLAINED.md) guide

---

## 🔗 Related Resources

- [Databricks Runtime Release Notes](https://docs.databricks.com/release-notes/runtime/index.html)
- [Apache Spark 4.0 Migration Guide](https://spark.apache.org/docs/latest/migration-guide.html)
- [Delta Lake Upgrade Guide](https://docs.delta.io/latest/versioning.html)
- [Scala 2.13 Release Notes](https://www.scala-lang.org/news/2.13.0/)

---

## 📊 Version History

| Version | Date | Changes |
|---------|------|---------|
| 4.0.0 | 2026-01-26 | Markdown templates extracted, simplified developer guide |
| 3.6.0 | 2026-01-26 | Added markdown summary cell generation after scan/fix |
| 3.2.0 | 2026-01-25 | Multi-file project support, skill renamed to databricks-dbr-migration |
| 2.0.0 | 2026-01-23 | Complete rewrite with automated fixes |
| 1.0.0 | 2025-12-15 | Initial release with scanning capability |

---

**Built with ❤️ by Databricks Solution Architects**
