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
├── databricks-dbr-migration/     # Agent skill definition
│   ├── SKILL.md                  # Main skill configuration
│   ├── assets/
│   │   ├── fix-patterns.json     # Automated fix patterns
│   │   ├── version-matrix.json   # DBR version compatibility
│   │   └── migration-template.sql
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
│   └── README.md
│
├── developer-guide/              # Developer documentation
│   ├── README.md                 # Quick start guide
│   ├── 01-skill-setup.md
│   ├── 02-using-assistant.md
│   ├── 03-quality-validation.md
│   ├── 04-performance-testing.md
│   ├── 05-rollout-checklist.md
│   ├── 06-output-validation-criteria.md
│   ├── 07-testing-validation-signoff-guide.md
│   ├── 08-tracker-templates.md
│   ├── BREAKING-CHANGES-EXPLAINED.md  # Detailed explanations
│   └── workspace-profiler.py          # Account-level scanner
│
├── VALIDATION-REPORT.md          # Quality validation results
├── SCRIPTS-VALIDATION-REPORT.md  # Script testing results
├── OPUS-REVIEW.md                # Expert review summary
└── README.md                     # This file
```

---

## 🚀 Quick Start

### Using Databricks Assistant (Recommended)

In any Databricks notebook:

```
@databricks-dbr-migration scan this notebook for breaking changes when upgrading from DBR 13.3 to DBR 17.3
```

```
@databricks-dbr-migration fix all breaking changes in this notebook
```

```
@databricks-dbr-migration validate that all breaking changes have been fixed
```

> 💡 **Pro Tip:** See [Effective Prompts Guide](developer-guide/09-effective-prompts-guide.md) for 50+ ready-to-use prompts!

### Using Python Scripts

**1️⃣ Scan for Breaking Changes**

Use the Python script to scan your code:

```bash
python databricks-dbr-migration/scripts/scan-breaking-changes.py \
  --path /path/to/your/code \
  --output scan-results.json
```

Or use the workspace profiler to scan all jobs and notebooks:

```python
# Run in Databricks notebook
%run ./developer-guide/workspace-profiler.py
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

### For POD Teams (Start Here)

| Document | Description |
|----------|-------------|
| [Testing & Validation Guide](developer-guide/07-testing-validation-signoff-guide.md) | ⭐ **Main Guide** - Step-by-step testing & sign-off |
| [Breaking Changes Explained](developer-guide/BREAKING-CHANGES-EXPLAINED.md) | 📖 Every breaking change with examples |
| [Output Validation Criteria](developer-guide/06-output-validation-criteria.md) | Which jobs need output comparison |
| [Tracker Templates](developer-guide/08-tracker-templates.md) | Issue, Performance, and Sign-Off trackers |

### Technical Deep-Dives

| Document | Description |
|----------|-------------|
| [Skill Setup](developer-guide/01-skill-setup.md) | Install the DBR migration skill |
| [Using Assistant](developer-guide/02-using-assistant.md) | Use Databricks Assistant to scan & fix |
| **[Effective Prompts Guide](developer-guide/09-effective-prompts-guide.md)** | 🎯 **Ready-to-use prompts** for running the agent |
| [Quality Validation](developer-guide/03-quality-validation.md) | Validate code quality after migration |
| [Performance Testing](developer-guide/04-performance-testing.md) | Test for performance regressions |
| [Rollout Checklist](developer-guide/05-rollout-checklist.md) | Complete production rollout checklist |

### Reference Guides

| Document | Description |
|----------|-------------|
| [Breaking Changes Reference](databricks-dbr-migration/references/BREAKING-CHANGES.md) | Complete list of all breaking changes |
| [Migration Checklist](databricks-dbr-migration/references/MIGRATION-CHECKLIST.md) | Step-by-step migration checklist |
| [Quick Reference](databricks-dbr-migration/references/QUICK-REFERENCE.md) | Quick lookup for common issues |
| [Scala 2.13 Guide](databricks-dbr-migration/references/SCALA-213-GUIDE.md) | Scala 2.12 → 2.13 migration |
| [Spark Connect Guide](databricks-dbr-migration/references/SPARK-CONNECT-GUIDE.md) | Spark Connect compatibility |

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

### Scan a Single Notebook

```python
# In Databricks Assistant
@databricks-dbr-migration scan this notebook for breaking changes from DBR 13.3 to 17.3
```

### Fix Specific Issues

```python
# In Databricks Assistant
@databricks-dbr-migration fix the MERGE INTO syntax issues in this notebook
```

### Generate Migration Report

```python
# In Databricks Assistant
@databricks-dbr-migration generate a complete migration report for my workspace
```

---

## 🏆 Success Metrics

Based on validation testing:

- ✅ **100%** detection rate for known breaking changes
- ✅ **95%** automatic fix success rate
- ✅ **Zero** false negatives on critical issues
- ✅ **<5%** false positive rate
- ✅ **Validated** on 50+ real-world notebooks

See [VALIDATION-REPORT.md](VALIDATION-REPORT.md) for detailed results.

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
| 2.0.0 | 2026-01-23 | Complete rewrite with automated fixes |
| 1.0.0 | 2025-12-15 | Initial release with scanning capability |

---

**Built with ❤️ by Databricks Solution Architects**
