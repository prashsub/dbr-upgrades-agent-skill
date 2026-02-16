# Final Audit: DBR Migration Artifacts

**Scope:** Agent skill (`databricks-dbr-migration/`), demo/test notebooks (`demo/`), workspace profiler (`workspace-profiler/workspace-profiler.py`)  
**Date:** 2026-01-23  

---

## Findings (ordered by severity)

### High

- **Coverage claims do not match actual scanner/validator behavior**  
  `SKILL.md` claims the agent scans all 35 patterns, but `scan-breaking-changes.py` implements a subset, and `validate-migration.py` checks only a limited set of breaking patterns. This makes “scan all 35” and validation assertions inaccurate.  
  ```20:23:databricks-dbr-migration/SKILL.md
  1. **SCAN** - Find ALL breaking changes in code (35 patterns)
  2. **FIX** - Apply automatic remediations (10 patterns)
  3. **FLAG** - Explicitly flag items requiring manual review (12 patterns) or configuration testing (8 patterns)
  ```
  ```50:277:databricks-dbr-migration/scripts/scan-breaking-changes.py
  PATTERNS = [
      { "id": "BC-17.3-001", ... },
      { "id": "BC-SC-003", ... },
      { "id": "BC-SC-004", ... },
      { "id": "BC-15.4-003", ... },
      { "id": "BC-15.4-003b", ... },
      { "id": "BC-15.4-001", ... },
      { "id": "BC-16.4-001a", ... },
      { "id": "BC-16.4-001b", ... },
      { "id": "BC-16.4-001c", ... },
      { "id": "BC-16.4-001d", ... },
      { "id": "BC-16.4-001e", ... },
      { "id": "BC-16.4-001f", ... },
      { "id": "BC-16.4-001g", ... },
      { "id": "BC-16.4-001h", ... },
      { "id": "BC-16.4-001i", ... },
      { "id": "BC-16.4-002", ... },
      { "id": "BC-13.3-001", ... },
      { "id": "BC-17.3-002", ... },
      { "id": "BC-13.3-002", ... },
      { "id": "BC-15.4-002", ... },
      { "id": "BC-15.4-004", ... },
      { "id": "BC-16.4-004", ... },
  ]
  ```
  ```39:137:databricks-dbr-migration/scripts/validate-migration.py
  BREAKING_PATTERNS = [
      { "id": "BC-17.3-001", ... },
      { "id": "BC-15.4-001", ... },
      { "id": "BC-15.4-003", ... },
      { "id": "BC-15.4-003b", ... },
      { "id": "BC-16.4-001a", ... },
      { "id": "BC-16.4-001b", ... },
      { "id": "BC-16.4-001c", ... },
      { "id": "BC-16.4-001d", ... },
      { "id": "BC-16.4-001e", ... },
      { "id": "BC-16.4-001f", ... },
      { "id": "BC-16.4-001g", ... },
      { "id": "BC-16.4-001i", ... },
  ]
  ```

- **BC‑17.3‑004 is mis-identified across artifacts**  
  The reference file defines BC‑17.3‑004 as **Delta null struct handling**, but the profiler and demo notebook use BC‑17.3‑004 to describe **Spark Connect decimal precision**. This is an ID/definition mismatch that will confuse users and mislabel findings.  
  ```637:678:databricks-dbr-migration/references/BREAKING-CHANGES.md
  ### BC-17.3-004: Null Struct Handling in Delta
  ...
  ### BC-17.3-005: Decimal Precision in Spark Connect
  ```
  ```465:473:workspace-profiler/workspace-profiler.py
  id="BC-17.3-004",
  name="Spark Connect Decimal Precision",
  description="[Review] Spark Connect: decimal precision in array/map literals defaults to (38,18)",
  ```
  ```1396:1411:demo/dbr_migration_demo_notebook.py
  | BC-17.3-004 | `DecimalType` | Specify precision/scale |
  ```

---

### Medium

- **`apply-fixes.py` does not honor specific Scala fix IDs in `--fix`**  
  The CLI lists granular fix IDs (BC‑16.4‑001a/b/c/…), but the execution path only checks for `BC-16.4-001`, so running `--fix BC-16.4-001a` will not apply anything.  
  ```675:712:databricks-dbr-migration/scripts/apply-fixes.py
  if file_type == '.scala':
      if fix_ids is None or 'BC-16.4-001' in fix_ids:
          content, applied, warnings = fix_scala_213_collections(content)
  ```
  ```892:948:databricks-dbr-migration/scripts/apply-fixes.py
  FIX_DEFINITIONS = {
      "BC-16.4-001": { ... },
      "BC-16.4-001a": { ... },
      "BC-16.4-001b": { ... },
      "BC-16.4-001c": { ... },
      ...
  }
  ```

- **Profiler flags `overwriteSchema` OR `partitionOverwriteMode` (not both)**  
  The pattern intended to catch the **combination** of overwriteSchema + dynamic partitioning instead flags either term alone, which can over-report.  
  ```288:295:workspace-profiler/workspace-profiler.py
  pattern=r"overwriteSchema.*true|partitionOverwriteMode.*dynamic",
  description="[Review] Cannot combine overwriteSchema=true with dynamic partition overwrites",
  ```

- **Demo notebook overstates “all 17 patterns” without examples for some IDs**  
  The notebook claims the assistant should find all 17 patterns, but BC‑15.4‑006 and BC‑17.3‑004 appear only in summary tables without dedicated example sections.  
  ```1440:1441:demo/dbr_migration_demo_notebook.py
  **Expected Result:** Assistant identifies all 17 breaking change patterns
  ```
  ```1396:1411:demo/dbr_migration_demo_notebook.py
  | BC-15.4-006 | `CREATE VIEW` | Review schema binding changes |
  | BC-17.3-004 | `DecimalType` | Specify precision/scale |
  ```

- **Test notebook summary table contains count and coverage mismatches**  
  The “Config” row says 5 examples but lists only 4 IDs. The “All Breaking Changes in This Notebook” table includes BC‑16.4‑003 and BC‑17.3‑003, but there are no example sections labeled for them in the notebook content.  
  ```9:13:demo/dbr_migration_test_notebook.py
  | ⚙️ Config | 5 | BC-13.3-002, BC-15.4-002, BC-16.4-004, BC-17.3-002 |
  ```
  ```494:519:demo/dbr_migration_test_notebook.py
  | BC-16.4-003 | ⚙️ Config | Cache options | **FLAG** - Test cached reads |
  | BC-17.3-003 | 🟡 Manual | array/map/struct | **FLAG** - Handle nulls explicitly |
  ```

- **Guidance for `_metadata.file_name` replacement is unsafe in `SKILL.md`**  
  The recommended Python fix uses `withColumnRenamed("file_name", ...)` after selecting `_metadata.file_name`. If the dataset already has a `file_name` column, this renames the wrong column.  
  ```412:424:databricks-dbr-migration/SKILL.md
  df.select("*", "_metadata.file_name").withColumnRenamed("file_name", "source")
  ```

---

### Low

- **Profiler’s BC‑SC‑001 regex is narrow and can miss common patterns**  
  The regex requires a `try:` immediately followed by a single-line transform, which can miss multi-line transforms or other error-handling idioms.  
  ```204:211:workspace-profiler/workspace-profiler.py
  pattern=r"try\s*:\s*\n[^#]*\.(filter|select|where|withColumn|join)\s*\(",
  ```

---

## Recommendations

- Align the **35-pattern claim** with actual coverage (scanner + validator), or expand both to match `fix-patterns.json`.  
- Fix **BC‑17.3 ID mapping** across references, profiler, and notebooks (BC‑17.3‑004 vs BC‑17.3‑005).  
- In `apply-fixes.py`, honor **granular `--fix` IDs** (BC‑16.4‑001a/b/c/…) or remove them from the list.  
- Tighten profiler logic for **overwriteSchema + dynamic partitions** to require both conditions.  
- Update notebook claims/tables to reflect **actual examples present**.  
- Replace the `_metadata.file_name` guidance in `SKILL.md` with a safe alias pattern.

---

## Overall Assessment

- **Accuracy:** Several internal inconsistencies (especially BC‑17.3 ID mapping and scan/validate coverage).  
- **Coverage:** Profiler is the most complete; agent scripts lag the declared scope.  
- **Status:** ⚠️ Final alignment needed before treating outputs as authoritative.
