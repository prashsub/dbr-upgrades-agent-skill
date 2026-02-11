# Validation Workflow

Steps for validating that all fixes have been correctly applied. Includes syntax validation, replacement verification, assisted fix status checks, and report generation.

---

## Capability 3: VALIDATE - Verify Fixes

After applying fixes, validate they are correct:

### Step 1: Syntax Validation

**Verify no breaking patterns remain:**
```bash
# Should return no results after fixes
grep -rn "input_file_name\s*(" --include="*.py" --include="*.ipynb" --include="*.sql" --include="*.scala" /path/to/scan
grep -rn "(IF|IS)\s*!" --include="*.sql" --include="*.ipynb" /path/to/scan
grep -rn "VariantType" --include="*.py" --include="*.ipynb" /path/to/scan
grep -rn "scala.collection.JavaConverters" --include="*.scala" --include="*.ipynb" /path/to/scan
```

### Step 2: Replacement Validation

**Verify correct replacements exist:**
```bash
# Should find _metadata.file_name replacements
grep -rn "_metadata.file_name" --include="*.py" --include="*.ipynb" --include="*.sql" --include="*.scala" /path/to/scan

# Should find NOT instead of !
grep -rn "IF NOT EXISTS\|IS NOT NULL\|NOT IN\|NOT BETWEEN\|NOT LIKE" --include="*.sql" --include="*.ipynb" /path/to/scan

# Should find new Scala imports
grep -rn "scala.jdk.CollectionConverters" --include="*.scala" --include="*.ipynb" /path/to/scan
```

### Step 3: Code Structure Validation

**For Python files (`.py` and `.ipynb`), verify:**
- Import statements are valid
- Function signatures are correct
- Column references use proper syntax
- For `.ipynb` files: verify the fix was applied to the correct cell's `"source"` array in the JSON structure

**For SQL files, verify:**
- SQL syntax is valid
- Keywords are properly spaced
- Identifiers are correctly quoted if needed

**For Scala files, verify:**
- Imports compile (no deprecated imports)
- Collection operations use new syntax
- Type annotations are present where needed

### Step 4: Verify Assisted Fix Items Are Addressed

Re-run the detection patterns for each Assisted Fix item and report status:

| BC-ID | What to Check | Status Values |
|-------|---------------|---------------|
| BC-SC-002 | No duplicate temp view names remain | Applied / Not Applied / N/A |
| BC-SC-003 | UDFs with external variables are wrapped in factory functions | Applied / Not Applied / N/A |
| BC-SC-004 | Schema access cached outside loops | Applied / Not Applied / N/A |
| BC-17.3-005 | No bare `cast("decimal")` without explicit precision | Applied / Not Applied / N/A |
| BC-13.3-002 | Parquet timestamp config line present if Parquet reads exist | Applied / Not Applied / N/A |
| BC-15.4-002 | JDBC self-test run (diff_count == 0 → safe, > 0 → config applied) | Applied / Not Applied / N/A |
| BC-15.4-005 | JDBC self-test run (diff_count == 0 → safe, > 0 → config applied) | Applied / Not Applied / N/A |
| BC-16.4-003 | Cache config line present if cached reads exist | Applied / Not Applied / N/A |
| BC-16.4-004 | `materializeSource=none` replaced with `auto` | Applied / Not Applied / N/A |
| BC-16.4-006 | cleanSource config reviewed | Applied / Not Applied / N/A |
| BC-17.3-002 | Auto Loader has explicit incremental listing option | Applied / Not Applied / N/A |

### Step 5: Verify Manual Review Items Are Acknowledged

Check that each manual review item has been either:
- **Addressed** by the developer
- **Acknowledged** as acceptable with documented reason

### Step 6: Generate Validation Report

```
## Validation Report

### Auto-Fix Validation
✅ No input_file_name() found
✅ No '!' syntax for NOT found  
✅ No deprecated Scala imports found

### Replacement Verification
✅ Found 3 instances of _metadata.file_name
✅ Found 5 instances of NOT syntax
✅ Found 2 instances of CollectionConverters

### Assisted Fix Validation
✅ BC-SC-002: Applied - Unique view names confirmed
✅ BC-SC-003: Applied - Factory pattern wrapping external variables
✅ BC-17.3-002: Applied - Explicit incremental listing option added
⚠️ BC-15.4-005: Not Applied - JDBC self-test not yet run (run self-comparison to decide)

### Manual Review Status
✅ BC-17.3-003: [REVIEWED] - Developer confirmed null preservation is acceptable

### Summary
Status: ✅ PASSED - Ready for DBR 17.3 upgrade
Files validated: 15
All breaking changes addressed
```
