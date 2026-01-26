# Markdown Templates for DBR Migration

This directory contains reusable markdown templates that agents use to generate summary cells in Databricks notebooks after scanning, fixing, or validating code for DBR migration.

## Templates

### 1. `scan-summary.md`
**When to use:** After scanning code for breaking changes  
**Variables to replace:**
- `{SCAN_DATE}` - Date/time of scan (YYYY-MM-DD HH:MM)
- `{TARGET_VERSION}` - Target DBR version (e.g., "17.3")
- `{AUTO_FIX_COUNT}` - Number of auto-fixable issues found
- `{MANUAL_REVIEW_COUNT}` - Number of issues requiring manual review
- `{CONFIG_CHECK_COUNT}` - Number of config items to test
- `{AUTO_FIX_ITEMS}` - Table rows for auto-fixable items
- `{MANUAL_REVIEW_ITEMS}` - Table rows for manual review items
- `{CONFIG_CHECK_ITEMS}` - Table rows for config check items

**Example row format:**
```
| 42 | BC-17.3-001 | `input_file_name()` | Replace with `_metadata.file_name` |
```

### 2. `fix-summary.md`
**When to use:** After applying automatic fixes  
**Variables to replace:**
- `{FIX_DATE}` - Date/time of fix (YYYY-MM-DD HH:MM)
- `{TARGET_VERSION}` - Target DBR version (e.g., "17.3")
- `{FIXED_COUNT}` - Number of issues fixed
- `{MANUAL_REVIEW_COUNT}` - Number of issues still requiring manual review
- `{CONFIG_CHECK_COUNT}` - Number of config items still to test
- `{APPLIED_FIXES}` - Table rows for applied fixes
- `{MANUAL_REVIEW_ITEMS}` - Table rows for remaining manual review items
- `{CONFIG_CHECK_ITEMS}` - Table rows for remaining config check items

**Example row format:**
```
| 42 | BC-17.3-001 | `input_file_name()` | Replaced with `_metadata.file_name` |
```

### 3. `validation-report.md`
**When to use:** After validating that fixes were applied correctly  
**Variables to replace:**
- `{VALIDATION_DATE}` - Date/time of validation (YYYY-MM-DD HH:MM)
- `{TARGET_VERSION}` - Target DBR version (e.g., "17.3")
- `{VALIDATED_COUNT}` - Number of fixes validated successfully
- `{ISSUES_COUNT}` - Number of issues found during validation
- `{VALIDATED_ITEMS}` - Table rows for validated items
- `{ISSUES_FOUND}` - Table rows for issues found
- `{OVERALL_STATUS}` - Overall readiness status (e.g., "✅ Ready for DBR 17.3" or "⚠️ Issues require attention")
- `{READINESS_DETAILS}` - Detailed readiness assessment
- `{NEXT_STEPS}` - Numbered list of next steps

## Agent Usage

When an agent needs to add a summary cell:

1. **Read the appropriate template** from this directory
2. **Replace all variables** with actual data from the scan/fix/validation
3. **Add as a new markdown cell** at the end of the notebook

Example:
```python
# Read template
with open('assets/markdown-templates/scan-summary.md', 'r') as f:
    template = f.read()

# Replace variables
summary = template.replace('{SCAN_DATE}', '2026-01-26 14:30')
summary = summary.replace('{TARGET_VERSION}', '17.3')
summary = summary.replace('{AUTO_FIX_COUNT}', '5')
# ... replace other variables ...

# Add as new cell at end of notebook
```

## Benefits

- **Separation of concerns**: Templates don't clutter SKILL.md logic
- **Easy maintenance**: Update templates without touching core skill
- **Reusability**: Scripts, tools, and agents can all use same templates
- **Consistency**: All summaries follow the same format
- **Extensibility**: Easy to add new templates for new use cases
