# MAGIC %md
# MAGIC ## 📋 DBR Migration Scan Results
# MAGIC 
# MAGIC **Scan Date:** {SCAN_DATE}  
# MAGIC **Target DBR Version:** {TARGET_VERSION}
# MAGIC 
# MAGIC ### Summary
# MAGIC | Category | Count |
# MAGIC |----------|-------|
# MAGIC | 🔴 Auto-Fix | {AUTO_FIX_COUNT} |
# MAGIC | 🟡 Manual Review | {MANUAL_REVIEW_COUNT} |
# MAGIC | ⚙️ Config Check | {CONFIG_CHECK_COUNT} |
# MAGIC 
# MAGIC ### 🔴 Auto-Fix Required
# MAGIC | Line | BC-ID | Pattern | Fix |
# MAGIC |------|-------|---------|-----|
# MAGIC {AUTO_FIX_ITEMS}
# MAGIC 
# MAGIC ### 🟡 Manual Review Required
# MAGIC | Line | BC-ID | Issue | Action |
# MAGIC |------|-------|-------|--------|
# MAGIC {MANUAL_REVIEW_ITEMS}
# MAGIC 
# MAGIC ### ⚙️ Config Check (Test First)
# MAGIC | Line | BC-ID | Issue | Config If Needed |
# MAGIC |------|-------|-------|------------------|
# MAGIC {CONFIG_CHECK_ITEMS}
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Run: `@databricks-dbr-migration fix all auto-fixable issues`
# MAGIC 2. Review manual items above
# MAGIC 3. Test config changes on DBR {TARGET_VERSION}
