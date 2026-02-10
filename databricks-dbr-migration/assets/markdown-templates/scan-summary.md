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
# MAGIC | 🔧 Assisted Fix | {ASSISTED_FIX_COUNT} |
# MAGIC | 🟡 Manual Review | {MANUAL_REVIEW_COUNT} |
# MAGIC 
# MAGIC ### 🔴 Auto-Fix Required
# MAGIC | Line | BC-ID | Pattern | Fix |
# MAGIC |------|-------|---------|-----|
# MAGIC {AUTO_FIX_ITEMS}
# MAGIC 
# MAGIC ### 🔧 Assisted Fix (Review Suggested Code)
# MAGIC | Line | BC-ID | Issue | Suggested Fix |
# MAGIC |------|-------|-------|---------------|
# MAGIC {ASSISTED_FIX_ITEMS}
# MAGIC 
# MAGIC {ASSISTED_FIX_SNIPPETS}
# MAGIC 
# MAGIC ### 🟡 Manual Review Required
# MAGIC | Line | BC-ID | Issue | Action |
# MAGIC |------|-------|-------|--------|
# MAGIC {MANUAL_REVIEW_ITEMS}
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Run: `@databricks-dbr-migration fix all auto-fixable issues`
# MAGIC 2. Review assisted fix snippets above and apply as appropriate
# MAGIC 3. Review manual items
# MAGIC 4. Test on DBR {TARGET_VERSION}
