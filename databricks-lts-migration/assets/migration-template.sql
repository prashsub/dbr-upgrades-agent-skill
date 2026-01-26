-- ============================================================================
-- Databricks LTS Migration Configuration Template
-- ============================================================================
-- Apply these settings to preserve legacy behavior during migration from
-- DBR 13.3 LTS to DBR 17.3 LTS.
--
-- Usage: Run at session start or add to cluster Spark configuration.
-- Review each setting and remove once migration is complete.
-- ============================================================================

-- ============================================================================
-- DBR 13.3 LTS COMPATIBILITY SETTINGS
-- ============================================================================

-- Parquet Timestamp Inference (BC-13.3-002)
-- Description: int64 timestamps now infer as TIMESTAMP_NTZ instead of TIMESTAMP
-- Impact: Reading external Parquet files may fail
-- Action: Set to false to preserve old behavior
SET spark.sql.parquet.inferTimestampNTZ.enabled = false;


-- ============================================================================
-- DBR 15.4 LTS COMPATIBILITY SETTINGS
-- ============================================================================

-- JDBC Null Calendar (BC-15.4-002)
-- Description: Default changed from false to true
-- Impact: JDBC queries returning TIMESTAMP values
-- Action: Set to false to preserve old behavior
SET spark.sql.legacy.jdbc.useNullCalendar = false;


-- ============================================================================
-- DBR 16.4 LTS COMPATIBILITY SETTINGS
-- ============================================================================

-- Data Source Cache Options (BC-16.4-003)
-- Description: Cached plans now respect options for all data source plans
-- Impact: Queries with different options on same table
-- Action: Set to true to restore old caching behavior
SET spark.sql.legacy.readFileSourceTableCacheIgnoreOptions = true;


-- ============================================================================
-- DBR 17.3 LTS COMPATIBILITY NOTES
-- ============================================================================

-- Auto Loader Incremental Listing (BC-17.3-002)
-- Description: Default changed from 'auto' to 'false'
-- Note: This must be set in readStream options, not as a session config
-- 
-- Python example:
-- spark.readStream.format("cloudFiles") \
--     .option("cloudFiles.useIncrementalListing", "auto") \
--     .load("/path")
--
-- Action: Set in streaming job code if needed

-- input_file_name() Removal (BC-17.3-001)
-- Description: Function removed, use _metadata.file_name instead
-- Note: No config flag - code must be updated
-- 
-- Before: SELECT input_file_name(), * FROM my_table
-- After:  SELECT _metadata.file_name, * FROM my_table


-- ============================================================================
-- MIGRATION VALIDATION QUERIES
-- ============================================================================

-- Validate current runtime version
SELECT current_version() as databricks_runtime_version;

-- Check applied settings
SHOW ALL;

-- Test _metadata.file_name (should work on 13.3+)
-- SELECT _metadata.file_name, COUNT(*) FROM my_table GROUP BY 1 LIMIT 5;

-- Test timestamp handling
-- SELECT CAST('2024-01-01 12:00:00' AS TIMESTAMP) as ts_test;


-- ============================================================================
-- POST-MIGRATION CLEANUP
-- ============================================================================
-- Once migration is complete and verified, remove legacy settings:
--
-- RESET spark.sql.parquet.inferTimestampNTZ.enabled;
-- RESET spark.sql.legacy.jdbc.useNullCalendar;
-- RESET spark.sql.legacy.readFileSourceTableCacheIgnoreOptions;
--
-- Update Auto Loader jobs to use file events for best performance:
-- .option("cloudFiles.useNotifications", "true")
-- ============================================================================
