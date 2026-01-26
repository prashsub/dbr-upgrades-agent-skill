# Migration Checklist

Use this checklist when upgrading Databricks Runtime between LTS versions.

---

## Pre-Migration Phase

### Code Inventory

- [ ] **Scan for `input_file_name()`** - Search all notebooks, SQL files, and Python/Scala code
  ```bash
  grep -r "input_file_name" --include="*.py" --include="*.sql" --include="*.scala" .
  ```

- [ ] **Scan for `!` SQL syntax** - Find invalid NOT syntax
  ```bash
  grep -rE "(IF|IS)\s*!" --include="*.sql" .
  ```

- [ ] **Scan for VARIANT in Python UDFs** - Find Python functions using VARIANT type
  ```bash
  grep -r "VariantType" --include="*.py" .
  ```

- [ ] **Inventory Scala collections** - Review all Scala code for collection usage
  ```bash
  grep -r "scala.collection" --include="*.scala" .
  grep -r "JavaConverters" --include="*.scala" .
  ```

- [ ] **Check Auto Loader configurations** - List all Auto Loader jobs
  ```bash
  grep -r "cloudFiles" --include="*.py" --include="*.scala" .
  ```

- [ ] **Review views** - List views that may use deprecated functions
  ```sql
  SHOW VIEWS IN my_schema;
  ```

### Dependency Inventory

- [ ] **List cluster libraries** - Document all JAR dependencies
- [ ] **Check Scala version compatibility** - Ensure all JARs are compatible with target Scala version
- [ ] **Review init scripts** - Check for deprecated configurations

### Environment Preparation

- [ ] **Create test workspace** - Set up isolated environment for testing
- [ ] **Clone production jobs** - Create test versions of critical workflows
- [ ] **Snapshot critical tables** - Create point-in-time copies for validation

---

## Migration Phase (Per LTS Version)

### 13.3 → 14.3 Migration

- [ ] Test Spark 3.5.0 compatibility
- [ ] Verify Thriftserver connections (if used)
- [ ] Check for Hive aux JAR usage
- [ ] Validate MERGE/UPDATE operations with type casting

### 14.3 → 15.4 Migration

- [ ] Replace VARIANT usage in Python UDFs
- [ ] Update `!` syntax to `NOT` in SQL
- [ ] Review view column definitions
- [ ] Test JDBC timestamp handling
- [ ] Set `spark.sql.legacy.jdbc.useNullCalendar` if needed

### 15.4 → 16.4 Migration (CRITICAL)

- [ ] **Scala 2.13 Migration**
  - [ ] Update collection imports
  - [ ] Fix type inference issues
  - [ ] Update deprecated syntax (single quotes, postfix operators)
  - [ ] Test hash-dependent code
  
- [ ] Rebuild all Scala JARs for 2.13
- [ ] Test all Scala UDFs
- [ ] Verify third-party library compatibility
- [ ] Review MERGE materialization settings
- [ ] Test data source caching behavior

### 16.4 → 17.3 Migration

- [ ] Replace all `input_file_name()` with `_metadata.file_name`
- [ ] Recreate views using `input_file_name()`
- [ ] Configure Auto Loader incremental listing
- [ ] Test Spark 4.0 API compatibility
- [ ] Validate Spark Connect applications (if used)
- [ ] Test struct null handling

---

## Testing Phase

### Unit Tests

- [ ] Run all existing unit tests
- [ ] Add tests for breaking change remediations
- [ ] Validate type casting in MERGE/UPDATE
- [ ] Test null handling in structs

### Integration Tests

- [ ] End-to-end ETL pipeline tests
- [ ] Auto Loader streaming tests
- [ ] JDBC/ODBC connectivity tests
- [ ] BI tool integration tests

### Performance Tests

- [ ] Compare query performance
- [ ] Measure Auto Loader latency
- [ ] Check memory usage patterns
- [ ] Validate caching behavior

### Data Validation

- [ ] Compare row counts
- [ ] Validate data types
- [ ] Check null handling
- [ ] Verify timestamp values

---

## Rollout Phase

### Staged Rollout

- [ ] **Stage 1: Dev/Test** - Upgrade non-production workspaces
- [ ] **Stage 2: Low-risk Production** - Upgrade less critical jobs first
- [ ] **Stage 3: Critical Production** - Upgrade mission-critical workflows
- [ ] **Stage 4: Full Rollout** - Complete workspace migration

### Monitoring

- [ ] Set up alerting for job failures
- [ ] Monitor query performance metrics
- [ ] Track Auto Loader lag
- [ ] Watch for error rate changes

### Rollback Plan

- [ ] Document rollback procedure
- [ ] Test rollback in non-production
- [ ] Keep previous runtime available
- [ ] Maintain table snapshots

---

## Post-Migration Phase

### Cleanup

- [ ] Remove legacy configuration flags
- [ ] Delete deprecated code paths
- [ ] Update documentation
- [ ] Archive migration artifacts

### Validation

- [ ] Confirm all jobs running successfully
- [ ] Validate data quality
- [ ] Check performance baselines
- [ ] Review error logs

### Documentation

- [ ] Update runbooks
- [ ] Document configuration changes
- [ ] Record known issues and workarounds
- [ ] Update team training materials

---

## Version-Specific Validation Queries

### Validate input_file_name Replacement
```sql
-- Should work on 17.3+
SELECT _metadata.file_name, COUNT(*) 
FROM my_table 
GROUP BY _metadata.file_name;
```

### Validate MERGE Type Casting
```sql
-- Test MERGE with potential overflow
MERGE INTO target USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.value = source.large_value;
-- Should error if overflow, not silently NULL
```

### Validate Auto Loader
```python
# Test Auto Loader with explicit incremental setting
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.useIncrementalListing", "auto") \
    .load("/test/path")
```

### Validate Null Struct Handling
```sql
-- Check null vs empty struct distinction
SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN struct_col IS NULL THEN 1 END) as null_structs,
    COUNT(CASE WHEN struct_col IS NOT NULL AND struct_col.field IS NULL THEN 1 END) as null_fields
FROM my_table;
```
