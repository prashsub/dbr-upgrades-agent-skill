# Databricks Runtime Breaking Changes: 13.3 LTS → 17.3 LTS

## Executive Summary

This document catalogs all **breaking changes** and **significant behavioral changes** when upgrading from Databricks Runtime 13.3 LTS (Apache Spark 3.4.1) to 17.3 LTS (Apache Spark 4.0.0), based on official Microsoft Learn documentation.

---

## Version Progression

| Runtime Version | Apache Spark Version | Release Date | End of Support |
|-----------------|---------------------|--------------|----------------|
| 13.3 LTS | 3.4.1 | August 2023 | August 2026 |
| 14.3 LTS | 3.5.0 | February 2024 | February 2027 |
| 15.4 LTS | 3.5.0 | August 2024 | August 2027 |
| 16.4 LTS | 3.5.2 | May 2025 | May 2028 |
| 17.3 LTS | 4.0.0 | October 2025 | October 2028 |

---

## Breaking Changes by Runtime Version

### 🔴 Databricks Runtime 13.3 LTS Breaking Changes

#### 1. Changes to Implicit Casting with MERGE INTO and UPDATE
- **Severity**: HIGH
- **Description**: Azure Databricks now follows `spark.sql.storeAssignmentPolicy` for implicit casting when storing rows. The default `ANSI` throws an error when storing values that overflow. Previously, values would be stored as `NULL`.
- **Impact**: `MERGE INTO` and `UPDATE` statements with type mismatches
- **Remediation**:
  ```sql
  -- Rewrite table to use broader type
  ALTER TABLE MyTable SET TBLPROPERTIES (
      'delta.minReaderVersion' = '2',
      'delta.minWriterVersion' = '5',
      'delta.columnMapping.mode' = 'name'
  )
  ALTER TABLE MyTable RENAME ID to ID_old
  ALTER TABLE MyTable ADD COLUMN ID BIGINT
  UPDATE MyTable SET ID = ID_old
  ALTER TABLE MyTable DROP COLUMN ID_old
  ```

#### 2. Parquet Schema Inference Changes
- **Severity**: MEDIUM
- **Description**: `int64` timestamp columns annotated with `isAdjustedToUTC=false` now default to `TIMESTAMP_NTZ` type instead of `TIMESTAMP`.
- **Impact**: Reading external Parquet files into Delta tables may fail if `timestampNtz` feature is not enabled
- **Error**: `Your table schema requires manual enablement of the following table feature(s): timestampNtz`
- **Remediation**: Set `spark.sql.parquet.inferTimestampNTZ.enabled` to `false`

#### 3. Block Schema Overwrite with Dynamic Partition Overwrites
- **Severity**: MEDIUM  
- **Description**: Cannot set `overwriteSchema` to `true` in combination with dynamic partition overwrites in Delta Lake.
- **Impact**: Prevents table corruption due to schema mismatch
- **Remediation**: Separate schema evolution from dynamic partition overwrites

#### 4. File Modification Detection
- **Severity**: LOW
- **Description**: Queries return an error if a file is updated between query planning and invocation.
- **Impact**: Long-running queries over mutable file sources
- **Remediation**: Ensure file immutability during query execution

---

### 🔴 Databricks Runtime 14.3 LTS Breaking Changes

#### 1. Thriftserver Obsolete Features Removed
- **Severity**: LOW
- **Description**: The following Thriftserver configurations are no longer supported:
  - Hive auxiliary JARs (`hive.aux.jars.path`)
  - Hive global init file (`.hiverc`) via `hive.server2.global.init.file.location`
- **Impact**: `hive-thriftserver` connections using these features
- **Remediation**: Migrate to alternative configuration methods

---

### 🔴 Databricks Runtime 15.4 LTS Breaking Changes

#### 1. VARIANT Type with Python UDF/UDAF/UDTF
- **Severity**: HIGH (Breaking)
- **Description**: Calling any Python UDF, UDAF, or UDTF that uses `VARIANT` type as an argument or return value now throws an exception.
- **Impact**: All Python functions using VARIANT type
- **Remediation**: Avoid using `VARIANT` type with Python UDFs/UDAFs/UDTFs; use alternative data types

#### 2. JDBC useNullCalendar Default Changed
- **Severity**: MEDIUM
- **Description**: `spark.sql.legacy.jdbc.useNullCalendar` is now set to `true` by default.
- **Impact**: Queries returning `TIMESTAMP` values via JDBC
- **Remediation**: Set to `false` if queries break

#### 3. View Schema Binding Mode Changed
- **Severity**: MEDIUM
- **Description**: Default schema binding mode changed from `BINDING` to schema compensation with regular casting rules.
- **Impact**: Views referencing tables with schema changes
- **Remediation**: Review view definitions and explicit casting

#### 4. Disallow `!` Syntax Instead of `NOT`
- **Severity**: MEDIUM
- **Description**: Using `!` as synonym for `NOT` outside boolean expressions is no longer allowed.
- **Impact**: SQL statements using `!` syntax incorrectly
- **Before**: `CREATE ... IF ! EXISTS`, `IS ! NULL`, `! IN`
- **After**: `CREATE ... IF NOT EXISTS`, `IS NOT NULL`, `NOT IN`
- **Remediation**: Replace all `!` with `NOT` in appropriate contexts

#### 5. Disallow Undocumented Column Definition Syntax in Views
- **Severity**: LOW
- **Description**: Column types, `NOT NULL` constraints, or `DEFAULT` specifications in `CREATE VIEW` are no longer allowed.
- **Impact**: Views using undocumented column definition syntax
- **Remediation**: Remove unsupported column definitions from views

#### 6. Consistent Base64 Decoding Error Handling
- **Severity**: LOW
- **Description**: Photon now consistently raises the same exceptions as Spark during Base64 decoding errors.
- **Impact**: Error handling code for Base64 operations
- **Remediation**: Update error handling logic

#### 7. CHECK Constraint Error Class Changed
- **Severity**: LOW
- **Description**: `ALTER TABLE ADD CONSTRAINT` with invalid column now returns `UNRESOLVED_COLUMN.WITH_SUGGESTION` instead of `INTERNAL_ERROR`.
- **Impact**: Error handling for constraint operations
- **Remediation**: Update exception handling code

---

### 🔴 Databricks Runtime 16.4 LTS Breaking Changes

#### Major Breaking Changes (Scala 2.13 Migration)

##### 1. Collection Incompatibility
- **Severity**: CRITICAL
- **Description**: Scala 2.13 introduces significant collection library changes affecting API parameters and return types.
- **Impact**: All Scala code using collections
- **Remediation**: Follow [Scala 2.13 migration guide](https://docs.scala-lang.org/overviews/core/collections-migration-213.html)

##### 2. Hash Algorithm Changes
- **Severity**: HIGH
- **Description**: Data structures without guaranteed ordering (`HashMap`, `Set`) may order elements differently in Scala 2.13.
- **Impact**: Code relying on implicit iteration order
- **Remediation**: Do not rely on implicit ordering; use explicit sorting

#### Minor Breaking Changes (Scala 2.13)

##### 3. Stricter Type Inference
- **Severity**: MEDIUM
- **Description**: Compiler may report type inference failures requiring explicit annotations.
- **Impact**: Complex generic code
- **Remediation**: Add explicit type annotations

##### 4. Deprecated Syntax Removed
- **Severity**: MEDIUM
- **Description**: 
  - Single-quote string literals deprecated
  - `+` operator for non-String left operand deprecated
  - Postfix operators deprecated (use dot notation)
- **Remediation**: Update syntax to use double quotes, explicit `.toString`, and dot notation

#### Behavioral Changes

##### 5. Data Source Cached Plans Fix
- **Severity**: MEDIUM
- **Description**: Table reads now respect options for all data source plans when cached, not just the first.
- **Impact**: Queries with different options on same table
- **Remediation**: Set `spark.sql.legacy.readFileSourceTableCacheIgnoreOptions` to `true` to restore old behavior

##### 6. Redaction Rule Moved
- **Severity**: LOW
- **Description**: Redaction rule moved from analyzer to optimizer, affecting DataFrames saved to tables.
- **Impact**: DataFrames using SECRET SQL functions
- **Remediation**: Review redacted value handling

##### 7. variant_get and get_json_object Path Handling
- **Severity**: LOW
- **Description**: Now considers leading spaces in paths when Photon is disabled.
- **Impact**: JSON path extraction with leading whitespace
- **Remediation**: Update paths if relying on space trimming

##### 8. MERGE Source Materialization
- **Severity**: LOW
- **Description**: Setting `merge.materializeSource` to `none` now causes an error.
- **Impact**: MERGE operations with materialization disabled
- **Remediation**: Remove `merge.materializeSource = none` configuration

##### 9. Partition Metadata Log Enablement
- **Severity**: LOW
- **Description**: `spark.databricks.nonDelta.partitionLog.enabled` now anchored to table, not cluster.
- **Impact**: Non-Delta tables using partition metadata log
- **Remediation**: Set configuration at table level

##### 10. Json4s Downgrade
- **Severity**: LOW
- **Description**: Json4s downgraded from 4.0.7 to 3.7.0-M11 for Scala 2.13 compatibility.
- **Impact**: Applications using Json4s APIs
- **Remediation**: Review Json4s API usage for compatibility

---

### 🔴 Databricks Runtime 17.3 LTS Breaking Changes

#### 1. input_file_name Function Removed
- **Severity**: HIGH
- **Description**: The `input_file_name` function (deprecated since 13.3 LTS) is no longer supported due to unreliability.
- **Impact**: All queries using `input_file_name()`
- **Remediation**: Use `_metadata.file_name` instead

#### 2. Auto Loader Incremental Listing Default Changed
- **Severity**: HIGH
- **Description**: Default for `cloudFiles.useIncrementalListing` changed from `auto` to `false`. Full directory listings now performed instead of incremental.
- **Impact**: Auto Loader jobs relying on incremental listing performance
- **Remediation**: 
  - Migrate to file events for faster discovery
  - Or explicitly set `cloudFiles.useIncrementalListing` to `auto`

#### 3. Decimal Precision and Scale in Spark Connect
- **Severity**: MEDIUM
- **Description**: Decimal precision and scale in array/map literals changed to `SYSTEM_DEFAULT` (38, 18) in Spark Connect mode.
- **Impact**: Logical plan analysis (not query results)
- **Remediation**: Explicit precision/scale if needed for plan comparison

#### 4. Null Values in Array, Map, Struct Literals (Connect)
- **Severity**: MEDIUM
- **Description**: Null values now preserved instead of being replaced with protobuf default values (0, empty string, false).
- **Impact**: Spark Connect applications expecting default value substitution
- **Remediation**: Handle null values explicitly

#### 5. Nullability Preservation for Typed Literals
- **Severity**: LOW
- **Description**: Array elements and map values now correctly preserve nullability specification.
- **Impact**: Type checking in Spark Connect Scala client
- **Remediation**: Review type definitions

#### 6. Null Struct Handling in Delta Tables
- **Severity**: MEDIUM
- **Description**: Null struct values now correctly preserved when dropping `NullType` columns.
- **Impact**: Tables with nullable struct columns
- **Previous Behavior**: Null structs replaced with non-null struct (all fields null)
- **Current Behavior**: Null structs remain null
- **Remediation**: Review struct null handling logic

#### 7. Null Struct Handling in Parquet
- **Severity**: LOW
- **Description**: Consistent null struct detection between Photon and non-Photon readers.
- **Impact**: Parquet reads with missing struct fields
- **Remediation**: Update null handling if relying on previous inconsistent behavior

---

## Migration Checklist

### Pre-Migration
- [ ] Inventory all Scala code for collection usage
- [ ] Search for `input_file_name` function usage
- [ ] Identify Auto Loader jobs with incremental listing
- [ ] Review VARIANT type usage in Python UDFs
- [ ] Check for `!` syntax in SQL statements
- [ ] Audit views for column definition syntax
- [ ] Document MERGE INTO/UPDATE type casting behavior

### Testing Phase
- [ ] Run full regression tests on intermediate LTS version
- [ ] Validate Delta table read/write operations
- [ ] Test all Python UDF/UDAF/UDTF functions
- [ ] Verify JDBC timestamp handling
- [ ] Test Auto Loader performance
- [ ] Validate Spark Connect applications (if used)

### Post-Migration
- [ ] Monitor query performance changes
- [ ] Verify error handling behavior
- [ ] Check partition metadata functionality
- [ ] Validate struct null handling in Delta tables

---

## Quick Reference: Remediation Commands

```sql
-- Preserve old JDBC timestamp behavior
SET spark.sql.legacy.jdbc.useNullCalendar = false;

-- Preserve old Parquet timestamp inference
SET spark.sql.parquet.inferTimestampNTZ.enabled = false;

-- Preserve old data source cache behavior
SET spark.sql.legacy.readFileSourceTableCacheIgnoreOptions = true;

-- Preserve old Auto Loader incremental listing
SET cloudFiles.useIncrementalListing = auto;
```

---

## Sources

1. [Databricks Runtime 13.3 LTS Release Notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/13.3lts)
2. [Databricks Runtime 14.3 LTS Release Notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/14.3lts)
3. [Databricks Runtime 15.4 LTS Release Notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/15.4lts)
4. [Databricks Runtime 16.4 LTS Release Notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/16.4lts)
5. [Databricks Runtime 17.3 LTS Release Notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/17.3lts)

---

*Document generated from official Microsoft Learn documentation on January 23, 2026*
