# Step 3: Quality Validation

After applying migration fixes, validate that your code is correct and produces the expected results.

## Prerequisites

- [ ] Breaking changes fixed (see [02-using-assistant.md](02-using-assistant.md))
- [ ] Access to both source and target DBR clusters for comparison
- [ ] Test datasets available

---

## Phase 1: Static Code Validation

### 3.1.1: Syntax Verification

Run the validation script to ensure no breaking patterns remain:

```python
# In a notebook on target DBR version
%pip install -q pathlib

# Run the validation script from the skill
exec(open("/Workspace/Users/{username}/.assistant/skills/databricks-lts-migration/scripts/validate-migration.py").read())

# Or ask the Assistant:
# "Validate that all breaking changes are fixed in /Workspace/my-project/"
```

**Expected Result:** All checks should pass with ✅

### 3.1.2: Import Verification

Create a test notebook to verify all imports work:

```python
# test_imports.py - Run on target DBR
import sys
print(f"Python version: {sys.version}")
print(f"Spark version: {spark.version}")

# Test your project imports
try:
    from my_project import etl_pipeline
    from my_project import data_loader
    from my_project import transform_utils
    print("✅ All imports successful")
except ImportError as e:
    print(f"❌ Import failed: {e}")
```

### 3.1.3: Type Checking (Optional)

For Python projects with type hints:

```bash
# Install mypy
%pip install mypy

# Run type checking
!mypy /Workspace/my-project/ --ignore-missing-imports
```

---

## Phase 2: Unit Test Execution

### 3.2.1: Run Existing Unit Tests

```python
# Install pytest if needed
%pip install pytest pytest-spark

# Run your test suite
import pytest
pytest.main([
    "/Workspace/my-project/tests/",
    "-v",
    "--tb=short",
    "-x"  # Stop on first failure
])
```

### 3.2.2: Create Migration-Specific Tests

Add tests specifically for migrated functionality:

```python
# tests/test_migration.py

import pytest
from pyspark.sql import SparkSession

class TestMigrationFixes:
    """Tests to validate DBR migration fixes"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        return SparkSession.builder.getOrCreate()
    
    def test_metadata_file_name_works(self, spark):
        """Verify _metadata.file_name replacement for input_file_name()"""
        # Create test data
        df = spark.read.parquet("/tmp/test_data/")
        
        # Test the new pattern
        result = df.select("*", "_metadata.file_name")
        
        assert "file_name" in result.columns
        assert result.count() > 0
        
    def test_not_syntax_valid(self, spark):
        """Verify NOT syntax works correctly"""
        spark.sql("CREATE TABLE IF NOT EXISTS test_migration_table (id INT)")
        result = spark.sql("SELECT * FROM test_migration_table WHERE id IS NOT NULL")
        # Should not throw ParseException
        assert result is not None
        
    def test_auto_loader_config(self, spark):
        """Verify Auto Loader configuration"""
        # Test that explicit incremental listing works
        schema = "id INT, name STRING"
        
        # This should not fail
        df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.useIncrementalListing", "auto")
              .schema(schema)
              .load("/tmp/test_stream/"))
        
        assert df.isStreaming
```

### 3.2.3: Test Coverage Report

```python
# Generate coverage report
%pip install pytest-cov

pytest.main([
    "/Workspace/my-project/tests/",
    "--cov=/Workspace/my-project/src/",
    "--cov-report=html",
    "-v"
])

print("Coverage report saved to htmlcov/index.html")
```

---

## Phase 3: Output Comparison (Data Validation)

### 3.3.1: Set Up Comparison Environment

Create clusters for both DBR versions:

| Cluster | DBR Version | Purpose |
|---------|-------------|---------|
| `migration-source` | 13.3 LTS | Baseline (current) |
| `migration-target` | 17.3 LTS | Target version |

### 3.3.2: Create Comparison Framework

```python
# comparison_utils.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hash, concat_ws
import hashlib

def compare_dataframes(
    df_source: DataFrame,
    df_target: DataFrame,
    key_columns: list,
    compare_columns: list = None
) -> dict:
    """
    Compare two DataFrames for data equivalence.
    
    Returns dict with comparison results.
    """
    results = {
        "row_count_match": False,
        "schema_match": False,
        "data_match": False,
        "differences": []
    }
    
    # Row count comparison
    source_count = df_source.count()
    target_count = df_target.count()
    results["source_count"] = source_count
    results["target_count"] = target_count
    results["row_count_match"] = source_count == target_count
    
    # Schema comparison
    source_schema = set(df_source.columns)
    target_schema = set(df_target.columns)
    results["schema_match"] = source_schema == target_schema
    
    if not results["schema_match"]:
        results["missing_in_target"] = list(source_schema - target_schema)
        results["extra_in_target"] = list(target_schema - source_schema)
    
    # Data comparison (using hash)
    if compare_columns is None:
        compare_columns = [c for c in df_source.columns if c not in key_columns]
    
    def add_row_hash(df, cols):
        return df.withColumn(
            "_row_hash",
            hash(concat_ws("||", *[col(c).cast("string") for c in cols]))
        )
    
    source_hashed = add_row_hash(df_source, compare_columns)
    target_hashed = add_row_hash(df_target, compare_columns)
    
    # Join and compare
    comparison = (
        source_hashed.alias("s")
        .join(target_hashed.alias("t"), key_columns, "full_outer")
        .filter(
            (col("s._row_hash") != col("t._row_hash")) |
            col("s._row_hash").isNull() |
            col("t._row_hash").isNull()
        )
    )
    
    diff_count = comparison.count()
    results["data_match"] = diff_count == 0
    results["difference_count"] = diff_count
    
    if diff_count > 0 and diff_count <= 100:
        results["sample_differences"] = comparison.limit(10).collect()
    
    return results


def validate_output(source_df, target_df, key_cols, name=""):
    """Run comparison and print report"""
    print(f"\n{'='*60}")
    print(f"Validating: {name}")
    print('='*60)
    
    results = compare_dataframes(source_df, target_df, key_cols)
    
    # Row count
    status = "✅" if results["row_count_match"] else "❌"
    print(f"{status} Row Count: {results['source_count']} vs {results['target_count']}")
    
    # Schema
    status = "✅" if results["schema_match"] else "❌"
    print(f"{status} Schema Match")
    if not results["schema_match"]:
        print(f"   Missing: {results.get('missing_in_target', [])}")
        print(f"   Extra: {results.get('extra_in_target', [])}")
    
    # Data
    status = "✅" if results["data_match"] else "❌"
    print(f"{status} Data Match (Differences: {results['difference_count']})")
    
    return results
```

### 3.3.3: Run Output Comparisons

```python
# Run on BOTH clusters and compare results

# Step 1: Run pipeline on source DBR (13.3)
# Connect to migration-source cluster
df_source = run_etl_pipeline(input_path="/data/input/")
df_source.write.mode("overwrite").parquet("/tmp/migration/output_source")

# Step 2: Run pipeline on target DBR (17.3)
# Connect to migration-target cluster
df_target = run_etl_pipeline(input_path="/data/input/")
df_target.write.mode("overwrite").parquet("/tmp/migration/output_target")

# Step 3: Compare (run on either cluster)
source = spark.read.parquet("/tmp/migration/output_source")
target = spark.read.parquet("/tmp/migration/output_target")

results = validate_output(
    source, target,
    key_cols=["id", "date"],
    name="ETL Pipeline Output"
)
```

---

## Phase 4: Edge Case Testing

### 3.4.1: Null Handling Tests

DBR 17.3 changed null struct handling. Test explicitly:

```python
def test_null_struct_handling():
    """Test that null structs are handled correctly"""
    
    # Create test data with null structs
    data = [
        (1, {"field1": "a", "field2": 1}),
        (2, None),  # Null struct
        (3, {"field1": None, "field2": None}),  # Struct with null fields
    ]
    
    df = spark.createDataFrame(data, ["id", "struct_col"])
    
    # Write and read back
    df.write.mode("overwrite").format("delta").save("/tmp/test_null_struct")
    df_read = spark.read.format("delta").load("/tmp/test_null_struct")
    
    # Verify null handling
    null_struct_count = df_read.filter("struct_col IS NULL").count()
    null_field_count = df_read.filter("struct_col IS NOT NULL AND struct_col.field1 IS NULL").count()
    
    print(f"Null structs: {null_struct_count}")  # Should be 1
    print(f"Structs with null fields: {null_field_count}")  # Should be 1
    
    assert null_struct_count == 1, "Null struct not preserved correctly"
    assert null_field_count == 1, "Struct with null fields not handled correctly"

test_null_struct_handling()
```

### 3.4.2: Timestamp Handling Tests

```python
def test_timestamp_handling():
    """Test timestamp type inference"""
    
    # Test with Parquet files that have different timestamp annotations
    from pyspark.sql.types import TimestampType, TimestampNTZType
    
    # Read Parquet and check inferred types
    df = spark.read.parquet("/path/to/parquet/with/timestamps")
    
    for field in df.schema.fields:
        if isinstance(field.dataType, (TimestampType, TimestampNTZType)):
            print(f"Column {field.name}: {field.dataType}")
            
    # Verify expected types
    # Adjust assertions based on your data
```

### 3.4.3: Auto Loader Tests

```python
def test_auto_loader_file_discovery():
    """Test Auto Loader discovers all files correctly"""
    
    import time
    from pyspark.sql.streaming import StreamingQuery
    
    # Create test files with non-lexicographic names
    test_files = [
        "/tmp/autoloader_test/file_10.json",
        "/tmp/autoloader_test/file_2.json",
        "/tmp/autoloader_test/file_1.json",
    ]
    
    for f in test_files:
        spark.createDataFrame([(1,)], ["id"]).write.mode("overwrite").json(f)
    
    # Run Auto Loader
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.useIncrementalListing", "false")  # New default
          .schema("id INT")
          .load("/tmp/autoloader_test/"))
    
    # Collect results
    query = df.writeStream.format("memory").queryName("test").start()
    time.sleep(10)
    query.stop()
    
    result = spark.sql("SELECT * FROM test")
    file_count = result.count()
    
    print(f"Files processed: {file_count}")
    assert file_count == 3, f"Expected 3 files, got {file_count}"

test_auto_loader_file_discovery()
```

---

## Phase 5: Validation Report

### Generate Final Validation Report

```python
# validation_report.py

def generate_validation_report(
    syntax_results: dict,
    unit_test_results: dict,
    comparison_results: dict,
    edge_case_results: dict
) -> str:
    """Generate comprehensive validation report"""
    
    report = []
    report.append("# Migration Validation Report")
    report.append(f"Generated: {datetime.now().isoformat()}")
    report.append(f"Target DBR: 17.3 LTS")
    report.append("")
    
    # Syntax Validation
    report.append("## 1. Syntax Validation")
    for check, passed in syntax_results.items():
        status = "✅" if passed else "❌"
        report.append(f"- {status} {check}")
    
    # Unit Tests
    report.append("")
    report.append("## 2. Unit Tests")
    report.append(f"- Total: {unit_test_results['total']}")
    report.append(f"- Passed: {unit_test_results['passed']}")
    report.append(f"- Failed: {unit_test_results['failed']}")
    
    # Data Comparison
    report.append("")
    report.append("## 3. Data Comparison")
    for name, result in comparison_results.items():
        status = "✅" if result["data_match"] else "❌"
        report.append(f"- {status} {name}: {result['difference_count']} differences")
    
    # Edge Cases
    report.append("")
    report.append("## 4. Edge Case Tests")
    for test, passed in edge_case_results.items():
        status = "✅" if passed else "❌"
        report.append(f"- {status} {test}")
    
    # Overall Status
    report.append("")
    report.append("## Overall Status")
    
    all_passed = (
        all(syntax_results.values()) and
        unit_test_results['failed'] == 0 and
        all(r["data_match"] for r in comparison_results.values()) and
        all(edge_case_results.values())
    )
    
    if all_passed:
        report.append("### ✅ VALIDATION PASSED")
        report.append("Code is ready for DBR 17.3 upgrade.")
    else:
        report.append("### ❌ VALIDATION FAILED")
        report.append("Please review and fix the failing checks before proceeding.")
    
    return "\n".join(report)
```

---

## Validation Checklist

### Pre-Validation
- [ ] All breaking changes fixed
- [ ] Source and target clusters configured
- [ ] Test data prepared

### Static Validation
- [ ] No breaking patterns remain
- [ ] All imports work on target DBR
- [ ] Type checking passes (if applicable)

### Unit Tests
- [ ] All existing tests pass
- [ ] Migration-specific tests pass
- [ ] Coverage maintained

### Data Comparison
- [ ] Row counts match
- [ ] Schemas match
- [ ] Data values match

### Edge Cases
- [ ] Null handling correct
- [ ] Timestamp types correct
- [ ] Auto Loader file discovery works

---

## Next Steps

After quality validation passes:

→ **[04-performance-testing.md](04-performance-testing.md)**: Test for performance regressions

---

## References

- [Delta Lake Testing Best Practices](https://docs.delta.io/latest/best-practices.html)
- [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)
