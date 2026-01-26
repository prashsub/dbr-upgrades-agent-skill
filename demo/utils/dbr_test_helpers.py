"""
DBR Migration Test Helpers
Helper functions and utilities for testing DBR breaking changes
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Dict, List, Tuple
import json


# ============================================================================
# Breaking Change Test Data Generators
# ============================================================================

def generate_test_data(spark, num_rows: int = 100) -> DataFrame:
    """
    Generate test data for DBR migration testing
    
    Args:
        spark: SparkSession
        num_rows: Number of rows to generate
    
    Returns:
        DataFrame with test data
    """
    return spark.range(num_rows).select(
        col("id"),
        (col("id") * 10).alias("value"),
        lit("test_data").alias("category"),
        current_timestamp().alias("created_at")
    )


def create_breaking_change_sample(spark, bc_id: str) -> Dict:
    """
    Create sample data for specific breaking change test cases
    
    Args:
        spark: SparkSession
        bc_id: Breaking change ID (e.g., "BC-17.3-001")
    
    Returns:
        Dictionary with test case information
    """
    samples = {
        "BC-17.3-001": {
            "description": "input_file_name() removed",
            "old_code": "from pyspark.sql.functions import input_file_name",
            "new_code": "# Use col('_metadata.file_name') instead",
            "severity": "HIGH"
        },
        "BC-15.4-003": {
            "description": "! syntax for NOT disallowed",
            "old_code": "CREATE TABLE IF ! EXISTS",
            "new_code": "CREATE TABLE IF NOT EXISTS",
            "severity": "HIGH"
        },
        "BC-15.4-001": {
            "description": "VARIANT type in Python UDF",
            "old_code": "@udf(returnType=VariantType())",
            "new_code": "@udf(returnType=StringType())",
            "severity": "MEDIUM"
        }
    }
    return samples.get(bc_id, {"description": "Unknown breaking change", "severity": "UNKNOWN"})


# ============================================================================
# Validation Functions
# ============================================================================

def validate_dataframe_schema(df: DataFrame, expected_columns: List[str]) -> Tuple[bool, str]:
    """
    Validate that DataFrame has expected columns
    
    Args:
        df: DataFrame to validate
        expected_columns: List of expected column names
    
    Returns:
        Tuple of (is_valid, message)
    """
    actual_columns = set(df.columns)
    expected_columns_set = set(expected_columns)
    
    missing = expected_columns_set - actual_columns
    extra = actual_columns - expected_columns_set
    
    if missing or extra:
        msg = []
        if missing:
            msg.append(f"Missing columns: {missing}")
        if extra:
            msg.append(f"Extra columns: {extra}")
        return False, "; ".join(msg)
    
    return True, "Schema validation passed"


def compare_outputs(df1: DataFrame, df2: DataFrame, key_columns: List[str]) -> Dict:
    """
    Compare two DataFrames for equivalence (for testing before/after fixes)
    
    Args:
        df1: First DataFrame (before fix)
        df2: Second DataFrame (after fix)
        key_columns: Columns to use as keys for comparison
    
    Returns:
        Dictionary with comparison results
    """
    count1 = df1.count()
    count2 = df2.count()
    
    result = {
        "row_count_match": count1 == count2,
        "df1_count": count1,
        "df2_count": count2,
        "schema_match": df1.schema == df2.schema
    }
    
    if result["row_count_match"] and result["schema_match"]:
        # Check for data differences
        df1_sorted = df1.orderBy(*key_columns)
        df2_sorted = df2.orderBy(*key_columns)
        
        # Simple comparison - in production would be more sophisticated
        result["data_match"] = df1_sorted.subtract(df2_sorted).count() == 0
    else:
        result["data_match"] = False
    
    return result


# ============================================================================
# DBR Version Detection
# ============================================================================

def get_dbr_version(spark) -> Dict[str, str]:
    """
    Get the current DBR version information
    
    Args:
        spark: SparkSession
    
    Returns:
        Dictionary with version information
    """
    try:
        dbr_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown")
        spark_version = spark.version
        
        # Parse DBR version (e.g., "13.3.x-scala2.12" -> "13.3")
        dbr_major_minor = ".".join(dbr_version.split(".")[:2]) if "." in dbr_version else dbr_version
        
        return {
            "dbr_full": dbr_version,
            "dbr_version": dbr_major_minor,
            "spark_version": spark_version,
            "is_lts": "lts" in dbr_version.lower()
        }
    except Exception as e:
        return {
            "dbr_full": "unknown",
            "dbr_version": "unknown",
            "spark_version": spark.version,
            "is_lts": False,
            "error": str(e)
        }


def is_breaking_change_applicable(spark, bc_id: str) -> bool:
    """
    Check if a breaking change applies to the current DBR version
    
    Args:
        spark: SparkSession
        bc_id: Breaking change ID
    
    Returns:
        True if the breaking change applies to this DBR version
    """
    version_info = get_dbr_version(spark)
    dbr_version = version_info.get("dbr_version", "unknown")
    
    # Map breaking changes to DBR versions
    bc_dbr_map = {
        "BC-13.3": ["13.3", "14.3", "15.4", "16.4", "17.3"],
        "BC-15.4": ["15.4", "16.4", "17.3"],
        "BC-16.4": ["16.4", "17.3"],
        "BC-17.3": ["17.3"],
        "BC-SC": ["13.3", "14.3", "15.4", "16.4", "17.3"]  # Spark Connect issues
    }
    
    # Extract prefix from bc_id (e.g., "BC-17.3-001" -> "BC-17.3")
    bc_prefix = "-".join(bc_id.split("-")[:2])
    
    applicable_versions = bc_dbr_map.get(bc_prefix, [])
    return dbr_version in applicable_versions


# ============================================================================
# Test Result Tracking
# ============================================================================

class BreakingChangeTestResult:
    """Track test results for breaking change validation"""
    
    def __init__(self, bc_id: str, description: str):
        self.bc_id = bc_id
        self.description = description
        self.tests_run = 0
        self.tests_passed = 0
        self.tests_failed = 0
        self.errors = []
    
    def add_test_result(self, passed: bool, message: str = ""):
        """Add a test result"""
        self.tests_run += 1
        if passed:
            self.tests_passed += 1
        else:
            self.tests_failed += 1
            if message:
                self.errors.append(message)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for reporting"""
        return {
            "bc_id": self.bc_id,
            "description": self.description,
            "tests_run": self.tests_run,
            "tests_passed": self.tests_passed,
            "tests_failed": self.tests_failed,
            "success_rate": f"{(self.tests_passed/self.tests_run*100):.1f}%" if self.tests_run > 0 else "0%",
            "errors": self.errors
        }
    
    def __str__(self) -> str:
        """String representation"""
        return (f"{self.bc_id}: {self.description}\n"
                f"  Tests: {self.tests_run} | Passed: {self.tests_passed} | "
                f"Failed: {self.tests_failed}")


# ============================================================================
# Configuration Helpers
# ============================================================================

def get_legacy_config_settings() -> Dict[str, str]:
    """
    Get recommended legacy configuration settings for DBR compatibility
    
    Returns:
        Dictionary of config key-value pairs
    """
    return {
        "spark.sql.parquet.inferTimestampNTZ.enabled": "false",
        "spark.sql.legacy.jdbc.useNullCalendar": "false",
        "spark.databricks.delta.merge.materializeSource": "auto"
    }


def apply_legacy_configs(spark, configs: Dict[str, str] = None):
    """
    Apply legacy configuration settings to SparkSession
    
    Args:
        spark: SparkSession
        configs: Optional dictionary of configs to apply. If None, uses defaults.
    """
    if configs is None:
        configs = get_legacy_config_settings()
    
    applied = []
    failed = []
    
    for key, value in configs.items():
        try:
            spark.conf.set(key, value)
            applied.append(f"{key} = {value}")
        except Exception as e:
            failed.append(f"{key}: {str(e)}")
    
    return {
        "applied": applied,
        "failed": failed,
        "success_count": len(applied),
        "failure_count": len(failed)
    }


# ============================================================================
# Report Generation
# ============================================================================

def generate_test_report(results: List[BreakingChangeTestResult]) -> str:
    """
    Generate a formatted test report
    
    Args:
        results: List of BreakingChangeTestResult objects
    
    Returns:
        Formatted report string
    """
    report = []
    report.append("=" * 80)
    report.append("DBR MIGRATION TEST REPORT")
    report.append("=" * 80)
    report.append("")
    
    total_tests = sum(r.tests_run for r in results)
    total_passed = sum(r.tests_passed for r in results)
    total_failed = sum(r.tests_failed for r in results)
    
    report.append(f"Total Tests: {total_tests}")
    report.append(f"Passed: {total_passed} ({total_passed/total_tests*100:.1f}%)" if total_tests > 0 else "Passed: 0")
    report.append(f"Failed: {total_failed}")
    report.append("")
    report.append("-" * 80)
    report.append("")
    
    for result in results:
        report.append(str(result))
        if result.errors:
            report.append("  Errors:")
            for error in result.errors:
                report.append(f"    - {error}")
        report.append("")
    
    report.append("=" * 80)
    
    return "\n".join(report)
