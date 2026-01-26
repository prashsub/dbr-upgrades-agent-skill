# Step 4: Performance Testing

After validating code correctness, test for performance regressions to ensure the upgrade doesn't negatively impact your workloads.

## Prerequisites

- [ ] Quality validation passed (see [03-quality-validation.md](03-quality-validation.md))
- [ ] Access to create test clusters
- [ ] Representative test datasets
- [ ] Baseline performance metrics from current DBR

---

## Phase 1: Environment Setup

### 4.1.1: Create Matching Test Clusters

Create clusters with identical configurations except for DBR version:

| Setting | Source Cluster | Target Cluster |
|---------|---------------|----------------|
| DBR Version | 13.3 LTS | 17.3 LTS |
| Node Type | Same | Same |
| Worker Count | Same | Same |
| Autoscaling | Same | Same |
| Spark Config | Same (where possible) | Same (where possible) |
| Photon | Same | Same |

```python
# Cluster configuration example
cluster_config = {
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 4,
    "spark_conf": {
        "spark.sql.shuffle.partitions": "200",
        "spark.databricks.delta.optimizeWrite.enabled": "true"
    },
    "autoscale": {
        "min_workers": 2,
        "max_workers": 8
    }
}
```

### 4.1.2: Prepare Test Data

Use production-representative data:

```python
# Create test dataset that mirrors production scale
def prepare_test_data(scale_factor=0.1):
    """
    Prepare test data at specified scale.
    scale_factor=0.1 means 10% of production size
    """
    
    # Sample from production tables
    production_tables = [
        "catalog.schema.customers",
        "catalog.schema.transactions",
        "catalog.schema.events"
    ]
    
    for table in production_tables:
        df = spark.table(table).sample(scale_factor)
        test_table = f"test_migration.{table.split('.')[-1]}"
        df.write.mode("overwrite").saveAsTable(test_table)
        print(f"Created {test_table} with {df.count()} rows")

prepare_test_data(scale_factor=0.1)
```

---

## Phase 2: Benchmark Framework

### 4.2.1: Performance Measurement Utilities

```python
# perf_utils.py

import time
import statistics
from dataclasses import dataclass, field
from typing import List, Callable, Any, Dict
from datetime import datetime

@dataclass
class BenchmarkResult:
    """Results from a single benchmark run"""
    name: str
    dbr_version: str
    execution_times: List[float] = field(default_factory=list)
    row_counts: List[int] = field(default_factory=list)
    bytes_scanned: List[int] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    @property
    def avg_time(self) -> float:
        return statistics.mean(self.execution_times) if self.execution_times else 0
    
    @property
    def std_dev(self) -> float:
        return statistics.stdev(self.execution_times) if len(self.execution_times) > 1 else 0
    
    @property
    def min_time(self) -> float:
        return min(self.execution_times) if self.execution_times else 0
    
    @property
    def max_time(self) -> float:
        return max(self.execution_times) if self.execution_times else 0


def run_benchmark(
    name: str,
    func: Callable,
    iterations: int = 5,
    warmup: int = 1,
    dbr_version: str = ""
) -> BenchmarkResult:
    """
    Run a benchmark multiple times and collect metrics.
    
    Args:
        name: Benchmark name
        func: Function to benchmark (should return DataFrame or row count)
        iterations: Number of timed runs
        warmup: Number of warmup runs (not timed)
        dbr_version: Current DBR version for labeling
    """
    result = BenchmarkResult(name=name, dbr_version=dbr_version)
    
    # Warmup runs
    print(f"Running {warmup} warmup iteration(s)...")
    for i in range(warmup):
        try:
            func()
            spark.catalog.clearCache()
        except Exception as e:
            print(f"Warmup error: {e}")
    
    # Timed runs
    print(f"Running {iterations} timed iteration(s)...")
    for i in range(iterations):
        try:
            # Clear caches
            spark.catalog.clearCache()
            
            # Time the execution
            start_time = time.time()
            output = func()
            
            # Force execution if DataFrame
            if hasattr(output, 'count'):
                row_count = output.count()
                result.row_counts.append(row_count)
            
            end_time = time.time()
            execution_time = end_time - start_time
            result.execution_times.append(execution_time)
            
            print(f"  Iteration {i+1}: {execution_time:.2f}s")
            
        except Exception as e:
            result.errors.append(str(e))
            print(f"  Iteration {i+1}: ERROR - {e}")
    
    return result


def compare_results(
    source_result: BenchmarkResult,
    target_result: BenchmarkResult,
    threshold_pct: float = 10.0
) -> Dict:
    """
    Compare benchmark results between two DBR versions.
    
    Args:
        source_result: Results from source DBR
        target_result: Results from target DBR
        threshold_pct: Acceptable performance degradation percentage
    
    Returns:
        Comparison dictionary with status and metrics
    """
    comparison = {
        "name": source_result.name,
        "source_dbr": source_result.dbr_version,
        "target_dbr": target_result.dbr_version,
        "source_avg_time": source_result.avg_time,
        "target_avg_time": target_result.avg_time,
        "difference_seconds": target_result.avg_time - source_result.avg_time,
        "difference_percent": 0,
        "status": "UNKNOWN",
        "regression": False
    }
    
    if source_result.avg_time > 0:
        diff_pct = ((target_result.avg_time - source_result.avg_time) / source_result.avg_time) * 100
        comparison["difference_percent"] = diff_pct
        
        if diff_pct > threshold_pct:
            comparison["status"] = "REGRESSION"
            comparison["regression"] = True
        elif diff_pct < -threshold_pct:
            comparison["status"] = "IMPROVEMENT"
        else:
            comparison["status"] = "STABLE"
    
    return comparison
```

### 4.2.2: Query Benchmarks

```python
# benchmarks.py

# Define your benchmark queries/operations
BENCHMARKS = {
    "simple_scan": lambda: spark.table("test_migration.transactions").count(),
    
    "filtered_aggregation": lambda: (
        spark.table("test_migration.transactions")
        .filter("amount > 100")
        .groupBy("customer_id")
        .agg({"amount": "sum", "id": "count"})
    ),
    
    "join_operation": lambda: (
        spark.table("test_migration.transactions").alias("t")
        .join(
            spark.table("test_migration.customers").alias("c"),
            col("t.customer_id") == col("c.id")
        )
        .select("t.*", "c.name", "c.region")
    ),
    
    "window_function": lambda: (
        spark.table("test_migration.transactions")
        .withColumn("running_total", 
            F.sum("amount").over(
                Window.partitionBy("customer_id").orderBy("timestamp")
            ))
    ),
    
    "complex_etl": lambda: run_etl_pipeline(),  # Your actual ETL
    
    "delta_merge": lambda: run_delta_merge_benchmark(),
    
    "auto_loader_batch": lambda: run_auto_loader_benchmark(),
}
```

---

## Phase 3: Run Performance Tests

### 4.3.1: Execute Benchmarks on Both DBRs

```python
# run_perf_tests.py

def run_all_benchmarks(dbr_version: str, iterations: int = 5) -> Dict[str, BenchmarkResult]:
    """Run all benchmarks and return results"""
    
    results = {}
    
    print(f"\n{'='*60}")
    print(f"Running benchmarks on DBR {dbr_version}")
    print('='*60)
    
    for name, func in BENCHMARKS.items():
        print(f"\n--- Benchmark: {name} ---")
        result = run_benchmark(
            name=name,
            func=func,
            iterations=iterations,
            warmup=1,
            dbr_version=dbr_version
        )
        results[name] = result
        
        print(f"Average: {result.avg_time:.2f}s (±{result.std_dev:.2f}s)")
    
    return results


# Run on source DBR (13.3)
source_results = run_all_benchmarks("13.3 LTS", iterations=5)

# Save results
import json
with open("/tmp/perf_source.json", "w") as f:
    json.dump({k: v.__dict__ for k, v in source_results.items()}, f)


# --- Switch to target cluster (17.3) ---

# Run on target DBR (17.3)
target_results = run_all_benchmarks("17.3 LTS", iterations=5)

# Save results
with open("/tmp/perf_target.json", "w") as f:
    json.dump({k: v.__dict__ for k, v in target_results.items()}, f)
```

### 4.3.2: Compare Results

```python
# compare_perf.py

def generate_performance_report(
    source_results: Dict[str, BenchmarkResult],
    target_results: Dict[str, BenchmarkResult],
    threshold_pct: float = 10.0
) -> str:
    """Generate performance comparison report"""
    
    report = []
    report.append("# Performance Comparison Report")
    report.append(f"Generated: {datetime.now().isoformat()}")
    report.append(f"Threshold for regression: {threshold_pct}%")
    report.append("")
    
    # Summary table
    report.append("## Summary")
    report.append("")
    report.append("| Benchmark | Source (s) | Target (s) | Diff (%) | Status |")
    report.append("|-----------|------------|------------|----------|--------|")
    
    regressions = []
    improvements = []
    
    for name in source_results:
        if name not in target_results:
            continue
            
        comparison = compare_results(
            source_results[name],
            target_results[name],
            threshold_pct
        )
        
        status_emoji = {
            "REGRESSION": "🔴",
            "IMPROVEMENT": "🟢",
            "STABLE": "🟡"
        }.get(comparison["status"], "⚪")
        
        report.append(
            f"| {name} | {comparison['source_avg_time']:.2f} | "
            f"{comparison['target_avg_time']:.2f} | "
            f"{comparison['difference_percent']:+.1f}% | "
            f"{status_emoji} {comparison['status']} |"
        )
        
        if comparison["regression"]:
            regressions.append(comparison)
        elif comparison["status"] == "IMPROVEMENT":
            improvements.append(comparison)
    
    # Regressions detail
    if regressions:
        report.append("")
        report.append("## ⚠️ Regressions Detected")
        report.append("")
        for r in regressions:
            report.append(f"### {r['name']}")
            report.append(f"- Source: {r['source_avg_time']:.2f}s")
            report.append(f"- Target: {r['target_avg_time']:.2f}s")
            report.append(f"- Degradation: {r['difference_percent']:.1f}%")
            report.append("")
    
    # Improvements detail
    if improvements:
        report.append("")
        report.append("## ✅ Improvements")
        report.append("")
        for i in improvements:
            report.append(f"- **{i['name']}**: {abs(i['difference_percent']):.1f}% faster")
    
    # Overall status
    report.append("")
    report.append("## Overall Status")
    
    if not regressions:
        report.append("")
        report.append("### ✅ PERFORMANCE ACCEPTABLE")
        report.append("No significant performance regressions detected.")
    else:
        report.append("")
        report.append("### ⚠️ PERFORMANCE REVIEW REQUIRED")
        report.append(f"{len(regressions)} benchmark(s) showed regression.")
        report.append("Please investigate before proceeding with upgrade.")
    
    return "\n".join(report)


# Generate report
report = generate_performance_report(source_results, target_results)
print(report)

# Save report
with open("/Workspace/migration/performance_report.md", "w") as f:
    f.write(report)
```

---

## Phase 4: Investigate Regressions

### 4.4.1: Spark UI Analysis

For any regressions, compare Spark UI metrics:

```python
def capture_query_metrics(func: Callable, name: str) -> Dict:
    """Capture detailed Spark metrics for a query"""
    
    # Enable extended metrics
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
    # Run query
    start = time.time()
    df = func()
    if hasattr(df, 'count'):
        df.count()
    end = time.time()
    
    # Get last query execution ID
    # Note: This requires accessing Spark internals
    metrics = {
        "name": name,
        "total_time": end - start,
        # Add more metrics from Spark UI
    }
    
    return metrics
```

### 4.4.2: Query Plan Comparison

```python
def compare_query_plans(func: Callable, name: str):
    """Compare query plans between DBR versions"""
    
    df = func()
    
    print(f"=== Query Plan for {name} ===")
    print("\n--- Logical Plan ---")
    print(df._jdf.queryExecution().logical().toString())
    
    print("\n--- Physical Plan ---")
    df.explain(mode="extended")
    
    print("\n--- Cost-Based Statistics ---")
    df.explain(mode="cost")
```

### 4.4.3: Common Regression Causes

| Symptom | Possible Cause | Investigation |
|---------|---------------|---------------|
| Slower scans | Different Parquet reading | Check `spark.sql.parquet.*` configs |
| Slower joins | Changed join strategy | Compare physical plans |
| Higher memory | Different spill behavior | Check executor memory metrics |
| Auto Loader slower | Incremental listing disabled | Review `cloudFiles.useIncrementalListing` |
| Scala code slower | Collection API changes | Profile Scala code specifically |

### 4.4.4: Configuration Tuning

If regressions are found, try these configurations:

```python
# Performance tuning configs to try on target DBR
performance_configs = {
    # Preserve some legacy behaviors
    "spark.sql.legacy.readFileSourceTableCacheIgnoreOptions": "true",
    
    # Tune shuffle partitions
    "spark.sql.shuffle.partitions": "auto",
    
    # Adaptive query execution
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    
    # Photon optimization
    "spark.databricks.photon.enabled": "true",
}

for key, value in performance_configs.items():
    spark.conf.set(key, value)

# Re-run benchmark
result = run_benchmark("problem_query", problem_func, iterations=3)
```

---

## Phase 5: Performance Report

### 4.5.1: Final Performance Summary

```python
# Generate final report
def create_final_perf_report(
    source_results: Dict,
    target_results: Dict,
    tuning_applied: Dict = None
) -> str:
    """Create final performance report for sign-off"""
    
    report = []
    report.append("# DBR Migration Performance Report")
    report.append(f"Date: {datetime.now().strftime('%Y-%m-%d')}")
    report.append(f"Source: DBR 13.3 LTS → Target: DBR 17.3 LTS")
    report.append("")
    
    # Executive summary
    report.append("## Executive Summary")
    
    total_benchmarks = len(source_results)
    regressions = sum(1 for name in source_results 
                      if compare_results(source_results[name], target_results[name])["regression"])
    improvements = sum(1 for name in source_results
                       if compare_results(source_results[name], target_results[name])["status"] == "IMPROVEMENT")
    
    report.append(f"- Total benchmarks: {total_benchmarks}")
    report.append(f"- Regressions: {regressions}")
    report.append(f"- Improvements: {improvements}")
    report.append(f"- Stable: {total_benchmarks - regressions - improvements}")
    report.append("")
    
    # Recommendation
    if regressions == 0:
        report.append("### ✅ Recommendation: PROCEED WITH UPGRADE")
        report.append("No performance regressions detected. Safe to upgrade.")
    elif regressions <= 2 and tuning_applied:
        report.append("### ⚠️ Recommendation: PROCEED WITH CAUTION")
        report.append("Minor regressions detected but mitigated with configuration tuning.")
    else:
        report.append("### ❌ Recommendation: INVESTIGATE FURTHER")
        report.append("Significant regressions require investigation before upgrade.")
    
    return "\n".join(report)
```

---

## Performance Testing Checklist

### Setup
- [ ] Source cluster (13.3 LTS) configured
- [ ] Target cluster (17.3 LTS) configured
- [ ] Test data prepared (production-representative)
- [ ] Benchmark suite defined

### Execution
- [ ] Warmup runs completed
- [ ] Multiple iterations run (minimum 5)
- [ ] Results saved for both DBRs
- [ ] Caches cleared between runs

### Analysis
- [ ] Comparison report generated
- [ ] Regressions identified
- [ ] Query plans compared for regressions
- [ ] Tuning attempted for regressions

### Sign-off
- [ ] No regressions > 10% threshold
- [ ] Or regressions mitigated with config
- [ ] Performance report approved
- [ ] Ready for production rollout

---

## Next Steps

After performance testing is complete:

→ **[05-rollout-checklist.md](05-rollout-checklist.md)**: Complete checklist for production rollout

---

## References

- [Databricks Performance Tuning](https://docs.databricks.com/en/optimizations/index.html)
- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Photon Runtime](https://docs.databricks.com/en/compute/photon.html)
