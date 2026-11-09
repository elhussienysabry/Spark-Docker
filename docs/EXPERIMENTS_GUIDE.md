# Catalog of Apache Spark Empirical Experiments

> **A structured laboratory guide of 30+ hands-on experiments to observe Spark internals in action.**  
> Every experiment includes: Objective · Hypothesis · Execution Steps · Expected Observation · Engineering Takeaway.

---

## Experiment Catalog Overview

| # | Experiment Name | Focus Area | Key Concepts | Related Notebook |
|---|---|---|---|---|
| **E01** | Lazy Evaluation Latency Benchmark | Execution Engine | DAG, Action vs Transformation | `04_execution_engine/01_lazy_evaluation.ipynb` |
| **E02** | Narrow vs Wide Transformation Stages | Execution Engine | Shuffle, Stage Boundaries | `04_execution_engine/06_narrow_and_wide_transformations.ipynb` |
| **E03** | RDD `groupByKey` vs `reduceByKey` | Shuffle & RDD | Map-side Combining, Network IO | `02_rdd/02_rdd_transformations_and_actions.ipynb` |
| **E04** | `repartition()` vs `coalesce()` Cost | Partitions | Full Shuffle vs Local Merge | `05_partitions_and_shuffle/02_repartition_and_coalesce.ipynb` |
| **E05** | Partition Count Sizing & Core Utilization | Partitions | Parallelism, Sched Overhead | `05_partitions_and_shuffle/01_partitioning_fundamentals.ipynb` |
| **E06** | Inducing & Observing Data Skew | Performance | Stragglers, Task Distribution | `05_partitions_and_shuffle/06_data_skew_and_solutions.ipynb` |
| **E07** | Salting a Skewed Join Key | Performance | Skew Remediation, Salting | `10_performance_and_optimization/06_data_skew_optimization.ipynb` |
| **E08** | Broadcast Hash Join vs Sort-Merge Join | Joins | Network Shuffle Elimination | `07_spark_sql_internals/04_join_strategies.ipynb` |
| **E09** | Caching Benefit in Iterative Actions | Memory | Storage Pool, Deserialization | `06_memory_and_storage/04_cache_and_persist.ipynb` |
| **E10** | Caching Single-Use Anti-Pattern | Memory | Memory Contention, Overhead | `06_memory_and_storage/04_cache_and_persist.ipynb` |
| **E11** | Simulating Execution Memory Spill | Memory | Spill (Memory), Spill (Disk) | `06_memory_and_storage/05_spill_to_disk.ipynb` |
| **E12** | Broadcast Variable Memory Savings | Memory | P2P Distribution, BitTorrent | `06_memory_and_storage/06_broadcast_variables.ipynb` |
| **E13** | Catalyst Predicate Pushdown Verification | SQL Internals | Row Group Filtering, Parquet | `07_spark_sql_internals/01_catalyst_optimizer.ipynb` |
| **E14** | Whole-Stage Code Generation Speedup | SQL Internals | Tungsten, Bytecode Loops | `07_spark_sql_internals/03_tungsten_and_whole_stage_codegen.ipynb` |
| **E15** | AQE Dynamic Partition Coalescing | SQL Internals | Runtime Re-optimization | `07_spark_sql_internals/05_adaptive_query_execution.ipynb` |
| **E16** | AQE Dynamic Skew Join Splitting | SQL Internals | Automatic Skew Handling | `07_spark_sql_internals/05_adaptive_query_execution.ipynb` |
| **E17** | Parquet vs CSV Compression & Read I/O | Data Sources | Columnar Storage, Footprint | `08_data_sources_and_formats/02_parquet_and_orc.ipynb` |
| **E18** | Partitioned Directory Data Lake Pruning | Data Sources | Partition Pruning, Directory | `08_data_sources_and_formats/03_partitioned_data_lake.ipynb` |
| **E19** | Python UDF vs Vectorized Pandas UDF | Optimization | Py4J, Apache Arrow Zero-Copy | `03_dataframe_and_spark_sql/08_udfs_and_pandas_udfs.ipynb` |
| **E20** | Lineage Recomputation on Executor Death | Fault Tolerance | RDD Lineage, Resilience | `11_fault_tolerance_and_reliability/01_lineage_and_recomputation.ipynb` |
| **E21** | Checkpointing Lineage Truncation | Fault Tolerance | Graph Truncation, StackOverflow| `11_fault_tolerance_and_reliability/04_checkpointing.ipynb` |
| **E22** | Structured Streaming File Directory Trigger | Streaming | Micro-batch, File Source | `09_streaming/02_structured_streaming.ipynb` |
| **E23** | Streaming Watermark Late Event Drop | Streaming | Watermarks, State Store | `09_streaming/04_watermarks_and_late_data.ipynb` |
| **E24** | Streaming Checkpoint Failure Recovery | Streaming | Exactly-Once, Offset WAL | `09_streaming/05_checkpoints_and_fault_tolerance.ipynb` |
| **E25** | Window Function Dense Rank vs Row Number | SQL Analytics | Analytical Partitions | `03_dataframe_and_spark_sql/06_window_functions.ipynb` |

---

## Selected Experiment Protocols

### Experiment E01: Lazy Evaluation Latency Benchmark
- **Objective**: Prove that transformations execute zero computation regardless of complexity until an action is invoked.
- **Procedure**:
  ```python
  import time
  start = time.time()
  # Build a 10-step transformation pipeline
  df_trans = (
      spark.read.csv("/opt/spark-data/retail/orders.csv", header=True)
      .filter("status ='Completed'")
      .withColumn("tax", F.col("unit_price") * 0.14)
      .groupBy("product_id")
      .count()
)
  trans_time = time.time() - start

  action_start = time.time()
  count_val = df_trans.count()
  action_time = time.time() - action_start

  print(f"Transformations setup time: {trans_time:.4f}s")
  print(f"Action execution time:      {action_time:.4f}s")
  ```
- **Expected Observation**: `trans_time` $< 0.05$ seconds (merely building logical plan objects). `action_time` takes multiple seconds as it executes the entire distributed pipeline.
- **Lesson**: Code latency during transformation declaration is irrelevant; performance optimization must target the physical execution triggered by actions.

### Experiment E04: `repartition()` vs `coalesce()` Cost
- **Objective**: Measure the network shuffle difference between `repartition` and `coalesce`.
- **Procedure**:
  ```python
  df = spark.read.csv("/opt/spark-data/retail/orders.csv", header=True).filter("quantity > 2")
  # Test coalesce (no shuffle)
  df.coalesce(2).write.mode("overwrite").format("noop").save() # Check Spark UI Exchange count

  # Test repartition (full shuffle)
  df.repartition(2).write.mode("overwrite").format("noop").save() # Check Spark UI Exchange count
  ```
- **Expected Observation**: `repartition(2)` creates an extra stage with an `Exchange` node and non-zero `Shuffle Write` bytes. `coalesce(2)` runs in a single stage with zero shuffle bytes.
- **Lesson**: Use `coalesce` when reducing partitions to eliminate unneeded shuffles.

### Experiment E08: Broadcast Hash Join vs Sort-Merge Join
- **Objective**: Compare performance and execution plans between BHJ and SMJ on a small-large join.
- **Procedure**:
  ```python
  orders = spark.read.csv("/opt/spark-data/retail/orders.csv", header=True)
  products = spark.read.csv("/opt/spark-data/retail/products.csv", header=True)

  # 1. Force Sort-Merge Join
  smj = orders.join(products.hint("merge"),"product_id")
  smj.explain()

  # 2. Force Broadcast Hash Join
  bhj = orders.join(products.hint("broadcast"),"product_id")
  bhj.explain()
  ```
- **Expected Observation**: SMJ shows two `Exchange hashpartitioning` nodes and two `Sort` nodes. BHJ shows one `BroadcastExchange` node, zero shuffles on the large orders table, and executes significantly faster.
- **Lesson**: Broadcasting lookup tables eliminates network shuffles on massive fact tables.
