#!/usr/bin/env python3
"""
Generates GLOSSARY.md, COMMON_MISTAKES.md, EXPERIMENTS_GUIDE.md, and PRODUCTION_CHECKLIST.md
"""
import os

DOCS_DIR = "/home/elhussieny/spark-docker/docs"

def write_glossary():
    path = os.path.join(DOCS_DIR, "GLOSSARY.md")
    content = """#  Apache Spark Technical Glossary

> **Comprehensive definitions of essential distributed computing and Spark terminology.**  
> Format: **Term** · *Simple Explanation* · **Technical Deep Dive** · **Related Concepts**

---

### Action
- **Simple**: A command that tells Spark to actually execute computations and give you back an answer or save data to disk.
- **Technical**: A terminal operation in the RDD/DataFrame DAG (e.g., `count()`, `collect()`, `write()`) that triggers the evaluation of the accumulated logical plan, submitting a `Job` to the `DAGScheduler`.
- **Related**: *Transformation*, *Lazy Evaluation*, *Job*, *DAGScheduler*.

### Accumulator
- **Simple**: A distributed counter or sum variable that workers can add to, but only the driver can read.
- **Technical**: A shared variable updated via associative and commutative operations. Workers send delta increments back to the driver with block status updates; reliable updates are guaranteed only inside Actions.
- **Related**: *Broadcast Variable*, *Action*.

### Adaptive Query Execution (AQE)
- **Simple**: Spark's runtime brain that re-checks query plans after every stage and tunes partitions, joins, and skew on the fly.
- **Technical**: Dynamic query re-optimization introduced in Spark 3.0+. Between shuffle stages, AQE inspects materialized stage statistics to coalesce small partitions, switch Sort-Merge Joins to Broadcast Hash Joins, and split skewed partitions.
- **Related**: *Catalyst Optimizer*, *Sort-Merge Join*, *Data Skew*, *Shuffle*.

### BlockManager
- **Simple**: The local storage manager running on each worker and driver that keeps track of data chunks in RAM and disk.
- **Technical**: A key-value storage subsystem running in every JVM process (`BlockManagerMaster` on Driver, `BlockManagerSlave` on Executors) responsible for storing, caching, fetching, and replicating data blocks, shuffle map outputs, and broadcast blocks.
- **Related**: *Storage Memory*, *Broadcast Variable*, *Shuffle*.

### Broadcast Variable
- **Simple**: A large read-only lookup table or dataset copied to each worker machine once, instead of sending it with every individual task.
- **Technical**: An immutable shared variable distributed to executor BlockManagers via a BitTorrent-like peer-to-peer protocol. Eliminates task serialization duplication and saves network bandwidth.
- **Related**: *Broadcast Hash Join*, *Accumulator*, *BlockManager*.

### Broadcast Hash Join (BHJ)
- **Simple**: The fastest join in Spark: the smaller table is sent to all workers so each worker can join its slice locally with no network shuffle.
- **Technical**: A join strategy where the build-side table (default < 10 MB) is collected to the Driver and broadcast to all executors. Each executor builds an in-memory hash table and streams through its local partitions of the stream-side table in $O(M)$ time.
- **Related**: *Sort-Merge Join*, *Exchange*, *Shuffle*.

### Catalyst Optimizer
- **Simple**: The internal query planner and optimizer that turns your SQL or DataFrame code into fast executable bytecode.
- **Technical**: An extensible rule-based and cost-based query optimization framework written in Scala. It executes four primary phases: Analysis (catalog resolution), Logical Optimization (predicate pushdown, column pruning), Physical Planning (join selection), and Code Generation.
- **Related**: *Tungsten*, *Whole-Stage Code Generation*, *Logical Plan*, *Physical Plan*.

### Checkpoint
- **Simple**: Saving a DataFrame or RDD directly to durable storage (like HDFS or disk) and completely cutting its historical lineage.
- **Technical**: A fault-tolerance mechanism that writes materialized partitions to reliable storage and severs all references to upstream parent RDDs, resetting the lineage graph to prevent JVM `StackOverflowError` in iterative algorithms.
- **Related**: *Lineage*, *Persist*, *Fault Tolerance*.

### Coalesce
- **Simple**: A way to reduce the number of partitions without doing an expensive full network shuffle.
- **Technical**: A narrow transformation that merges adjacent partitions residing on the same worker node without redistributing records over the network. Can only decrease partition count.
- **Related**: *Repartition*, *Partition*, *Narrow Dependency*.

### Column Pruning
- **Simple**: Reading only the columns you actually need in your query, ignoring all other columns from the file.
- **Technical**: A Catalyst logical optimization rule that pushes projections down to columnar storage readers (Parquet, ORC), preventing unreferenced column blocks from being deserialized or transferred.
- **Related**: *Predicate Pushdown*, *Parquet*, *Catalyst Optimizer*.

### DAG (Directed Acyclic Graph)
- **Simple**: The step-by-step roadmap of transformations that Spark builds before executing any work.
- **Technical**: A finite graph with directed edges and no cycles where vertices represent RDDs/DataFrames and edges represent operations. Handled by the `DAGScheduler` to construct execution stages.
- **Related**: *DAGScheduler*, *Lazy Evaluation*, *Lineage*.

### DAGScheduler
- **Simple**: The component on the Driver that breaks down the execution plan into discrete stages.
- **Technical**: High-level scheduler that computes a DAG of stages for each job, determines stage boundaries based on shuffle dependencies, and submits `TaskSets` to the `TaskScheduler`.
- **Related**: *TaskScheduler*, *Stage*, *ShuffleDependency*.

### Data Skew
- **Simple**: When one or two workers get stuck with 90% of the data while the other workers finish quickly and sit idle.
- **Technical**: An asymmetric distribution of records across partitions caused by low-cardinality keys, null values, or uneven hash distributions, causing straggler tasks and memory spills.
- **Related**: *AQE*, *Salting*, *Straggler*.

### Driver
- **Simple**: The master brain of your Spark application that coordinates the work and runs your Python/Jupyter script.
- **Technical**: The JVM process hosting the `SparkSession`, `SparkContext`, `DAGScheduler`, `TaskScheduler`, `BlockManagerMaster`, and Web UI (Port 4040).
- **Related**: *Executor*, *Cluster Manager*, *Worker*.

### Executor
- **Simple**: A worker process that actually executes tasks and stores cached data.
- **Technical**: A dedicated JVM process (`CoarseGrainedExecutorBackend`) launched by a Worker daemon for a specific application. It hosts a thread pool for executing tasks and maintains storage and execution memory pools.
- **Related**: *Worker*, *Driver*, *Task*.

### Lazy Evaluation
- **Simple**: Spark waiting until you explicitly ask for an output (Action) before doing any actual data reading or processing.
- **Technical**: An execution paradigm where transformations append logical operators to an unresolved/analyzed tree rather than materializing data immediately, enabling global query optimizations by Catalyst.
- **Related**: *Action*, *Transformation*, *DAG*.

### Lineage
- **Simple**: The family tree of an RDD that remembers where it came from and how to recreate it if lost.
- **Technical**: The deterministic acyclic graph of parent dependencies that enables Spark's fault tolerance without data replication.
- **Related**: *Fault Tolerance*, *Checkpoint*, *Narrow Dependency*.

### Narrow Dependency
- **Simple**: A step where each input data chunk only affects one output data chunk, requiring no network shuffling.
- **Technical**: A dependency where each partition of the parent RDD is used by at most one partition of the child RDD (e.g. `map`, `filter`). Pipelined in-memory.
- **Related**: *Wide Dependency*, *Stage*, *Shuffle*.

### Partition
- **Simple**: A slice or slice of a large dataset processed by one CPU core at a time.
- **Technical**: The fundamental unit of physical data distribution and parallelism in Spark. Each partition corresponds to exactly one task in a stage.
- **Related**: *Task*, *Repartition*, *Coalesce*.

### Predicate Pushdown
- **Simple**: Pushing your `filter()` conditions directly into the storage layer so Spark only reads rows that match.
- **Technical**: A Catalyst optimization rule that converts DataFrame filter expressions into storage-native filter predicates (e.g. Parquet row-group statistics) to skip non-matching data blocks at the disk level.
- **Related**: *Column Pruning*, *Catalyst Optimizer*, *Parquet*.

### Repartition
- **Simple**: Shuffling your entire dataset across the network to create an exact new number of evenly-sized partitions.
- **Technical**: A wide transformation that performs a full network shuffle using `RoundRobinPartitioning` or `HashPartitioning` to increase or decrease partition counts.
- **Related**: *Coalesce*, *Shuffle*, *Wide Dependency*.

### Shuffle
- **Simple**: The process of redistributing data across all machines so rows with the same key end up in the same place.
- **Technical**: The all-to-all data exchange mechanism across executor nodes. Map tasks sort and write partitioned files to disk, and reduce tasks fetch remote blocks over Netty RPC.
- **Related**: *Sort-Merge Join*, *Exchange*, *Spill*.

### Sort-Merge Join (SMJ)
- **Simple**: The default way Spark joins two large tables: it shuffles them by key, sorts them, and merges them together.
- **Technical**: A distributed join algorithm where both datasets are hash-partitioned on the join key, sorted by key within each partition, and streamed concurrently through a merge-pointer loop.
- **Related**: *Broadcast Hash Join*, *Shuffle*, *Exchange*.

### Task
- **Simple**: The smallest piece of work sent to a single CPU core to process a single data partition.
- **Technical**: The atomic execution unit dispatched by the `TaskScheduler` to an executor thread. Runs the stage bytecode on a single input partition.
- **Related**: *Stage*, *Partition*, *Executor*.

### Tungsten
- **Simple**: Spark's low-level engine that manages raw memory and generates super-fast bytecode.
- **Technical**: An execution engine project optimizing CPU and memory efficiency via binary off-heap row representation (`UnsafeRow`), cache-aware algorithms, and whole-stage code generation.
- **Related**: *Whole-Stage Code Generation*, *Catalyst Optimizer*.

### Watermark
- **Simple**: A moving time threshold in streaming that tells Spark how late data can arrive before being discarded.
- **Technical**: In Structured Streaming, a state eviction threshold defined as $\text{MaxEventTime} - \text{AllowedDelay}$. State entries older than the watermark are dropped from the state store to bound memory usage.
- **Related**: *Structured Streaming*, *Late Data*.

### Wide Dependency
- **Simple**: An operation where output chunks depend on multiple input chunks from different machines, forcing a shuffle.
- **Technical**: A dependency where multiple child partitions depend on data from a single parent partition (`ShuffleDependency`), drawing stage boundaries.
- **Related**: *Narrow Dependency*, *Stage*, *Shuffle*.
"""
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content.strip() + '\n')
    print(f"Wrote {path}")

def write_common_mistakes():
    path = os.path.join(DOCS_DIR, "COMMON_MISTAKES.md")
    content = """#  35 Common Apache Spark Mistakes & Anti-Patterns

> **A diagnostic guide to common pitfalls, why they break, and production-tested solutions.**

---

##  Category 1: Beginner & API Mistakes

### 1. Calling `.collect()` on Production Datasets
- **The Mistake**: Running `df.collect()` to inspect results or iterate with Python `for` loops.
- **Why It's Wrong**: Brings all distributed rows across every executor into the Driver JVM heap, causing an instant `OutOfMemoryError`.
- **Correct Approach**: Use `df.take(10)`, `df.limit(10).show()`, or write output to partitioned files.

### 2. Confusing Transformations with Actions
- **The Mistake**: Writing multiple transformations (`df.filter()`, `df.withColumn()`) and wondering why no computation happened or time wasn't spent.
- **Why It's Wrong**: Transformations are lazy. Nothing runs until an Action (`count()`, `write()`, `collect()`) is called.
- **Correct Approach**: Understand lazy evaluation: build your transformation chain and trigger execution with one intentional terminal action.

### 3. Using Python `for` Loops Instead of DataFrame Operations
- **The Mistake**: Iterating over rows to compute values in Python.
- **Why It's Wrong**: Forces data serialization to the driver and executes single-threaded Python code, completely bypassing Spark's distributed engine.
- **Correct Approach**: Use vectorized column operations (`F.col()`, `F.when()`, Window functions).

### 4. Overusing Python UDFs Instead of Native Built-in Functions
- **The Mistake**: Writing custom Python functions (`@udf`) for string manipulation or math that already exists in `pyspark.sql.functions`.
- **Why It's Wrong**: Python UDFs force row-by-row socket serialization between JVM and Python worker processes, completely disabling Catalyst optimizations and Whole-Stage Codegen.
- **Correct Approach**: Always use built-in functions (`F.regexp_replace`, `F.datediff`, `F.coalesce`). If custom Python is necessary, use Arrow-vectorized Pandas UDFs (`@pandas_udf`).

### 5. Writing Output with `inferSchema=True` Everywhere
- **The Mistake**: Relying on schema inference in automated daily batch jobs.
- **Why It's Wrong**: Triggers an extra full scan of the data before processing and risks schema instability when dirty data arrives.
- **Correct Approach**: Define explicit schemas using `StructType` and `StructField`.

### 6. Calling `count()` Multiple Times for Debugging
- **The Mistake**: Sprinkling `df.count()` throughout a script to print progress.
- **Why It's Wrong**: Each `count()` triggers a brand-new distributed Spark Job that recomputes the entire upstream transformation DAG from scratch!
- **Correct Approach**: Use caching if intermediate counts are mandatory, or inspect counts in the Spark UI.

### 7. Overwriting Variables in Linear Transforms
- **The Mistake**: `df = df.filter(...)` followed by 50 lines of mutations without structured testing.
- **Why It's Wrong**: Hard to debug, inspect execution plans, or isolate broken stages.
- **Correct Approach**: Break pipelines into logical transformation blocks with clear intermediate names.

---

##  Category 2: Performance & Shuffle Anti-Patterns

### 8. The "Small Files Problem"
- **The Mistake**: Writing a 500 MB dataset with default 200 shuffle partitions, producing 200 files of 2.5 MB each.
- **Why It's Wrong**: Floods HDFS/S3 with tiny files, overwhelming storage metadata and slowing down subsequent reads.
- **Correct Approach**: Use `.coalesce(N)` before writing to ensure output files are ~128 MB to 512 MB each.

### 9. Using `groupByKey()` in RDDs
- **The Mistake**: `rdd.groupByKey()` to aggregate values.
- **Why It's Wrong**: Shuffles all raw records across the network without map-side combining, causing massive shuffle files and OOM.
- **Correct Approach**: Use `rdd.reduceByKey()` or `rdd.aggregateByKey()`, or switch to DataFrames.

### 10. Forgetting to Broadcast Small Tables in Joins
- **The Mistake**: Joining a 10 GB table with a 5 MB lookup table using default joins.
- **Why It's Wrong**: Spark may trigger an expensive Sort-Merge Join requiring a full network shuffle of the 10 GB dataset.
- **Correct Approach**: Use `broadcast(small_df)` to eliminate the shuffle entirely.

### 11. Over-Partitioning Small Datasets
- **The Mistake**: Setting `spark.sql.shuffle.partitions=2000` on a 50 MB dataset.
- **Why It's Wrong**: Task scheduling and RPC coordination take 95% of total runtime; tasks finish in 2 milliseconds.
- **Correct Approach**: Size partitions so each partition processes 100 MB to 200 MB of data.

### 12. Under-Partitioning Large Datasets
- **The Mistake**: Processing 500 GB of data with 10 partitions.
- **Why It's Wrong**: Tasks exceed executor memory, causing massive disk spilling and executor OOM crashes.
- **Correct Approach**: Calculate partitions: $500{,}000\text{ MB} / 128\text{ MB} \approx 3{,}900$ partitions.

### 13. Caching Single-Use DataFrames
- **The Mistake**: Calling `df.cache()` when the DataFrame is only written to disk once.
- **Why It's Wrong**: Adds block manager overhead and wastes memory with zero recomputation savings.
- **Correct Approach**: Only cache DataFrames that are consumed by 2 or more distinct actions.

### 14. Forgetting `.unpersist()`
- **The Mistake**: Caching DataFrames in a loop or long pipeline without unpersisting them.
- **Why It's Wrong**: Storage memory fills up, evicting other blocks and choking execution memory for shuffles.
- **Correct Approach**: Always call `df.unpersist()` immediately after the multi-action pipeline finishes.

### 15. Unintentional Cartesian Products (Cross Joins)
- **The Mistake**: Joining two tables without an explicit join key or using non-equi join conditions.
- **Why It's Wrong**: Generates $M \times N$ rows, exploding memory and causing cluster freeze.
- **Correct Approach**: Always provide equi-join predicates (`on="id"`).

---

##  Category 3: Memory & Resource Misconceptions

### 16. Assuming "In-Memory" Means Everything Must Fit in RAM
- **The Mistake**: Believing Spark will crash if your dataset is 10 TB and your cluster RAM is 1 TB.
- **Why It's Wrong**: Spark processes data in **partitions** as streams. Partitions flow through memory and spill to disk if necessary.
- **Correct Approach**: Design pipelines with appropriate partition counts and understand spill mechanics.

### 17. Setting Single-Core Executors
- **The Mistake**: Configuring `spark.executor.cores=1` and launching 100 executors.
- **Why It's Wrong**: Wastes memory on JVM overhead and loses the ability to share broadcast variables and cached blocks across cores.
- **Correct Approach**: Set `spark.executor.cores` between 3 and 5.

### 18. Setting Massive JVM Heaps (>32 GB)
- **The Mistake**: Creating 1 executor with 120 GB of RAM.
- **Why It's Wrong**: JVM Garbage Collection pauses can freeze the executor for minutes, causing heartbeat loss and node timeouts.
- **Correct Approach**: Use multiple executors of 20 GB to 30 GB each.

### 19. Misunderstanding Driver vs Worker Memory
- **The Mistake**: Allocating 64 GB to the Driver when the Driver only coordinates tasks.
- **Why It's Wrong**: Starves executors of physical machine memory.
- **Correct Approach**: Allocate 2 GB to 4 GB for the Driver unless doing heavy local broadcast generation or model training.

### 20. Ignoring Docker Memory Limits
- **The Mistake**: Configuring `spark.executor.memory=4g` inside a container limited to 4 GB.
- **Why It's Wrong**: Off-heap memory, Metaspace, and OS memory push total usage over 4 GB; Linux kernel kills container with `Exit Code 137`.
- **Correct Approach**: Container limit $\ge \text{Heap} \times 1.25$.

---

##  Category 4: Streaming & Production Pitfalls

### 21. Streaming Without Watermarks on Aggregations
- **The Mistake**: Grouping by event time in Structured Streaming without defining `withWatermark()`.
- **Why It's Wrong**: Spark maintains historical aggregate states indefinitely, eventually crashing with an OOM.
- **Correct Approach**: Always provide a watermark: `df.withWatermark("timestamp", "10 minutes")`.

### 22. Changing Streaming Query Schema Without Changing Checkpoint Directory
- **The Mistake**: Modifying streaming query logic or schema while pointing to an existing checkpoint directory.
- **Why It's Wrong**: Checkpoint metadata contains serialized state schemas; schema mismatch crashes the job on startup.
- **Correct Approach**: Create a new checkpoint directory or handle schema evolution explicitly.

### 23. Storing Checkpoints on Ephemeral Disks
- **The Mistake**: Pointing checkpoint location to `/tmp` in container storage.
- **Why It's Wrong**: If the container restarts, all checkpoint state is wiped out, destroying exactly-once guarantees.
- **Correct Approach**: Use durable persistent storage (HDFS, S3, or mounted persistent volumes).

### 24. Reading Kafka from `earliest` in Production Without Checkpoints
- **The Mistake**: Setting `startingOffsets="earliest"` without a checkpoint directory.
- **Why It's Wrong**: Every job restart will reprocess the entire Kafka topic from the beginning.
- **Correct Approach**: Always specify a durable `checkpointLocation`.

---

##  Category 5: Architecture & Diagnostics Pitfalls

### 25. Ignoring the Spark UI
- **The Mistake**: Trying to debug slow jobs by reading driver log print statements.
- **Why It's Wrong**: The Spark UI provides the exact timeline, shuffle read/write bytes, memory spill, and task distributions.
- **Correct Approach**: Open `localhost:4040` (or `localhost:18080`) and inspect the SQL and Stages tabs.

### 26. Not Checking for Data Skew
- **The Mistake**: Assuming all tasks take equal time.
- **Why It's Wrong**: 1 skewed partition can hold up an entire 1000-task stage for an hour.
- **Correct Approach**: Check Min/Median/Max task durations in the Stages tab.

### 27. Relying on Localhost Inside Docker Containers
- **The Mistake**: Setting `spark.driver.host=localhost` in a containerized driver.
- **Why It's Wrong**: Workers look for the driver on their own loopback address and fail to connect.
- **Correct Approach**: Use the container's Docker network DNS hostname (e.g. `spark.driver.host=jupyter`).

### 28. Using Non-Deterministic UDFs in Lineage
- **The Mistake**: Generating random IDs or current timestamps inside an RDD transformation.
- **Why It's Wrong**: If a task fails and recomputes, the recomputed partition will have different values than the original, corrupting joins.
- **Correct Approach**: Use deterministic functions or generate unique IDs using `monotonically_increasing_id()`.

### 29. Neglecting Predicate Pushdown Verification
- **The Mistake**: Assuming all filters are automatically pushed down to Parquet files.
- **Why It's Wrong**: Wrapping columns in custom functions (`filter(upper(col) == 'A')`) disables predicate pushdown.
- **Correct Approach**: Inspect `explain()` output to confirm `PushedFilters` are active.

### 30. Assuming Dynamic Allocation Replaces Resource Planning
- **The Mistake**: Enabling dynamic allocation and assuming Spark will never waste resources.
- **Why It's Wrong**: Dynamic allocation still requires upper limits (`maxExecutors`) and idle timeouts to avoid monopolizing shared clusters.
- **Correct Approach**: Set explicit dynamic allocation bounds (`minExecutors`, `maxExecutors`).
"""
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content.strip() + '\n')
    print(f"Wrote {path}")

def write_experiments_guide():
    path = os.path.join(DOCS_DIR, "EXPERIMENTS_GUIDE.md")
    content = """#  Catalog of Apache Spark Empirical Experiments

> **A structured laboratory guide of 30+ hands-on experiments to observe Spark internals in action.**  
> Every experiment includes: Objective · Hypothesis · Execution Steps · Expected Observation · Engineering Takeaway.

---

##  Experiment Catalog Overview

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

##  Selected Experiment Protocols

### Experiment E01: Lazy Evaluation Latency Benchmark
- **Objective**: Prove that transformations execute zero computation regardless of complexity until an action is invoked.
- **Procedure**:
  ```python
  import time
  start = time.time()
  # Build a 10-step transformation pipeline
  df_trans = (
      spark.read.csv("/opt/spark-data/retail/orders.csv", header=True)
      .filter("status = 'Completed'")
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
  smj = orders.join(products.hint("merge"), "product_id")
  smj.explain()

  # 2. Force Broadcast Hash Join
  bhj = orders.join(products.hint("broadcast"), "product_id")
  bhj.explain()
  ```
- **Expected Observation**: SMJ shows two `Exchange hashpartitioning` nodes and two `Sort` nodes. BHJ shows one `BroadcastExchange` node, zero shuffles on the large orders table, and executes significantly faster.
- **Lesson**: Broadcasting lookup tables eliminates network shuffles on massive fact tables.
"""
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content.strip() + '\n')
    print(f"Wrote {path}")

def write_production_checklist():
    path = os.path.join(DOCS_DIR, "PRODUCTION_CHECKLIST.md")
    content = """#  Apache Spark Production Engineering Readiness Checklist

> **A comprehensive operational checklist for deploying and running Spark workloads in production.**

---

##  1. Cluster & Resource Allocation
- [ ] **Executor Sizing**: Configured with 3 to 5 cores per executor (`spark.executor.cores=4`).
- [ ] **Executor Heap Sizing**: Executor memory set between 16 GB and 30 GB to prevent severe GC pauses (`spark.executor.memory=24g`).
- [ ] **Off-Heap Overhead Buffer**: Allocated at least 15% to 20% memory overhead for PySpark workers and Netty buffers (`spark.executor.memoryOverhead=4g`).
- [ ] **Driver Sizing**: Allocated sufficient driver memory (2 GB to 8 GB) with `spark.driver.maxResultSize` set to prevent driver OOM.
- [ ] **Garbage Collector**: Enabled G1GC with tuning flags (`-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35`).
- [ ] **Dynamic Allocation**: Configured `spark.dynamicAllocation.enabled=true` with safe `minExecutors` and `maxExecutors`.

---

##  2. Storage & Data Lake Architecture
- [ ] **Columnar File Format**: Production data stored in Parquet or Delta Lake format, compressed with Snappy or ZSTD.
- [ ] **Explicit Schemas**: All batch ingestion jobs use explicit `StructType` schemas; `inferSchema=True` is disabled.
- [ ] **Target File Size**: Output files sized between 128 MB and 512 MB, avoiding the small files problem.
- [ ] **Partition Sizing**: Data lake directory partitioning restricted to low-to-medium cardinality columns (e.g. `year`, `month`, `region`). Avoided partitioning on high-cardinality keys like `timestamp` or `customer_id`.
- [ ] **Partition Pruning Verification**: Validated via `df.explain()` that queries prune partition directories when filtering by partition columns.

---

##  3. Shuffles & Query Optimization
- [ ] **Shuffle Partitions Sizing**: Tuned `spark.sql.shuffle.partitions` to target 100 MB to 200 MB per shuffle partition.
- [ ] **Adaptive Query Execution**: Ensured `spark.sql.adaptive.enabled=true` and `spark.sql.adaptive.coalescePartitions.enabled=true`.
- [ ] **Broadcast Threshold**: Verified small dimension tables are broadcast using `broadcast()` hints or `spark.sql.autoBroadcastJoinThreshold`.
- [ ] **Data Skew Handled**: Addressed join key hotspots using Salting or AQE skew join (`spark.sql.adaptive.skewJoin.enabled=true`).
- [ ] **Serialization**: Enabled Kryo serializer (`spark.serializer=org.apache.spark.serializer.KryoSerializer`).

---

##  4. Memory & Caching Management
- [ ] **Selective Caching**: Only cached DataFrames consumed by 2 or more distinct actions.
- [ ] **Immediate Unpersisting**: Paired every `.persist()` / `.cache()` call with an explicit `.unpersist()` when the branching pipeline concludes.
- [ ] **Storage Level Selection**: Used `MEMORY_AND_DISK_SER` for large cached collections to minimize RAM footprint.
- [ ] **Zero Memory Spill**: Verified in Spark UI Stages tab that `Spill (Memory)` and `Spill (Disk)` are zero or minimal.

---

##  5. Streaming & Reliability
- [ ] **Durable Checkpoints**: Checkpoint directories configured on resilient persistent storage (`checkpointLocation`).
- [ ] **Watermarking Defined**: Every streaming stateful aggregation has an explicit `withWatermark()` to bound state store size.
- [ ] **Graceful Shutdown**: Enabled graceful shutdown configuration (`spark.streaming.stopGracefullyOnShutdown=true`).
- [ ] **Retry Configuration**: Verified `spark.task.maxFailures` is set to 4 (default) to tolerate transient node hiccups.

---

##  6. Observability & Monitoring
- [ ] **Event Logging Active**: Configured `spark.eventLog.enabled=true` and pointing to shared directory.
- [ ] **History Server Verified**: Confirmed completed job event logs render correctly in Spark History Server (`localhost:18080`).
- [ ] **Metrics Export**: Integrated Prometheus / Graphite metrics sink for cluster monitoring if running at scale.
- [ ] **Log Level**: Set driver/executor log level to `WARN` in production to prevent spamming log disks.
"""
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content.strip() + '\n')
    print(f"Wrote {path}")

if __name__ == '__main__':
    write_glossary()
    write_common_mistakes()
    write_experiments_guide()
    write_production_checklist()
