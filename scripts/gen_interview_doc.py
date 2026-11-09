#!/usr/bin/env python3
"""
Generates the comprehensive docs/INTERVIEW_PREPARATION.md
Contains 120+ technical questions across 17 categories + 16 scenarios.
"""
import sys

def build_interview_guide():
    path = '/home/elhussieny/spark-docker/docs/INTERVIEW_PREPARATION.md'
    with open(path, 'w', encoding='utf-8') as f:
        f.write("""#  Comprehensive Apache Spark Interview Preparation Guide

> **Audience**: Data Engineers, Senior Data Engineers, Big Data Architects  
> **Coverage**: 125+ In-Depth Questions & Real-World Scenarios with Concrete Code, Architectural Traceability, Misconceptions, and Interviewer Follow-Ups  
> **Environment**: Apache Spark 4.2.1 · PySpark · Standalone / YARN / K8s · Java 21 · Scala 2.13  

---

##  Table of Contents
1. [Core Scenarios & Architectural Deep Dives](#1-core-scenarios--architectural-deep-dives) (16 Mandatory Scenarios)
2. [Fundamentals & Ecosystem](#2-fundamentals--ecosystem) (10 Questions)
3. [Cluster Architecture & Resource Management](#3-cluster-architecture--resource-management) (12 Questions)
4. [RDD Internals & Low-Level API](#4-rdd-internals--low-level-api) (8 Questions)
5. [DataFrames & Spark SQL API](#5-dataframes--spark-sql-api) (10 Questions)
6. [Transformations, Actions & Lazy Evaluation](#6-transformations-actions--lazy-evaluation) (8 Questions)
7. [DAG, Jobs, Stages & Tasks Execution Flow](#7-dag-jobs-stages--tasks-execution-flow) (10 Questions)
8. [Narrow vs. Wide Dependencies](#8-narrow-vs-wide-dependencies) (6 Questions)
9. [Partitions & Sizing Strategies](#9-partitions--sizing-strategies) (8 Questions)
10. [Shuffle Internals & Spilling](#10-shuffle-internals--spilling) (10 Questions)
11. [Unified Memory Management & Garbage Collection](#11-unified-memory-management--garbage-collection) (10 Questions)
12. [Distributed Joins & Optimizations](#12-distributed-joins--optimizations) (8 Questions)
13. [Catalyst Optimizer & Adaptive Query Execution (AQE)](#13-catalyst-optimizer--adaptive-query-execution-aqe) (8 Questions)
14. [Fault Tolerance, Lineage & Checkpointing](#14-fault-tolerance-lineage--checkpointing) (6 Questions)
15. [Structured Streaming & State Management](#15-structured-streaming--state-management) (6 Questions)
16. [Performance Engineering & Data Skew](#16-performance-engineering--data-skew) (12 Questions)
17. [Spark UI, Event Logs & Diagnostic Forensics](#17-spark-ui-event-logs--diagnostic-forensics) (6 Questions)
18. [Docker & Containerized Infrastructure](#18-docker--containerized-infrastructure) (4 Questions)

---

## 1. Core Scenarios & Architectural Deep Dives

### Q1. Why is a Spark job slow even though it has many executors allocated?
- **Detailed Answer**:
  Allocating dozens or hundreds of executors does not guarantee linear scaling. Common root causes include:
  1. **Data Skew / Partition Imbalance**: If 95% of records hash to a single key, 1 task processes 95% of data while all other executors sit idle. The job duration is bounded by the slowest task (the straggler).
  2. **Insufficient Partitions**: If an application has 100 executor cores but only 4 input partitions, exactly 4 cores do work while 96 cores remain completely idle. Parallelism is bounded by the number of partitions ($\text{Active Tasks} = \min(\text{Cores}, \text{Partitions})$).
  3. **Too Many Partitions (Over-partitioning)**: Having 50,000 partitions for a 500 MB dataset causes massive overhead in task serialization, TaskScheduler coordination, Netty RPC message floods, and microscopic shuffle file creation.
  4. **Driver Bottleneck**: Heavy client operations such as calling `.collect()` on large datasets, iterating single rows on the driver, broadcast variable serialization contention, or slow driver garbage collection.
  5. **Network / Shuffle IO Bottlenecks**: Network saturation or slow remote disks during shuffle read/write phases.
  6. **Excessive Garbage Collection**: Executors spending >20% of their CPU cycles performing JVM Full GC pauses due to memory pressure or inefficient object creation in Python UDFs.
- **Concrete Example**:
  ```python
  # Problem: 100 executors available, but only 2 partitions exist!
  df = spark.read.csv("/opt/spark-data/large_dataset.csv") # only 2 split files
  print(df.rdd.getNumPartitions()) # Output: 2 -> only 2 cores active!

  # Solution: Repartition to utilize cluster capacity
  df = df.repartition(200) # Now 200 tasks run across 100 cores (2 waves)
  ```
- **Common Misconceptions**:
  Believing that doubling executors automatically halves runtime ("Amdahl's Law" applies: sequential portions and communication overhead bound the speedup).
- **Follow-up Questions**:
  - *Interviewer*: "How do you detect this in the Spark UI?"
  - *Candidate*: "Look at the Event Timeline in the Stages tab: if a few tasks take minutes while most finish in seconds, it's skew. If executor core utilization bars show massive white gaps, tasks are under-partitioned."

---

### Q2. Why does `groupBy()` cause a shuffle?
- **Detailed Answer**:
  In a distributed dataset, rows with the same key reside randomly across different initial partitions and worker nodes. To compute an aggregation like `COUNT()`, `SUM()`, or `MAX()` for a specific key, **all records sharing that key must be co-located into the exact same physical partition** on a single executor.
  Spark computes a hash of the grouping key ($\text{hash}(key) \pmod{\text{numPartitions}}$) to assign each row to a target reducer partition. Moving these records across the network between different executors is, by definition, a **Shuffle** (`ShuffleDependency`).
- **Concrete Example**:
  ```python
  # Initial state: Partition 0 has ('A', 1), ('B', 2); Partition 1 has ('A', 3), ('B', 4)
  # Neither partition can calculate the final sum of 'A' independently!
  df.groupBy("key").sum("val").explain()
  # Physical Plan reveals:
  # +- HashAggregate(keys=[key], functions=[sum(val)])
  #    +- Exchange hashpartitioning(key, 8)  <-- THE SHUFFLE
  #       +- HashAggregate(keys=[key], functions=[partial_sum(val)])
  ```
- **Common Misconceptions**:
  Thinking that Spark transmits raw input rows across the network during `groupBy().sum()`. In reality, Spark performs **map-side partial aggregation** (`partial_sum`) first, sending only pre-aggregated values across the network.
- **Follow-up Questions**:
  - *Interviewer*: "What is the difference between RDD `groupByKey()` and `reduceByKey()`?"
  - *Candidate*: "`groupByKey()` shuffles all raw values across the network without map-side combining, risking severe OOM. `reduceByKey()` applies map-side merge before shuffling, drastically reducing network I/O."

---

### Q3. What happens when an executor fails?
- **Detailed Answer**:
  When an executor process crashes (due to JVM OOM, hardware failure, or container preemption):
  1. **Heartbeat Loss**: The Driver detects the missing executor because the executor stops sending periodic heartbeats (default 10s interval, `spark.network.timeout` default 120s).
  2. **Cluster Manager Notification**: In Standalone mode, the Worker notifies the Master; in YARN/K8s, NodeManager/kubelet detects container exit. The Driver requests a replacement executor from the Cluster Manager.
  3. **Task Rescheduling**: Any tasks that were actively running on the failed executor are re-queued by the `TaskScheduler` and assigned to remaining healthy executors.
  4. **Shuffle Data Invalidation & Recomputation**: If the failed executor held shuffle map files that are needed by downstream stages and an External Shuffle Service was NOT enabled, those shuffle files are permanently lost. The `DAGScheduler` invalidates the upstream `ShuffleMapStage` and recomputes the lost map tasks using RDD lineage.
  5. **Cached Data Invalidation**: Any in-memory cached partitions on the lost executor are marked missing in the Driver's `BlockManagerMaster`. When requested again, they are recomputed from lineage.
- **Common Misconceptions**:
  Assuming the entire job fails immediately when an executor dies. Spark is explicitly designed to tolerate executor losses seamlessly via lineage recomputation.
- **Follow-up Questions**:
  - *Interviewer*: "What happens if the Driver dies?"
  - *Candidate*: "In Client mode, the entire application dies immediately. In Cluster mode with a cluster manager like YARN or Standalone with `--supervise`, the cluster manager will restart the Driver from scratch."

---

### Q4. How would you investigate and resolve a data skew problem?
- **Detailed Answer**:
  **Investigation Steps**:
  1. Open the Spark UI `Stages` tab for the slow stage.
  2. Examine the **Task Metrics Summary Table**: Compare Min, 25th percentile, Median, 75th percentile, and Max task durations. If Median is 2 seconds and Max is 45 minutes, severe data skew is present.
  3. Check the **Shuffle Read Size / Records** columns: Identify whether the straggler task processed gigabytes while others processed megabytes.
  4. Identify the skewed join/aggregation key by profiling key distributions:
     ```python
     df.groupBy("skewed_key").count().orderBy(F.desc("count")).show(10)
     ```
  **Remediation Techniques**:
  1. **Broadcast Join**: If one of the tables is small (<100 MB), use `broadcast(small_df)`. This avoids the shuffle entirely, completely eliminating key-based skew.
  2. **Salting the Key**: For skewed large-large joins, append a random integer prefix (`0..K-1`) to the skewed key on the left table, explode the right table by $K$ with matching salts, and join on `(salted_key)`.
  3. **AQE Skew Join**: Ensure `spark.sql.adaptive.enabled=true` and `spark.sql.adaptive.skewJoin.enabled=true`. AQE automatically detects skewed partitions at runtime and splits them into smaller sub-partitions.
  4. **Filter / Isolate Skewed Keys**: Separate out nulls or placeholder IDs (e.g. `customer_id = -1`), process them in a separate path, and union results.
- **Concrete Salting Example**:
  ```python
  import pyspark.sql.functions as F

  SALT_BUCKETS = 8
  # 1. Salt left table (skewed)
  left_salted = left_df.withColumn("salt", F.concat(F.col("skew_key"), F.lit("_"), F.floor(F.rand() * SALT_BUCKETS)))

  # 2. Replicate right table by SALT_BUCKETS
  right_replicated = right_df.withColumn("salt_array", F.array([F.lit(i) for i in range(SALT_BUCKETS)])) \\
                             .withColumn("salt_num", F.explode("salt_array")) \\
                             .withColumn("salt", F.concat(F.col("skew_key"), F.lit("_"), F.col("salt_num")))

  # 3. Join on the salted key (distributes the hotspot across 8 independent partitions)
  joined = left_salted.join(right_replicated, on="salt").drop("salt", "salt_array", "salt_num")
  ```
- **Follow-up Questions**:
  - *Interviewer*: "Why does Adaptive Query Execution (AQE) skew join not work for all queries?"
  - *Candidate*: "AQE skew join applies to Sort-Merge Joins where shuffle metrics have been gathered. It does not fix skew in complex non-equi joins or custom RDD operations."

---

### Q5. Why is `.collect()` dangerous on large datasets?
- **Detailed Answer**:
  `.collect()` is an Action that retrieves **all distributed partitions from every executor across the cluster and sends them over the network into the Driver's single JVM heap memory**.
  **Dangers**:
  1. **Driver OutOfMemoryError**: If the distributed DataFrame is 50 GB and Driver heap is 2 GB, the Driver crashes immediately with `java.lang.OutOfMemoryError: Java heap space`.
  2. **Network Saturation**: Sending gigabytes of uncompressed row objects to the Driver saturates the Driver's network interface card (NIC), starving control RPCs and causing heartbeat timeouts.
  3. **JVM GC Freezes**: The Driver JVM enters catastrophic Stop-The-World GC loops attempting to allocate millions of deserialized Java/Python row objects.
- **Best Practices**:
  Use `.take(N)`, `.limit(N).collect()`, or write to distributed storage (`.write.parquet(...)`).
- **Follow-up Questions**:
  - *Interviewer*: "What configuration limits driver memory blowup during collect?"
  - *Candidate*: "`spark.driver.maxResultSize` (e.g. `2g` or `4g`), which aborts the job if the cumulative collected result exceeds the threshold."

---

### Q6. When would you use `repartition()` instead of `coalesce()`?
- **Detailed Answer**:
  - `repartition(N)`:
    - Performs a **full network shuffle** across all executors.
    - Guarantees uniform partition sizes using RoundRobin or Hash partitioning.
    - Can both **increase** and **decrease** the partition count.
    - **Use Cases**:
      1. When increasing parallelism (e.g. from 4 to 200 after reading a few large compressed files).
      2. When eliminating severe data skew by redistributing rows evenly.
      3. `repartition(N, 'col')`: When partitioning by key prior to repeated joins or bucketed writes.
  - `coalesce(N)`:
    - **Avoids a full shuffle** by merging adjacent partitions on the same worker node.
    - Can **only decrease** the partition count efficiently. If $N > \text{current partitions}$, it has no effect.
    - Can cause partition size skew if merged partitions have unequal sizes.
    - **Use Cases**:
      1. After a heavy `filter()` that removes 90% of data, reducing partition count without a shuffle.
      2. Right before writing output to disk to control target file count (e.g., `df.coalesce(1).write.csv(...)` for single-file delivery).
- **Follow-up Questions**:
  - *Interviewer*: "Can coalesce ever cause a shuffle?"
  - *Candidate*: "Yes, if you pass `shuffle=True`, `coalesce(N, shuffle=True)` behaves identically to `repartition(N)`."

---

### Q7. How do you decide the optimal number of partitions?
- **Detailed Answer**:
  The rule of thumb for partition sizing is:
  1. **Target Partition Size**: Aim for **100 MB to 200 MB** uncompressed in memory per partition.
  2. **Core Sizing Ratio**: Aim for **2x to 4x the total executor cores** in the cluster. For example, with 50 total executor cores, set `spark.sql.shuffle.partitions` to 100–200. This ensures all cores remain busy and allows faster tasks to finish while slower tasks execute in subsequent waves without idle cores.
  3. **Avoid Extremes**:
     - *Too Few Partitions* ($< \text{Cores}$): Underutilized cluster, memory spill to disk, risk of Executor OOM.
     - *Too Many Partitions* ($> 10,000$ for small data): Excessive task scheduling latency, TaskSet serialization overhead, thousands of microscopic files in HDFS/S3.
- **Follow-up Questions**:
  - *Interviewer*: "What controls initial partition count when reading from files?"
  - *Candidate*: "`spark.sql.files.maxPartitionBytes` (default 128 MB) and `spark.sql.files.openCostInBytes` (default 4 MB)."

---

### Q8. Why can caching make a Spark application slower?
- **Detailed Answer**:
  Caching is not free. Caching a DataFrame introduces distinct overheads:
  1. **Materialization & Serialization Cost**: Writing blocks to the BlockManager requires serializing rows into memory buffers (`StorageLevel.MEMORY_ONLY_SER` or off-heap).
  2. **Storage vs Execution Contention**: Cached blocks occupy the **Storage Memory Pool**. Under the Unified Memory Manager, while execution can evict storage blocks, storage blocks reduce the contiguous memory available for shuffles and hash aggregations, forcing shuffles to spill to disk earlier!
  3. **Single-Use Overhead**: If a cached DataFrame is only consumed by **one downstream action**, Spark performs the serialization and storage management with zero benefit.
  4. **Disk Spilling**: If cached data exceeds RAM (`MEMORY_AND_DISK`), Spark writes serialized partitions to local disks, adding costly disk I/O.
- **Follow-up Questions**:
  - *Interviewer*: "How do you properly clean up cache?"
  - *Candidate*: "Call `.unpersist()` as soon as the branching operations that need the cached data complete."

---

### Q9. What is the difference between executor memory and worker memory?
- **Detailed Answer**:
  - **Worker Memory (`SPARK_WORKER_MEMORY`)**: The total physical memory allocated to the **Worker Daemon** on that machine node. It represents the *resource quota pool* from which the master can carve out executor allocations.
  - **Executor Memory (`spark.executor.memory`)**: The JVM heap size assigned to a **single executor process** spawned by that worker.
  - **Relationship**:
    $$\text{Worker Memory} \ge \sum (\text{Executor Memory} + \text{Executor Memory Overhead})$$
  In our Docker Compose environment:
  - Each worker has `SPARK_WORKER_MEMORY = 4g`.
  - Spark applications default to `spark.executor.memory = 2g`.
  - Therefore, each worker can host at most one 2GB executor (plus overhead) or two 1.5GB executors.
- **Follow-up Questions**:
  - *Interviewer*: "What happens if executor memory + overhead exceeds worker memory?"
  - *Candidate*: "The Master will reject or indefinitely queue the executor launch request because no worker has sufficient free memory capacity."

---

### Q10. What happens between calling an Action and receiving its result?
- **Detailed Answer**:
  The step-by-step internal execution lifecycle:
  1. **Action Invocation**: User code calls an Action (e.g., `df.count()`).
  2. **Logical Plan Construction**: DataFrame operations append to the Logical Plan tree.
  3. **Catalyst Optimization**:
     - *Analysis*: Catalog resolves column and table names.
     - *Optimization*: Applies rules (predicate pushdown, column pruning).
     - *Physical Planning*: Generates physical operators and cost model selects the best physical plan.
  4. **Job Submission**: The Driver's `SparkContext` calls `runJob()` on the `DAGScheduler`.
  5. **Stage Decomposition**: `DAGScheduler` traverses the RDD graph backward from the final target, splitting at `ShuffleDependency` boundaries into a DAG of `ShuffleMapStage`s and a final `ResultStage`.
  6. **Task Creation**: For each stage, the `DAGScheduler` creates a `TaskSet` containing exactly one `Task` per partition (`ShuffleMapTask` or `ResultTask`).
  7. **Task Scheduling**: `TaskScheduler` submits tasks to executors based on data locality via `SchedulerBackend`.
  8. **Bytecode Execution**: Executors run tasks using thread pools, executing Whole-Stage Codegen generated Java bytecode, managing memory through Unified Memory Manager, and spilling to disk if required.
  9. **Shuffle Exchange**: Intermediate map outputs are written to disk; downstream tasks pull blocks via Netty RPC.
  10. **Result Aggregation**: Result partitions are serialized back to the Driver over RPC and aggregated into the return value.

---

### Q11. Why can a DataFrame query be faster than equivalent RDD operations?
- **Detailed Answer**:
  1. **Catalyst Optimization**: DataFrames are declarative. Spark understands the semantics of the schema and operations. Catalyst can reorder filters, push predicates directly to Parquet row groups, and prune unused columns. RDD operations are opaque Python/Java function pointers (`lambda x: ...`) where Spark cannot inspect or optimize the logic.
  2. **Tungsten Binary Memory Management**: DataFrames use Tungsten's `UnsafeRow` off-heap binary format. Data is stored in compact raw memory bytes without JVM object wrappers, avoiding Java object headers (16–24 bytes per object) and eliminating JVM Garbage Collection pressure.
  3. **Whole-Stage Code Generation (WSCG)**: Tungsten synthesizes the entire query plan into a single clean Java bytecode loop, eliminating virtual function dispatch and keeping data in CPU L1/L2 registers.
  4. **PySpark Serialization Overhead**: RDD transformations in PySpark require serializing data across a socket between the JVM and Python worker processes (`pickle`). DataFrames execute entirely inside the JVM via Tungsten; Python only receives the final result!

---

### Q12. How would you investigate an expensive join?
- **Detailed Answer**:
  1. Run `df.explain("formatted")` to identify the join strategy chosen by Spark (`BroadcastHashJoin`, `SortMergeJoin`, `ShuffledHashJoin`, or `BroadcastNestedLoopJoin`).
  2. Check for missing join keys or non-equi predicates (`df1.id > df2.id`) which force devastating $O(M \times N)$ `BroadcastNestedLoopJoinExec`.
  3. Inspect table sizes: Determine if one side is small enough to broadcast using `broadcast(df)` or by increasing `spark.sql.autoBroadcastJoinThreshold`.
  4. Check the Spark UI `SQL` and `Stages` tabs:
     - Check shuffle read/write bytes around the `Exchange` nodes.
     - Check if sort time is excessive in `SortMergeJoin`.
     - Check for data skew on join keys.
  5. Verify join key types match exactly: Joining a `StringType` key to an `IntegerType` key prevents broadcast/hash optimization and may force implicit casting or full scans.

---

### Q13. What is the difference between persistence and checkpointing?
- **Detailed Answer**:
  | Feature | `persist()` / `cache()` | `checkpoint()` |
  | :--- | :--- | :--- |
  | **Storage Location** | Executor JVM memory / local worker scratch disk. | Reliable, distributed, durable storage (HDFS, S3, or shared volume). |
  | **Lineage Impact** | **Preserves** full RDD lineage graph. | **Completely truncates** and severs the lineage graph. |
  | **Failure Recovery** | Recomputes lost partitions from original source via lineage. | Reads directly from checkpointed storage file; zero recomputation. |
  | **Execution Timing** | Lazy; materialized when the first Action runs. | Eager by default (triggers an internal Spark job to write to disk). |
  | **Primary Use Case** | Reuse in linear or branching ETL across 2+ actions. | Iterative algorithms (ML, graph algorithms) to prevent JVM `StackOverflowError` in lineage DAG. |

---

### Q14. What is the difference between the Spark UI and the History Server?
- **Detailed Answer**:
  - **Spark UI (Port 4040)**:
    - Embedded web server hosted directly by the **active Driver process**.
    - Only available **while the application is actively running**.
    - Pulls live, real-time metrics from the in-memory `LiveListenerBus`.
    - Disappears immediately when `spark.stop()` is called or the Driver process exits.
  - **Spark History Server (Port 18080)**:
    - Independent standalone service daemon running continuously.
    - Reconstructs and serves web UIs for **completed (and in-progress) applications**.
    - Reads persistent event log files written to `spark.eventLog.dir` (`file:///opt/spark-events` in our lab).
    - Allows post-mortem forensic analysis days or weeks after the job finished.

---

### Q15. What happens when a task fails?
- **Detailed Answer**:
  1. **Exception Raised**: The task throws an unhandled exception inside the Executor thread (e.g., Python `ZeroDivisionError`, `NullPointerException`, or executor heart-beat timeout).
  2. **Failure Report**: The executor notifies the Driver's `TaskScheduler` with a `TaskKilled` or `TaskFailedReason` message.
  3. **Retry Mechanism**:
     - `TaskScheduler` checks `spark.task.maxFailures` (default 4).
     - If failure count $< 4$, the task is re-queued. The scheduler often attempts to place the retry on a **different executor/worker** to avoid localized hardware/disk faults.
  4. **Stage Failure**: If any single task fails 4 times, the entire `Stage` is declared failed.
  5. **Stage Retry**: `DAGScheduler` may retry the stage if the failure was caused by a transient fetch failure (`FetchFailedException`).
  6. **Application Abort**: If stage retries are exhausted, the entire application aborts with an error.

---

### Q16. How do Docker resource limits interact with Spark resource configurations?
- **Detailed Answer**:
  Docker container limits set hard cgroups boundaries on the host, while Spark properties configure internal JVM heuristics:
  1. **Memory Limits**: If a Docker container is capped at 3 GB (`mem_limit: 3g`) and Spark is configured with `spark.executor.memory=2.5g` plus `spark.executor.memoryOverhead=512m` plus JVM Metaspace/OS overhead (~500m), the total process footprint exceeds 3.5 GB. The Linux kernel OOM killer immediately sends `SIGKILL` (`Exit Code 137`).
  2. **CPU Limits**: If Docker assigns 2 virtual cores to the container but Spark is configured with `spark.executor.cores=4`, Spark launches 4 concurrent task execution threads. The threads compete for 2 underlying CPU slots, causing intense thread context-switching overhead and CPU throttling.
  3. **Rule of Thumb**: Docker container limits must always provide a **15% to 25% safety buffer** above Spark's configured heap + off-heap overhead.

---

## 2. Fundamentals & Ecosystem (10 Questions)

### Q17. What is Apache Spark and why was it created?
- **Answer**: Apache Spark is a multi-language, open-source, distributed general-purpose cluster-computing framework. It was created at UC Berkeley AMPLab in 2009 to overcome the architectural bottlenecks of Hadoop MapReduce—specifically MapReduce's mandatory materialization of intermediate results to disk between successive map and reduce jobs, which made iterative data processing (ML, graph algorithms, interactive queries) prohibitively slow. Spark introduced in-memory computing abstractions (RDDs) and DAG pipelining, delivering up to 100x faster performance.
- **Misconception**: Thinking Spark completely replaces Hadoop. Spark replaces MapReduce, but routinely runs on Hadoop YARN and reads from HDFS.

### Q18. Compare Apache Spark with Apache Flink.
- **Answer**:
  - **Spark**: Historically a batch-first engine. Streaming is implemented primarily as micro-batches (Structured Streaming), though continuous processing mode exists for sub-millisecond needs. Best-in-class batch ecosystem, SQL optimizer (Catalyst), and unified analytics.
  - **Flink**: True stream-first engine. Processes events record-by-record with native event-time processing and low-latency stateful streaming. Treats batch as a special case of streaming with bounded streams.
- **Follow-up**: "When would you pick Flink over Spark?" -> "For ultra-low-latency (<10ms) stateful stream processing with complex event processing (CEP)."

### Q19. What are the key components of the unified Spark ecosystem?
- **Answer**:
  1. **Spark Core**: Base engine handling memory management, fault recovery, scheduling, and RDD primitives.
  2. **Spark SQL**: Structured data processing with DataFrames, Datasets, and Catalyst optimization.
  3. **Structured Streaming**: Stream processing built on Spark SQL engine.
  4. **MLlib**: Scalable distributed machine learning library.
  5. **GraphX**: Graph computation framework.
  6. **Spark Connect**: Decoupled client-server architecture introduced in Spark 3.4+.

### Q20. What is an RDD vs a DataFrame vs a Dataset?
- **Answer**:
  - **RDD (Resilient Distributed Dataset)**: Low-level immutable collection of JVM objects partitioned across nodes. Type-safe in Scala/Java, compile-time checked, but opaque to Catalyst optimization.
  - **DataFrame**: Untyped Dataset (`Dataset[Row]`) conceptually equivalent to a distributed table with named columns and schemas. Optimized by Catalyst and Tungsten.
  - **Dataset**: Strongly-typed object-oriented interface available in Scala/Java combining type-safety of RDDs with Catalyst optimization. (Not available in Python due to dynamic typing).

### Q21. When should you use the RDD API instead of DataFrames?
- **Answer**:
  Use RDDs only when:
  1. You need fine-grained low-level control over physical data placement, custom partitioners, or memory buffers.
  2. You are processing unstructured, binary, or non-tabular data that cannot naturally fit a relational schema.
  3. You are maintaining legacy Spark 1.x codebases.
  In 98% of modern Data Engineering tasks, DataFrames are strictly superior.

### Q22. What is Spark Connect?
- **Answer**: Introduced in Spark 3.4+, Spark Connect decouples the client from the Spark driver using a lightweight gRPC API. The client generates an unresolved logical plan and sends it over gRPC to a remote Spark Connect Server running on the cluster. This allows running PySpark from any IDE or serverless environment without requiring a local JVM installation.

### Q23. What is the role of Py4J in PySpark?
- **Answer**: Py4J is the inter-process communication bridge that enables Python programs running in a Python interpreter to dynamically access Java/JVM objects in the Spark Driver process. When you call `spark.read.csv()`, Py4J translates the Python method call into Java reflection calls on the underlying JVM `SparkSession`.

### Q24. How does Apache Arrow optimize PySpark operations?
- **Answer**: Apache Arrow provides a standardized, language-independent columnar in-memory format. In PySpark Pandas UDFs and `toPandas()`, Arrow eliminates expensive row-by-row serialization and deserialization between the JVM and Python worker by sharing raw memory buffers zero-copy.

### Q25. What is the default port for Spark Standalone Master Web UI?
- **Answer**: Port 8080. (In our Docker Compose setup, mapped to `http://localhost:8080`).

### Q26. What language is Apache Spark written in?
- **Answer**: Scala (compiled to Java bytecode running on the JVM).

---

## 3. Cluster Architecture & Resource Management (12 Questions)

### Q27. Explain the role of the Standalone Master.
- **Answer**: The Standalone Master is the central manager daemon in a Standalone cluster. It tracks active workers, registers submitted applications, allocates executor cores and memory slots based on resource availability, and monitors worker heartbeats. It does not execute tasks or process data.

### Q28. What is the difference between static and dynamic resource allocation?
- **Answer**:
  - **Static Allocation**: Fixed number of executors (`spark.executor.instances`) allocated for the entire lifetime of the application.
  - **Dynamic Allocation** (`spark.dynamicAllocation.enabled=true`): Spark dynamically requests executors from the cluster manager when tasks queue up and releases idle executors after an inactivity timeout (`spark.dynamicAllocation.executorIdleTimeout`).

### Q29. How does Spark determine data locality?
- **Answer**: Spark inspects where data blocks reside (HDFS block locations, cached partitions, or in-memory tables) and assigns tasks using 5 locality levels in preferential order:
  1. `PROCESS_LOCAL`: Task runs in the same executor JVM where data is cached in memory.
  2. `NODE_LOCAL`: Task runs on the same physical node where data resides on local disk/HDFS.
  3. `NO_PREF`: Data has no physical locality preference.
  4. `RACK_LOCAL`: Task runs on a node in the same server rack.
  5. `ANY`: Task runs on any available node in the cluster.

### Q30. What is speculative execution?
- **Answer**: Speculative execution (`spark.speculation=true`) is a mechanism to mitigate straggler tasks caused by faulty hardware or degraded workers. If a task runs significantly slower than the median task in the same stage, Spark launches an identical duplicate copy of the task on another worker. Whichever task finishes first commits its output; the other is killed.

### Q31. What is the Master RPC URL syntax for Standalone clusters?
- **Answer**: `spark://<master-host>:<master-port>`, e.g., `spark://spark-master:7077`.

### Q32. Explain the difference between `spark.executor.cores` and physical CPU cores.
- **Answer**: `spark.executor.cores` defines the number of concurrent task execution slots (threads) inside that executor JVM. If configured with 4 cores, the executor will run up to 4 tasks simultaneously. Physical cores represent the actual CPU hardware capabilities of the host machine.

### Q33. Why is it recommended to set `spark.executor.cores` between 3 and 5 in production?
- **Answer**:
  - Tiny executors (1 core): Underutilizes JVM shared memory and broadcast variables; requires more total JVMs, increasing JVM overhead.
  - Fat executors (>6 cores): Leads to severe JVM Garbage Collection pauses (Stop-The-World pauses can exceed 30 seconds on large heaps), and saturates HDFS/disk I/O throughput.
  - Sizing at 4 or 5 cores balances GC efficiency, HDFS write throughput, and shared broadcast caching.

### Q34. What is the Spark BlockManager?
- **Answer**: The `BlockManager` is a key-value store running inside the Driver and every Executor JVM. It manages the storage, replication, and retrieval of physical data blocks (cached partitions, broadcast variables, and shuffle map outputs) both in memory and on local disk.

### Q35. What is the MapOutputTracker?
- **Answer**: A component running on the Driver (`MapOutputTrackerMaster`) and Executors (`MapOutputTrackerWorker`). It maintains metadata regarding the physical network locations of all shuffle map output blocks so that downstream reducer tasks know which executors to fetch shuffle blocks from.

### Q36. What is the purpose of the Spark History Server?
- **Answer**: It reads persisted event log files from distributed or local storage and renders the complete web UI for completed applications, enabling retrospective debugging, monitoring, and audit analysis.

### Q37. What happens if the Master node fails in Standalone mode?
- **Answer**: Existing running applications continue executing without interruption because the Driver communicates directly with Executors! However, no new applications can be submitted and no new executors can be launched until the Master is restored. Master High Availability (HA) can be configured with Apache ZooKeeper.

### Q38. How does client mode differ from cluster mode regarding driver failure?
- **Answer**: In client mode, if the client terminal closes or loses network connectivity, the driver dies and the application aborts. In cluster mode, the driver runs isolated on a worker node and continues uninterrupted even if the submitting client disconnects.

---

## 4. RDD Internals & Low-Level API (8 Questions)

### Q39. List the five fundamental properties of an RDD.
- **Answer**:
  1. A list of **Partitions** (atomic splits of the dataset).
  2. A **Compute function** producing an Iterator over elements given a partition.
  3. A list of **Dependencies** on parent RDDs (Lineage).
  4. (Optional) A **Partitioner** for key-value PairRDDs (e.g. HashPartitioner).
  5. (Optional) A list of **Preferred Locations** for data locality (e.g. HDFS block host IP addresses).

### Q40. Why are RDDs immutable?
- **Answer**: Immutability guarantees fault tolerance without distributed locking. If a partition computation fails or a worker crashes, the lost partition can be deterministically recomputed from its immutable parent lineage without side effects or concurrency race conditions.

### Q41. What is the difference between `map()` and `flatMap()`?
- **Answer**:
  - `map(func)`: 1-to-1 transformation. Applies function to each element and returns an RDD of the exact same size ($N$ items $\to N$ items).
  - `flatMap(func)`: 1-to-0..N transformation. Maps each element to an iterator/sequence and flattens the resulting iterators into a single RDD.

### Q42. What is `glom()` and when is it useful?
- **Answer**: `rdd.glom()` transforms an `RDD[T]` into an `RDD[Array[T]]`, coalescing all elements of each partition into a single array per partition. It is invaluable for debugging data skew and inspecting partition boundaries.

### Q43. What is an Accumulator and what are its caveats?
- **Answer**: An Accumulator is a shared write-only distributed variable aggregated across workers via associative and commutative operations (e.g. counters, metrics). Caveat: Inside transformations (like `map()`), if a task is re-executed due to failure, the accumulator may be incremented multiple times. Safe accurate updates only occur inside Actions (`foreach()`).

### Q44. How does `reduceByKey()` differ from `aggregateByKey()`?
- **Answer**: `reduceByKey()` requires input and output types to be identical (`(V, V) => V`). `aggregateByKey()` allows the aggregated result type $U$ to differ from the input value type $V$, requiring a zero value, a sequence combiner `(U, V) => U`, and a partition merge combiner `(U, U) => U`.

### Q45. What is the difference between `count()` on an RDD and `count()` on a DataFrame?
- **Answer**: RDD `count()` sends a task to every partition, counts elements row-by-row via iteration, and sums partition counts at the Driver. DataFrame `count()` is analyzed by Catalyst: if reading Parquet or an optimized source, it can read metadata statistics directly or optimize into a vectorized aggregation without row deserialization.

### Q46. What is `rdd.toDebugString()`?
- **Answer**: A method that prints the complete recursive dependency graph and lineage of an RDD, showing stage boundaries, shuffle dependencies, and partition counts.

---

## 5. DataFrames & Spark SQL API (10 Questions)

### Q47. What is a Schema in Spark SQL?
- **Answer**: A `StructType` containing an array of `StructField`s, each defining the column name, `DataType` (e.g. `StringType`, `IntegerType`), and a boolean flag indicating whether the column allows `null` values.

### Q48. Why is `inferSchema=True` discouraged in production batch ETL?
- **Answer**: `inferSchema=True` forces Spark to trigger an entire extra full scan of the dataset upfront before processing begins to guess data types. Furthermore, type inference can be brittle if a column has inconsistent formats across files (e.g. strings mixed into an integer column).

### Q49. What is the difference between `select()` and `selectExpr()`?
- **Answer**: `select()` accepts Column objects (`col("units") * col("price")`). `selectExpr()` accepts raw SQL string expressions directly (`selectExpr("units * price as total", "upper(region)")`), offering concise SQL expression syntax.

### Q50. Explain the syntax and behavior of `when().otherwise()`.
- **Answer**: It is the programmatic equivalent of SQL `CASE WHEN ... THEN ... ELSE ... END`:
  ```python
  from pyspark.sql import functions as F
  df.withColumn("category", F.when(F.col("units") > 10, "Bulk").otherwise("Single"))
  ```

### Q51. How do Window Functions work in Spark?
- **Answer**: Window functions compute values over a defined slice or frame of rows related to the current row without collapsing rows into a single aggregation.
  Components:
  1. `partitionBy()`: Segregates data into logical windows.
  2. `orderBy()`: Defines the sorting sequence within the window.
  3. `rowsBetween()` / `rangeBetween()`: Defines the physical/logical boundary of the frame (e.g. rolling 3-day average).

### Q52. Differentiate between `row_number()`, `rank()`, and `dense_rank()`.
- **Answer**:
  Given tied values `[100, 100, 80]`:
  - `row_number()`: Sequential non-repeating integers $\to [1, 2, 3]$.
  - `rank()`: Identical rank for ties, skips subsequent numbers $\to [1, 1, 3]$.
  - `dense_rank()`: Identical rank for ties, does not skip numbers $\to [1, 1, 2]$.

### Q53. What is a Pandas UDF (Vectorized UDF)?
- **Answer**: A user-defined function implemented with Python type hints and Apache Arrow. Instead of passing rows individually between JVM and Python workers, Spark transfers entire Apache Arrow batches directly into Pandas Series or DataFrames, achieving 10x–100x speedup over standard Python UDFs.

### Q54. What happens when you register a Temporary View (`createOrReplaceTempView`)?
- **Answer**: Spark registers the DataFrame's logical plan in the internal session catalog. It creates no physical data copies on disk or in memory; it simply assigns an alias enabling SQL queries via `spark.sql("SELECT * FROM view_name")`.

### Q55. How does Spark handle `null` values during aggregations?
- **Answer**: Standard aggregate functions (`SUM`, `AVG`, `COUNT(col)`) ignore `null` values. However, `COUNT(*)` counts all rows including those with nulls.

### Q56. What is the null-safe equality operator in Spark?
- **Answer**: `eqNullSafe()` in DataFrame API or `<=>` in SQL. Returns `true` if both values are `null`, `false` if one is `null` and the other is not, and standard equality otherwise.

---

## 6. Transformations, Actions & Lazy Evaluation (8 Questions)

### Q57. What is the fundamental difference between a Transformation and an Action?
- **Answer**:
  - **Transformation**: Produces a new RDD/DataFrame from an existing one (e.g. `filter`, `select`, `join`). It is **lazy** and executes zero computation when invoked.
  - **Action**: Triggers physical execution of the accumulated DAG of transformations and either returns a value to the Driver or writes data to an external storage system (e.g. `count`, `collect`, `save`).

### Q58. List 5 common Transformations and 5 common Actions.
- **Answer**:
  - Transformations: `filter()`, `select()`, `groupBy()`, `join()`, `distinct()`.
  - Actions: `count()`, `collect()`, `take()`, `first()`, `write()`.

### Q59. Why did Spark adopt Lazy Evaluation?
- **Answer**: Lazy evaluation allows Spark to view queries globally. Instead of executing steps imperatively one by one, Spark constructs a complete logical plan, which Catalyst optimizes by combining operations, pushing filters to storage, and pruning unused columns before generating physical bytecode.

### Q60. Is `dropDuplicates()` a transformation or an action?
- **Answer**: Transformation (wide transformation requiring a shuffle).

### Q61. Why is `first()` faster than `collect()[0]`?
- **Answer**: `collect()[0]` retrieves all partitions across the entire cluster to the driver and extracts the first index. `first()` triggers a job that processes only the first partition and immediately terminates when one row is found.

### Q62. Does `sample()` cause a shuffle?
- **Answer**: No, `sample()` is a narrow transformation that evaluates items partition-by-partition.

### Q63. What is the difference between `show()` and `display()`?
- **Answer**: `show()` prints ASCII tables to stdout. `display()` is a Jupyter/Databricks notebook helper that renders rich HTML tables.

### Q64. Can an action trigger multiple Spark jobs?
- **Answer**: Yes! For example, `take(N)` first scans the first partition; if that partition contains fewer than $N$ rows, Spark submits a secondary job to scan subsequent partitions.

---

## 7. DAG, Jobs, Stages & Tasks Execution Flow (10 Questions)

### Q65. What is a DAG in Spark?
- **Answer**: Directed Acyclic Graph: a mathematical representation of execution flow where vertices represent RDDs/DataFrames and directed edges represent transformations. It is acyclic (contains no loops).

### Q66. What determines the boundary between two Stages?
- **Answer**: A **Shuffle Dependency (Wide Dependency)**. Whenever data must be partitioned across the cluster (e.g. `groupBy`, `join`, `repartition`), a stage boundary is drawn.

### Q67. What are the two types of Stages in Spark?
- **Answer**:
  1. `ShuffleMapStage`: An intermediate stage that executes transformations and writes its output as partitioned shuffle files to local disk for consumption by downstream stages.
  2. `ResultStage`: The terminal stage of a job that computes the final action and sends results back to the Driver or writes to storage.

### Q68. What is the formula for the number of Tasks in a Stage?
- **Answer**: $\text{Number of Tasks} = \text{Number of Partitions in the input RDD/DataFrame of that Stage}$.

### Q69. What is pipelining within a Stage?
- **Answer**: The execution of multiple consecutive narrow transformations (e.g. `map` $\to$ `filter` $\to$ `project`) on a single record inside a single CPU register/thread loop without serializing data or writing to disk.

### Q70. What is a TaskSet?
- **Answer**: A collection of low-level individual tasks belonging to the exact same stage, dispatched together by the `TaskScheduler` to execute across available executor cores.

### Q71. What does the DAGScheduler do?
- **Answer**: Converts the logical RDD DAG into physical execution stages, determines optimal task locations, and handles stage-level retry logic.

### Q72. What does the TaskScheduler do?
- **Answer**: Receives `TaskSets` from the `DAGScheduler`, schedules them on executors according to locality preferences (`PROCESS_LOCAL`, etc.), tracks individual task attempts, and handles task-level retries.

### Q73. What is a Stage ID?
- **Answer**: A monotonic integer assigned to each Stage within a Spark application.

### Q74. Can two Stages run in parallel in the same Job?
- **Answer**: Yes! If two upstream stages do not depend on each other (e.g. scanning and filtering two separate tables prior to a join), the `DAGScheduler` submits both stages concurrently if cluster cores are available.

---

## 8. Narrow vs. Wide Dependencies (6 Questions)

### Q75. Define Narrow Dependency with examples.
- **Answer**: A dependency where each partition of the parent RDD is used by at most one partition of the child RDD. Examples: `map()`, `filter()`, `union()`, `flatMap()`.

### Q76. Define Wide Dependency with examples.
- **Answer**: A dependency where multiple child partitions depend on data from a single parent partition, requiring data shuffling across the network. Examples: `groupBy()`, `reduceByKey()`, `join()`, `repartition()`, `distinct()`.

### Q77. Why do Narrow dependencies tolerate failure better than Wide dependencies?
- **Answer**: If a partition in a narrow dependency is lost, it can be recomputed in isolation by evaluating only its single corresponding parent partition. If a partition in a wide dependency is lost, recomputation may require rerunning all parent map tasks across the entire cluster.

### Q78. Is `union()` narrow or wide?
- **Answer**: Narrow. The partitions of parent RDD A and parent RDD B simply become individual partitions in the child RDD without data movement.

### Q79. Can a join ever be a Narrow transformation?
- **Answer**: Yes! If both DataFrames/RDDs are already co-partitioned with the exact same partitioner and partition count (e.g. both bucketed on the join key), Spark performs a narrow 1-to-1 join without a shuffle.

### Q80. How are wide dependencies represented in execution plans?
- **Answer**: Represented as `Exchange` nodes (e.g. `Exchange hashpartitioning(...)`).

---

## 9. Partitions & Sizing Strategies (8 Questions)

### Q81. What is a Partition?
- **Answer**: The fundamental atomic chunk of data in a distributed Spark dataset. It resides in memory or on disk on a specific worker node and is processed by a single task thread.

### Q82. What is `spark.sql.shuffle.partitions`?
- **Answer**: The configuration setting that determines the number of output partitions created during shuffle operations (joins, aggregations). Defaults to 200 in vanilla Spark (configured to 8 in our Docker lab).

### Q83. What are the symptoms of having too few partitions?
- **Answer**: High memory pressure, JVM heap exhaustion, frequent disk spilling during shuffles, long-running tasks, and underutilized cluster cores.

### Q84. What are the symptoms of having too many partitions?
- **Answer**: High overhead in task scheduling, Driver memory saturation managing task metadata, slow performance due to microscopic shuffle files, and excessive small files written to storage.

### Q85. How does `spark.default.parallelism` differ from `spark.sql.shuffle.partitions`?
- **Answer**: `spark.default.parallelism` governs low-level RDD operations (defaults to total cores in cluster). `spark.sql.shuffle.partitions` governs DataFrame and Spark SQL shuffle operations.

### Q86. What is Hash Partitioning?
- **Answer**: A partitioning strategy where an element's target partition index is computed as:
  $$\text{partitionIndex} = \text{abs}(\text{key.hashCode}()) \pmod{\text{numPartitions}}$$

### Q87. What is Range Partitioning?
- **Answer**: A partitioning strategy used for ordered datasets (e.g. `orderBy()`). Spark samples the dataset to determine quantile range boundaries so that partition 0 contains keys $A..D$, partition 1 contains $E..H$, and so on.

### Q88. How can you find the number of partitions in PySpark?
- **Answer**: `df.rdd.getNumPartitions()`.

---

## 10. Shuffle Internals & Spilling (10 Questions)

### Q89. Explain the Sort-Based Shuffle Manager.
- **Answer**: Spark's default shuffle implementation. In the map phase, records are collected into an in-memory buffer, sorted by target reducer partition ID and key, and spilled to a single consolidated data file (`.data`) accompanied by an index file (`.index`) that records byte offsets for each partition.

### Q90. What is the role of the shuffle index file?
- **Answer**: The `.index` file allows reducers to quickly seek and fetch only their specific partition segment from the consolidated `.data` file using byte range offsets.

### Q91. What causes Spill (Memory) and Spill (Disk)?
- **Answer**: When the in-memory shuffle aggregation/sorter buffer exceeds available execution memory, Spark flushes the buffer to local disk.
  - `Spill (Memory)`: Uncompressed size of data in RAM before writing.
  - `Spill (Disk)`: Compressed serialized size written to local disk.

### Q92. What is the External Shuffle Service (ESS)?
- **Answer**: An auxiliary daemon running on each worker node that serves shuffle files to downstream reducers independently of executor processes. This allows executors to be safely deallocated under Dynamic Resource Allocation without losing their shuffle map outputs.

### Q93. How does map-side aggregation reduce shuffle overhead?
- **Answer**: By pre-aggregating values with the same key inside the mapper's local memory before writing to shuffle files, drastically reducing network transmission volume.

### Q94. Why is network bandwidth often the bottleneck during shuffles?
- **Answer**: Because in an all-to-all shuffle across $N$ executors, data must traverse physical top-of-rack network switches, saturating network interface cards.

### Q95. How can you minimize shuffles in a data pipeline?
- **Answer**:
  1. Use Broadcast Joins for small lookup tables.
  2. Pre-partition or bucket datasets on frequently joined keys.
  3. Combine successive filter and transformation steps prior to shuffles.
  4. Avoid unneeded distinct or repartition calls.

### Q96. What is `spark.shuffle.compress`?
- **Answer**: Configuration (default `true`) that compresses shuffle map output files using fast codecs (LZ4, Zstandard) to save disk I/O and network transfer time.

### Q97. What is a FetchFailedException?
- **Answer**: An exception thrown by a reducer task when it cannot retrieve shuffle output blocks from a remote executor/worker (due to network timeout, executor crash, or deleted file).

### Q98. What does `spark.sql.shuffle.partitions` default to?
- **Answer**: 200.

---

## 11. Unified Memory Management & Garbage Collection (10 Questions)

### Q99. What are the components of Spark's Unified Memory pool?
- **Answer**:
  1. **Execution Memory**: Used for shuffles, joins, aggregations, and sorting buffers.
  2. **Storage Memory**: Used for cached RDDs/DataFrames, broadcast variables, and unroll buffers.

### Q100. Can Execution Memory evict Storage Memory?
- **Answer**: Yes! If execution requires memory, it can evict cached storage blocks until storage memory reaches `spark.memory.storageFraction`.

### Q101. Can Storage Memory evict Execution Memory?
- **Answer**: No! Execution memory blocks cannot be evicted; if execution memory is full, new allocations must wait or spill to disk.

### Q102. What is User Memory in the Spark JVM?
- **Answer**: The remaining fraction (default 40% of heap minus 300MB reserved) reserved for user data structures, metadata, and custom classes.

### Q103. What is Reserved Memory?
- **Answer**: A hardcoded 300 MB block reserved for Spark internal system processes to prevent internal OOMs.

### Q104. What is `spark.executor.memoryOverhead`?
- **Answer**: Off-heap memory allocated outside the JVM heap for PySpark worker processes, Netty shuffle buffers, and JVM Metaspace.

### Q105. How do you recognize excessive JVM Garbage Collection in the Spark UI?
- **Answer**: In the Stages tab, check the Task Timeline: if task bars show prominent **red segments** or if Task GC Time exceeds 10% of total duration.

### Q106. Which JVM Garbage Collector is recommended for Spark on Java 21?
- **Answer**: G1GC (`Garbage-First GC`) or ZGC for low-latency Stop-The-World pauses.

### Q107. What is Off-Heap Memory in Spark?
- **Answer**: Memory managed directly outside the JVM heap using native memory allocators (via Project Tungsten). It avoids Java object headers and JVM GC pauses entirely.

### Q108. What is `StorageLevel.MEMORY_AND_DISK_SER`?
- **Answer**: A storage level where partitions are cached as serialized byte arrays in memory; if RAM is full, excess blocks spill to disk.

---

## 12. Distributed Joins & Optimizations (8 Questions)

### Q109. List the 5 join strategies supported by Spark SQL.
- **Answer**:
  1. Broadcast Hash Join (BHJ)
  2. Shuffled Hash Join (SHJ)
  3. Shuffle Sort-Merge Join (SMJ)
  4. Broadcast Nested Loop Join (BNLJ)
  5. Cartesian Product Join (CPJ)

### Q110. What are the prerequisites for Broadcast Hash Join?
- **Answer**:
  1. An equi-join condition (`=`).
  2. One of the tables must be smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10 MB) or explicitly hinted with `broadcast()`.

### Q111. How does Shuffle Sort-Merge Join work?
- **Answer**:
  1. Both datasets are hash-shuffled on the join key into $N$ partitions.
  2. Each partition sorts rows by join key.
  3. Reducer iterates through both sorted streams simultaneously, merging matching keys.

### Q112. Why is Sort-Merge Join robust against OOM?
- **Answer**: Because sorted streams can be read iteratively row-by-row without loading the entire dataset into memory hash maps, spilling to disk if memory pressure occurs.

### Q113. What is a Broadcast Nested Loop Join and why is it dangerous?
- **Answer**: A fallback join that broadcasts one table and performs an $O(M \times N)$ nested loop comparison across all rows. It occurs during non-equi joins (`<`, `>`, `!=`) and can paralyze a cluster.

### Q114. How do you force a Sort-Merge Join in PySpark?
- **Answer**: `df1.hint("merge").join(df2, on="key")`.

### Q115. What is a Left Semi Join?
- **Answer**: Returns rows from the left table that have at least one matching key in the right table without duplicating left rows or including right columns.

### Q116. What is a Left Anti Join?
- **Answer**: Returns rows from the left table that have **no** matching keys in the right table (ideal for finding missing/orphan records).

---

## 13. Catalyst Optimizer & Adaptive Query Execution (AQE) (8 Questions)

### Q117. What are the 4 phases of Catalyst query optimization?
- **Answer**:
  1. Analysis (resolving names against Catalog)
  2. Logical Optimization (rule-based heuristic rewrites)
  3. Physical Planning (cost-based algorithm selection)
  4. Code Generation (Whole-Stage Java bytecode generation)

### Q118. What is Predicate Pushdown?
- **Answer**: A Catalyst optimization that pushes `WHERE` filter conditions down to the data source layer (e.g. Parquet row-group statistics) so unneeded rows are filtered before being read into memory.

### Q119. What is Column Pruning?
- **Answer**: Reading only the specific columns referenced in the query, discarding unused columns at the storage reader level.

### Q120. What is Whole-Stage Code Generation (Tungsten)?
- **Answer**: Compiling multiple physical operators across a stage into a single, compact Java bytecode loop, avoiding virtual function calls and caching data in CPU registers.

### Q121. What are the 3 major features of Adaptive Query Execution (AQE)?
- **Answer**:
  1. Dynamically coalescing post-shuffle partitions.
  2. Dynamically switching join strategies (e.g. SMJ $\to$ BHJ).
  3. Dynamically optimizing data skew in joins.

### Q122. How does AQE dynamically switch to a Broadcast Join?
- **Answer**: If one side of a join has a heavy filter, static analysis might estimate it as large. After the filter stage completes, AQE checks the actual materialized size; if below 10 MB, it converts the join to a Broadcast Hash Join.

### Q123. What does `df.explain("codegen")` show?
- **Answer**: The synthesized Java bytecode generated by Whole-Stage Code Generation.

### Q124. Is AQE enabled by default in Spark 3.x and 4.x?
- **Answer**: Yes (`spark.sql.adaptive.enabled = true`).

---

## 14. Fault Tolerance, Lineage & Checkpointing (6 Questions)

### Q125. How does Spark achieve fault tolerance without replicating data?
- **Answer**: Through **RDD Lineage Graphs**. Spark records the deterministic chain of transformations used to build each dataset. If a partition is lost, it re-evaluates only that partition's upstream operations.

### Q126. When is RDD recomputation triggered?
- **Answer**: When an executor holding a cached or intermediate partition crashes, or when a task fails to read a partition block.

### Q127. Why does checkpointing truncate the lineage graph?
- **Answer**: It saves the actual materialized data to durable, reliable storage (e.g. HDFS) and removes parent references, protecting against recursive JVM stack overflow in long pipelines.

### Q128. What is `spark.task.maxFailures`?
- **Answer**: The maximum number of retry attempts for a single task before the stage and entire job are aborted (default 4).

### Q129. How does Structured Streaming maintain fault tolerance?
- **Answer**: By combining offset checkpointing in Write-Ahead Logs (WAL) with idempotent state stores and deterministic replayable data sources.

### Q130. Does `persist()` save data if the Driver crashes?
- **Answer**: No. Persisted blocks live in executor memory/disk; if the Driver crashes, the entire application shuts down.

---

## 15. Structured Streaming & State Management (6 Questions)

### Q131. What is the fundamental abstraction of Structured Streaming?
- **Answer**: Treating a live real-time stream as an **Unbounded Append-Only Table**.

### Q132. Explain the three Output Modes in Structured Streaming.
- **Answer**:
  1. **Append**: Only newly added rows are written to the sink.
  2. **Complete**: Entire updated result table is re-written to the sink.
  3. **Update**: Only rows updated since the previous trigger are written.

### Q133. What is a Watermark and why is it necessary?
- **Answer**: A moving threshold ($\text{MaxEventTime} - \text{Delay}$) that dictates how long Spark waits for late-arriving records before purging historical state from memory.

### Q134. What are the requirements for end-to-end Exactly-Once semantics?
- **Answer**:
  1. Replayable streaming source (e.g. Kafka, file directory).
  2. Deterministic processing logic.
  3. Idempotent or transactional sink (e.g. Delta Lake).
  4. Reliable checkpointing.

### Q135. What is the default Trigger in Structured Streaming?
- **Answer**: Micro-batch processing as fast as possible (processing time = 0).

### Q136. What is `Trigger.AvailableNow()`?
- **Answer**: Processes all currently available data in multiple micro-batches and then terminates the stream (cost-effective for incremental batch workloads).

---

## 16. Performance Engineering & Data Skew (12 Questions)

### Q137. How do you diagnose a straggler task?
- **Answer**: In the Spark UI Stages tab, sort tasks by duration. If a few tasks run for minutes while others finish in milliseconds, inspect their `Shuffle Read Size` and `Input Size` to confirm data skew.

### Q138. What is Salting?
- **Answer**: Adding a random suffix/prefix to the join key of a skewed dataset and replicating the matching table's keys to spread the hotspot across multiple partitions.

### Q139. How do you solve the "Small Files Problem"?
- **Answer**:
  1. Call `.coalesce(N)` before writing output.
  2. Configure `spark.sql.files.maxRecordsPerFile`.
  3. Enable AQE partition coalescing.

### Q140. What is the danger of cartesian joins?
- **Answer**: They produce $M \times N$ output rows, causing explosive data volume, disk thrashing, and job paralysis.

### Q141. How does Kryo serialization improve performance?
- **Answer**: `org.apache.spark.serializer.KryoSerializer` is up to 10x faster and significantly more compact than standard Java serialization.

### Q142. How does broadcast join eliminate shuffle?
- **Answer**: The entire small table is sent to every executor once, allowing each worker to join local partitions independently without network transfer.

### Q143. What is `spark.sql.files.maxPartitionBytes`?
- **Answer**: The maximum number of bytes packed into a single partition when reading file sources (default 128 MB).

### Q144. What happens when a broadcast variable exceeds driver memory?
- **Answer**: The Driver crashes with `OutOfMemoryError` during broadcast serialization.

### Q145. Why should you avoid `count()` after writing data?
- **Answer**: `.write` is an Action that completes the pipeline. Calling `.count()` immediately afterward triggers a second, redundant job that re-scans the dataset!

### Q146. How does partition pruning differ from predicate pushdown?
- **Answer**:
  - Partition Pruning: Skips entire directory folders based on partition column values (e.g. `year=2026/`).
  - Predicate Pushdown: Skips row groups inside individual files based on file metadata.

### Q147. What is dynamic partition overwrite?
- **Answer**: Overwriting only the specific partition directories touched by the incoming dataset rather than wiping out the entire table folder.

### Q148. What is Bucketing?
- **Answer**: Pre-sorting and pre-partitioning data on disk based on a hash key (`bucketBy`), allowing subsequent joins on that key to execute without runtime shuffles.

---

## 17. Spark UI, Event Logs & Diagnostic Forensics (6 Questions)

### Q149. Where in the Spark UI can you see memory spill?
- **Answer**: In the **Stages Tab**, under the Stage summary and Task tables, looking at the columns: `Spill (Memory)` and `Spill (Disk)`.

### Q150. What does the Executors tab show?
- **Answer**: Real-time memory usage (Storage vs On-Heap), active tasks, failed tasks, GC time, and cumulative shuffle read/write bytes for every executor JVM.

### Q151. What does the SQL tab show?
- **Answer**: The visual DAG query plan, showing physical operators (`FileScan`, `Exchange`, `HashAggregate`), row counts, and stage timings.

### Q152. What does an orange bar in the Event Timeline indicate?
- **Answer**: Shuffle Fetch Wait time (waiting for remote shuffle blocks over the network).

### Q153. What does a red bar in the Event Timeline indicate?
- **Answer**: Task JVM Garbage Collection pause time.

### Q154. What directory contains persistent event logs in our Docker environment?
- **Answer**: `/opt/spark-events` (mounted to `./events` on host).

---

## 18. Docker & Containerized Infrastructure (4 Questions)

### Q155. Why is `spark.driver.host=jupyter` necessary in Docker?
- **Answer**: In a bridge Docker network, containers communicate via container hostnames. If the driver defaults to `localhost`, executors running in worker containers will attempt to connect to themselves and fail.

### Q156. What user runs Spark daemons in the official container image?
- **Answer**: User `spark` with UID `185`.

### Q157. How do volume mounts enable persistent event logs?
- **Answer**: By bind-mounting the host directory `./events` to `/opt/spark-events` across both the Driver, Workers, and History Server containers.

### Q158. How do you scale Spark workers using Docker Compose?
- **Answer**: `docker compose up -d --scale spark-worker-1=3` (or defining dedicated worker services in `docker-compose.yml`).
""")
    print(f"Successfully generated {path}")

if __name__ == '__main__':
    build_interview_guide()
