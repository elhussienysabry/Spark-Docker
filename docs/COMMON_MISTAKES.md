# 35 Common Apache Spark Mistakes & Anti-Patterns

> **A diagnostic guide to common pitfalls, why they break, and production-tested solutions.**

---

## Category 1: Beginner & API Mistakes

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

## Category 2: Performance & Shuffle Anti-Patterns

### 8. The"Small Files Problem"
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
- **Correct Approach**: Calculate partitions: $500{,}000	ext{MB} / 128	ext{MB} pprox 3{,}900$ partitions.

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
- **Why It's Wrong**: Generates $M 	imes N$ rows, exploding memory and causing cluster freeze.
- **Correct Approach**: Always provide equi-join predicates (`on="id"`).

---

## Category 3: Memory & Resource Misconceptions

### 16. Assuming"In-Memory"Means Everything Must Fit in RAM
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
- **Correct Approach**: Container limit $\ge 	ext{Heap} 	imes 1.25$.

---

## Category 4: Streaming & Production Pitfalls

### 21. Streaming Without Watermarks on Aggregations
- **The Mistake**: Grouping by event time in Structured Streaming without defining `withWatermark()`.
- **Why It's Wrong**: Spark maintains historical aggregate states indefinitely, eventually crashing with an OOM.
- **Correct Approach**: Always provide a watermark: `df.withWatermark("timestamp","10 minutes")`.

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

## Category 5: Architecture & Diagnostics Pitfalls

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
- **Why It's Wrong**: Wrapping columns in custom functions (`filter(upper(col) =='A')`) disables predicate pushdown.
- **Correct Approach**: Inspect `explain()` output to confirm `PushedFilters` are active.

### 30. Assuming Dynamic Allocation Replaces Resource Planning
- **The Mistake**: Enabling dynamic allocation and assuming Spark will never waste resources.
- **Why It's Wrong**: Dynamic allocation still requires upper limits (`maxExecutors`) and idle timeouts to avoid monopolizing shared clusters.
- **Correct Approach**: Set explicit dynamic allocation bounds (`minExecutors`, `maxExecutors`).
