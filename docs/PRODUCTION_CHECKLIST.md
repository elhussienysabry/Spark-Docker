# Apache Spark Production Engineering Readiness Checklist

> **A comprehensive operational checklist for deploying and running Spark workloads in production.**

---

## 1. Cluster & Resource Allocation
- [] **Executor Sizing**: Configured with 3 to 5 cores per executor (`spark.executor.cores=4`).
- [] **Executor Heap Sizing**: Executor memory set between 16 GB and 30 GB to prevent severe GC pauses (`spark.executor.memory=24g`).
- [] **Off-Heap Overhead Buffer**: Allocated at least 15% to 20% memory overhead for PySpark workers and Netty buffers (`spark.executor.memoryOverhead=4g`).
- [] **Driver Sizing**: Allocated sufficient driver memory (2 GB to 8 GB) with `spark.driver.maxResultSize` set to prevent driver OOM.
- [] **Garbage Collector**: Enabled G1GC with tuning flags (`-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35`).
- [] **Dynamic Allocation**: Configured `spark.dynamicAllocation.enabled=true` with safe `minExecutors` and `maxExecutors`.

---

## 2. Storage & Data Lake Architecture
- [] **Columnar File Format**: Production data stored in Parquet or Delta Lake format, compressed with Snappy or ZSTD.
- [] **Explicit Schemas**: All batch ingestion jobs use explicit `StructType` schemas; `inferSchema=True` is disabled.
- [] **Target File Size**: Output files sized between 128 MB and 512 MB, avoiding the small files problem.
- [] **Partition Sizing**: Data lake directory partitioning restricted to low-to-medium cardinality columns (e.g. `year`, `month`, `region`). Avoided partitioning on high-cardinality keys like `timestamp` or `customer_id`.
- [] **Partition Pruning Verification**: Validated via `df.explain()` that queries prune partition directories when filtering by partition columns.

---

## 3. Shuffles & Query Optimization
- [] **Shuffle Partitions Sizing**: Tuned `spark.sql.shuffle.partitions` to target 100 MB to 200 MB per shuffle partition.
- [] **Adaptive Query Execution**: Ensured `spark.sql.adaptive.enabled=true` and `spark.sql.adaptive.coalescePartitions.enabled=true`.
- [] **Broadcast Threshold**: Verified small dimension tables are broadcast using `broadcast()` hints or `spark.sql.autoBroadcastJoinThreshold`.
- [] **Data Skew Handled**: Addressed join key hotspots using Salting or AQE skew join (`spark.sql.adaptive.skewJoin.enabled=true`).
- [] **Serialization**: Enabled Kryo serializer (`spark.serializer=org.apache.spark.serializer.KryoSerializer`).

---

## 4. Memory & Caching Management
- [] **Selective Caching**: Only cached DataFrames consumed by 2 or more distinct actions.
- [] **Immediate Unpersisting**: Paired every `.persist()` / `.cache()` call with an explicit `.unpersist()` when the branching pipeline concludes.
- [] **Storage Level Selection**: Used `MEMORY_AND_DISK_SER` for large cached collections to minimize RAM footprint.
- [] **Zero Memory Spill**: Verified in Spark UI Stages tab that `Spill (Memory)` and `Spill (Disk)` are zero or minimal.

---

## 5. Streaming & Reliability
- [] **Durable Checkpoints**: Checkpoint directories configured on resilient persistent storage (`checkpointLocation`).
- [] **Watermarking Defined**: Every streaming stateful aggregation has an explicit `withWatermark()` to bound state store size.
- [] **Graceful Shutdown**: Enabled graceful shutdown configuration (`spark.streaming.stopGracefullyOnShutdown=true`).
- [] **Retry Configuration**: Verified `spark.task.maxFailures` is set to 4 (default) to tolerate transient node hiccups.

---

## 6. Observability & Monitoring
- [] **Event Logging Active**: Configured `spark.eventLog.enabled=true` and pointing to shared directory.
- [] **History Server Verified**: Confirmed completed job event logs render correctly in Spark History Server (`localhost:18080`).
- [] **Metrics Export**: Integrated Prometheus / Graphite metrics sink for cluster monitoring if running at scale.
- [] **Log Level**: Set driver/executor log level to `WARN` in production to prevent spamming log disks.
