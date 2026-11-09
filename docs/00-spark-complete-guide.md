# Apache Spark: The Complete Guide

This is the long-form guide for the Docker stack in this folder. It explains what Apache Spark is, how it executes work, how this particular cluster is assembled, and how to modify, tune, operate, and troubleshoot it.

The short version is in [the project README](../README.md). Step-by-step procedures live in [the other guides](README.md). This file connects the concepts to the commands.

Everything below was verified against the running stack:

| Component | Version |
|---|---|
| Apache Spark | 4.2.1 |
| Scala | 2.13.18 |
| Java | 21.0.13 |
| Python (cluster and Jupyter) | 3.13.18 |
| PySpark | 4.2.1 |
| pandas / pyarrow / numpy | 2.3.3 / 25.0.0 / 2.5.4 |
| JupyterLab | 4.6.4 |

---

## Contents

- [The one-page mental model](#the-one-page-mental-model)
- [What Apache Spark is](#what-apache-spark-is)
- [When to use Spark](#when-to-use-spark)
- [Architecture](#architecture)
- [How an application runs](#how-an-application-runs)
- [Core abstractions](#core-abstractions)
- [Spark SQL and DataFrames](#spark-sql-and-dataframes)
- [Execution internals](#execution-internals)
- [This installation](#this-installation)
- [Daily workflow](#daily-workflow)
- [Running jobs](#running-jobs)
- [JupyterLab](#jupyterlab)
- [Working with data](#working-with-data)
- [Performance tuning](#performance-tuning)
- [Monitoring and debugging](#monitoring-and-debugging)
- [Modifying the stack](#modifying-the-stack)
- [Configuration reference](#configuration-reference)
- [Operations](#operations)
- [Troubleshooting](#troubleshooting)
- [Glossary](#glossary)
- [Learning resources](#learning-resources)

---

## The one-page mental model

Spark is a **distributed data processing engine**. You describe a computation, and Spark splits the data into **partitions**, builds a **plan**, and runs **tasks** for those partitions on **executors** in parallel.

```text
your code
   -> logical plan
   -> optimized physical plan
   -> jobs -> stages -> tasks
   -> executors process partitions
   -> results, files, or a stream
```

The driver is the coordinator. The executors do the work. A cluster manager (in this stack, Spark's standalone master) allocates executor resources.

Four things dominate Spark performance:

1. **Shuffles** — moving data between executors, usually for `groupBy`, `join`, `distinct`, `repartition`, and global sort.
2. **Data scanned** — reading only the columns, partitions, and files you need.
3. **Serialization** — converting data between JVM objects, Python objects, and binary formats.
4. **Spilling** — writing intermediate data to disk when memory is insufficient.

If you remember one sentence: **minimize shuffles, scan less data, and keep partitions reasonably sized.**

In this installation:

- One **master** container schedules applications.
- Two **worker** containers each offer **2 cores and 4 GiB**, for **4 cores and 8 GiB total**.
- The default executor is **1 core / 2 GiB**, so a full application can get **4 executor slots**.
- JupyterLab runs a **driver** in its own container.
- A **History Server** reads completed application event logs from `events/`.

---

## What Apache Spark is

Apache Spark is an open-source, distributed computing system for large-scale data processing. It provides a unified engine for:

| Layer | What it gives you |
|---|---|
| **Spark Core** | Scheduling, memory management, fault recovery, RDDs, accumulators, broadcast variables |
| **Spark SQL / DataFrames / Datasets** | Structured data, SQL, Catalyst optimizer, columnar execution |
| **Structured Streaming** | Incremental, fault-tolerant stream processing on the same engine |
| **MLlib** | Distributed machine-learning algorithms and pipelines |
| **GraphX** | Graph processing on RDDs (Scala API) |
| **Spark Connect** | A gRPC client/server protocol for remote Spark access |
| **PySpark** | Python bindings for the whole engine |

Spark runs on a single machine or a cluster. It can use its own **standalone** cluster manager, Apache Hadoop YARN, Kubernetes, or Apache Mesos (historically). It reads data from local files, HDFS, S3-compatible storage, relational databases through JDBC, Kafka, and many other sources.

### What Spark is not

- **Not a database.** It does not provide OLTP transactions, row-level locking, or a general-purpose serving layer.
- **Not a storage system.** It reads and writes storage; it does not replace HDFS, S3, or a database.
- **Not a cluster manager by itself.** The standalone master is a simple cluster manager; production clusters often use YARN or Kubernetes.
- **Not always faster than pandas.** For a few thousand rows on one machine, pandas is usually simpler and faster.
- **Not a low-latency request server.** Spark has job startup and scheduling overhead measured in seconds, not milliseconds.

### Spark 4 notes relevant to this stack

- **Scala 2.13** is the default and only Scala build line in Spark 4; Scala 2.12 is no longer shipped.
- **Java 17+** is required. This image uses Java 21.
- **ANSI SQL mode is enabled by default.** Invalid casts raise errors instead of silently returning `NULL`.
- **Spark Connect** is part of Spark 4, though it is not enabled in this stack.
- **Python 3.9+** is required; this stack uses Python 3.13.

### Spark compared with related tools

| Tool | Best at | Difference from Spark |
|---|---|---|
| **Hadoop MapReduce** | Batch processing on HDFS | Spark keeps more work in memory, has richer APIs, and is usually much faster for iterative work |
| **pandas** | Single-machine data analysis | pandas is limited by one machine's memory and CPU; Spark scales across machines |
| **Dask** | Python-native parallel computing | Dask integrates tightly with the Python ecosystem; Spark has a broader SQL/streaming/ML stack and a JVM core |
| **Flink** | Low-latency stateful streaming | Flink treats streaming as the primary model; Spark Structured Streaming is micro-batch oriented (with continuous modes in some cases) |
| **PostgreSQL / MySQL** | OLTP and transactional workloads | Databases serve small, fast queries and enforce transactions; Spark is for analytical batch work |

---

## When to use Spark

Spark is a good fit when:

- The data is too large for one machine's memory or disk.
- You need repeatable batch ETL over many files.
- You want SQL or DataFrame analytics over Parquet, CSV, JSON, or JDBC sources.
- You need distributed feature preparation or model training.
- You need fault-tolerant stream processing with the same code style as batch.
- You want one engine for SQL, Python, Scala, and Java workloads.

Spark is a poor fit when:

- The dataset fits comfortably in pandas or a single database.
- You need sub-second API responses.
- The work is a simple cron job over a few megabytes.
- The computation is highly sequential and cannot be partitioned.
- You need strong transactional guarantees or frequent small updates.

This Docker stack is sized for **learning and local development**: 4 cores, 8 GiB of executor capacity, and one driver. It is excellent for prototyping and for validating job logic. It is not a highly available production cluster.

---

## Architecture

### The main pieces

```text
                    +---------------------------+
                    |  Driver                   |
                    |  main() / SparkSession    |
                    |  DAG + task scheduling    |
                    |  Spark UI                 |
                    +-------------+-------------+
                                  |
                    submit app / request resources
                                  |
                                  v
                    +---------------------------+
                    |  Cluster manager          |
                    |  standalone master :7077  |
                    +------+-------------+------+
                           |             |
                  launch executors   launch executors
                           |             |
                           v             v
                    +-----------+  +-----------+
                    | Worker 1  |  | Worker 2  |
                    | 2 cores   |  | 2 cores   |
                    | 4 GiB     |  | 4 GiB     |
                    | executors |  | executors |
                    +-----------+  +-----------+
                           \             /
                            \           /
                             v         v
                         results / shuffle / cache
```

### Driver

The driver is the process that runs your `main()` function or notebook code. It:

- Creates the `SparkSession` and `SparkContext`.
- Converts your transformations into a logical plan.
- Asks Catalyst to optimize the plan.
- Converts the physical plan into a DAG of stages.
- Schedules tasks on executors through the cluster manager.
- Tracks task and stage progress.
- Collects results for actions.
- Serves the Spark UI.
- Writes event logs if event logging is enabled.

In **client mode**, the driver runs in the container where you ran `spark-submit` (or in the Jupyter kernel). In **cluster mode**, the cluster manager starts the driver on a worker.

### Executors

An executor is a JVM process on a worker that:

- Runs tasks assigned by the driver.
- Reads input partitions.
- Performs transformations, aggregations, joins, and sorts.
- Writes shuffle output.
- Caches data if asked.
- Reports task status and metrics back to the driver.

Executors are created for an application and normally live for the duration of that application. In this standalone cluster, each executor is configured as **1 core and 2 GiB**, so a worker with 2 cores and 4 GiB can host two executors.

### Cluster manager and workers

The cluster manager allocates resources. In this stack it is Spark's **standalone master**:

- Listens on `spark://spark-master:7077`.
- Tracks workers and their available cores and memory.
- Offers resources to applications.
- Launches executors on workers.
- Shows the cluster and application state at <http://localhost:8080>.

The **workers** register with the master and advertise resources. In this stack each worker advertises 2 cores and 4 GiB, and serves its own UI on container port 8081 (host ports 8081 and 8082).

### Application, job, stage, task, partition

| Term | Meaning |
|---|---|
| **Application** | One submission: a driver plus its executors. Example: one `spark-submit` invocation |
| **Job** | A unit of work triggered by an action such as `count()`, `collect()`, or `write()` |
| **Stage** | A set of tasks that can run without a shuffle; stages are separated by shuffle boundaries |
| **Task** | One unit of work for one partition in one stage |
| **Partition** | A chunk of a distributed dataset; the unit of parallelism |
| **Slot** | A core available to run a task; 4 cores means up to 4 tasks at once in this cluster |

A single application can contain many jobs. A job can contain many stages. A stage contains one task per partition (roughly), up to the number of available slots.

### Deployment modes

| Mode | Driver location | Use case |
|---|---|---|
| **Local** | Same JVM/process as the submission | Testing, small data |
| **Client** | The submitting container/machine | Interactive work, notebooks, visible logs |
| **Cluster** | A worker selected by the master | Production-style submissions, driver isolated from the client |

In this stack:

- `spark-submit` from `spark-master` defaults to **client mode**: driver in `spark-master`.
- Jupyter notebooks run the driver **inside the Jupyter container**.
- `--deploy-mode cluster` runs the driver on a worker. The submitting command returns when the application finishes, and driver stdout is not streamed to your terminal.

---

## How an application runs

### 1. Submission

You run `spark-submit` with an application file, a master URL, and optional resources and configuration. The submission client contacts the master. The master allocates executors on workers. In client mode, the driver starts in the submitting container; in cluster mode, a worker starts it.

### 2. Session creation

The driver creates a `SparkSession` (and underlying `SparkContext`). The session holds configuration, the catalog, the SQL engine, and the connection to the cluster.

### 3. Logical plan

Transformations build a **logical plan**:

```python
sales = spark.read.csv("/opt/spark-data/sales.csv", header=True, inferSchema=True)
result = (
    sales
    .filter("units > 0")
    .withColumn("revenue", F.col("units") * F.col("unit_price"))
    .groupBy("region")
    .sum("revenue")
)
```

Nothing runs yet. Spark records what you asked for.

### 4. Optimization

Catalyst analyzes the plan (resolving column names and types), then applies rules such as:

- Push filters closer to the data source.
- Read only the required columns.
- Fold constants.
- Reorder or change join strategies.
- Combine operations.

The result is an optimized logical plan, then a physical plan of operators.

### 5. DAG and stages

The DAG scheduler splits the physical plan at **shuffle boundaries**:

- Narrow transformations (`select`, `filter`, `withColumn`, `union`) can be pipelined in the same stage.
- Wide transformations (`groupBy`, `join`, `distinct`, `repartition`, global `orderBy`) require a shuffle and create a new stage.

### 6. Tasks

For each stage, the task scheduler creates one task per partition and sends tasks to executors. An executor runs a task on a core, reads its partition, applies the operations, and writes output or shuffle data.

### 7. Actions and results

An action such as `show()`, `count()`, `collect()`, or `write()` triggers execution. The driver collects small results; large results should be written to storage rather than collected into the driver.

### A concrete stage example

```python
(
    spark.read.parquet("/opt/spark-data/events")
    .filter("event_date >='2026-01-01'")      # narrow
    .select("user_id","event_type")           # narrow
    .groupBy("event_type")                      # wide: shuffle
    .count()                                    # action
    .show()
)
```

Possible stages:

1. **Scan + filter + select**: read partitions, apply filters and projection, possibly pipeline into the shuffle writer.
2. **Shuffle read + aggregate**: each task reads its share of shuffled data, aggregates by `event_type`, and returns results.

The shuffle is the expensive boundary. Everything before it can run in parallel per partition; everything after it depends on the shuffle output.

### Fault tolerance

Spark recovers from failures in several ways:

- **RDD lineage**: if a partition is lost, Spark recomputes it from its parent partitions.
- **Task retries**: failed tasks are retried (`spark.task.maxFailures`, default 4).
- **Stage retries**: if a shuffle map output is lost, Spark may recompute the map stage.
- **Executor replacement**: the cluster manager can allocate a new executor; the driver reschedules lost tasks.
- **Checkpointing**: for long lineages or streaming, Spark can write state to reliable storage and cut the lineage.

Fault tolerance is not free: recomputation can be expensive, and shuffle data on a lost executor must be regenerated. This is one reason to persist reused intermediate results selectively.

## Core abstractions

### RDD

An **RDD (Resilient Distributed Dataset)** is the original Spark abstraction: an immutable, partitioned collection with a lineage that can be recomputed. RDDs are still useful for:

- Low-level control over partitioning.
- Custom record-level processing.
- Legacy code and some graph algorithms.
- Cases where the DataFrame optimizer gets in the way.

```python
sc = spark.sparkContext
rdd = sc.parallelize(range(1, 1_000_001), 8)
total = rdd.map(lambda x: x * x).reduce(lambda a, b: a + b)
print(total)
print("partitions:", rdd.getNumPartitions())
```

RDD operations are either **transformations** (`map`, `filter`, `flatMap`, `reduceByKey`, `join`) or **actions** (`count`, `collect`, `take`, `reduce`, `saveAsTextFile`). Python RDDs move data between Python and the JVM, so they are usually slower than DataFrames for structured work.

### DataFrame

A **DataFrame** is a distributed table with named columns and a schema. It is the recommended API for structured data:

- Catalyst can optimize it.
- Tungsten can generate efficient code.
- It works across Python, Scala, Java, and R.
- It reads and writes CSV, JSON, Parquet, ORC, JDBC, and more.

```python
from pyspark.sql import functions as F

sales = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/opt/spark-data/sales.csv")
)

sales.select("region","product","units","unit_price").show(5)
```

In Python, a DataFrame is the main structured API. In Scala and Java, a typed **Dataset[T]** adds compile-time types on top of DataFrames.

### SparkSession and SparkContext

`SparkSession` is the entry point for DataFrames, SQL, catalog operations, and reading/writing data. `SparkContext` is the lower-level entry point for RDDs, accumulators, broadcast variables, and cluster configuration.

```python
spark = SparkSession.builder.appName("Demo").getOrCreate()
sc = spark.sparkContext
```

Create the session once per application or notebook kernel. Configuration that affects the cluster connection (master, driver host, executor memory) is fixed when the session starts.

### Transformations and actions

| Kind | Examples | Behavior |
|---|---|---|
| **Transformation** | `select`, `filter`, `withColumn`, `groupBy`, `join`, `repartition` | Lazy; builds a plan |
| **Action** | `show`, `count`, `collect`, `take`, `write`, `foreach` | Triggers execution and returns a result or writes data |

Lazy evaluation lets Spark optimize across many transformations before running anything.

### Narrow and wide transformations

| Transformation | Type | Shuffle? |
|---|---|---|
| `select`, `filter`, `withColumn`, `drop` | Narrow | No |
| `union` | Narrow | No |
| `map`, `flatMap`, `mapPartitions` | Narrow | No |
| `groupBy`, `agg` | Wide | Yes |
| `join` (usually) | Wide | Yes, unless broadcast |
| `distinct`, `dropDuplicates` | Wide | Yes |
| `repartition` | Wide | Yes |
| `coalesce` | Narrow | No, but can be uneven |
| `orderBy` / `sort` | Wide | Yes, for a global sort |

Narrow transformations can be pipelined within a stage. Wide transformations introduce a shuffle and a stage boundary.

### Partitions

A partition is a chunk of data processed by one task. Partition count controls parallelism:

- Too few partitions: idle cores.
- Too many partitions: scheduling overhead and tiny tasks.
- Uneven partitions: stragglers and skew.

For DataFrames, Spark often derives partitions from input files and shuffle settings. For RDDs, you control partitions with `parallelize(..., n)`, `repartition(n)`, or `coalesce(n)`.

### User-defined functions

There are three common kinds:

1. **Python UDFs** — flexible, but data is serialized between Python and the JVM; slower.
2. **pandas UDFs (vectorized UDFs)** — use Apache Arrow and process batches; much faster than row-at-a-time Python UDFs.
3. **Scala/Java UDFs** — run in the JVM; fastest when you can write them.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def category(product: str) -> str:
    return"premium"if product in {"Laptop","Phone"} else"standard"

sales.withColumn("category", category("product")).show(5)
```

Use built-in `pyspark.sql.functions` first. They are optimized and avoid serialization. Use a UDF only when the logic cannot be expressed with built-ins.

### Tables, views, and the catalog

- **Temporary view**: session-scoped name for a DataFrame, created with `createOrReplaceTempView`.
- **Global temporary view**: application-scoped name, created with `createOrReplaceGlobalTempView`.
- **Table**: metadata plus data, created with `saveAsTable` or SQL `CREATE TABLE`.
- **Managed table**: Spark manages the data location (usually under the warehouse directory).
- **External table**: metadata points at a location you manage.

This stack sets `spark.sql.warehouse.dir=file:///opt/spark-data/warehouse` but does not configure an external Hive metastore. The built-in session catalog is fine for a notebook session; use Parquet paths for data that must outlive the session.

### Structured Streaming

Structured Streaming treats a stream as an unbounded table. You write a query with `readStream`, a transformation, and `writeStream`:

```python
stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
)

query = (
    stream
    .groupBy("value")
    .count()
    .writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation","/opt/spark-data/checkpoints/rate")
    .start()
)
query.awaitTermination(30)
query.stop()
```

The stack has no Kafka service, but the `rate` source and file sources are enough to learn the model. Streaming checkpoints must be on shared, durable storage if the driver can move.

### MLlib

MLlib provides DataFrame-based estimators, transformers, and pipelines:

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

assembler = VectorAssembler(inputCols=["units"], outputCol="features")
training = assembler.transform(sales).select("features","revenue")
model = LinearRegression(featuresCol="features", labelCol="revenue").fit(training)
model.transform(training).show(5)
```

MLlib is included in Spark. There is no MLflow or model-serving service in this stack; save models to `/opt/spark-data/models` with `model.write().overwrite().save(...)` if you need persistence.

### GraphX

GraphX is Spark's RDD-based graph API, primarily Scala. Python users often use GraphFrames, which is a separate package, or DataFrame joins for simple graph work. GraphX is not exposed as a first-class Python API in this stack.

### Spark Connect

Spark Connect separates the client from the driver with a gRPC protocol. Spark 4 includes it, but this stack does not start a Connect server. It is useful when clients should not run a full Spark driver, or when you want remote access from a thin client. See [Enabling Spark Connect](#enable-spark-connect-optional) if you want to experiment.

---

## Spark SQL and DataFrames

### Catalyst

Catalyst is Spark SQL's query optimizer. It processes a query in phases:

1. **Parse** SQL or build a logical plan from the DataFrame API.
2. **Analyze** names, types, functions, and catalog objects.
3. **Optimize** with rules such as predicate pushdown, column pruning, constant folding, and join reordering.
4. **Plan** physical operators and choose join/sort strategies.
5. **Codegen** Java bytecode for many operators.

Example:

```python
sales = spark.read.parquet("/opt/spark-data/sales_parquet")
filtered = sales.filter("region ='East'").select("product","revenue")
filtered.explain("formatted")
```

`explain` shows the logical and physical plans. Look for:

- `FileScan` and which columns/partitions are read.
- `Filter` and whether it was pushed down.
- `Exchange` (a shuffle).
- `BroadcastHashJoin` vs `SortMergeJoin`.
- `AdaptiveSparkPlan` when AQE is active.

### Adaptive Query Execution (AQE)

AQE re-optimizes the physical plan at runtime using actual shuffle statistics. It can:

- Coalesce many small shuffle partitions into fewer partitions.
- Split skewed partitions.
- Convert a sort-merge join to a broadcast join when one side turns out to be small.
- Reuse local shuffle data.

In this stack `spark.sql.adaptive.enabled=true` and `spark.sql.adaptive.coalescePartitions.enabled=true`. AQE is one reason a fixed `spark.sql.shuffle.partitions=8` is not as rigid as it looks: AQE can reduce the number of partitions after the shuffle.

### Tungsten

Tungsten is Spark's physical execution layer: off-heap memory management, cache-friendly data layouts, and whole-stage code generation. You usually do not configure it directly. Its practical implications are:

- DataFrames are generally faster than Python RDDs for structured work.
- Columnar formats such as Parquet work well with Tungsten.
- UDFs can break codegen and force row-by-row processing.

### DataFrame and SQL examples

```python
from pyspark.sql import functions as F

sales = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/opt/spark-data/sales.csv")
)

sales.createOrReplaceTempView("sales")

by_region = spark.sql("""
    SELECT
        region,
        ROUND(SUM(units * unit_price), 2) AS revenue,
        SUM(units) AS units_sold,
        COUNT(DISTINCT order_id) AS orders
    FROM sales
    GROUP BY region
    ORDER BY revenue DESC
""")
by_region.show()
```

Equivalent DataFrame API:

```python
by_region = (
    sales
    .withColumn("revenue", F.col("units") * F.col("unit_price"))
    .groupBy("region")
    .agg(
        F.round(F.sum("revenue"), 2).alias("revenue"),
        F.sum("units").alias("units_sold"),
        F.countDistinct("order_id").alias("orders"),
)
    .orderBy(F.desc("revenue"))
)
```

### Joins

Spark chooses a join strategy based on size estimates and hints:

| Strategy | When it is used | Cost |
|---|---|---|
| **Broadcast hash join** | One side is small (below `spark.sql.autoBroadcastJoinThreshold`) | No shuffle of the small side; very fast |
| **Shuffle hash join** | One side is smaller but not broadcastable | Shuffle both sides |
| **Sort-merge join** | General large-large join | Shuffle and sort both sides |

Broadcast hint:

```python
from pyspark.sql.functions import broadcast

result = big.join(broadcast(small),"customer_id","left")
```

SQL hint:

```sql
SELECT /*+ BROADCAST(small) */ *
FROM big JOIN small ON big.id = small.id;
```

Only broadcast a table that fits comfortably in executor memory. The default threshold is 10 MB; this stack does not override it.

### ANSI SQL mode

Spark 4 enables ANSI mode by default. This makes invalid operations fail loudly:

```sql
SELECT CAST('abc'AS INT);
-- error instead of NULL
```

Use `TRY_CAST` when you want `NULL` for invalid values:

```sql
SELECT TRY_CAST('abc'AS INT);
```

To restore pre-Spark-4 behavior for a specific job:

```bash
--conf spark.sql.ansi.enabled=false
```

Be deliberate: silent `NULL`s can hide data-quality problems.

### File formats

| Format | Strengths | Notes |
|---|---|---|
| **Parquet** | Columnar, compressed, schema evolution, predicate pushdown | Best default for analytical tables |
| **ORC** | Columnar, Hive-friendly | Good alternative to Parquet |
| **CSV** | Human-readable, universal | No schema; slower; parse errors |
| **JSON** | Flexible/nested | Larger; schema inference can be expensive |
| **Avro** | Row-based, schema evolution | May require the Spark Avro package |
| **JDBC** | Relational databases | Requires a JDBC driver jar and network access |

For repeated analytics, convert CSV/JSON to Parquet and partition by low-cardinality columns you filter on.

### Managed tables and the warehouse

`spark.sql.warehouse.dir` points at `/opt/spark-data/warehouse`. Managed table data goes there. Because this stack has no external Hive metastore, table metadata is tied to the Spark session/catalog. For durable results:

- Write Parquet to an explicit path, or
- Configure a Hive metastore and enable Hive support in a custom session.

### SQL files and the SQL shell

```bash
docker compose exec spark-master spark-sql -e"SELECT 1 AS ok"
docker compose exec -T spark-master spark-sql -f /opt/spark-apps/report.sql
```

Interactive:

```bash
make sql
```

---

## Execution internals

### Parallelism

Parallelism is limited by:

- Number of partitions.
- Number of task slots (cores available to the application).
- Dependencies between stages.

This cluster has 4 cores, so at most 4 tasks run at once. If a stage has 100 partitions, it runs in waves of 4. If it has 2 partitions, 2 cores sit idle.

Relevant settings:

| Setting | This stack | Meaning |
|---|---|---|
| `spark.default.parallelism` | Not set; standalone default is total cores (4) | Default partition count for RDDs and some operations |
| `spark.sql.shuffle.partitions` | `8` | Partition count for SQL shuffles before AQE adjusts it |
| `spark.sql.files.maxPartitionBytes` | Spark default (128 MB) | Target maximum bytes per input partition |
| `spark.executor.cores` | `1` | Tasks per executor at once |
| `spark.executor.memory` | `2g` | Heap per executor |

### Shuffle

A shuffle redistributes data so that records with the same key land in the same partition. It is the most expensive common operation because it involves:

- Serializing map-side records.
- Writing shuffle files to local disk.
- Fetching blocks over the network.
- Merging and sorting on the reduce side.
- Spilling if memory is insufficient.

Common shuffle triggers:

- `groupBy` / `agg`
- `join` without broadcast
- `distinct` / `dropDuplicates`
- `repartition`
- Global `orderBy`
- Window functions with partitioning

Ways to reduce shuffle cost:

- Filter and project before the shuffle.
- Broadcast small tables.
- Use `reduceByKey` instead of `groupByKey` for RDDs (pre-aggregation).
- Choose a sensible shuffle partition count.
- Let AQE coalesce partitions.
- Avoid unnecessary `repartition`.
- Handle skew (see below).

### Memory model

An executor's memory is divided into regions:

- **Reserved memory** for Spark internals.
- **User memory** for your data structures and UDFs.
- **Unified memory** shared between execution and storage:
  - **Execution memory**: shuffles, joins, sorts, aggregations.
  - **Storage memory**: cached blocks.

Relevant settings:

| Setting | Default | Effect |
|---|---|---|
| `spark.memory.fraction` | 0.6 | Fraction of heap for unified execution/storage |
| `spark.memory.storageFraction` | 0.5 | Portion of unified memory reserved for storage |
| `spark.executor.memoryOverhead` | max(384 MB, 10% of executor memory) in many deployments | Off-heap/container overhead |
| `spark.memory.offHeap.enabled` | false | Enables off-heap memory if configured |

Symptoms of memory pressure:

- Spill (memory) and spill (disk) metrics in the Spark UI.
- Long GC times.
- `java.lang.OutOfMemoryError: Java heap space`.
- Executors killed by the container/host OOM killer.

Fixes:

- Increase executor memory or reduce data per task.
- Increase partitions to make tasks smaller.
- Avoid collecting large results to the driver.
- Use `MEMORY_AND_DISK` instead of `MEMORY_ONLY` for large caches.
- Remove unnecessary caches.
- Reduce shuffle data by filtering/projecting earlier.

### Caching and persistence

Caching keeps a computed dataset in memory or on disk so repeated actions do not recompute it.

```python
filtered = sales.filter("region ='East'").cache()
filtered.count()          # materializes the cache
filtered.groupBy("product").count().show()
filtered.unpersist()
```

Storage levels:

| Level | Meaning |
|---|---|
| `MEMORY_ONLY` | Cache in memory; recompute evicted partitions |
| `MEMORY_AND_DISK` | Spill evicted partitions to disk |
| `DISK_ONLY` | Cache on disk |
| `MEMORY_ONLY_SER` | Serialized objects in memory; less space, more CPU |
| `MEMORY_AND_DISK_SER` | Serialized memory plus disk |
| `OFF_HEAP` | Off-heap memory (requires configuration) |

RDD `cache()` uses `MEMORY_ONLY`. DataFrame `cache()` uses the dataset default, typically `MEMORY_AND_DISK`.

Cache only when:

- The same dataset is used by multiple actions.
- The cost of recomputation is high.
- The dataset fits (or mostly fits) in cluster memory.

Do not cache one-pass datasets; it adds memory pressure without reuse.

### Serialization

| Path | Format | Notes |
|---|---|---|
| JVM shuffle/cache | Kryo (configured here) | Faster and more compact than Java serialization |
| Python UDF | Pickle | Row-at-a-time, slow |
| pandas UDF | Arrow | Columnar/vectorized, much faster |
| Parquet/ORC | Columnar binary | Efficient storage and predicate pushdown |

This stack sets `spark.serializer=org.apache.spark.serializer.KryoSerializer`. Kryo is generally a good default for shuffles and caching.

### Data skew

Skew means a few partitions contain far more data than others. Symptoms:

- A handful of tasks take much longer than the rest.
- One executor spills heavily while others finish.
- A stage stays at 99% for a long time.

Fixes:

- Enable AQE skew join handling (`spark.sql.adaptive.skewJoin.enabled=true`; AQE is already on, but this specific option is worth checking).
- Broadcast the small side of a join.
- Salt hot keys: add a random suffix, aggregate, then aggregate again.
- Repartition by a better key.
- Pre-aggregate before joins.
- Separate hot-key processing from the rest.

### Small files

Many tiny files create many tasks and metadata overhead. Symptoms:

- Large numbers of tasks with tiny input sizes.
- Slow listing and planning.
- Poor compression.

Fixes:

- Compact files with a rewrite job.
- Use `coalesce`/`repartition` before writing, but not to a single file for large data.
- Partition by columns with moderate cardinality, not by user ID or timestamp at second granularity.
- Target file sizes in the tens to hundreds of MB for analytical tables.

### Fault tolerance and retries

- `spark.task.maxFailures` (default 4) controls task retries.
- A lost executor causes its tasks and cached blocks to be recomputed.
- A lost shuffle map output can force recomputation of the map stage.
- Checkpointing cuts long lineages and is required for some streaming stateful operations.
- Speculative execution can relaunch stragglers; it is off by default and can help with slow nodes, but it adds resource pressure.

### Scheduling

Within an application, Spark schedules jobs in **FIFO** order by default. You can use FAIR scheduling pools for multi-tenant workloads:

```python
spark.conf.set("spark.scheduler.mode","FAIR")
```

This stack does not configure pools; for a single user, FIFO is fine. The standalone master schedules applications against available cluster resources; a large application can occupy all 4 cores.

## This installation

### Project layout

```text
spark-docker/
├── docker-compose.yml          # services, images, ports, mounts
├── Dockerfile.spark            # custom Spark 4.2.1 image on Python 3.13
├── .dockerignore
├── .env                        # worker capacity, Jupyter token, timezone
├── Makefile                    # shortcuts for common commands
├── README.md                   # quick overview
├── apps/                       # job code, jars, helper zips
│   ├── pi.py
│   ├── sales_report.py
│   └── wordcount.py
├── conf/
│   └── spark-defaults.conf     # shared Spark configuration
├── data/
│   ├── sales.csv               # 48-row sample
│   ├── words.txt               # sample text
│   └── output/                 # generated results
├── docs/                       # guides, including this one
├── events/                     # Spark event logs for History Server
└── notebooks/
    └── 00_spark_quickstart.ipynb
```

### Services and URLs

| Service | Container | Image | Host URL / port | Role |
|---|---|---|---|---|
| `spark-master` | `spark-master` | `spark-local:4.2.1` | <http://localhost:8080>, RPC `7077`, client-mode driver UI `4041` | Standalone master |
| `spark-worker-1` | `spark-worker-1` | `spark-local:4.2.1` | <http://localhost:8081> | 2 cores / 4 GiB |
| `spark-worker-2` | `spark-worker-2` | `spark-local:4.2.1` | <http://localhost:8082> | 2 cores / 4 GiB |
| `spark-history` | `spark-history` | `spark-local:4.2.1` | <http://localhost:18080> | Completed application UIs |
| `jupyter` | `spark-jupyter` | `quay.io/jupyter/pyspark-notebook:spark-4.2.1` | <http://localhost:8888/?token=spark>, driver UI `4040` | JupyterLab + PySpark driver |

All host ports are bound to `127.0.0.1`, so they are reachable only from this machine.

### Why there is a custom Spark image

The official `apache/spark:4.2.1` image uses Python **3.10**. The Jupyter image uses Python **3.13**. PySpark refuses to run Python RDDs/UDFs when the driver and workers use different Python minor versions:

```text
[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.10
than that in driver: 3.13
```

`Dockerfile.spark` solves this by keeping the official Spark distribution but replacing the runtime:

1. Start from `python:3.13-slim-bookworm`.
2. Copy a JDK 21 from `eclipse-temurin:21-jre-jammy`.
3. Copy `/opt/spark` from `apache/spark:4.2.1`.
4. Install `passwd` and `procps`, create UID/GID 185 (`spark`), and chown `/opt/spark`.
5. Install pandas, pyarrow, and numpy pinned to the versions in the Jupyter image.
6. Run as the unprivileged `spark` user.

The image is built and tagged locally as `spark-local:4.2.1`. Compose builds it automatically when needed:

```bash
docker compose build
docker compose up -d --force-recreate spark-master spark-worker-1 spark-worker-2 spark-history
```

The image has no custom entrypoint; Compose passes the full `spark-class` command. That keeps the container behavior explicit and avoids hidden wrapper logic.

### UID, GID, and permissions

| Container | User | UID/GID | Notes |
|---|---|---|---|
| Spark services | `spark` | 185:185 | Matches the official Spark image's user |
| Jupyter | `jovyan` | 1000:100 | Host user is typically UID 1000 |
| History Server | `spark` | 185:185 plus supplementary GID 100 | Can read notebook event logs owned by 1000:100 |

Development conveniences in this stack:

- `data/` and `events/` roots are world-writable.
- `spark.hadoop.fs.permissions.umask-mode=000` makes job output world-readable/writable.
- Event-log directories are mode `770`; the History Server joins group 100.
- `make clean-output` runs as root in a one-off container so it can delete files created by either UID.

These are **development** settings. Do not use world-writable data directories in production. Use matching UIDs/GIDs, proper umasks, and least-privilege permissions.

### Networking

Compose creates a bridge network named `spark-net`. Containers resolve each other by service name:

- `spark-master:7077` — master RPC
- `spark-master:8080` — master UI
- `spark-worker-1:8081`, `spark-worker-2:8081` — worker UIs inside the network
- `spark-history:18080` — History Server
- `jupyter:8888` — Jupyter

The Jupyter driver must advertise an address the workers can reach:

```python
.config("spark.driver.host","jupyter")
.config("spark.driver.bindAddress","0.0.0.0")
```

Without this, the driver may advertise `localhost` or a hostname that workers cannot resolve, and jobs hang or fail with `Connection refused` / `Lost executor`.

### Volumes

| Host path | Container path | Mode | Contents |
|---|---|---|---|
| `./conf` | `/opt/spark-conf` | read-only | `spark-defaults.conf`; `SPARK_CONF_DIR` points here |
| `./apps` | `/opt/spark-apps` | read-only | Python jobs, jars, helper zips |
| `./data` | `/opt/spark-data` | read/write | Inputs, outputs, warehouse, checkpoints |
| `./events` | `/opt/spark-events` | read/write | Event logs read by History Server |
| `./notebooks` | `/home/jovyan/work` | read/write | Notebooks, persisted on the host |

The `:z` suffix relabels the bind mount for SELinux. Fedora with SELinux enforcing requires it (or an equivalent policy).

### Resource math

- 2 workers × 2 cores = **4 cores**.
- 2 workers × 4 GiB = **8 GiB** of executor capacity.
- Default executor: 1 core / 2 GiB.
- Full application: up to **4 executors** (2 per worker).
- Driver: `spark.driver.memory=1g` for JVM drivers; the Jupyter Python kernel is separate.
- Daemons: master, two workers, and History Server each use `SPARK_DAEMON_MEMORY=1g`.
- Jupyter plus the driver typically uses another 1–2 GiB.

Peak memory is roughly **12 GiB**, so 16 GiB of host RAM is comfortable; 8 GiB requires lowering worker memory or running one worker.

### Startup and health

Compose starts the master first. Workers and Jupyter wait for the master health check. The master and History Server have HTTP health checks; Jupyter has the health check built into its image; workers have no Compose health check.

```bash
docker compose ps
docker compose logs -f --tail=100 spark-master
```

Workers register with the master within a few seconds. If they do not appear, check the worker logs and name resolution.

### Event logs and History Server

Cluster jobs inherit:

```text
spark.eventLog.enabled=true
spark.eventLog.dir=file:///opt/spark-events
```

Notebook sessions must enable event logging in code. Event logs are written as rolling directories such as `events/eventlog_v2_app-20261103102812-0005/`. The History Server reads the same directory via:

```text
spark.history.fs.logDirectory=file:///opt/spark-events
```

Completed applications appear at <http://localhost:18080>. The stack retains 50 applications in memory and cleans event logs older than 7 days. `make clean-output` deletes all event logs.

### What is not included

- High availability for the master (no ZooKeeper recovery).
- Authentication, TLS, or Spark ACLs.
- An external Hive metastore.
- Kafka, S3, HDFS, or a database service.
- Prometheus/Grafana or a full observability stack.
- A production scheduler or multi-tenant security model.

---

## Daily workflow

### Start, inspect, stop

```bash
cd ~/spark-docker

sudo systemctl start docker      # Docker is not enabled at boot
docker compose up -d             # build/pull if needed, then start
docker compose ps                # status and health
docker compose logs -f --tail=100
docker compose down              # stop and remove containers; host data remains
```

### Make targets

| Command | What it does |
|---|---|
| `make help` | List targets |
| `make up` | `docker compose up -d` |
| `make down` | `docker compose down` |
| `make restart` | Restart containers |
| `make ps` | Container status |
| `make logs` | Follow logs |
| `make master-ui` | Open the master UI with `xdg-open` |
| `make history-ui` | Open the History Server UI |
| `make jupyter-url` | Print the Jupyter URL with the token from `.env` |
| `make shell` | Bash in `spark-master` |
| `make pyspark` | PySpark REPL connected to the cluster |
| `make sql` | Spark SQL REPL |
| `make submit APP=file.py` | Submit a job from `apps/` (default: `sales_report.py`) |
| `make pi` | Monte Carlo Pi |
| `make wordcount` | Word count |
| `make sales` | Sales report |
| `make test` | Run Pi, word count, and sales report sequentially |
| `make clean-output` | Delete `data/output`, `data/warehouse`, and event logs |

### Verify the installation

```bash
docker compose exec spark-master spark-submit --version
docker compose exec spark-master python3 --version
docker compose exec jupyter python -c"import pyspark, sys; print(pyspark.__version__, sys.version)"
```

Expected: Spark 4.2.1, Java 21, Python 3.13.x in both places. The Spark master UI should show two workers, each with 2 cores and 4096 MB.

### Run the sample jobs

```bash
make test
```

This runs:

- `apps/pi.py` — RDD-based Monte Carlo estimation.
- `apps/wordcount.py` — RDD word count over `data/words.txt`.
- `apps/sales_report.py` — DataFrame aggregation over `data/sales.csv`.

Outputs appear under `data/output/`; completed applications appear in the History Server.

### Stop and clean

```bash
make clean-output       # delete generated output, warehouse, and event logs
docker compose down     # stop containers; source data and notebooks remain
```

`make clean-output` does not delete `data/sales.csv`, `data/words.txt`, notebooks, or apps.

---

## Running jobs

### `spark-submit` anatomy

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name"My job"\
  --executor-memory 2g \
  --executor-cores 1 \
  --total-executor-cores 4 \
  --conf spark.sql.shuffle.partitions=8 \
  /opt/spark-apps/my_job.py arg1 arg2
```

The application path comes after all Spark options. Arguments after the application path go to your program.

### Common options

| Option | Meaning |
|---|---|
| `--master` | Cluster manager URL; `spark://spark-master:7077` here |
| `--deploy-mode` | `client` (default) or `cluster` |
| `--name` | Application name in the UIs |
| `--executor-memory` | Heap per executor |
| `--executor-cores` | Cores per executor |
| `--total-executor-cores` | Cap total cores for the application (standalone) |
| `--driver-memory` | Driver heap (mainly cluster mode / JVM drivers) |
| `--conf k=v` | Any Spark configuration |
| `--jars` | Extra JVM jars |
| `--packages` | Maven coordinates to download |
| `--py-files` | Python files/zip for driver and executors |
| `--files` | Files distributed to executors |
| `--archives` | Archives extracted on executors (venvs, native libs) |
| `--verbose` | Print resolved configuration and classpath |

### Client vs cluster mode

| | Client mode | Cluster mode |
|---|---|---|
| Driver runs in | Submitting container | A worker chosen by the master |
| Terminal output | Streamed | Not streamed; check worker/History |
| Driver UI | Submitting container's 4040 | Worker's 4040, not published here |
| Best for | Interactive work, notebooks | Production-style submissions |
| Script/data access | Submitting container mounts | Worker mounts must contain the files |

In this stack, `--deploy-mode client` from `spark-master` is the normal choice. The driver UI is available at <http://localhost:4041> because the master container maps host 4041 to container 4040.

For cluster mode, the driver runs on a worker and its UI is **not** mapped to the host. Use the History Server after completion, or add a port mapping to the worker if you really need the live driver UI. Application code and data must be under `/opt/spark-apps` and `/opt/spark-data`, which are mounted in every Spark container.

### Submit from `spark-master`

```bash
docker compose exec spark-master \
  spark-submit \
  --master spark://spark-master:7077 \
  --name"Sales report"\
  /opt/spark-apps/sales_report.py
```

### Submit from Jupyter

The driver is in the Jupyter container, so set its reachable hostname and bind address:

```bash
docker compose exec jupyter spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=jupyter \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///opt/spark-events \
  /opt/spark-apps/pi.py 8
```

### Python dependencies

- For pure-Python modules, use `--py-files deps.zip`.
- For binary packages used by executors, add them to `Dockerfile.spark` and rebuild.
- Installing a package only in Jupyter makes it available to the driver, not to Python UDFs on executors.

```bash
cd apps
zip -r deps.zip mypackage/
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark-apps/deps.zip \
  /opt/spark-apps/my_job.py
```

### JVM dependencies

Use `--jars` for local jars or `--packages` for Maven artifacts. Spark 4 uses Scala 2.13, so use `_2.13` artifacts:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-avro_2.13:4.2.1 \
  /opt/spark-apps/job.py
```

`--packages` requires network access from the container. For repeatable production images, bake jars into a custom image instead of downloading at submission time.

### Scala or Java application

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --class com.example.Main \
  /opt/spark-apps/my-app.jar
```

### Spark SQL

```bash
docker compose exec spark-master spark-sql -e"SELECT 1 AS ok"
docker compose exec -T spark-master spark-sql -f /opt/spark-apps/report.sql
```

Interactive SQL:

```bash
make sql
```

### Interactive shells

```bash
make pyspark    # PySpark REPL
make sql        # Spark SQL REPL
make shell      # bash in spark-master

docker compose exec spark-master spark-shell --master spark://spark-master:7077
```

### Monitoring a submission

- Driver UI: <http://localhost:4041> for client-mode jobs from `spark-master`.
- Jupyter driver UI: <http://localhost:4040>.
- Master UI: <http://localhost:8080>.
- Worker UIs: <http://localhost:8081>, <http://localhost:8082>.
- History Server: <http://localhost:18080> after completion.
- Logs: `docker compose logs -f spark-master spark-worker-1 spark-worker-2`.

Only one application can bind port 4040 in a container. If it is busy, Spark picks 4041 or later inside that container, but those ports are not published. Stop the other session or use the History Server.

### Canceling a job

Use the master UI's application page, or the standalone submission commands:

```bash
docker compose exec spark-master spark-submit --master spark://spark-master:7077 --status <submission-id>
docker compose exec spark-master spark-submit --master spark://spark-master:7077 --kill <submission-id>
```

You can also stop the container running the driver, but that is heavier-handed. For a notebook, call `spark.stop()` or restart the kernel.

---

## JupyterLab

### Open it

<http://localhost:8888/?token=spark>

The token comes from `JUPYTER_TOKEN` in `.env`. Print it with:

```bash
grep JUPYTER_TOKEN .env
```

### Notebook location

`./notebooks` on the host is `/home/jovyan/work` in the container. The included quickstart is `notebooks/00_spark_quickstart.ipynb`.

The Jupyter container sets `SPARK_HOME=/usr/local/spark` and a `PYTHONPATH` that includes the bundled PySpark and py4j libraries, so `import pyspark` works in kernels and terminals. It also sets `SPARK_MASTER=spark://spark-master:7077` as a convenience variable, but a `SparkSession` still needs `.master(...)` explicitly (or a Jupyter-side Spark configuration).

### Create a cluster session

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("My analysis")
    .master("spark://spark-master:7077")
    .config("spark.driver.host","jupyter")
    .config("spark.driver.bindAddress","0.0.0.0")
    .config("spark.eventLog.enabled","true")
    .config("spark.eventLog.dir","file:///opt/spark-events")
    .config("spark.sql.warehouse.dir","file:///opt/spark-data/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

| Setting | Why |
|---|---|
| `master("spark://spark-master:7077")` | Use the standalone cluster, not a local process |
| `spark.driver.host=jupyter` | Workers can resolve and connect back to the driver |
| `spark.driver.bindAddress=0.0.0.0` | Listen on all container interfaces |
| `spark.eventLog.*` | Make the app visible in the History Server |
| `spark.sql.warehouse.dir` | Keep managed table data in the shared data folder |

### Kernel lifecycle

- Create the session once and reuse it.
- Configuration that affects the connection is fixed after the session starts. To change it, call `spark.stop()`, restart the kernel, and create a new session.
- `spark.stop()` releases the driver UI port.
- If you start a second session while the first is active, the second may pick a different internal UI port that is not published.
- Restarting the Jupyter container loses the kernel state but keeps notebooks on the host.

### Python and pandas UDFs

Row-at-a-time Python UDF:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def category(product: str) -> str:
    return"premium"if product in {"Laptop","Phone"} else"standard"

sales.withColumn("category", category("product")).show(5)
```

Vectorized pandas UDF:

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

@pandas_udf(DoubleType())
def with_tax(amount: pd.Series) -> pd.Series:
    return amount * 1.14

sales.withColumn(
"revenue_with_tax",
    with_tax(F.col("units") * F.col("unit_price")),
).show(5)
```

Both run on the executors. The custom image includes pandas, pyarrow, and numpy so these examples work without extra installation. The driver in Jupyter has its own pandas/pyarrow/numpy stack, pinned to the same versions in this setup.

### Driver packages vs executor packages

| Where you install | Who gets it | Persistence |
|---|---|---|
| `docker compose exec jupyter pip install ...` | Jupyter driver only | Until the container is recreated |
| Custom Jupyter image | Jupyter driver | Persists in the image |
| `Dockerfile.spark` | Spark driver and executors | Persists in the Spark image |
| `--py-files` | Driver and executors for one app | Per submission |

If a UDF imports a package, install it in `Dockerfile.spark`; installing it in Jupyter is not enough for the executors.

### Headless notebook execution

```bash
docker compose exec jupyter bash -c \
'cd /home/jovyan/work && jupyter nbconvert --to notebook --execute --inplace \
   00_spark_quickstart.ipynb --ExecutePreprocessor.timeout=240'
```

This is useful for smoke tests and CI. It writes outputs into the notebook file.

### Jupyter tips

- Use `display(df)` for a rich table; `df.show()` for plain text.
- Use `df.explain("formatted")` to inspect the plan.
- Avoid `collect()` on large results; write to Parquet or aggregate first.
- Use `spark.sql(...)` after `createOrReplaceTempView`.
- Close idle kernels; each active driver consumes host memory.
- If the kernel dies, check host memory with `free -h` and the Jupyter container logs.

---

## Working with data

### Paths

Always use the container paths in Spark code:

| Container path | Host path | Use |
|---|---|---|
| `/opt/spark-apps` | `./apps` | Code and jars (read-only) |
| `/opt/spark-data` | `./data` | Inputs and outputs (read/write) |
| `/opt/spark-events` | `./events` | Event logs |
| `/home/jovyan/work` | `./notebooks` | Notebooks |

Do not use host paths such as `/home/elhussieny/spark-docker/data` inside Spark; the containers do not see them.

### Read CSV

```python
sales = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/opt/spark-data/sales.csv")
)
```

For production, provide an explicit schema instead of `inferSchema` to avoid an extra pass and unstable types:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("region", StringType(), True),
    StructField("product", StringType(), True),
    StructField("units", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
])

sales = spark.read.option("header", True).schema(schema).csv("/opt/spark-data/sales.csv")
```

### Write Parquet

```python
sales.write.mode("overwrite").parquet("/opt/spark-data/output/sales_parquet")
```

Partitioned output:

```python
sales.write.mode("overwrite").partitionBy("region") \
    .parquet("/opt/spark-data/output/sales_by_region")
```

Partition columns are encoded in the directory names and are not stored inside the files. Partition only by columns with moderate cardinality that you filter on.

### Read Parquet

```python
back = spark.read.parquet("/opt/spark-data/output/sales_parquet")
back.printSchema()
```

Parquet supports predicate pushdown and column pruning, so filters and column selection can avoid reading unnecessary data.

### JSON

```python
events = spark.read.json("/opt/spark-data/events.json")
events.write.mode("overwrite").json("/opt/spark-data/output/events_json")
```

For multi-line JSON, add `.option("multiLine", True)`. For streaming files, use `readStream`.

### Temporary views and SQL

```python
sales.createOrReplaceTempView("sales")
spark.sql("SELECT region, COUNT(*) AS rows FROM sales GROUP BY region").show()
```

### Save a table

```python
sales.write.mode("overwrite").saveAsTable("sales_table")
spark.sql("SELECT * FROM sales_table LIMIT 5").show()
```

The table uses the session catalog and the warehouse directory. Without an external metastore, metadata does not survive a new Spark session; the Parquet data may remain under `data/warehouse`.

### JDBC

```python
jdbc = (
    spark.read
    .format("jdbc")
    .option("url","jdbc:postgresql://db-host:5432/mydb")
    .option("dbtable","public.sales")
    .option("user","user")
    .option("password","secret")
    .load()
)
```

You need the JDBC driver jar on the driver and executors (`--jars` or a custom image), plus network access. Do not put production passwords in notebooks; use environment variables or a secret manager.

### S3, HDFS, and object stores

These are not included. To use S3-compatible storage, add `hadoop-aws` and the AWS SDK bundle, configure credentials, and use `s3a://...` paths. To use HDFS, point Spark at an HDFS cluster and ensure network access. Local files are the default in this stack.

### Output permissions and cleanup

- Spark writes as UID 185; Jupyter writes as UID 1000.
- Output files are world-readable/writable because of the development umask.
- Event-log directories are mode `770`.
- Remove generated files with `make clean-output`, or as root in a one-off container.

```bash
docker compose run --rm --no-deps --user root --entrypoint /bin/bash spark-master \
  -c'rm -rf /opt/spark-data/output/my_result'
```

## Performance tuning

### Golden rules

1. **Filter early.** Reduce rows before joins, aggregations, and shuffles.
2. **Select only needed columns.** Columnar formats can skip the rest.
3. **Use Parquet/ORC for repeated analytics.** Avoid repeatedly parsing CSV/JSON.
4. **Avoid `collect()` on large data.** Keep results distributed and write them out.
5. **Do not shuffle unnecessarily.** Every `repartition`, `distinct`, and large join has a cost.
6. **Broadcast small tables.** Avoid shuffling a large fact table against a tiny dimension.
7. **Cache only reused data.** One-pass datasets do not benefit.
8. **Prefer built-in functions over Python UDFs.** Use pandas UDFs when Python logic is required.
9. **Handle skew.** A single hot key can dominate a stage.
10. **Measure before and after.** Use the Spark UI, not intuition.

### Sizing this cluster

The current allocation:

```text
2 workers x 2 cores = 4 cores
2 workers x 4 GiB = 8 GiB executor capacity
executor = 1 core + 2 GiB
=> up to 4 executors, 4 concurrent tasks
```

For most local workloads, this is a good balance. If you increase executor size:

- `spark.executor.cores=2`, `spark.executor.memory=4g` gives one executor per worker, two total, and two tasks per executor.
- Fewer, larger executors reduce per-executor overhead but increase the impact of a single executor failure and can worsen skew.
- Many small executors increase parallelism but add JVM overhead.

Keep `spark.executor.memory` below the worker's advertised memory, leaving room for overhead and the worker JVM.

### Choose the right number of partitions

Rules of thumb:

- For CPU-bound work: **2–4 tasks per core** gives good utilization without excessive overhead. With 4 cores, 8–16 partitions is a reasonable starting point.
- For I/O-bound work: more partitions can help hide latency.
- For tiny data: fewer partitions avoid scheduling overhead.
- For shuffle-heavy work: start with `spark.sql.shuffle.partitions=8` here, then let AQE coalesce.

Avoid:

- One partition for a large dataset (one task does all the work).
- Thousands of tiny partitions (scheduling and filesystem overhead).
- `coalesce(1)` for large outputs (single writer, likely OOM/slow).

### AQE tuning

```python
spark.conf.set("spark.sql.adaptive.enabled","true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled","true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled","true")
```

AQE uses runtime statistics, so it can fix bad estimates. It is not a substitute for good data layout and join design.

### Broadcast joins

```python
from pyspark.sql.functions import broadcast
result = big.join(broadcast(small),"id")
```

Use when the small side is genuinely small (well under the broadcast threshold, accounting for compressed and in-memory size). Broadcasting a large table can cause executor OOM.

### Join ordering and keys

- Join on the same type and distribution to avoid unnecessary casts/shuffles.
- Filter both sides before the join.
- Avoid joining on skewed keys without a plan.
- Use bucketing for repeated large joins only if you understand the table-writing implications; it is not configured here.
- Check the physical plan for `Exchange` and `BroadcastHashJoin`.

### Caching strategy

```python
df = spark.read.parquet("/opt/spark-data/output/events").filter("year = 2026")
df.cache()
df.count()   # force materialization
# multiple actions...
df.unpersist()
```

Cache if the cost of recomputation is high and the data is reused. Monitor the Storage tab for cached size and evictions. If cached data does not fit, use `MEMORY_AND_DISK` and expect disk I/O.

### UDF performance

Performance order, fastest to slowest:

1. Built-in SQL/DataFrame functions.
2. Scala/Java UDFs.
3. pandas UDFs (Arrow, vectorized).
4. Python row-at-a-time UDFs.

Python UDFs serialize every row between the JVM and Python. For large data, that dominates. Use pandas UDFs for vectorized Python logic and built-ins wherever possible.

### Skew mitigation

If one key dominates:

```python
# Example: salt a hot key, aggregate twice
salted = df.withColumn("salt", (F.rand() * 10).cast("int"))
partial = salted.groupBy("key","salt").agg(F.sum("value").alias("value"))
result = partial.groupBy("key").agg(F.sum("value").alias("value"))
```

This increases parallelism for the hot key at the cost of a second aggregation. Use it only when the skew is real and measured.

### I/O tuning

- Use Parquet with Snappy or Zstd compression.
- Partition by low-cardinality filter columns (date, region), not by high-cardinality IDs.
- Avoid small files; compact them.
- Use predicate pushdown and column pruning.
- For CSV, provide a schema and avoid `inferSchema` on huge files.
- For JSON, avoid deeply nested explosive schemas if you do not need them.

### Memory and GC

Watch for:

- Spill (memory/disk) in the Stages tab.
- High GC time in the Executors tab.
- Executor loss/OOM.
- Long tasks with little input.

Fixes:

- More partitions, smaller tasks.
- Less cached data.
- Lower `spark.sql.shuffle.partitions` if there are too many tiny partitions (AQE may already do this).
- Increase executor memory if the host has room.
- Avoid wide rows and unnecessary string columns.
- Use `spark.sql.execution.arrow.pyspark.enabled=true` for faster pandas conversion (if not already enabled).

### Measuring

- Spark UI **Stages** tab: input size, shuffle read/write, spill, task duration distribution.
- **SQL** tab: plan, metrics per node, AQE plan changes.
- **Executors** tab: memory, disk, GC, task counts.
- `df.explain("formatted")`.
- `spark.sparkContext.statusTracker()` for programmatic job/stage status.
- Compare wall-clock time and resource usage before/after a change.

### Anti-patterns

- `collect()` a large DataFrame into the driver.
- `toPandas()` on a large dataset.
- `repartition(1)` or `coalesce(1)` for large writes.
- Repeated actions on an uncached DataFrame that is expensive to compute.
- Python UDFs for simple arithmetic.
- Joining two large tables without filtering.
- Caching everything.
- Using CSV as the storage format for repeated analytics.
- Ignoring skew because"the job finishes eventually."
- Setting `spark.sql.shuffle.partitions=1` to"make it faster."

---

## Monitoring and debugging

### The UIs

| UI | URL | What to look at |
|---|---|---|
| Master | <http://localhost:8080> | Workers, resources, running/completed applications |
| Worker 1 | <http://localhost:8081> | Executors, logs, resource use |
| Worker 2 | <http://localhost:8082> | Same for worker 2 |
| Jupyter driver | <http://localhost:4040> | Jobs, stages, SQL, storage for notebook apps |
| Master driver (client mode) | <http://localhost:4041> | Driver UI for jobs submitted from `spark-master` |
| History | <http://localhost:18080> | Completed applications from event logs |

### Spark UI tabs

- **Jobs**: one row per action; status, duration, stages.
- **Stages**: task-level detail; input/output size, shuffle read/write, spill, task duration.
- **Storage**: cached RDDs/DataFrames, size, fraction cached.
- **Environment**: effective Spark properties, classpath, JVM/system info.
- **Executors**: per-executor memory, disk, task counts, GC time, logs.
- **SQL / DataFrame**: query plans, metrics per operator, AQE changes.

### History Server

The History Server parses event logs. It is the right place for completed applications, especially cluster-mode jobs whose driver UI is not published. If an application is missing:

- Confirm event logging was enabled for that session.
- Confirm `spark.eventLog.dir` points to `/opt/spark-events`.
- Check `events/` permissions and the History Server logs.
- Wait a few seconds; the server polls for new logs.

### Logs

```bash
docker compose logs -f --tail=100 spark-master
docker compose logs -f --tail=100 spark-worker-1 spark-worker-2
docker compose logs -f --tail=100 spark-history
docker compose logs -f --tail=100 jupyter
```

In cluster mode, the driver runs on a worker. Its output is written under the worker's `/opt/spark/work` directory; find it with:

```bash
docker compose exec spark-worker-1 find /opt/spark/work -maxdepth 4 -type f | head
```

### REST endpoints

```bash
curl -s http://localhost:8080/json/ | python3 -m json.tool
curl -s http://localhost:18080/api/v1/applications | python3 -m json.tool
```

The master JSON shows workers and applications. The History API lists completed applications; append `/<app-id>` for details.

### Container and host metrics

```bash
docker stats --no-stream
free -h
df -h /home
docker compose ps
docker inspect --format'{{.Name}} {{.State.Health.Status}}'spark-master spark-history
```

### Diagnosing a slow job

1. Open the Stages tab. Is one task much slower than the rest? → skew.
2. Is there a large shuffle read/write? → wide transformation; consider broadcast/filtering.
3. Is there spill? → memory pressure or too few partitions.
4. Is the input size much larger than expected? → missing filter/partition pruning.
5. Are there many tiny tasks? → too many partitions/small files.
6. Is GC time high? → memory pressure or large objects.
7. Is the SQL plan doing a sort-merge join where a broadcast would work? → stats/hint.
8. Is the driver the bottleneck? → `collect()`/`toPandas()` or a huge result.

### Canceling and cleaning up

- Master UI → application → kill.
- `spark-submit --kill <submission-id> --master spark://spark-master:7077`.
- `spark.stop()` in a notebook, or restart the kernel.
- `docker compose restart` for a stuck service.
- `make clean-output` for generated data and event logs.

### A note about the native Hadoop warning

You may see:

```text
WARN NativeCodeLoader: Unable to load native-hadoop library for your platform...
```

This is a performance warning about optional native Hadoop libraries. Spark falls back to Java implementations and continues normally. It is not the cause of a failed job.

---

## Modifying the stack

This section is a cookbook. Each recipe names the file to edit and the command to apply the change.

### Before you edit

- Configuration in `conf/` is mounted read-only into the Spark containers. Edit it on the host.
- `.env` is read by Docker Compose, not automatically exported into your shell.
- Changes to `Dockerfile.spark` require a rebuild.
- Changes to `conf/`, `.env`, ports, or mounts require recreating the affected containers.
- Jupyter does **not** mount `conf/spark-defaults.conf`; it uses its own Spark installation. Set notebook configs in code or build a custom Jupyter image.

Apply a change:

```bash
docker compose build                 # only if the image changed
docker compose up -d --force-recreate <services>
docker compose ps
```

### Change worker capacity

Edit `.env`:

```dotenv
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=6g
```

Recreate the workers:

```bash
docker compose up -d --force-recreate spark-worker-1 spark-worker-2
```

Then adjust `spark.executor.memory` and `spark.executor.cores` in `conf/spark-defaults.conf` if needed. Executors are carved out of the worker's advertised resources. Do not exceed host RAM.

### Add a worker

Copy the `spark-worker-2` service block in `docker-compose.yml`, change:

- `container_name` to `spark-worker-3`
- `hostname` to `spark-worker-3`
- host UI port to `8083`
- `depends_on` remains the master health check

```yaml
  spark-worker-3:
    <<: *spark-common
    container_name: spark-worker-3
    hostname: spark-worker-3
    command:
      - /opt/spark/bin/spark-class
      - org.apache.spark.deploy.worker.Worker
      - spark://spark-master:7077
      - --host
      - spark-worker-3
      - --cores
      -"${SPARK_WORKER_CORES:-2}"
      - --memory
      -"${SPARK_WORKER_MEMORY:-4g}"
      - --webui-port
      -"8081"
    environment:
      <<: *spark-env
      SPARK_DAEMON_MEMORY: 1g
    ports:
      -"127.0.0.1:8083:8081"
    depends_on:
      spark-master:
        condition: service_healthy
```

Then:

```bash
docker compose up -d
```

The new worker appears in the master UI within a few seconds. Check host RAM before adding capacity; each worker also runs a JVM daemon.

### Change driver and executor defaults

Edit `conf/spark-defaults.conf`:

```text
spark.driver.memory       1g
spark.executor.memory     2g
spark.executor.cores      1
spark.sql.shuffle.partitions 8
```

Restart the Spark services:

```bash
docker compose restart spark-master spark-worker-1 spark-worker-2 spark-history
```

For a one-off job, override on the command line:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 3g \
  --executor-cores 2 \
  /opt/spark-apps/sales_report.py
```

`spark.driver.memory` applies to JVM drivers. In Jupyter, the Python kernel is a separate process and is not resized by that setting; use container memory limits/host resources for the kernel.

### Add a Python library for executors

Edit `Dockerfile.spark` **before** `USER spark`:

```dockerfile
RUN pip install --no-cache-dir scikit-learn
```

Rebuild and recreate:

```bash
docker compose build
docker compose up -d --force-recreate spark-master spark-worker-1 spark-worker-2 spark-history
```

The library is available to jobs submitted in `spark-master` and to Python UDFs on the executors. It is not automatically installed in the Jupyter driver; add it there too if the driver imports it.

### Add a Jupyter-only library

Temporary (lost when the container is recreated):

```bash
docker compose exec jupyter pip install seaborn
```

Persistent: create `Dockerfile.jupyter`:

```dockerfile
FROM quay.io/jupyter/pyspark-notebook:spark-4.2.1
RUN pip install --no-cache-dir seaborn
```

Build and switch the image:

```bash
docker build -f Dockerfile.jupyter -t spark-jupyter-custom:4.2.1 .
```

Then change the `jupyter` service's `image:` in `docker-compose.yml` to `spark-jupyter-custom:4.2.1` and:

```bash
docker compose up -d --force-recreate jupyter
```

Remember: a package installed only in Jupyter is driver-side. Python UDFs execute on the Spark workers.

### Add a JVM dependency

One-off Maven package:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-avro_2.13:4.2.1 \
  /opt/spark-apps/job.py
```

Local jar:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-apps/my-lib.jar \
  /opt/spark-apps/job.py
```

For a permanent dependency, add the jar to the image (copy it in `Dockerfile.spark`) or set `spark.jars.packages` in `conf/spark-defaults.conf`. Use Scala 2.13 artifacts with Spark 4.

### Add an OS package

Edit the `apt-get install` line in `Dockerfile.spark`, before the `USER spark` line:

```dockerfile
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends passwd procps curl; \
    rm -rf /var/lib/apt/lists/*
```

Rebuild and recreate the Spark services. Keep the layer small and clean the apt lists.

### Change host ports

Edit `docker-compose.yml`:

```yaml
ports:
  -"127.0.0.1:9090:8080"# master UI on host 9090
```

Recreate the service:

```bash
docker compose up -d --force-recreate spark-master
```

Check for conflicts first:

```bash
ss -ltnp | grep -E'7077|8080|8081|8082|8888|18080|4040|4041'
```

If you change the Jupyter port, update the URL and `make jupyter-url` expectations. If you expose any port beyond `127.0.0.1`, set a strong Jupyter token and understand that Spark standalone has no authentication.

### Add a bind mount

Add the volume to the shared `x-spark-common` block so all Spark services see it:

```yaml
volumes:
  - ./conf:/opt/spark-conf:ro,z
  - ./apps:/opt/spark-apps:ro,z
  - ./data:/opt/spark-data:rw,z
  - ./events:/opt/spark-events:rw,z
  - ./mydata:/opt/spark-mydata:rw,z
```

Create the host directory, set permissions so UID 185 can write, then recreate:

```bash
mkdir -p mydata
chmod a+rwX mydata
docker compose up -d --force-recreate spark-master spark-worker-1 spark-worker-2 spark-history
```

Use `:z` on SELinux systems. `:Z` creates a private label for one container; shared mounts should use `:z`.

### Change the Jupyter token

Edit `.env`:

```dotenv
JUPYTER_TOKEN=change-me
```

Recreate Jupyter:

```bash
docker compose up -d --force-recreate jupyter
```

Open `http://localhost:8888/?token=change-me`. The token is a development convenience, not a substitute for TLS and proper authentication.

### Change the timezone

Edit `.env`:

```dotenv
TZ=Europe/Berlin
```

Recreate all services:

```bash
docker compose up -d --force-recreate
```

Spark uses the JVM default timezone for timestamp display and some date functions. The container `TZ` affects logs and JVM defaults. For portability, set `spark.sql.session.timeZone` explicitly in jobs that depend on time zones.

### Add an application or jar

Put the file in `apps/` on the host. It appears at `/opt/spark-apps/` in every Spark container.

```bash
cp my_job.py apps/
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/my_job.py
```

For a jar, build it with the same Scala binary version as Spark (2.13 for Spark 4) and submit with `--class`.

### Change the Spark version

1. Check the Jupyter image tag first: <https://quay.io/repository/jupyter/pyspark-notebook?tab=tags>.
2. Check the Apache Spark base image tags: <https://hub.docker.com/r/apache/spark/tags>.
3. Update `Dockerfile.spark`:

```dockerfile
COPY --from=apache/spark:4.2.2 /opt/spark /opt/spark
```

4. Update the image tags in `docker-compose.yml`:

```yaml
x-spark-common: &spark-common
  image: spark-local:4.2.2
  build:
    context: .
    dockerfile: Dockerfile.spark

jupyter:
  image: quay.io/jupyter/pyspark-notebook:spark-4.2.2
```

5. If the new Jupyter image uses a different Python minor version, update `FROM python:X.Y-slim-bookworm` in `Dockerfile.spark` to match. The Python minor versions **must** match for RDD/UDF jobs.
6. Update the pandas/pyarrow/numpy pins if the new Jupyter image ships different versions.
7. Rebuild and recreate:

```bash
docker compose build --pull
docker compose pull jupyter
docker compose up -d --force-recreate
```

8. Clear old event logs if the Spark major/minor version changes:

```bash
make clean-output
```

9. Verify:

```bash
docker compose exec spark-master spark-submit --version
docker compose exec spark-master python3 --version
docker compose exec jupyter python -c"import pyspark, sys; print(pyspark.__version__, sys.version)"
```

### Change the Python version

The stack intentionally uses Python 3.13 to match Jupyter. To move to a different Python minor version:

1. Change `FROM python:3.13-slim-bookworm` in `Dockerfile.spark` to the target version.
2. Use a Jupyter image with the same Python minor version.
3. Rebuild the Spark image.
4. Re-run the RDD and pandas UDF checks; Python version mismatches fail at runtime, not at build time.
5. Update the package pins if wheels are unavailable for the new version.

Do not change the Spark image's Python without changing Jupyter (or vice versa). The whole point of the custom image is to keep them aligned.

### Enable Spark Connect (optional)

Spark Connect lets a thin client talk to a remote Spark server over gRPC. It is not enabled in this stack.

A minimal experiment inside the master container:

```bash
docker compose exec spark-master /opt/spark/sbin/start-connect-server.sh \
  --master spark://spark-master:7077 \
  --conf spark.connect.grpc.binding.port=15002
```

You would also need to publish port `15002` in `docker-compose.yml` and use a Spark Connect client. This is an advanced extension; the classic `spark-submit` path is simpler and is what the rest of this guide uses.

### Change event-log retention

Edit `conf/spark-defaults.conf`:

```text
spark.history.retainedApplications  50
spark.history.fs.cleaner.enabled    true
spark.history.fs.cleaner.interval   1d
spark.history.fs.cleaner.maxAge     7d
```

Restart the History Server:

```bash
docker compose restart spark-history
```

`make clean-output` removes all event logs immediately. The cleaner removes logs older than `maxAge` on its schedule.

### Use external data sources

- **JDBC**: add the driver jar with `--jars` or `--packages`; ensure network access; use `dbtable` or a subquery.
- **S3-compatible storage**: add `hadoop-aws` and the AWS SDK bundle; configure `fs.s3a.*`; use `s3a://` paths.
- **HDFS**: point at the NameNode; ensure network and Kerberos configuration if required.
- **Kafka**: use Structured Streaming with `spark-sql-kafka-0-10` and a Kafka broker; no broker is included here.

For production, put connectors and credentials in a custom image or a managed configuration, not in notebooks.

### Reset the stack

```bash
# Stop containers, keep host data
docker compose down

# Remove generated output, warehouse, and event logs
make clean-output

# Rebuild the Spark image and start fresh
docker compose build --no-cache
docker compose up -d
```

To remove the service images as well:

```bash
docker compose down --rmi all
```

Base build images (for example `apache/spark:4.2.1`, `python:3.13-slim-bookworm`, and the JDK image) remain cached. `docker builder prune` can remove build cache, but it affects all projects.

### Rebuild from scratch

If the image or containers are in an inconsistent state:

```bash
docker compose down
docker compose build --no-cache
docker compose up -d --force-recreate
docker compose ps
```

Then run `make test` and the Jupyter RDD/UDF checks again.

---

## Configuration reference

### Precedence

Spark configuration is resolved roughly in this order (highest priority first):

1. `SparkSession.builder.config(...)` or `spark.conf.set(...)` in code (for runtime-settable options).
2. `--conf` / `--executor-memory` / `--executor-cores` options on `spark-submit`.
3. `spark-defaults.conf` loaded from `$SPARK_CONF_DIR`.
4. Spark defaults compiled into the distribution.

Some options are static and must be set before the session/JVM starts (master, driver host, executor memory, deploy mode). Changing them with `spark.conf.set` after startup has no effect.

Environment variables can also matter:

| Variable | Purpose |
|---|---|
| `SPARK_HOME` | Spark installation |
| `SPARK_CONF_DIR` | Directory containing `spark-defaults.conf`; set to `/opt/spark-conf` in the Spark containers |
| `JAVA_HOME` | JDK location |
| `PYSPARK_PYTHON` | Python executable for PySpark on workers |
| `PYTHONPATH` | Python import path; Jupyter uses it for the bundled PySpark |
| `SPARK_DAEMON_MEMORY` | JVM heap for master/worker/history daemons |
| `SPARK_LOCAL_IP` | Address Spark binds to in some deployments; not set here |
| `TZ` | Container/JVM timezone |

### `conf/spark-defaults.conf`

| Setting | Value | What it does |
|---|---|---|
| `spark.master` | `spark://spark-master:7077` | Default master for submissions |
| `spark.eventLog.enabled` | `true` | Write event logs |
| `spark.eventLog.dir` | `file:///opt/spark-events` | Event-log location |
| `spark.history.fs.logDirectory` | `file:///opt/spark-events` | History Server input |
| `spark.history.ui.port` | `18080` | History UI port |
| `spark.history.retainedApplications` | `50` | In-memory app limit |
| `spark.history.fs.cleaner.enabled` | `true` | Delete old event logs |
| `spark.history.fs.cleaner.interval` | `1d` | Cleaner check interval |
| `spark.history.fs.cleaner.maxAge` | `7d` | Retention |
| `spark.driver.memory` | `1g` | Driver heap for JVM drivers |
| `spark.executor.memory` | `2g` | Executor heap |
| `spark.executor.cores` | `1` | Cores per executor |
| `spark.sql.adaptive.enabled` | `true` | Runtime query re-optimization |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Merge small shuffle partitions |
| `spark.sql.shuffle.partitions` | `8` | Initial shuffle partitions |
| `spark.sql.warehouse.dir` | `file:///opt/spark-data/warehouse` | Managed table data |
| `spark.hadoop.fs.permissions.umask-mode` | `000` | World-writable output (development) |
| `spark.serializer` | `KryoSerializer` | Shuffle/cache serialization |
| `spark.ui.port` | `4040` | Driver UI port |

### `.env`

| Variable | Default | Meaning |
|---|---|---|
| `SPARK_WORKER_CORES` | `2` | Cores offered by each worker |
| `SPARK_WORKER_MEMORY` | `4g` | Memory offered by each worker |
| `JUPYTER_TOKEN` | `spark` | Jupyter login token |
| `TZ` | `Africa/Cairo` | Container timezone |

Compose reads `.env` automatically for variable substitution. It is not a shell export file.

### Compose environment

The shared Spark environment sets:

```yaml
SPARK_CONF_DIR: /opt/spark-conf
TZ: ${TZ:-Africa/Cairo}
PATH: /opt/spark/bin:/opt/java/openjdk/bin:...
```

`SPARK_CONF_DIR` is why the mounted `conf/` is used. `PATH` makes `spark-submit`, `spark-sql`, and `spark-class` available in non-login shells. A login shell (`bash -l`) may reset `PATH`; use `bash -c` or the full `/opt/spark/bin/...` path if a command is not found.

### Key Spark options

| Option | Common value | Meaning |
|---|---|---|
| `spark.app.name` | any | Application name |
| `spark.master` | `spark://spark-master:7077` | Cluster manager |
| `spark.submit.deployMode` | `client` / `cluster` | Driver location |
| `spark.driver.memory` | `1g` | Driver heap |
| `spark.executor.memory` | `2g` | Executor heap |
| `spark.executor.cores` | `1` | Cores per executor |
| `spark.executor.instances` | not set | Static executor count (cluster managers that support it) |
| `spark.cores.max` | not set | Max total cores (standalone/YARN) |
| `spark.default.parallelism` | not set | Default RDD partition count |
| `spark.sql.shuffle.partitions` | `8` | SQL shuffle partitions |
| `spark.sql.adaptive.enabled` | `true` | AQE |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | AQE partition coalescing |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | AQE skew join |
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | Broadcast threshold |
| `spark.sql.files.maxPartitionBytes` | `128MB` | Input partition target |
| `spark.sql.ansi.enabled` | `true` in Spark 4 | ANSI behavior |
| `spark.sql.session.timeZone` | JVM default | Session timezone |
| `spark.sql.execution.arrow.pyspark.enabled` | `true` | Arrow conversion optimization |
| `spark.serializer` | `KryoSerializer` | Serializer |
| `spark.memory.fraction` | `0.6` | Unified memory fraction |
| `spark.memory.storageFraction` | `0.5` | Storage share |
| `spark.task.maxFailures` | `4` | Task retries |
| `spark.speculation` | `false` | Speculative execution |
| `spark.dynamicAllocation.enabled` | `false` | Dynamic executors (needs shuffle service) |
| `spark.shuffle.compress` | `true` | Compress shuffle output |
| `spark.shuffle.spill.compress` | `true` | Compress spills |
| `spark.reducer.maxSizeInFlight` | `48m` | Shuffle fetch window |
| `spark.shuffle.file.buffer` | `32k` | Shuffle write buffer |
| `spark.local.dir` | `/tmp` | Local scratch for shuffle/spill |
| `spark.sql.warehouse.dir` | `/opt/spark-data/warehouse` | Managed table location |
| `spark.sql.catalogImplementation` | `in-memory` | Catalog implementation |
| `spark.sql.parquet.compression.codec` | `snappy` | Parquet compression |
| `spark.sql.sources.partitionOverwriteMode` | `STATIC` | Overwrite behavior for partitioned writes |

### Check effective configuration

In a notebook or application:

```python
for key, value in sorted(spark.sparkContext.getConf().getAll()):
    print(f"{key}={value}")
```

Or:

```python
spark.conf.getAll()
```

The Spark UI **Environment** tab shows the resolved Spark properties for a running application. `spark-submit --verbose` prints the launch configuration.

### Jupyter configuration caveat

The Jupyter container has its own Spark installation at `/usr/local/spark` and its own configuration directory. It does **not** read `/opt/spark-conf/spark-defaults.conf`. For notebook sessions:

- Set master, driver host, event logging, and warehouse in `SparkSession.builder`.
- Use `spark.conf.set` for runtime options.
- Build a custom Jupyter image if you need persistent defaults.

---

## Operations

### Health checks

```bash
docker compose ps
docker inspect --format'{{.Name}} {{.State.Health.Status}}'spark-master spark-history
curl -s -o /dev/null -w'%{http_code}\n'http://localhost:8080
curl -s -o /dev/null -w'%{http_code}\n'http://localhost:18080
```

Workers have no Compose health check; check the master UI and worker logs. Jupyter's image provides a health check.

### Backups

Everything important is on the host:

| Path | Back up? | Notes |
|---|---|---|
| `notebooks/` | Yes | Your analysis |
| `apps/` | Yes | Job code |
| `data/` | Yes | Inputs and outputs; can be large |
| `conf/` | Yes | Cluster configuration |
| `events/` | Optional | Event logs; can be regenerated |
| `docker-compose.yml`, `Dockerfile.spark`, `.dockerignore`, `.env` | Yes | Stack definition |

```bash
tar czf ~/spark-docker-backup-$(date +%F).tgz \
  -C ~/spark-docker \
  docker-compose.yml Dockerfile.spark .dockerignore .env conf apps notebooks data
```

Restore by extracting the archive and running `docker compose up -d`. Container state (running kernels, master state) is not backed up; notebooks and data are.

### Upgrade Spark

See [Change the Spark version](#change-the-spark-version). Always verify:

- Spark version matches between the cluster and Jupyter.
- Python minor version matches.
- pandas/pyarrow/numpy pins are compatible.
- Existing event logs render; clear them if the Spark version changes significantly.

### Security

This is a development stack:

- Spark standalone has **no authentication** on port 7077.
- The Spark UIs have no authentication.
- Jupyter uses a single shared token.
- Ports are bound to `127.0.0.1`.
- There is no TLS or network encryption.

If you expose the stack:

- Put it behind a reverse proxy with TLS and strong authentication.
- Do not publish the master RPC port.
- Use a strong Jupyter token or an authenticating proxy.
- Restrict network access with firewall rules.
- Run containers as non-root with proper UID/GID mappings.
- Do not use world-writable data directories.

### SELinux

Fedora with SELinux enforcing needs labels on bind mounts. The Compose file uses `:z` on every mount. If you add a mount, add `:z` too. Check denials with:

```bash
sudo ausearch -m avc -ts recent
```

### Docker at boot

Docker is not enabled at boot on this machine. Start it with:

```bash
sudo systemctl start docker
```

Optionally enable it:

```bash
sudo systemctl enable docker
```

### Disk and memory

```bash
free -h
df -h /home
docker system df
docker stats --no-stream
```

Large shuffles and caches can fill the container writable layer or host disk. For heavy jobs, mount a host directory for `spark.local.dir` and monitor disk usage. `make clean-output` removes generated data and event logs but not source data.

### Cleanup

```bash
make clean-output            # generated output, warehouse, event logs
docker compose down          # stop containers; host data remains
docker compose down --rmi all  # also remove service images
docker builder prune         # optional: remove build cache (affects all projects)
```

`make clean-output` preserves `data/sales.csv`, `data/words.txt`, notebooks, and apps.

### Version alignment

Keep these aligned:

- Spark version in the custom image and the Jupyter image.
- Python minor version in the custom image and Jupyter.
- pandas/pyarrow/numpy versions if UDFs depend on them.
- Scala binary version for jars (`_2.13` for Spark 4).

A mismatch usually shows up as `ClassNotFoundException`, serialization errors, or `[PYTHON_VERSION_MISMATCH]`, not as a build error.

## Troubleshooting

This table is a quick index. Detailed procedures are in [Troubleshooting](05-troubleshooting.md).

| Symptom | Likely cause | Fix |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker is stopped | `sudo systemctl start docker` |
| `port is already allocated` | Host port in use | `ss -ltnp \| grep <port>`; stop the service or change the mapping |
| Workers do not appear in the master UI | Worker failed to start or cannot resolve the master | `docker compose logs spark-worker-1`; check `spark://spark-master:7077` |
| `[PYTHON_VERSION_MISMATCH]` | Driver/worker Python minor versions differ | Use the custom image; verify Python 3.13 on both |
| `ModuleNotFoundError` in an executor | Package missing from the worker image | Add it to `Dockerfile.spark` and rebuild |
| Notebook job hangs with no executors | Driver host is wrong | Set `spark.driver.host=jupyter`, bind `0.0.0.0` |
| `Connection refused` to the driver | Workers cannot reach the advertised driver address | Use the Docker service name, not `localhost` |
| History Server is empty | Event logging off or logs not shared | Enable `spark.eventLog.*`; check `events/` and History logs |
| `Permission denied` under `data/` or `events/` | UID mismatch (185 vs 1000) | Use `make clean-output` or a root one-off container; see permissions |
| SELinux denial | Missing `:z` on a bind mount | Add `:z`; check `ausearch` |
| Executor OOM / container killed | Too much data per task or too little memory | More partitions, less cache, lower executor size, more RAM |
| `Container name ... already in use` | Old or conflicting container | `docker ps -a --filter name=spark-`; remove the old container |
| `No space left on device` | Shuffle/cache/output filled disk | Clean outputs, prune images, mount more disk |
| `FetchFailedException` | Lost shuffle output or executor | Retry; check worker health, disk, and network |
| Version mismatch / serialization error | Spark or Scala version mismatch | Align Spark, Scala `_2.13`, and PySpark versions |
| `CAST_INVALID_INPUT` | ANSI mode rejects invalid cast | Clean the data, use `TRY_CAST`, or disable ANSI deliberately |
| Jupyter token rejected | Token changed or container not recreated | Read `.env`; recreate Jupyter |
| Kernel dies on a large job | Host/container memory exhausted | Reduce data/executor memory; close idle kernels |
| Port 4040 not reachable | Another app owns it | Stop the other session; check `uiWebUrl`; use History |
| `spark-submit: command not found` in `bash -l` | Login shell reset `PATH` | Use `bash -c`, `make`, or `/opt/spark/bin/spark-submit` |
| Custom image build fails | Network, base image, or apt/pip error | Read the build log; `docker compose build --no-cache` |
| Cluster-mode UI not on 4041 | Driver runs on a worker, its 4040 is not published | Use History Server or add a worker port mapping |
| Job is slow but succeeds | Skew, shuffle, spill, small files, or too few partitions | Inspect Stages/SQL tabs; tune partitions, joins, and caching |

### Docker daemon is not running

```bash
sudo systemctl start docker
docker info
```

Docker is not enabled at boot on this machine. Enable it if you want it to start automatically:

```bash
sudo systemctl enable docker
```

### A port is already in use

```bash
ss -ltnp | grep -E'7077|8080|8081|8082|8888|18080|4040|4041'
```

Stop the conflicting service or change the host side of the mapping in `docker-compose.yml`.

### Workers do not register

```bash
docker compose ps
docker compose logs spark-master
docker compose logs spark-worker-1
docker compose exec spark-worker-1 getent hosts spark-master
docker compose exec spark-worker-1 bash -c'exec 3<>/dev/tcp/spark-master/7077 && echo reachable'
```

Check that the worker command still points at `spark://spark-master:7077` and that service names/hostnames are unique.

### Python version mismatch

```bash
docker compose exec spark-master python3 --version
docker compose exec jupyter python3 --version
```

Both must report the same **minor** version (3.13.x here). If not, fix `FROM python:X.Y-slim-bookworm` in `Dockerfile.spark`, rebuild, and recreate. A mismatch is a runtime failure, not a build failure.

### Executor `ModuleNotFoundError`

The workers use `spark-local:4.2.1`, which includes pandas, numpy, and pyarrow but not every package in Jupyter. Add the package to `Dockerfile.spark` before `USER spark`, rebuild, and recreate. Installing it only in Jupyter is not enough for Python UDFs.

### Driver connection problems

If a notebook job hangs or workers log `Connection refused`:

- Set `spark.driver.host=jupyter`.
- Set `spark.driver.bindAddress=0.0.0.0`.
- Do not use `localhost` for the driver host; inside a worker, `localhost` is the worker itself.
- Confirm the Jupyter container is on `spark-net`.

### History Server is empty

- Cluster jobs need `spark.eventLog.enabled=true` and `spark.eventLog.dir=file:///opt/spark-events`.
- Notebook sessions must set those in code.
- `events/` must be mounted in the Spark containers and the History Server.
- Check `docker compose logs spark-history`.
- Wait a few seconds; the server polls for new logs.

### Permissions under `data/` or `events/`

Spark writes as UID 185; Jupyter writes as UID 1000. The development stack makes the roots world-writable and sets a permissive umask. To delete files owned by either UID, use:

```bash
make clean-output
```

Or remove a single path as root:

```bash
docker compose run --rm --no-deps --user root --entrypoint /bin/bash spark-master \
  -c'rm -rf /opt/spark-data/output/my_result'
```

For production, use a custom image with matching UID/GID and proper permissions instead of world-writable directories.

### SELinux

Fedora with SELinux enforcing requires `:z` on bind mounts. If you add a mount without it, you may see permission errors even though Unix permissions look correct:

```bash
sudo ausearch -m avc -ts recent
```

### Executor out of memory

```bash
docker inspect spark-worker-1 --format'{{.State.OOMKilled}}'
docker compose logs --tail=200 spark-worker-1
free -h
```

Reduce `SPARK_WORKER_MEMORY` or `spark.executor.memory`, increase partitions, reduce cached data, or avoid large `collect()` calls. Close idle Jupyter kernels before big jobs.

### Version mismatch

```bash
docker compose exec spark-master spark-submit --version
docker compose exec jupyter python -c"import pyspark, sys; print(pyspark.__version__, sys.version)"
```

Spark and PySpark must match, and Python minor versions must match. Scala jars must use the same binary version (`_2.13` for Spark 4).

### ANSI cast errors

Spark 4 enables ANSI mode by default:

```sql
SELECT CAST('abc'AS INT);       -- error
SELECT TRY_CAST('abc'AS INT);   -- NULL
```

Fix the data or use `TRY_CAST`. Disable ANSI only if you understand the trade-off:

```bash
--conf spark.sql.ansi.enabled=false
```

### Jupyter token rejected

```bash
grep JUPYTER_TOKEN .env
docker compose up -d --force-recreate jupyter
```

Then open the URL with the current token.

### `spark-submit` not found in a login shell

The image sets `PATH` in the Compose environment. A login shell (`bash -l`) may reset it. Use:

```bash
docker compose exec spark-master bash -c'spark-submit --version'
```

or the full path:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --version
```

`make shell` opens a non-login bash, so the PATH is preserved.

### Cluster-mode driver UI

In cluster mode the driver runs on a worker, and its UI port 4040 is inside that worker container. It is not published to the host by this Compose file. Use the History Server for completed applications. To get a live UI, add a host mapping to the worker and arrange for the driver to land on that worker, or run in client mode.

### Slow jobs

Check, in order:

1. Stage task duration distribution (skew).
2. Shuffle read/write and spill.
3. Input size and partition pruning.
4. Join strategy (broadcast vs sort-merge).
5. Number of partitions and task size.
6. GC time and memory pressure.
7. Driver-side `collect()`/`toPandas()`.

### Rebuilding the custom image

If a build fails:

```bash
docker compose build --no-cache --progress=plain
```

Check:

- Docker daemon is running.
- Network access to Docker Hub, Debian mirrors, and PyPI.
- Base image tags still exist.
- Package pins are available for the Python version.
- The `COPY --from=apache/spark:4.2.1` path is correct.

### Getting more help

- [Troubleshooting guide](05-troubleshooting.md)
- [Operations guide](04-operations.md)
- Spark UI and History Server logs
- `docker compose logs` for the affected service
- Official Spark documentation links below

---

## Glossary

| Term | Definition |
|---|---|
| **Action** | An operation that triggers execution and returns a result or writes data (for example `count`, `collect`, `write`) |
| **Application** | A driver plus its executors, created by one submission |
| **AQE** | Adaptive Query Execution; runtime re-optimization of the physical plan |
| **Broadcast join** | A join where the small side is sent to every executor, avoiding a shuffle |
| **Catalyst** | Spark SQL's query parser and optimizer |
| **Checkpoint** | A durable copy of state/lineage used for recovery or streaming |
| **Client mode** | Driver runs in the submitting container/machine |
| **Cluster manager** | Software that allocates cluster resources (standalone master, YARN, Kubernetes) |
| **Cluster mode** | Driver runs on a worker selected by the cluster manager |
| **DataFrame** | A distributed table with a schema, optimized by Catalyst |
| **Dataset** | A typed DataFrame API, primarily Scala/Java |
| **DAG** | Directed acyclic graph of stages and dependencies |
| **Driver** | The process that runs the application's main logic and schedules work |
| **Executor** | A JVM process on a worker that runs tasks and stores cached/shuffle data |
| **Event log** | A record of application events used by the History Server |
| **History Server** | A service that serves completed application UIs from event logs |
| **Job** | A unit of work triggered by an action |
| **Kryo** | A fast serialization library used for shuffles/caching |
| **Lineage** | The graph of transformations used to recompute lost RDD partitions |
| **Narrow transformation** | A transformation that does not require a shuffle |
| **Partition** | A chunk of a distributed dataset; the unit of task parallelism |
| **Persist/cache** | Keep a computed dataset in memory/disk for reuse |
| **PySpark** | Python API for Spark |
| **RDD** | Resilient Distributed Dataset; the low-level distributed collection API |
| **Shuffle** | Redistribution of data across partitions, usually for joins/aggregations |
| **Slot** | A core available to run a task |
| **Spark Connect** | A gRPC client/server protocol for remote Spark access |
| **SparkSession** | The entry point for DataFrames, SQL, and catalog operations |
| **Stage** | A set of tasks separated by shuffle boundaries |
| **Standalone** | Spark's built-in cluster manager |
| **Structured Streaming** | Spark's incremental stream-processing API |
| **Task** | A unit of work for one partition in one stage |
| **Tungsten** | Spark's physical execution and memory-management layer |
| **UDF** | User-defined function |
| **Wide transformation** | A transformation that requires a shuffle |
| **Worker** | A node/container that offers resources and runs executors |

---

## Learning resources

Official Apache Spark 4.2.1 documentation:

- [Spark documentation](https://spark.apache.org/docs/4.2.1/)
- [Quick start](https://spark.apache.org/docs/4.2.1/quick-start.html)
- [RDD programming guide](https://spark.apache.org/docs/4.2.1/rdd-programming-guide.html)
- [Spark SQL, DataFrames, and Datasets](https://spark.apache.org/docs/4.2.1/sql-programming-guide.html)
- [SQL reference](https://spark.apache.org/docs/4.2.1/sql-ref.html)
- [Tuning guide](https://spark.apache.org/docs/4.2.1/tuning.html)
- [Configuration reference](https://spark.apache.org/docs/4.2.1/configuration.html)
- [Monitoring and instrumentation](https://spark.apache.org/docs/4.2.1/monitoring.html)
- [Submitting applications](https://spark.apache.org/docs/4.2.1/submitting-applications.html)
- [Standalone mode](https://spark.apache.org/docs/4.2.1/spark-standalone.html)
- [Structured Streaming](https://spark.apache.org/docs/4.2.1/structured-streaming-programming-guide.html)
- [MLlib](https://spark.apache.org/docs/4.2.1/ml-guide.html)
- [GraphX](https://spark.apache.org/docs/4.2.1/graphx-programming-guide.html)
- [Spark Connect](https://spark.apache.org/docs/4.2.1/spark-connect-overview.html)

Related project documentation:

- [Project README](../README.md)
- [Quickstart and verification](01-quickstart.md)
- [Running jobs](02-running-jobs.md)
- [JupyterLab and PySpark](03-jupyter.md)
- [Operations, scaling, and maintenance](04-operations.md)
- [Troubleshooting](05-troubleshooting.md)

Docker:

- [Docker Compose documentation](https://docs.docker.com/compose/)
- [Docker volumes and bind mounts](https://docs.docker.com/storage/bind-mounts/)
- [Dockerfile reference](https://docs.docker.com/reference/dockerfile/)

---

## Quick reference

### URLs

| What | URL |
|---|---|
| JupyterLab | <http://localhost:8888/?token=spark> |
| Spark Master | <http://localhost:8080> |
| Worker 1 | <http://localhost:8081> |
| Worker 2 | <http://localhost:8082> |
| History Server | <http://localhost:18080> |
| Jupyter driver UI | <http://localhost:4040> |
| Master client-mode driver UI | <http://localhost:4041> |

### Commands

```bash
cd ~/spark-docker

docker compose up -d
docker compose ps
docker compose logs -f --tail=100
docker compose down

make test
make clean-output
make shell
make pyspark
make sql

docker compose exec spark-master spark-submit --version
docker compose exec spark-master python3 --version
docker compose exec jupyter python -c"import pyspark, sys; print(pyspark.__version__, sys.version)"
```

### Paths

| Container | Host |
|---|---|
| `/opt/spark-apps` | `./apps` |
| `/opt/spark-data` | `./data` |
| `/opt/spark-events` | `./events` |
| `/opt/spark-conf` | `./conf` |
| `/home/jovyan/work` | `./notebooks` |

### The three checks that catch most setup problems

```bash
# 1. Are the containers healthy?
docker compose ps

# 2. Do Spark and Python versions match?
docker compose exec spark-master spark-submit --version
docker compose exec spark-master python3 --version
docker compose exec jupyter python3 --version

# 3. Can Jupyter run a Python RDD on the workers?
docker compose exec jupyter spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=jupyter \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /opt/spark-apps/pi.py 8
```

