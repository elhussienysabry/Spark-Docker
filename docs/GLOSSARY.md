# Apache Spark Technical Glossary

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
- **Technical**: In Structured Streaming, a state eviction threshold defined as $	ext{MaxEventTime} - 	ext{AllowedDelay}$. State entries older than the watermark are dropped from the state store to bound memory usage.
- **Related**: *Structured Streaming*, *Late Data*.

### Wide Dependency
- **Simple**: An operation where output chunks depend on multiple input chunks from different machines, forcing a shuffle.
- **Technical**: A dependency where multiple child partitions depend on data from a single parent partition (`ShuffleDependency`), drawing stage boundaries.
- **Related**: *Narrow Dependency*, *Stage*, *Shuffle*.
