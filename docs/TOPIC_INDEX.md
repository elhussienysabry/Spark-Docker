# Topic Index

> **Generated file**: this index is built from `scripts/curriculum_metadata.py` by
> `scripts/generate_navigation_docs.py`. Edit the metadata, not this table, then re-run
> the generator.

This index covers the 76 core curriculum notebooks. The four business case studies are
indexed separately in [CASE_STUDIES.md](CASE_STUDIES.md); their business context is in
[BUSINESS_CONTEXT.md](BUSINESS_CONTEXT.md).

## How to use this index

1. Pick a role path below, or start with the **Essential** topics in module order.
2. Read the prerequisite column before opening a notebook.
3. Use the experiment column as the minimum hands-on check for the topic.
4. Follow the `Next topic` link at the end of each notebook.

## Priority levels

| Level | Meaning |
|---|---|
| **Essential** | Needed for almost every data-engineering role. Learn deeply. |
| **Important** | Needed for practical production work. Learn with hands-on practice. |
| **Advanced** | Needed for optimization, platform work, and specialized workloads. |
| **Optional** | Learn when a project or role requires it. |

## Role paths

| Role | Start here | Then | Depth target |
|---|---|---|---|
| **Data Engineer** | Modules 00–05 | Modules 08, 10, 13 | Build and operate reliable batch pipelines |
| **Senior Data Engineer** | Modules 04–07 | Modules 09–12, case studies | Diagnose plans, memory, shuffle, and reliability |
| **Analytics Engineer** | Modules 03, 08 | Modules 10, 13, case studies | Curated marts, SQL, data quality, and cost |
| **Platform / Infrastructure Engineer** | Modules 01, 04, 12 | Modules 06, 10, 11 | Cluster operations, resources, and isolation |
| **Spark Specialist** | Modules 04–07 | Modules 10–12, source reading | Scheduler, Catalyst, Tungsten, and tuning |

## Module 00 — Environment and Spark setup

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Spark 4.2.1 Cluster Environment Verification & Architecture Inspection](../notebooks/00_environment_and_spark_setup/00_spark_environment.ipynb) | Essential | 1–2 h | Docker Engine, Docker Compose, basic terminal skills | Control-plane vs data-plane verification |

## Module 01 — Spark fundamentals

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Introduction to Apache Spark: Philosophy, Ecosystem & Primitives](../notebooks/01_spark_fundamentals/01_introduction_to_spark.ipynb) | Essential | 1–2 h | None | Measure the cost of starting a Spark job |
| [Deep Dive into Apache Spark Cluster Architecture](../notebooks/01_spark_fundamentals/02_spark_architecture.ipynb) | Essential | 2–3 h | Introduction to Spark | Map the control plane and data plane |
| [SparkSession, SparkContext, and Configuration Management](../notebooks/01_spark_fundamentals/03_spark_session_and_spark_context.ipynb) | Essential | 1–2 h | Spark architecture | Configuration precedence |
| [End-to-End Cluster Execution Flow: From Code to Tasks](../notebooks/01_spark_fundamentals/04_spark_cluster_execution.ipynb) | Essential | 2–3 h | Spark architecture, SparkSession | Predict stages before running |

## Module 02 — Resilient Distributed Datasets

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [RDD Fundamentals](../notebooks/02_rdd/01_rdd_fundamentals.ipynb) | Important | 3–4 h | Lazy evaluation, partitions, basic Python | RDD vs DataFrame on the same aggregation |
| [RDD Transformations and Actions: From Map to ReduceByKey](../notebooks/02_rdd/02_rdd_transformations_and_actions.ipynb) | Important | 2–3 h | RDD fundamentals | groupByKey vs reduceByKey shuffle volume |
| [RDD Partitions, Partitioners, and Parallelism Inspection](../notebooks/02_rdd/03_rdd_partitions.ipynb) | Important | 2–3 h | RDD transformations | Partition count vs runtime |
| [RDD Dependencies, Lineage Graphs, and Fault Tolerance](../notebooks/02_rdd/04_rdd_dependencies_and_lineage.ipynb) | Important | 2–3 h | RDD partitions | Read a lineage graph |
| [RDD Caching and Persistence Strategies](../notebooks/02_rdd/05_rdd_caching_and_persistence.ipynb) | Important | 2–3 h | RDD lineage, memory basics | Cache hit vs recomputation |

## Module 03 — DataFrames and Spark SQL

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [DataFrame Fundamentals](../notebooks/03_dataframe_and_spark_sql/01_dataframe_fundamentals.ipynb) | Essential | 2–3 h | Lazy evaluation, basic Python | Logical plan vs physical plan |
| [Schemas and Data Types: Explicit StructTypes, Nested Types & Schema Enforcement](../notebooks/03_dataframe_and_spark_sql/02_schemas_and_data_types.ipynb) | Essential | 2–3 h | DataFrame fundamentals | Inference vs explicit schema |
| [DataFrame Transformations: Select, Filter, WithColumn & Expressions](../notebooks/03_dataframe_and_spark_sql/03_dataframe_transformations.ipynb) | Essential | 2–3 h | DataFrame fundamentals, schemas | Filter placement |
| [Spark SQL: Temporary Views, Global Views, and SQL Execution](../notebooks/03_dataframe_and_spark_sql/04_spark_sql.ipynb) | Essential | 2–3 h | DataFrames, schemas | View scope and catalog |
| [Distributed Joins and Complex Aggregations](../notebooks/03_dataframe_and_spark_sql/05_joins_and_aggregations.ipynb) | Essential | 3–4 h | DataFrames, Spark SQL, partitions | Broadcast vs sort-merge plan |
| [Analytical Window Functions: Rankings, Lead/Lag, and Moving Averages](../notebooks/03_dataframe_and_spark_sql/06_window_functions.ipynb) | Essential | 3–4 h | Joins, aggregations, Spark SQL | row_number vs rank vs dense_rank |
| [Null Handling, Missing Data Strategies, and Quality Validation](../notebooks/03_dataframe_and_spark_sql/07_nulls_and_data_quality.ipynb) | Essential | 2–3 h | DataFrames, SQL, joins | Null behavior in joins and aggregations |
| [UDFs vs Vectorized Pandas UDFs: Architecture & Performance Benchmarks](../notebooks/03_dataframe_and_spark_sql/08_udfs_and_pandas_udfs.ipynb) | Important | 2–3 h | DataFrames, Python, Arrow basics | Built-in vs Python UDF vs pandas UDF |

## Module 04 — Execution engine

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Lazy Evaluation: Philosophy, Mechanics, and Optimization Benefits](../notebooks/04_execution_engine/01_lazy_evaluation.ipynb) | Essential | 1–2 h | DataFrames, basic execution model | Transformation vs action timing |
| [The DAG and Query Execution Plans: Parsed, Analyzed, Optimized, and Physical](../notebooks/04_execution_engine/02_dag_and_execution_plans.ipynb) | Important | 2–3 h | Lazy evaluation, Catalyst basics | Read a formatted plan |
| [The DAGScheduler: Converting DAGs into Execution Stages](../notebooks/04_execution_engine/03_dag_scheduler.ipynb) | Advanced | 2–3 h | DAG/plans, jobs/stages/tasks | Stage attempts after an executor failure |
| [The TaskScheduler: Task Distribution and Data Locality](../notebooks/04_execution_engine/04_task_scheduler.ipynb) | Advanced | 2–3 h | DAGScheduler, cluster architecture | Locality levels and wait time |
| [Jobs, Stages, and Tasks: The Three Tiers of Execution](../notebooks/04_execution_engine/05_jobs_stages_and_tasks.ipynb) | Essential | 2–3 h | Lazy evaluation, DAG basics | Count jobs, stages, and tasks |
| [Narrow vs Wide Transformations: Shuffles and Stage Boundaries](../notebooks/04_execution_engine/06_narrow_and_wide_transformations.ipynb) | Essential | 2–3 h | Jobs/stages/tasks, partitions | Classify and verify |
| [Dependencies and Stage Boundaries: The Scheduler Algorithm](../notebooks/04_execution_engine/07_dependencies_and_stage_boundaries.ipynb) | Advanced | 2–3 h | Narrow/wide transformations, DAGScheduler | Derive stages from a dependency graph |

## Module 05 — Partitions and shuffle

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Partitioning Fundamentals: Parallelism and Data Distribution](../notebooks/05_partitions_and_shuffle/01_partitioning_fundamentals.ipynb) | Essential | 2–3 h | RDD partitions, lazy evaluation, cluster architecture | Find the parallelism knee |
| [Repartition vs Coalesce: Full Shuffle vs Local Partition Merge](../notebooks/05_partitions_and_shuffle/02_repartition_and_coalesce.ipynb) | Essential | 2–3 h | Partitioning fundamentals, lazy evaluation | Measure the cost of repartition versus coalesce |
| [Shuffle Internals: Map-Side Write, Spilling, and Reduce-Side Fetch](../notebooks/05_partitions_and_shuffle/03_shuffle_internals.ipynb) | Advanced | 3–4 h | Partitioning, narrow/wide transformations, Spark UI | Trace one shuffle end to end |
| [Shuffle Operations: GroupBy, Distinct, Joins, and Partition Exchanges](../notebooks/05_partitions_and_shuffle/04_shuffle_operations.ipynb) | Essential | 2–3 h | Shuffle internals, DataFrame transformations | Classify operators by shuffle behavior |
| [Partitioning Strategies: Hash, Range, and Data Lake Partition Layouts](../notebooks/05_partitions_and_shuffle/05_partitioning_strategies.ipynb) | Important | 2–3 h | Shuffle operations, file formats, data lakes | Compare partition layouts by scan bytes |
| [Data Skew: Detection, Diagnostics, and Production Remediation](../notebooks/05_partitions_and_shuffle/06_data_skew_and_solutions.ipynb) | Essential | 3–4 h | Shuffle internals, joins, aggregations, Spark UI | Detect and fix a hot key |

## Module 06 — Memory and storage

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Spark Memory Architecture: Unified Memory Model](../notebooks/06_memory_and_storage/01_spark_memory_architecture.ipynb) | Important | 2–3 h | JVM basics, executors, partitioning | Watch the unified pool under pressure |
| [Executor Memory Sizing and Overhead](../notebooks/06_memory_and_storage/02_executor_memory.ipynb) | Essential | 2–3 h | Spark memory architecture, container basics | Calculate a safe container budget |
| [Storage and Execution Memory: Dynamic Borrowing](../notebooks/06_memory_and_storage/03_storage_and_execution_memory.ipynb) | Advanced | 2–3 h | Unified memory model, cache/persist | Force cache eviction |
| [Cache and Persist: StorageLevels, Serialization, and Deserialization](../notebooks/06_memory_and_storage/04_cache_and_persist.ipynb) | Essential | 2–3 h | Storage/execution memory, lazy evaluation | Prove that a cache pays off |
| [Memory Spilling: Diagnosing Spill (Memory) and Spill (Disk)](../notebooks/06_memory_and_storage/05_spill_to_disk.ipynb) | Important | 2–3 h | Memory architecture, shuffle internals | Reduce spill by projecting columns |
| [Broadcast Variables: Efficient Lookup Distribution via P2P](../notebooks/06_memory_and_storage/06_broadcast_variables.ipynb) | Important | 2–3 h | Executors, serialization, memory model | Compare closure capture with broadcast |
| [Broadcast Hash Joins: Eliminating Network Shuffles](../notebooks/06_memory_and_storage/07_broadcast_join.ipynb) | Essential | 2–3 h | Joins, broadcast variables, AQE | Find the broadcast threshold |

## Module 07 — Spark SQL internals

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [The Catalyst Optimizer: Rule-Based and Cost-Based Optimizations](../notebooks/07_spark_sql_internals/01_catalyst_optimizer.ipynb) | Advanced | 3–4 h | DataFrames, Spark SQL, logical plans | Watch a predicate move |
| [Logical and Physical Plans: Deep Reading and Plan Forensics](../notebooks/07_spark_sql_internals/02_logical_and_physical_plans.ipynb) | Advanced | 2–3 h | Catalyst basics, DataFrame API | Diff two plans |
| [Project Tungsten: Binary Memory Management and Whole-Stage Codegen](../notebooks/07_spark_sql_internals/03_tungsten_and_whole_stage_codegen.ipynb) | Advanced | 3–4 h | JVM, physical plans, serialization | Find a codegen fallback |
| [Join Strategies: Broadcast Hash, Sort-Merge, and Shuffle Hash Joins](../notebooks/07_spark_sql_internals/04_join_strategies.ipynb) | Essential | 3–4 h | Physical plans, broadcast joins, partitioning | Compare join strategies |
| [Adaptive Query Execution (AQE): Runtime Query Optimization](../notebooks/07_spark_sql_internals/05_adaptive_query_execution.ipynb) | Important | 3–4 h | Join strategies, shuffle, physical plans | See AQE change the plan |

## Module 08 — Data sources and formats

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Working with CSV and JSON: Options, Modes, and Nested Structures](../notebooks/08_data_sources_and_formats/01_csv_and_json.ipynb) | Essential | 2–3 h | DataFrame basics, schemas, file paths | Compare inference with an explicit schema |
| [Parquet and ORC: Columnar Storage, Compression, and Speedups](../notebooks/08_data_sources_and_formats/02_parquet_and_orc.ipynb) | Essential | 2–3 h | CSV/JSON, schemas, columnar storage basics | Measure column pruning |
| [Partitioned Data Lake Architecture: Designing Directory Layouts](../notebooks/08_data_sources_and_formats/03_partitioned_data_lake.ipynb) | Important | 3–4 h | Parquet/ORC, partitioning strategies | Prove partition pruning |
| [Schema Evolution: Merging Schemas in Parquet and Handling Field Changes](../notebooks/08_data_sources_and_formats/04_schema_evolution.ipynb) | Important | 2–3 h | Parquet/ORC, partitioned lakes, schema basics | Read old and new files together |
| [DataFrameReader and DataFrameWriter Comprehensive Reference](../notebooks/08_data_sources_and_formats/05_reading_and_writing_data.ipynb) | Essential | 2–3 h | Data sources, schemas, partitioning | Make a write idempotent |

## Module 09 — Structured Streaming

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Stream Processing Fundamentals: Micro-Batch vs Continuous Processing](../notebooks/09_streaming/01_streaming_fundamentals.ipynb) | Important | 2–3 h | DataFrames, sources/sinks, basic architecture | Observe micro-batch progress |
| [Structured Streaming: File Source and Ingestion Directory Monitoring](../notebooks/09_streaming/02_structured_streaming.ipynb) | Essential | 2–3 h | Streaming fundamentals, DataFrame API | Stream a landing directory |
| [Streaming Triggers and Output Modes: Append, Complete, and Update](../notebooks/09_streaming/03_triggers_and_output_modes.ipynb) | Essential | 2–3 h | Structured Streaming basics, DataFrames | Compare output modes |
| [Watermarks and Late-Arriving Data: Bounding Streaming State Memory](../notebooks/09_streaming/04_watermarks_and_late_data.ipynb) | Important | 3–4 h | Streaming aggregations, event time, output modes | Measure late-data loss |
| [Streaming Checkpoints and Fault Tolerance: Exactly-Once Recovery](../notebooks/09_streaming/05_checkpoints_and_fault_tolerance.ipynb) | Advanced | 3–4 h | Streaming queries, sources/sinks, delivery semantics | Simulate a failure between sink and commit |
| [Apache Kafka Integration: Producers, Consumers, and Schema Handling (Optional)](../notebooks/09_streaming/06_kafka_integration.ipynb) | Advanced | 3–4 h | Streaming fundamentals, checkpoints, Kafka basics | Read Kafka with bounded batches |

## Module 10 — Performance and optimization

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Spark UI Deep Dive: Forensic Analysis of Jobs, Stages, and Tasks](../notebooks/10_performance_and_optimization/01_spark_ui_analysis.ipynb) | Essential | 2–3 h | Jobs/stages/tasks, shuffle, memory basics | Find the dominant stage |
| [Execution Plan Analysis: Reading Physical Operators and Bottlenecks](../notebooks/10_performance_and_optimization/02_explain_and_execution_plans.ipynb) | Important | 2–3 h | Logical/physical plans, Spark UI | Read a plan bottom-up |
| [Query Optimization Techniques: Filter Pushdown, Pruning, and Join Order](../notebooks/10_performance_and_optimization/03_query_optimization.ipynb) | Essential | 3–4 h | SQL, joins, partitions, plans | Optimize one query at a time |
| [Shuffle Optimization: Partition Tuning, Bucket Joins, and Reducing I/O](../notebooks/10_performance_and_optimization/04_shuffle_optimization.ipynb) | Important | 3–4 h | Shuffle internals, partitioning, joins | Cut shuffle bytes with projection |
| [Memory Optimization: Garbage Collection Tuning, Off-Heap, and Kryo](../notebooks/10_performance_and_optimization/05_memory_optimization.ipynb) | Advanced | 3–4 h | Memory architecture, serialization, Spark UI | Compare serialized and deserialized cache |
| [Data Skew Optimization: Salting, Skew Hints, and AQE Skew Joins](../notebooks/10_performance_and_optimization/06_data_skew_optimization.ipynb) | Essential | 3–4 h | Data skew basics, joins, AQE | Compare skew remedies |
| [Performance Benchmarking: Building Reproducible Spark Benchmark Suites](../notebooks/10_performance_and_optimization/07_performance_benchmarking.ipynb) | Important | 2–3 h | Spark UI, plans, statistics | Run a controlled comparison |

## Module 11 — Fault tolerance and reliability

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Fault Tolerance: RDD Lineage and Partition Recomputation](../notebooks/11_fault_tolerance_and_reliability/01_lineage_and_recomputation.ipynb) | Important | 2–3 h | RDD dependencies, shuffle, stages | Observe partition recomputation |
| [Executor Failures: Node Eviction, Heartbeats, and State Handling](../notebooks/11_fault_tolerance_and_reliability/02_executor_failure.ipynb) | Important | 2–3 h | Cluster architecture, lineage, shuffle | Observe executor loss |
| [Task Retries, Stage Retries, and Failure Handling](../notebooks/11_fault_tolerance_and_reliability/03_task_retries.ipynb) | Essential | 2–3 h | Task scheduling, executor failure, Spark UI | Force a task failure |
| [Checkpointing: Truncating Lineage and Durable State Preservation](../notebooks/11_fault_tolerance_and_reliability/04_checkpointing.ipynb) | Advanced | 3–4 h | Lineage, persistence, reliable storage | Compare cache and checkpoint |
| [Fault Tolerance Experiments: Empirical Resilience Verification](../notebooks/11_fault_tolerance_and_reliability/05_fault_tolerance_experiments.ipynb) | Advanced | 3–4 h | Executor failure, task retries, checkpointing | Kill an executor and verify output |

## Module 12 — Advanced topics

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Spark on Apache Hadoop YARN: Architecture and Resource Allocation](../notebooks/12_advanced_topics/01_spark_on_yarn.ipynb) | Advanced | 3–4 h | Cluster architecture, resource managers, deployment modes | Trace a YARN submission |
| [Spark on Kubernetes: Cloud-Native Pod-Based Architecture](../notebooks/12_advanced_topics/02_spark_on_kubernetes.ipynb) | Advanced | 3–4 h | Containers, Kubernetes basics, Spark architecture | Inspect the pod lifecycle |
| [Client Mode vs Cluster Mode: Deep Comparison and Use Cases](../notebooks/12_advanced_topics/03_cluster_vs_client_mode.ipynb) | Important | 2–3 h | Deployment modes, cluster architecture, networking | Compare driver locations |
| [Resource Allocation: Cores, Memory, and Executor Sizing Formula](../notebooks/12_advanced_topics/04_resource_allocation.ipynb) | Important | 3–4 h | Executor sizing, cluster resources, memory overhead | Compare executor shapes |
| [Spark Connect: Thin Client Architecture and Decoupled Execution](../notebooks/12_advanced_topics/05_spark_connect.ipynb) | Advanced | 2–3 h | SparkSession, client/server architecture, gRPC | Connect to a remote server |
| [PySpark Internals: Py4J Bridge, Python Daemons, and Serialization Overhead](../notebooks/12_advanced_topics/06_pyspark_internals.ipynb) | Advanced | 3–4 h | PySpark basics, JVM, serialization | Compare UDF styles |

## Module 13 — End-to-end projects

| Notebook | Priority | Effort | Prerequisites | Experiment |
|---|---|---|---|---|
| [Project 1: Production Batch ETL Pipeline (CSV/JSON to Partitioned Parquet Lake)](../notebooks/13_end_to_end_projects/01_batch_etl_pipeline.ipynb) | Important | 4–6 h | DataFrames, data sources, partitioning, writes | Run the pipeline twice |
| [Project 2: Large-Scale Sales Analytics & Windowed Business Intelligence](../notebooks/13_end_to_end_projects/02_large_scale_sales_analytics.ipynb) | Important | 4–6 h | Joins, windows, partitioning, query optimization | Validate a sales mart |
| [Project 3: Production Data Quality Gate and Automated Audit Pipeline](../notebooks/13_end_to_end_projects/03_data_quality_pipeline.ipynb) | Important | 4–6 h | Data validation, SQL, pipeline design | Add a quality gate |
| [Project 4: Real-Time Event Streaming Analytics with Watermarking and Checkpoints](../notebooks/13_end_to_end_projects/04_streaming_analytics_pipeline.ipynb) | Advanced | 4–6 h | Structured Streaming, watermarks, checkpoints, sinks | Test late and duplicate events |

## Business case studies

| Notebook | Priority | Effort | Prerequisites | Business question |
|---|---|---|---|---|
| [E-commerce analytics](../notebooks/14_business_case_studies/01_ecommerce_analytics.ipynb) | Important | 3–4 h | Joins, windows, broadcast joins, partitioned writes | Where is profitable growth, and which customers retain? |
| [Telecom network analytics](../notebooks/14_business_case_studies/02_telecom_network_analytics.ipynb) | Important | 3–4 h | Aggregations, percentiles, windows, event time | Which cells need capacity, and where is the latency tail? |
| [Financial fraud patterns](../notebooks/14_business_case_studies/03_financial_fraud_patterns.ipynb) | Advanced | 3–4 h | Window functions, event time, precision/recall | Which transactions should analysts review first? |
| [Logistics delivery analytics](../notebooks/14_business_case_studies/04_logistics_delivery_analytics.ipynb) | Important | 3–4 h | Date/time handling, joins, windows, SLA design | Which carriers and lanes are failing their promises? |

## Related navigation

- [Learning roadmap](LEARNING_ROADMAP.md)
- [Use-case index](USE_CASES.md)
- [Business context](BUSINESS_CONTEXT.md)
- [Case studies](CASE_STUDIES.md)
- [Notebook template](NOTEBOOK_TEMPLATE.md)
