# Real-World Use Cases

> **Generated file**: the topic map is built from `scripts/curriculum_metadata.py`.
> Edit the metadata and re-run `scripts/generate_navigation_docs.py`.

Spark is not the answer to every data problem. This page connects each curriculum topic
to realistic scenarios, the business decision behind it, and the trade-offs that matter.
Use it to choose a practice project, prepare for an interview, or explain why a design
exists. The four runnable case studies are in [CASE_STUDIES.md](CASE_STUDIES.md).

> **Numbers**: any cost, speed, or revenue figure in the notebooks or docs is illustrative
> unless the notebook prints it from the deterministic lab data. This repository does not
> claim production benchmarks.

## The four runnable case studies

| Case study | Industry | Core Spark patterns | Primary decision |
|---|---|---|---|
| [E-commerce analytics](../notebooks/14_business_case_studies/01_ecommerce_analytics.ipynb) | Retail | Joins, broadcast dimensions, windows, cohort/RFM, partitioned Parquet | Where to invest in assortment and retention |
| [Telecom network analytics](../notebooks/14_business_case_studies/02_telecom_network_analytics.ipynb) | Telecommunications | Event-grain aggregation, percentile metrics, busy-hour ranking | Which cells need capacity first |
| [Financial fraud patterns](../notebooks/14_business_case_studies/03_financial_fraud_patterns.ipynb) | Payments / banking | Event-time velocity, explainable rules, precision/recall | Which alerts analysts review |
| [Logistics delivery analytics](../notebooks/14_business_case_studies/04_logistics_delivery_analytics.ipynb) | Logistics | SLA semantics, carrier/lane scorecards, late-arrival handling | Which carriers and lanes to renegotiate |

## Cross-industry patterns

| Pattern | Typical industries | Spark mechanism | Watch out for |
|---|---|---|---|
| **Event-time windows** | Telecom, payments, IoT, media | `Window` with `rangeBetween`, watermarks | Late data, time zones, state size |
| **Entity 360** | Retail, banking, telecom | Joins, deduplication, window ranking | Grain fan-out, PII, identity resolution |
| **Incremental marts** | Any analytics team | Partitioned writes, MERGE/overwrite patterns | Idempotency, backfills, small files |
| **Anomaly review** | Payments, security, operations | Velocity features, rules, percentiles | Label delay, false positives, explainability |
| **SLA scorecards** | Logistics, telecom, SaaS | Aggregations, percentile metrics, volume filters | Denominator definitions, missing scans |
| **Data quality gates** | All industries | Null/duplicate checks, quarantine, reconciliation | Silent drops, schema drift, ownership |

## Topic-to-use-case map

Each row lists the topic-specific scenarios written into the notebook. The scenarios are
teaching examples, not claims about a specific company.

### Module 00 — Environment and Spark setup

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Spark 4.2.1 Cluster Environment Verification & Architecture Inspection](../notebooks/00_environment_and_spark_setup/00_spark_environment.ipynb) | Essential | **Any data platform team — New data engineer onboarding**: A new engineer clones the repository and must prove that the local cluster is healthy before starting the curriculum.<br><br>**Infrastructure / platform engineering — Post-migration smoke test**: A platform team moves Spark from one Docker host to another and needs a repeatable smoke test. |

### Module 01 — Spark fundamentals

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Introduction to Apache Spark: Philosophy, Ecosystem & Primitives](../notebooks/01_spark_fundamentals/01_introduction_to_spark.ipynb) | Essential | **Retail / e-commerce — Replacing a nightly MapReduce ETL**: A retailer joins orders, products, and customers every night to build a revenue mart.<br><br>**IoT / logistics — Unified batch and streaming analytics**: A logistics company processes historical delivery data in batch and live vehicle events in streaming. |
| [Deep Dive into Apache Spark Cluster Architecture](../notebooks/01_spark_fundamentals/02_spark_architecture.ipynb) | Essential | **Enterprise data platform — Capacity planning for a shared cluster**: Several teams share a Standalone or YARN cluster and need to understand why jobs queue.<br><br>**Platform engineering — Executor registration failure after a network change**: After a Docker network change, executors start but never register with the driver. |
| [SparkSession, SparkContext, and Configuration Management](../notebooks/01_spark_fundamentals/03_spark_session_and_spark_context.ipynb) | Essential | **Enterprise data engineering — Standardized session factory for a team**: Every notebook and job creates its own session, and settings drift between environments.<br><br>**Data quality / testing — Test isolation in a notebook**: A notebook runs several experiments and one experiment changes `spark.sql.shuffle.partitions`. |
| [End-to-End Cluster Execution Flow: From Code to Tasks](../notebooks/01_spark_fundamentals/04_spark_cluster_execution.ipynb) | Essential | **Banking — SLA investigation for a nightly pipeline**: A nightly reconciliation job misses its 6 a.m. deadline even though the cluster has free capacity.<br><br>**Education / training — Onboarding through a controlled pipeline**: A new engineer needs to see how a simple `groupBy` becomes a distributed job. |

### Module 02 — Resilient Distributed Datasets

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [RDD Fundamentals](../notebooks/02_rdd/01_rdd_fundamentals.ipynb) | Important | **Telecommunications — Legacy log-processing pipeline**: A telecom team maintains an old RDD pipeline that parses proprietary binary call records.<br><br>**Fraud detection — Iterative graph-like computation**: A risk team propagates risk scores through a graph of accounts and transactions. |
| [RDD Transformations and Actions: From Map to ReduceByKey](../notebooks/02_rdd/02_rdd_transformations_and_actions.ipynb) | Important | **E-commerce — Clickstream sessionization**: A retailer groups page-view events by session to compute pages per session and conversion.<br><br>**Media / publishing — Legacy word count at scale**: A publisher counts term frequency across millions of documents for search indexing. |
| [RDD Partitions, Partitioners, and Parallelism Inspection](../notebooks/02_rdd/03_rdd_partitions.ipynb) | Important | **Retail / data lake — Ingesting many small files**: A daily ingestion job reads 20,000 small JSON files and creates one partition per file.<br><br>**Advertising / clickstream — Custom partitioning for a hot key**: A campaign ID is extremely hot, and hash partitioning sends most events to one reducer. |
| [RDD Dependencies, Lineage Graphs, and Fault Tolerance](../notebooks/02_rdd/04_rdd_dependencies_and_lineage.ipynb) | Important | **Banking — Audit trail for regulatory reporting**: A bank must reproduce a daily risk report months later from immutable source data.<br><br>**E-commerce — Recovering from a lost executor**: A worker dies during a long aggregation and the job continues without restarting. |
| [RDD Caching and Persistence Strategies](../notebooks/02_rdd/05_rdd_caching_and_persistence.ipynb) | Important | **Recommendation systems — Iterative machine-learning feature preparation**: A recommender repeatedly uses the same user-item feature matrix across many iterations.<br><br>**Retail analytics — Interactive analyst session**: An analyst runs several queries against the same filtered orders dataset. |

### Module 03 — DataFrames and Spark SQL

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [DataFrame Fundamentals](../notebooks/03_dataframe_and_spark_sql/01_dataframe_fundamentals.ipynb) | Essential | **Retail — Standardized ETL for a retail data mart**: A team builds a daily revenue mart from orders, products, and customers.<br><br>**Media / publishing — Self-service analytics on a data lake**: Analysts want to query Parquet files without learning RDDs. |
| [Schemas and Data Types: Explicit StructTypes, Nested Types & Schema Enforcement](../notebooks/03_dataframe_and_spark_sql/02_schemas_and_data_types.ipynb) | Essential | **Banking — Ingestion contract for a payments feed**: A bank ingests daily payment files from several vendors.<br><br>**E-commerce — Nested event data from a mobile app**: A mobile app sends JSON events with nested `order.items` arrays. |
| [DataFrame Transformations: Select, Filter, WithColumn & Expressions](../notebooks/03_dataframe_and_spark_sql/03_dataframe_transformations.ipynb) | Essential | **Telecommunications — Customer 360 feature engineering**: A telecom team builds a customer feature table from usage, billing, and support data.<br><br>**Advertising — Cleaning a marketing clickstream**: A marketing team cleans bot traffic, normalizes URLs, and derives campaign fields. |
| [Spark SQL: Temporary Views, Global Views, and SQL Execution](../notebooks/03_dataframe_and_spark_sql/04_spark_sql.ipynb) | Essential | **Retail — BI reporting layer over a lake**: Analysts write SQL against curated order and product views.<br><br>**Banking — Migrating warehouse SQL to a lakehouse**: A bank moves some reporting SQL from a warehouse to a lakehouse. |
| [Distributed Joins and Complex Aggregations](../notebooks/03_dataframe_and_spark_sql/05_joins_and_aggregations.ipynb) | Essential | **E-commerce — Order enrichment with a product dimension**: A retailer joins a large orders fact table to a small product catalog.<br><br>**Banking — Fraud aggregation by account and day**: A risk team aggregates transactions by account and day to compute velocity features. |
| [Analytical Window Functions: Rankings, Lead/Lag, and Moving Averages](../notebooks/03_dataframe_and_spark_sql/06_window_functions.ipynb) | Essential | **Retail — Top products per category**: A retailer ranks products by revenue within each category every week.<br><br>**Banking — Transaction velocity for fraud review**: A risk team flags accounts with many transactions in a short window. |
| [Null Handling, Missing Data Strategies, and Quality Validation](../notebooks/03_dataframe_and_spark_sql/07_nulls_and_data_quality.ipynb) | Essential | **Retail — Customer data quality for CRM**: A CRM ingests customer records from web, stores, and partners.<br><br>**Banking — Financial reconciliation controls**: A finance team reconciles transactions against ledger entries. |
| [UDFs vs Vectorized Pandas UDFs: Architecture & Performance Benchmarks](../notebooks/03_dataframe_and_spark_sql/08_udfs_and_pandas_udfs.ipynb) | Important | **Healthcare / finance — PII masking in a customer dataset**: A team masks email addresses and phone numbers before sharing data with analysts.<br><br>**Recommendation systems — Feature engineering for a recommendation model**: A data scientist needs a complex text-similarity feature that is not a Spark built-in. |

### Module 04 — Execution engine

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Lazy Evaluation: Philosophy, Mechanics, and Optimization Benefits](../notebooks/04_execution_engine/01_lazy_evaluation.ipynb) | Essential | **Retail — Cost control in a nightly pipeline**: A pipeline calls `count()` for logging, then writes the same DataFrame.<br><br>**Data science — Interactive exploration**: An analyst builds a complex transformation chain and wants to inspect intermediate results. |
| [The DAG and Query Execution Plans: Parsed, Analyzed, Optimized, and Physical](../notebooks/04_execution_engine/02_dag_and_execution_plans.ipynb) | Important | **Banking — Plan review before a production release**: A team reviews a changed reporting query before it reaches production.<br><br>**E-commerce — Diagnosing a slow join**: A join that used to be fast now shuffles both sides. |
| [The DAGScheduler: Converting DAGs into Execution Stages](../notebooks/04_execution_engine/03_dag_scheduler.ipynb) | Advanced | **Telecommunications — Debugging a shuffle-stage failure**: A nightly call-record aggregation fails after a worker restart.<br><br>**E-commerce — Understanding job recovery**: An executor dies mid-job and the application continues. |
| [The TaskScheduler: Task Distribution and Data Locality](../notebooks/04_execution_engine/04_task_scheduler.ipynb) | Advanced | **Media — Locality tuning for a data lake**: A Spark job reads large Parquet files from shared storage and tasks wait for locality.<br><br>**Advertising — Straggler mitigation**: A few tasks in a large aggregation run much longer than the rest. |
| [Jobs, Stages, and Tasks: The Three Tiers of Execution](../notebooks/04_execution_engine/05_jobs_stages_and_tasks.ipynb) | Essential | **Banking — SLA reporting for a nightly job**: A nightly job misses its deadline and the team needs to know which stage is responsible.<br><br>**Education — Teaching distributed execution**: A learner needs to see how one `groupBy` becomes many tasks. |
| [Narrow vs Wide Transformations: Shuffles and Stage Boundaries](../notebooks/04_execution_engine/06_narrow_and_wide_transformations.ipynb) | Essential | **Retail — Reducing shuffle cost in an ETL pipeline**: A pipeline performs several `repartition` calls and joins before aggregating.<br><br>**Telecommunications — Choosing a join strategy**: A large call-record table joins a small tower dimension. |
| [Dependencies and Stage Boundaries: The Scheduler Algorithm](../notebooks/04_execution_engine/07_dependencies_and_stage_boundaries.ipynb) | Advanced | **Machine learning — Placing a checkpoint in a long pipeline**: An iterative algorithm has a long lineage and fails after many iterations.<br><br>**Logistics — Explaining stage recomputation**: A shuffle stage fails after a worker restart and the job recomputes an earlier stage. |

### Module 05 — Partitions and shuffle

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Partitioning Fundamentals: Parallelism and Data Distribution](../notebooks/05_partitions_and_shuffle/01_partitioning_fundamentals.ipynb) | Essential | **Industrial IoT — IoT sensor backfill**: A backfill reads years of sensor files and runs a per-device aggregation.<br><br>**Financial services — Fraud scoring at peak**: A scoring job uses the default 200 shuffle partitions on a small cluster with 4 cores. |
| [Repartition vs Coalesce: Full Shuffle vs Local Partition Merge](../notebooks/05_partitions_and_shuffle/02_repartition_and_coalesce.ipynb) | Essential | **Retail analytics — Right-sizing a nightly export**: A nightly job writes a 40 GB curated table to object storage for a BI engine.<br><br>**Telecom — Preparing data for a downstream join**: A call-detail pipeline joins a 500 GB fact table to a subscriber dimension on `subscriber_id`. |
| [Shuffle Internals: Map-Side Write, Spilling, and Reduce-Side Fetch](../notebooks/05_partitions_and_shuffle/03_shuffle_internals.ipynb) | Advanced | **Financial services — Diagnosing a nightly join slowdown**: A nightly reconciliation job grew from 40 minutes to 4 hours after a data-volume increase.<br><br>**Digital media — Tuning a groupBy on clickstream data**: A daily sessionization job groups billions of events by `session_id`. |
| [Shuffle Operations: GroupBy, Distinct, Joins, and Partition Exchanges](../notebooks/05_partitions_and_shuffle/04_shuffle_operations.ipynb) | Essential | **E-commerce — Removing a redundant distinct**: A product-view pipeline calls `distinct()` after a `dropDuplicates` on the same key.<br><br>**IoT telemetry — Choosing union versus join**: Two device-event datasets with the same schema must be combined for a daily report. |
| [Partitioning Strategies: Hash, Range, and Data Lake Partition Layouts](../notebooks/05_partitions_and_shuffle/05_partitioning_strategies.ipynb) | Important | **Advertising technology — Designing a clickstream lake layout**: Billions of ad events arrive hourly and analysts filter by date, campaign, and country.<br><br>**Telecom — Bucketing a recurring join**: Two large tables are joined nightly on `subscriber_id`. |
| [Data Skew: Detection, Diagnostics, and Production Remediation](../notebooks/05_partitions_and_shuffle/06_data_skew_and_solutions.ipynb) | Essential | **E-commerce — Null-key join explosion**: Orders are left-joined to a customer dimension, and millions of orders have a null customer ID.<br><br>**Digital media — Popular product in a clickstream aggregation**: A session aggregation is dominated by one viral product ID.<br><br>**Financial services — Skewed fraud alerts**: A fraud-detection join is slow because one merchant account generates most transactions. |

### Module 06 — Memory and storage

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Spark Memory Architecture: Unified Memory Model](../notebooks/06_memory_and_storage/01_spark_memory_architecture.ipynb) | Important | **Insurance — Unexplained executor OOM**: An executor with 8 GB heap fails while the Spark UI shows only a few GB of cached data.<br><br>**Business intelligence — Cache-heavy dashboard job**: A job caches five intermediate DataFrames to speed up a dashboard refresh. |
| [Executor Memory Sizing and Overhead](../notebooks/06_memory_and_storage/02_executor_memory.ipynb) | Essential | **SaaS platform — Kubernetes executor eviction**: Spark executors on Kubernetes are evicted with OOMKilled during large shuffles.<br><br>**Marketing analytics — Python UDF memory spike**: A pandas UDF job runs fine on small data but fails on the full dataset. |
| [Storage and Execution Memory: Dynamic Borrowing](../notebooks/06_memory_and_storage/03_storage_and_execution_memory.ipynb) | Advanced | **Retail finance — Cache thrash during month-end reporting**: A month-end job caches a large fact table, then runs several shuffle-heavy aggregations.<br><br>**Data science — Interactive notebook slowdown**: An analyst caches several exploratory DataFrames and later runs a large join. |
| [Cache and Persist: StorageLevels, Serialization, and Deserialization](../notebooks/06_memory_and_storage/04_cache_and_persist.ipynb) | Essential | **Machine learning — Iterative ML feature engineering**: A feature pipeline reuses the same cleaned DataFrame across many model iterations.<br><br>**Business intelligence — Repeated dashboard query**: A dashboard refreshes the same aggregated table several times per hour. |
| [Memory Spilling: Diagnosing Spill (Memory) and Spill (Disk)](../notebooks/06_memory_and_storage/05_spill_to_disk.ipynb) | Important | **Logistics — Sort-merge join with heavy spill**: A nightly shipment join spills hundreds of gigabytes on each reduce task.<br><br>**Healthcare — Aggregation with wide rows**: A claims aggregation spills despite a modest row count. |
| [Broadcast Variables: Efficient Lookup Distribution via P2P](../notebooks/06_memory_and_storage/06_broadcast_variables.ipynb) | Important | **Payments — Country and currency lookup**: Every task needs a small table of country codes and currency rules.<br><br>**Ad technology — Machine-learning model scoring**: A scoring job loads a compact model and applies it to billions of impressions. |
| [Broadcast Hash Joins: Eliminating Network Shuffles](../notebooks/06_memory_and_storage/07_broadcast_join.ipynb) | Essential | **E-commerce — Enriching events with a small dimension**: Billions of click events are enriched with a product catalog of a few megabytes.<br><br>**Telecom — The dimension that outgrew the threshold**: A subscriber dimension grows from 5 MB to 2 GB, but the job still forces a broadcast hint. |

### Module 07 — Spark SQL internals

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [The Catalyst Optimizer: Rule-Based and Cost-Based Optimizations](../notebooks/07_spark_sql_internals/01_catalyst_optimizer.ipynb) | Advanced | **Retail — Filter that never reaches the source**: A query filters on a derived column and scans the full Parquet table.<br><br>**Telecom — Missing statistics, wrong join**: The optimizer chooses a sort-merge join for a small dimension that would fit in memory. |
| [Logical and Physical Plans: Deep Reading and Plan Forensics](../notebooks/07_spark_sql_internals/02_logical_and_physical_plans.ipynb) | Advanced | **SaaS analytics — Regression after a library upgrade**: A query becomes three times slower after an upgrade without code changes.<br><br>**Media — Unexpected full scan**: A dashboard query scans the entire event table despite a date filter. |
| [Project Tungsten: Binary Memory Management and Whole-Stage Codegen](../notebooks/07_spark_sql_internals/03_tungsten_and_whole_stage_codegen.ipynb) | Advanced | **Ad technology — CPU-bound aggregation**: A large aggregation is CPU-bound even though shuffle bytes are modest.<br><br>**Risk analytics — Python UDF bottleneck**: A scoring pipeline uses a Python UDF for every row and is much slower than expected. |
| [Join Strategies: Broadcast Hash, Sort-Merge, and Shuffle Hash Joins](../notebooks/07_spark_sql_internals/04_join_strategies.ipynb) | Essential | **Retail — Star-schema enrichment**: A sales fact table joins five small dimensions.<br><br>**Telecom — Large-large join with skew**: Two multi-terabyte tables join on subscriber ID, and a few IDs are hot.<br><br>**Insurance — Non-equi join**: Claims are joined to policy periods using a range condition rather than equality. |
| [Adaptive Query Execution (AQE): Runtime Query Optimization](../notebooks/07_spark_sql_internals/05_adaptive_query_execution.ipynb) | Important | **E-commerce — Variable daily data volume**: A daily job processes anywhere from 20 GB to 400 GB depending on promotions.<br><br>**Financial services — Join that becomes broadcast after filtering**: A dimension table is large overall but a selective filter leaves a small build side.<br><br>**Telecom — Skewed partition after a shuffle**: One subscriber ID dominates a join, creating a single oversized reduce partition. |

### Module 08 — Data sources and formats

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Working with CSV and JSON: Options, Modes, and Nested Structures](../notebooks/08_data_sources_and_formats/01_csv_and_json.ipynb) | Essential | **Retail — Partner feed ingestion**: A retailer receives daily CSV feeds from dozens of suppliers with inconsistent headers and types.<br><br>**SaaS — Event JSON with evolving fields**: Application events arrive as JSON lines and new optional fields are added frequently. |
| [Parquet and ORC: Columnar Storage, Compression, and Speedups](../notebooks/08_data_sources_and_formats/02_parquet_and_orc.ipynb) | Essential | **Retail — Analytics lake migration**: A team moves daily CSV extracts to Parquet for a BI workload.<br><br>**IoT — Small-file accumulation**: Hourly ingestion creates thousands of tiny Parquet files. |
| [Partitioned Data Lake Architecture: Designing Directory Layouts](../notebooks/08_data_sources_and_formats/03_partitioned_data_lake.ipynb) | Important | **Advertising — Date-partitioned event lake**: Billions of events are queried mostly by date and campaign.<br><br>**Logistics — Overwriting one day without deleting everything**: A backfill must replace one day's partition while preserving the rest. |
| [Schema Evolution: Merging Schemas in Parquet and Handling Field Changes](../notebooks/08_data_sources_and_formats/04_schema_evolution.ipynb) | Important | **SaaS — Adding a nullable field**: A product team adds an optional `referral_code` field to an event stream.<br><br>**Financial services — Renaming a column**: A team renames `amount` to `transaction_amount` for clarity. |
| [DataFrameReader and DataFrameWriter Comprehensive Reference](../notebooks/08_data_sources_and_formats/05_reading_and_writing_data.ipynb) | Essential | **Retail — Idempotent daily load**: A daily job appends sales data and is sometimes rerun after a failure.<br><br>**Logistics — Exporting for a warehouse**: A Spark job exports a curated table to a JDBC warehouse. |

### Module 09 — Structured Streaming

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Stream Processing Fundamentals: Micro-Batch vs Continuous Processing](../notebooks/09_streaming/01_streaming_fundamentals.ipynb) | Important | **Digital media — Clickstream session metrics**: A media company needs near-real-time session counts and top pages.<br><br>**Financial services — Fraud alert enrichment**: Transactions must be enriched with merchant risk scores within seconds. |
| [Structured Streaming: File Source and Ingestion Directory Monitoring](../notebooks/09_streaming/02_structured_streaming.ipynb) | Essential | **Logistics — Landing-zone ingestion**: Partners drop CSV files into an object-store landing zone throughout the day.<br><br>**Retail — Schema drift in partner files**: A partner adds a column to a daily CSV without notice. |
| [Streaming Triggers and Output Modes: Append, Complete, and Update](../notebooks/09_streaming/03_triggers_and_output_modes.ipynb) | Essential | **Business intelligence — Dashboard with update mode**: A dashboard needs the latest counts per category, not the full history on every refresh.<br><br>**Logistics — Backfill with availableNow**: A team needs to process a backlog of files once and then stop. |
| [Watermarks and Late-Arriving Data: Bounding Streaming State Memory](../notebooks/09_streaming/04_watermarks_and_late_data.ipynb) | Important | **Industrial IoT — Late sensor readings**: Sensors buffer readings and send them minutes late after connectivity outages.<br><br>**Mobile apps — Sessionization with mobile events**: Events arrive out of order because devices retry after reconnect. |
| [Streaming Checkpoints and Fault Tolerance: Exactly-Once Recovery](../notebooks/09_streaming/05_checkpoints_and_fault_tolerance.ipynb) | Advanced | **Retail — Exactly-once ingestion into a lake**: A streaming job writes events to a partitioned table and must not duplicate rows after failures.<br><br>**Telecom — Checkpoint corruption after a configuration change**: A job is restarted with a different query and fails to recover from the old checkpoint. |
| [Apache Kafka Integration: Producers, Consumers, and Schema Handling (Optional)](../notebooks/09_streaming/06_kafka_integration.ipynb) | Advanced | **E-commerce — Real-time order events**: Order events flow through Kafka and must be aggregated for operational dashboards.<br><br>**Payments — Dead-letter handling**: Some Kafka records cannot be deserialized or fail validation. |

### Module 10 — Performance and optimization

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Spark UI Deep Dive: Forensic Analysis of Jobs, Stages, and Tasks](../notebooks/10_performance_and_optimization/01_spark_ui_analysis.ipynb) | Essential | **Retail — Nightly SLA breach**: A nightly revenue job misses its SLA by two hours.<br><br>**SaaS — Cost review**: A platform team must reduce Spark spend by 20%. |
| [Execution Plan Analysis: Reading Physical Operators and Bottlenecks](../notebooks/10_performance_and_optimization/02_explain_and_execution_plans.ipynb) | Important | **Financial services — Query review before production**: A new risk query is about to be deployed to a shared cluster.<br><br>**E-commerce — Explaining a regression**: A recommendation query slows down after a data growth spurt. |
| [Query Optimization Techniques: Filter Pushdown, Pruning, and Join Order](../notebooks/10_performance_and_optimization/03_query_optimization.ipynb) | Essential | **Retail — BI query cost reduction**: A dashboard query scans a wide sales table and joins several dimensions.<br><br>**Machine learning — Feature pipeline optimization**: A feature engineering job recomputes the same joins for every model. |
| [Shuffle Optimization: Partition Tuning, Bucket Joins, and Reducing I/O](../notebooks/10_performance_and_optimization/04_shuffle_optimization.ipynb) | Important | **Telecom — Repeated join on the same key**: A pipeline joins the same two large tables in several steps.<br><br>**Healthcare — Wide-row shuffle**: A claims pipeline shuffles records with many unused fields. |
| [Memory Optimization: Garbage Collection Tuning, Off-Heap, and Kryo](../notebooks/10_performance_and_optimization/05_memory_optimization.ipynb) | Advanced | **Advertising — GC-dominated aggregation**: A large aggregation spends a significant share of CPU in GC.<br><br>**Media — Cache with serialized storage**: A cached dataset consumes more memory than expected and evicts other blocks. |
| [Data Skew Optimization: Salting, Skew Hints, and AQE Skew Joins](../notebooks/10_performance_and_optimization/06_data_skew_optimization.ipynb) | Essential | **Digital media — Viral content aggregation**: A daily aggregation is dominated by one content ID that receives most events.<br><br>**Payments — Skewed merchant join**: A fraud feature join is slow because one merchant has millions of transactions. |
| [Performance Benchmarking: Building Reproducible Spark Benchmark Suites](../notebooks/10_performance_and_optimization/07_performance_benchmarking.ipynb) | Important | **SaaS — Validating a tuning change**: A team wants to prove that a new partition count improves a nightly job.<br><br>**Financial services — Vendor benchmark claim**: A vendor claims a 5x speedup over the current platform. |

### Module 11 — Fault tolerance and reliability

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Fault Tolerance: RDD Lineage and Partition Recomputation](../notebooks/11_fault_tolerance_and_reliability/01_lineage_and_recomputation.ipynb) | Important | **Financial services — Risk model recomputation**: A long feature pipeline fails near the end after a node restart.<br><br>**Advertising — Non-deterministic pipeline bug**: A job produces different results after a retry. |
| [Executor Failures: Node Eviction, Heartbeats, and State Handling](../notebooks/11_fault_tolerance_and_reliability/02_executor_failure.ipynb) | Important | **Cloud SaaS — Spot-instance churn**: A Spark job runs on spot instances and loses executors during price spikes.<br><br>**Telecom — Node drain during maintenance**: A cluster operator drains a node while a long ETL job is running. |
| [Task Retries, Stage Retries, and Failure Handling](../notebooks/11_fault_tolerance_and_reliability/03_task_retries.ipynb) | Essential | **Telecom — Flaky network shuffle**: A shuffle stage occasionally fails with fetch errors during peak network congestion.<br><br>**Retail — Poison record in a parser**: A malformed record crashes the task on every attempt. |
| [Checkpointing: Truncating Lineage and Durable State Preservation](../notebooks/11_fault_tolerance_and_reliability/04_checkpointing.ipynb) | Advanced | **Machine learning — Iterative model training**: A model trains for many iterations and the lineage grows with each iteration.<br><br>**Financial services — Regulatory audit trail**: A risk pipeline must be reproducible and recoverable for audit. |
| [Fault Tolerance Experiments: Empirical Resilience Verification](../notebooks/11_fault_tolerance_and_reliability/05_fault_tolerance_experiments.ipynb) | Advanced | **SaaS — Chaos testing a nightly ETL**: A platform team wants confidence that executor loss does not corrupt the nightly load.<br><br>**Financial services — Validating a checkpoint strategy**: A long risk pipeline adds checkpointing to reduce recovery time. |

### Module 12 — Advanced topics

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Spark on Apache Hadoop YARN: Architecture and Resource Allocation](../notebooks/12_advanced_topics/01_spark_on_yarn.ipynb) | Advanced | **Enterprise data platform — Shared cluster queue contention**: A finance Spark job waits hours in a YARN queue while other jobs run.<br><br>**Telecom — Container memory mismatch**: A Spark job on YARN is killed for exceeding container memory. |
| [Spark on Kubernetes: Cloud-Native Pod-Based Architecture](../notebooks/12_advanced_topics/02_spark_on_kubernetes.ipynb) | Advanced | **SaaS — Elastic batch platform**: A company runs Spark jobs on a shared Kubernetes cluster with autoscaling.<br><br>**Financial services — Multi-tenant isolation**: Multiple teams share a Kubernetes cluster and need resource isolation. |
| [Client Mode vs Cluster Mode: Deep Comparison and Use Cases](../notebooks/12_advanced_topics/03_cluster_vs_client_mode.ipynb) | Important | **Data science — Notebook exploration**: An analyst wants to interact with a Spark cluster from a Jupyter notebook.<br><br>**Logistics — Production batch reliability**: A nightly job fails when the submitting laptop disconnects. |
| [Resource Allocation: Cores, Memory, and Executor Sizing Formula](../notebooks/12_advanced_topics/04_resource_allocation.ipynb) | Important | **Enterprise platform — Right-sizing a shared cluster**: A cluster runs a mix of small and large jobs with fixed executor settings.<br><br>**Retail — Memory-heavy aggregation**: A large aggregation spills heavily with four-core executors. |
| [Spark Connect: Thin Client Architecture and Decoupled Execution](../notebooks/12_advanced_topics/05_spark_connect.ipynb) | Advanced | **SaaS — Remote data science workspace**: Analysts use lightweight laptops but need the power of a shared Spark cluster.<br><br>**Enterprise — Multi-language access**: Teams use Python, Scala, and SQL against the same platform. |
| [PySpark Internals: Py4J Bridge, Python Daemons, and Serialization Overhead](../notebooks/12_advanced_topics/06_pyspark_internals.ipynb) | Advanced | **Ad technology — Scoring pipeline slowdown**: A PySpark scoring job is much slower than the equivalent SQL pipeline.<br><br>**Risk analytics — Python worker OOM**: A pandas UDF job fails because Python workers exceed their memory. |

### Module 13 — End-to-end projects

| Notebook | Priority | Real-world use cases |
|---|---|---|
| [Project 1: Production Batch ETL Pipeline (CSV/JSON to Partitioned Parquet Lake)](../notebooks/13_end_to_end_projects/01_batch_etl_pipeline.ipynb) | Important | **Retail — Retail order ingestion**: Daily order files arrive from an e-commerce platform and must be published for finance and operations.<br><br>**Logistics — Partner feed onboarding**: A new partner sends shipment events in a different schema and cadence.<br><br>**Financial services — Historical backfill**: A regulatory change requires reprocessing two years of transactions. |
| [Project 2: Large-Scale Sales Analytics & Windowed Business Intelligence](../notebooks/13_end_to_end_projects/02_large_scale_sales_analytics.ipynb) | Important | **Retail — Retail revenue mart**: Finance and merchandising need daily revenue, margin, and product rankings.<br><br>**E-commerce — Marketplace seller analytics**: Sellers need rankings and period-over-period comparisons for their products.<br><br>**Consumer goods — Promotion effectiveness**: Marketing needs to compare sales before, during, and after promotions. |
| [Project 3: Production Data Quality Gate and Automated Audit Pipeline](../notebooks/13_end_to_end_projects/03_data_quality_pipeline.ipynb) | Important | **Telecom — Telecom CDR validation**: Call detail records arrive from many switches with occasional duplicates and missing fields.<br><br>**Healthcare — Healthcare claims audit**: Claims data must satisfy referential integrity with member and provider dimensions.<br><br>**Retail — E-commerce catalog quality**: Product attributes are incomplete or inconsistent across suppliers. |
| [Project 4: Real-Time Event Streaming Analytics with Watermarking and Checkpoints](../notebooks/13_end_to_end_projects/04_streaming_analytics_pipeline.ipynb) | Advanced | **Digital media — Clickstream analytics**: A media company needs minute-level page views and session metrics.<br><br>**Financial services — Fraud alert pipeline**: Transactions must be scored and alerted within seconds.<br><br>**Logistics — IoT fleet monitoring**: Vehicle telemetry arrives continuously and drives maintenance alerts. |

## How to turn a use case into a project

1. **Write the decision first.** Who will act differently because of the output?
2. **Define the grain and the metric.** A dashboard without a grain is a future dispute.
3. **Find the smallest dataset that can prove the mechanics.** Use the lab data first.
4. **Add one quality gate and one failure test.** A successful run is not correctness.
5. **Measure the plan.** Inspect joins, shuffles, task skew, and output file sizes.
6. **Write the trade-off.** Explain why Spark, why this join, and why this partition layout.
7. **State what you did not validate.** Synthetic data cannot prove production behavior.

## When another tool is a better fit

| Situation | Better starting point | Why |
|---|---|---|
| Data fits comfortably on one machine | DuckDB, pandas, Polars | Lower operational overhead and faster iteration |
| Governed, concurrent BI is the main requirement | Warehouse + semantic layer | Concurrency, access control, and metric governance |
| Sub-second event processing | Flink, Kafka Streams, a streaming database | Lower latency and mature state handling |
| Simple scheduled SQL transforms | dbt + warehouse | Easier testing, lineage, and collaboration |
| Ad-hoc exploration of a small extract | Notebook + pandas | No cluster startup or shuffle overhead |

Spark earns its place when data volume, history, multi-source joins, or shared batch/streaming
pipelines exceed what a single machine or warehouse query comfortably handles. Measure before
assuming that threshold has been crossed.
