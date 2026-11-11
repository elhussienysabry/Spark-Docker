# Apache Spark Learning Roadmap

> **From Zero to Production-Ready Spark Engineer in 12 Weeks**

This roadmap provides a structured, week-by-week learning path through Apache Spark—from
first-time setup to production-grade optimization. Every topic links to hands-on notebooks,
lists prerequisites, and tags a priority level so you can focus on what matters most for
your role.

---

## Table of Contents

1. [How to Use This Roadmap](#how-to-use-this-roadmap)
2. [Phase 1: Foundation (Weeks 1–2)](#phase-1-foundation-weeks-12)
3. [Phase 2: Core Data Processing (Weeks 3–4)](#phase-2-core-data-processing-weeks-34)
4. [Phase 3: Execution Engine (Weeks 5–6)](#phase-3-execution-engine-weeks-56)
5. [Phase 4: Performance & Internals (Weeks 7–8)](#phase-4-performance--internals-weeks-78)
6. [Phase 5: Advanced Topics (Weeks 9–10)](#phase-5-advanced-topics-weeks-910)
7. [Phase 6: Production & Projects (Weeks 11–12)](#phase-6-production--projects-weeks-1112)
8. [Topic Priority Matrix](#topic-priority-matrix)
9. [Topic Dependency Graph](#topic-dependency-graph)
10. [Role-Based Learning Paths](#role-based-learning-paths)
11. [Study Tips & Learning Strategies](#study-tips--learning-strategies)
12. [Self-Assessment Checklists](#self-assessment-checklists)

---

## How to Use This Roadmap

| Symbol | Meaning |
|--------|---------|
| Essential | **Essential** — Cannot function as a Spark engineer without this |
| Important | **Important** — Required for intermediate-level work |
| Advanced | **Advanced** — Needed for senior / performance-critical work |
| Optional | **Optional** — Nice-to-have, niche, or deeply specialized |

**Recommended daily schedule (1–2 hours/day):**

```
30 min  — Read the concept doc / notebook markdown cells
45 min  — Run the notebook, modify parameters, observe results
15 min  — Review Spark UI, write notes, complete the checkpoint quiz
```

> [!TIP]
> Don't rush. Running a cell, predicting the output, then checking it against reality
> teaches more than reading ten pages of documentation.

---

## Phase 1: Foundation (Weeks 1–2)

**Goal:** Understand what Spark is, how the cluster operates, and write your first
transformations.

### Week 1: Environment & Architecture

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Docker Compose cluster setup | Essential | 1 h | `00_environment_and_spark_setup/` |
| 2 | Spark Master, Workers, Executors | Essential | 1.5 h | `00_environment_and_spark_setup/` |
| 3 | SparkSession creation & configuration | Essential | 1 h | `00_environment_and_spark_setup/` |
| 4 | Spark application lifecycle | Essential | 1 h | `01_spark_fundamentals/` |
| 5 | Navigating Spark UI (Master, Worker, Driver) | Essential | 1.5 h | `01_spark_fundamentals/` |

**Key Concepts to Master:**
- Driver vs Executor vs Cluster Manager responsibilities
- SparkContext vs SparkSession — when you need each
- How `spark-submit` parameters map to cluster resources
- Reading the Spark Master UI at `http://localhost:8080`

**Checkpoint:** You should be able to:
- [ ] Start the cluster and confirm 2 workers are registered
- [ ] Create a SparkSession with custom app name and configurations
- [ ] Explain the role of Driver, Executor, and Cluster Manager from memory
- [ ] Find a running application in the Spark Master UI

### Week 2: Spark Programming Model Basics

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Immutability & distributed collections | Essential | 1 h | `01_spark_fundamentals/` |
| 2 | Transformations vs Actions | Essential | 1.5 h | `01_spark_fundamentals/` |
| 3 | Narrow vs Wide transformations | Essential | 1.5 h | `01_spark_fundamentals/` |
| 4 | Basic DataFrame operations (select, filter, withColumn) | Essential | 1.5 h | `01_spark_fundamentals/` |
| 5 | Review & practice mini-project | Essential | 2 h | `01_spark_fundamentals/` |

**Key Concepts to Master:**
- Why transformations are lazy and what triggers execution
- The difference between `map` (narrow) and `groupBy` (wide)
- How Spark represents data as distributed partitions
- Schema inference vs explicit schema definition

**Checkpoint:** You should be able to:
- [ ] Explain lazy evaluation and give 3 examples of actions
- [ ] Classify any transformation as narrow or wide
- [ ] Create a DataFrame from a list, CSV, and JSON source
- [ ] Chain 5+ transformations and trigger with a single action

---

## Phase 2: Core Data Processing (Weeks 3–4)

**Goal:** Become fluent in Spark's primary APIs — RDD, DataFrame, and Spark SQL.

### Week 3: RDDs & DataFrame Deep-Dive

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | RDD creation (parallelize, textFile) | Important | 1.5 h | `02_rdd/` |
| 2 | RDD transformations (map, flatMap, filter, reduceByKey) | Important | 1.5 h | `02_rdd/` |
| 3 | RDD actions (collect, count, take, saveAsTextFile) | Important | 1 h | `02_rdd/` |
| 4 | Pair RDDs & key-value operations | Important | 1.5 h | `02_rdd/` |
| 5 | When to use RDD vs DataFrame | Essential | 1 h | `02_rdd/` |

> [!NOTE]
> While DataFrames are the preferred API in modern Spark, understanding RDD mechanics is
> still useful. DataFrames do not literally compile into a hand-written RDD job: Catalyst
> produces a physical plan that runs through Spark SQL operators and Whole-Stage codegen.
> RDD knowledge helps when you debug shuffles, skew, lineage, and partition recovery.

**Key Concepts to Master:**
- RDD lineage and how it enables fault tolerance
- `reduceByKey` vs `groupByKey` — why it matters for performance
- Accumulators and Broadcast variables
- Why DataFrames outperform RDDs in most cases (Catalyst + Tungsten)

### Week 4: Spark SQL, Joins & Aggregations

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Spark SQL basics (createOrReplaceTempView, spark.sql) | Essential | 1.5 h | `03_dataframe_and_spark_sql/` |
| 2 | Column expressions & built-in functions | Essential | 1.5 h | `03_dataframe_and_spark_sql/` |
| 3 | Join types (inner, left, right, full, cross, semi, anti) | Essential | 2 h | `03_dataframe_and_spark_sql/` |
| 4 | Aggregations, groupBy, window functions | Essential | 2 h | `03_dataframe_and_spark_sql/` |
| 5 | UDFs and Pandas UDFs | Important | 1.5 h | `03_dataframe_and_spark_sql/` |

**Key Concepts to Master:**
- How Spark SQL compiles queries to the same execution plan as DataFrame API
- Join strategies: Broadcast Hash Join vs Sort-Merge Join vs Shuffle Hash Join
- Window functions: `row_number`, `rank`, `lead`, `lag`, running totals
- Why scalar UDFs are slow and when Pandas UDFs solve the problem

**Checkpoint:** You should be able to:
- [ ] Write a multi-table join with mixed join types
- [ ] Implement a running total using window functions
- [ ] Explain why `select` + `filter` is equivalent to SQL `SELECT ... WHERE`
- [ ] Read an `explain()` output and identify the join strategy

---

## Phase 3: Execution Engine (Weeks 5–6)

**Goal:** Understand *how* Spark executes your code — the DAG, stages, tasks, and shuffle.

### Week 5: Lazy Evaluation, DAG & Job Execution

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Lazy evaluation internals — logical plan building | Essential | 1.5 h | `04_execution_engine/` |
| 2 | DAG construction and visualization | Essential | 1.5 h | `04_execution_engine/` |
| 3 | Jobs → Stages → Tasks hierarchy | Essential | 2 h | `04_execution_engine/` |
| 4 | Stage boundaries and shuffle dependencies | Essential | 1.5 h | `04_execution_engine/` |
| 5 | Reading the Spark UI: Jobs, Stages, Tasks tabs | Essential | 2 h | `04_execution_engine/` |

**Key Concepts to Master:**

```
                      ┌───────────────────────┐
   Your Code ───────► │   Logical Plan        │
                      │  (Unresolved → Resolved → Optimized)
                      └──────────┬────────────┘
                                 │ Catalyst Optimizer
                      ┌──────────▼────────────┐
                      │   Physical Plan        │
                      │  (Selected Strategy)   │
                      └──────────┬────────────┘
                                 │
                      ┌──────────▼────────────┐
                      │   DAG of RDD Stages    │
                      │  Stage 1 ──► Stage 2   │
                      └──────────┬────────────┘
                                 │
                      ┌──────────▼────────────┐
                      │   Tasks on Executors   │
                      │  (1 task per partition) │
                      └───────────────────────┘
```

- How an action triggers job submission to the DAGScheduler
- Why stage boundaries occur at wide dependencies (shuffle)
- Task scheduling: locality levels (PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL → ANY)
- How to count expected stages from a query plan

### Week 6: Partitions & Shuffle

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | What are partitions — size, count, distribution | Essential | 1.5 h | `05_partitions_and_shuffle/` |
| 2 | Hash partitioning vs Range partitioning | Important | 1.5 h | `05_partitions_and_shuffle/` |
| 3 | Repartition vs Coalesce — when to use each | Essential | 1 h | `05_partitions_and_shuffle/` |
| 4 | Shuffle deep-dive — map side, sort, reduce side | Essential | 2 h | `05_partitions_and_shuffle/` |
| 5 | Diagnosing shuffle issues in Spark UI | Important | 2 h | `05_partitions_and_shuffle/` |

**Key Concepts to Master:**
- The relationship: `spark.sql.shuffle.partitions` → number of reduce tasks
- How data skew causes straggler tasks in shuffle stages
- Shuffle spill: when and why it happens (memory pressure)
- The 128 MB partition guideline and when to deviate

**Checkpoint:** You should be able to:
- [ ] Draw the DAG for a query with 2 joins and 1 aggregation
- [ ] Predict the number of stages from an `explain()` output
- [ ] Identify shuffle-heavy operations and propose alternatives
- [ ] Set optimal partition count for a given data size

---

## Phase 4: Performance & Internals (Weeks 7–8)

**Goal:** Master Spark's internal optimization engine and memory management.

### Week 7: Memory Management & Caching

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Unified Memory Manager — Storage vs Execution memory | Important | 2 h | `06_memory_and_storage/` |
| 2 | On-heap vs Off-heap memory | Advanced | 1.5 h | `06_memory_and_storage/` |
| 3 | Cache vs Persist — storage levels explained | Essential | 1.5 h | `06_memory_and_storage/` |
| 4 | When to cache (and when NOT to cache) | Essential | 1 h | `06_memory_and_storage/` |
| 5 | Memory tuning: `spark.executor.memory`, fractions, overhead | Important | 2 h | `06_memory_and_storage/` |

**Key Concepts to Master:**

```
Executor JVM Heap (spark.executor.memory)
┌──────────────────────────────────────────────────┐
│                                                  │
│  Reserved Memory (300 MB fixed)                  │
│                                                  │
│  ┌────────────────────────────────────────────┐  │
│  │  Unified Memory (spark.memory.fraction)    │  │
│  │  ┌──────────────┬───────────────────────┐  │  │
│  │  │  Storage     │   Execution           │  │  │
│  │  │  (cached     │   (shuffle, sort,     │  │  │
│  │  │   RDDs)      │    aggregation)       │  │  │
│  │  │              │                       │  │  │
│  │  │  ◄── Can borrow from each other ──►  │  │  │
│  │  └──────────────┴───────────────────────┘  │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  User Memory (remaining)                         │
│  (data structures, UDF variables, metadata)      │
│                                                  │
└──────────────────────────────────────────────────┘
```

- Storage levels: MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, OFF_HEAP, serialized variants
- Eviction policies and the LRU cache
- GC pressure from excessive caching
- Reading the Storage tab in Spark UI

### Week 8: Catalyst Optimizer & Tungsten Engine

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Catalyst: Unresolved → Analyzed → Optimized → Physical Plan | Important | 2 h | `07_spark_sql_internals/` |
| 2 | Rule-based optimizations (predicate pushdown, constant folding, column pruning) | Important | 1.5 h | `07_spark_sql_internals/` |
| 3 | Cost-based optimization (CBO) & statistics | Advanced | 1.5 h | `07_spark_sql_internals/` |
| 4 | Tungsten: whole-stage code generation & memory layout | Advanced | 2 h | `07_spark_sql_internals/` |
| 5 | Reading and interpreting `explain(mode="formatted")` | Essential | 1.5 h | `07_spark_sql_internals/` |

**Key Concepts to Master:**
- Why predicate pushdown matters for Parquet/ORC files
- How column pruning eliminates unnecessary I/O
- What the `*` prefix in physical plans means (whole-stage codegen)
- AQE: Adaptive Query Execution — runtime re-optimization

**Checkpoint:** You should be able to:
- [ ] Explain the 4 phases of Catalyst optimization
- [ ] Read an `explain(mode="formatted")` and identify optimizations applied
- [ ] Calculate executor memory breakdown given config parameters
- [ ] Decide whether to cache a DataFrame based on its usage pattern

---

## Phase 5: Advanced Topics (Weeks 9–10)

**Goal:** Extend knowledge to streaming, data formats, and fault tolerance.

### Week 9: Data Sources, Formats & Streaming

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Reading/writing Parquet, ORC, CSV, JSON | Essential | 1.5 h | `08_data_sources_and_formats/` |
| 2 | Schema evolution & partition discovery | Important | 1.5 h | `08_data_sources_and_formats/` |
| 3 | Structured Streaming fundamentals | Important | 2 h | `09_streaming/` |
| 4 | Triggers, output modes, watermarking | Important | 2 h | `09_streaming/` |
| 5 | Streaming joins & stateful processing | Advanced | 2 h | `09_streaming/` |

**Key Concepts to Master:**
- Why Parquet is the de facto standard (columnar, compressed, predicate pushdown)
- Partition discovery: `path/year=2024/month=01/` → automatic partition columns
- Structured Streaming micro-batch vs continuous processing
- Watermarking: handling late-arriving data

### Week 10: Fault Tolerance & Reliability

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | RDD lineage and recomputation | Important | 1.5 h | `11_fault_tolerance_and_reliability/` |
| 2 | Checkpointing: reliable vs local | Important | 1.5 h | `11_fault_tolerance_and_reliability/` |
| 3 | Speculative execution | Advanced | 1 h | `11_fault_tolerance_and_reliability/` |
| 4 | Data deduplication & exactly-once semantics | Advanced | 1.5 h | `11_fault_tolerance_and_reliability/` |
| 5 | Handling corrupt records & data quality | Important | 1.5 h | `11_fault_tolerance_and_reliability/` |

**Key Concepts to Master:**
- How Spark recovers from executor failures without restarting the job
- When to checkpoint vs when to cache
- Write-ahead logs in Structured Streaming
- `failfast` / `permissive` / `dropMalformed` read modes

**Checkpoint:** You should be able to:
- [ ] Explain how Spark recovers when an executor is killed mid-stage
- [ ] Set up a Structured Streaming job with watermarking
- [ ] Choose the right file format for a given workload
- [ ] Configure checkpointing for a streaming application

---

## Phase 6: Production & Projects (Weeks 11–12)

**Goal:** Apply everything in realistic, end-to-end projects and production scenarios.

### Week 11: Optimization & Production Patterns

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Bucketing & Z-ordering for join optimization | Important | 1.5 h | `10_performance_and_optimization/` |
| 2 | Broadcast joins & join hint strategies | Essential | 1.5 h | `10_performance_and_optimization/` |
| 3 | Handling data skew (salting, adaptive skew join) | Important | 2 h | `10_performance_and_optimization/` |
| 4 | Dynamic resource allocation & executor tuning | Advanced | 1.5 h | `10_performance_and_optimization/` |
| 5 | Monitoring with Spark History Server | Important | 1.5 h | `10_performance_and_optimization/` |

### Week 12: End-to-End Projects

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Project 1: ETL pipeline (read → transform → write) | Essential | 3 h | `13_end_to_end_projects/` |
| 2 | Project 2: Analytical queries on large datasets | Essential | 3 h | `13_end_to_end_projects/` |
| 3 | Project 3: Streaming pipeline | Important | 3 h | `13_end_to_end_projects/` |
| 4 | Project 4: Performance diagnosis & tuning exercise | Important | 3 h | `13_end_to_end_projects/` |
| 5 | Case study: e-commerce, telecom, fraud, or logistics | Important | 3–4 h | `14_business_case_studies/` |
| 6 | Capstone: design review, code review, presentation | Essential | 3 h | `13_end_to_end_projects/` |

**Checkpoint:** You should be able to:
- [ ] Build an end-to-end ETL pipeline with proper partitioning and error handling
- [ ] Diagnose a slow query using Spark UI and apply ≥3 optimizations
- [ ] Set up a streaming pipeline with end-to-end exactly-once semantics where the source,
  checkpoint, and sink support them; explain what breaks when one does not
- [ ] Conduct a code review of Spark code and identify anti-patterns

---

## Topic Priority Matrix

The complete reference matrix — use this to look up any topic quickly.

| # | Topic | Priority | Est. Time | Prerequisites | Notebooks | Key Concepts |
|---|-------|----------|-----------|---------------|-----------|-------------|
| 1 | Cluster Setup & Docker | Essential | 1.5 h | None | `00_environment_and_spark_setup/` | Docker Compose, master/worker, ports |
| 2 | Spark Architecture | Essential | 2 h | #1 | `00_environment_and_spark_setup/` | Driver, Executor, Cluster Manager |
| 3 | SparkSession & Config | Essential | 1.5 h | #1 | `00_environment_and_spark_setup/` | SparkSession.builder, config keys |
| 4 | Transformations & Actions | Essential | 2 h | #3 | `01_spark_fundamentals/` | Lazy eval, narrow/wide, action triggers |
| 5 | DataFrame Basics | Essential | 2 h | #3 | `01_spark_fundamentals/` | select, filter, withColumn, schema |
| 6 | RDD API | Important | 3 h | #4 | `02_rdd/` | parallelize, map, reduceByKey, lineage |
| 7 | Pair RDDs | Important | 2 h | #6 | `02_rdd/` | groupByKey, reduceByKey, aggregateByKey |
| 8 | Broadcast & Accumulators | Important | 1.5 h | #6 | `02_rdd/` | Shared variables, side effects |
| 9 | Spark SQL | Essential | 2 h | #5 | `03_dataframe_and_spark_sql/` | SQL queries, temp views, catalog |
| 10 | Column Functions | Essential | 2 h | #5 | `03_dataframe_and_spark_sql/` | F.col, F.lit, F.when, type casting |
| 11 | Join Operations | Essential | 3 h | #5, #9 | `03_dataframe_and_spark_sql/` | 7 join types, join conditions |
| 12 | Window Functions | Essential | 2.5 h | #10 | `03_dataframe_and_spark_sql/` | row_number, rank, lead/lag, frames |
| 13 | Aggregations | Essential | 2 h | #10 | `03_dataframe_and_spark_sql/` | groupBy, agg, pivot, cube, rollup |
| 14 | UDFs & Pandas UDFs | Important | 2 h | #10 | `03_dataframe_and_spark_sql/` | Scalar UDF, Pandas UDF, Arrow |
| 15 | DAG & Job Execution | Essential | 2.5 h | #4 | `04_execution_engine/` | DAGScheduler, stages, task slots |
| 16 | Spark UI Navigation | Essential | 2 h | #15 | `04_execution_engine/` | Jobs, Stages, Tasks, SQL tabs |
| 17 | Partitions | Essential | 2 h | #4 | `05_partitions_and_shuffle/` | Partition count, size, distribution |
| 18 | Repartition & Coalesce | Essential | 1.5 h | #17 | `05_partitions_and_shuffle/` | Full shuffle vs merge partitions |
| 19 | Shuffle Deep-Dive | Essential | 2.5 h | #15, #17 | `05_partitions_and_shuffle/` | Map-side, reduce-side, sort |
| 20 | Hash vs Range Partitioning | Important | 1.5 h | #17 | `05_partitions_and_shuffle/` | HashPartitioner, RangePartitioner |
| 21 | Memory Management | Important | 2.5 h | #15 | `06_memory_and_storage/` | Unified memory, fractions, overhead |
| 22 | Cache & Persist | Essential | 2 h | #21 | `06_memory_and_storage/` | Storage levels, unpersist, when to cache |
| 23 | Off-Heap Memory | Advanced | 1.5 h | #21 | `06_memory_and_storage/` | sun.misc.Unsafe, Tungsten memory |
| 24 | Catalyst Optimizer | Important | 2.5 h | #15 | `07_spark_sql_internals/` | 4 phases, rule-based optimization |
| 25 | Predicate Pushdown | Important | 1.5 h | #24 | `07_spark_sql_internals/` | Filter pushdown to data source |
| 26 | Cost-Based Optimization | Advanced | 2 h | #24 | `07_spark_sql_internals/` | Table stats, ANALYZE TABLE, histograms |
| 27 | Whole-Stage Codegen | Advanced | 2 h | #24 | `07_spark_sql_internals/` | Tungsten, Java bytecode, * prefix |
| 28 | AQE (Adaptive Query Execution) | Important | 2 h | #24, #19 | `07_spark_sql_internals/` | Runtime re-optimization, skew handling |
| 29 | File Formats (Parquet/ORC/CSV/JSON) | Essential | 2 h | #5 | `08_data_sources_and_formats/` | Columnar vs row, compression |
| 30 | Schema Evolution | Important | 1.5 h | #29 | `08_data_sources_and_formats/` | mergeSchema, column additions |
| 31 | Partition Discovery | Important | 1 h | #29, #17 | `08_data_sources_and_formats/` | Hive-style partitioning, pruning |
| 32 | Structured Streaming | Important | 3 h | #5, #9 | `09_streaming/` | Micro-batch, triggers, output modes |
| 33 | Watermarking | Important | 2 h | #32 | `09_streaming/` | Late data, event-time processing |
| 34 | Streaming Joins | Advanced | 2 h | #32, #11 | `09_streaming/` | Stream-stream, stream-static |
| 35 | RDD Lineage & Recomputation | Important | 1.5 h | #6, #15 | `11_fault_tolerance_and_reliability/` | DAG replay, lost partitions |
| 36 | Checkpointing | Important | 1.5 h | #35 | `11_fault_tolerance_and_reliability/` | Reliable vs local checkpointing |
| 37 | Speculative Execution | Advanced | 1 h | #15 | `11_fault_tolerance_and_reliability/` | Straggler detection, duplicate tasks |
| 38 | Broadcast Joins & Hints | Essential | 2 h | #11 | `10_performance_and_optimization/` | Threshold, hint syntax, BHJ vs SMJ |
| 39 | Data Skew Handling | Important | 2.5 h | #19, #17 | `10_performance_and_optimization/` | Salting, AQE skew join |
| 40 | Bucketing | Important | 1.5 h | #11, #29 | `10_performance_and_optimization/` | Pre-shuffle, sort within buckets |
| 41 | Dynamic Resource Allocation | Advanced | 1.5 h | #2 | `10_performance_and_optimization/` | Min/max executors, idle timeout |
| 42 | History Server | Important | 1 h | #16 | `10_performance_and_optimization/` | Event logs, completed apps |
| 43 | End-to-End ETL Project | Essential | 3 h | #29, #11, #13 | `13_end_to_end_projects/` | Full pipeline design |
| 44 | End-to-End Streaming Project | Important | 3 h | #32, #33 | `13_end_to_end_projects/` | Streaming pipeline design |
| 45 | Advanced Configurations | Advanced | 2 h | #21, #41 | `12_advanced_topics/` | Tuning configs, serialization |
| 46 | Business Case Studies | Important | 12–16 h | #11, #13, #29, #32 | `14_business_case_studies/` | Grain, metric definitions, quality, production trade-offs |

---

## Detailed Notebook Matrix

This appendix maps every curriculum notebook to its priority, estimated effort, prerequisites,
hands-on experiment, and primary role focus. Effort is an estimate for a learner who already
meets the prerequisites; the experiment column names the lab in the notebook, not an additional
assignment. Role focus is a shorthand: **DE** = data engineer, **Senior** = senior data engineer,
**Specialist** = Spark/platform specialist.

### Module 00 — Environment and setup

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`00_spark_environment`](../notebooks/00_environment_and_spark_setup/00_spark_environment.ipynb) | Essential | 1–2 h | Docker Engine, Docker Compose, basic terminal skills | Control-plane vs data-plane verification | DE foundation |

### Module 01 — Spark fundamentals

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_introduction_to_spark`](../notebooks/01_spark_fundamentals/01_introduction_to_spark.ipynb) | Essential | 1–2 h | None | Measure the cost of starting a Spark job | DE foundation |
| [`02_spark_architecture`](../notebooks/01_spark_fundamentals/02_spark_architecture.ipynb) | Essential | 2–3 h | Introduction to Spark | Map the control plane and data plane | DE foundation |
| [`03_spark_session_and_spark_context`](../notebooks/01_spark_fundamentals/03_spark_session_and_spark_context.ipynb) | Essential | 1–2 h | Spark architecture | Configuration precedence | DE foundation |
| [`04_spark_cluster_execution`](../notebooks/01_spark_fundamentals/04_spark_cluster_execution.ipynb) | Essential | 2–3 h | Spark architecture, SparkSession | Predict stages before running | DE foundation |

### Module 02 — RDDs

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_rdd_fundamentals`](../notebooks/02_rdd/01_rdd_fundamentals.ipynb) | Important | 3–4 h | Lazy evaluation, partitions, basic Python | RDD vs DataFrame on the same aggregation | DE + Senior |
| [`02_rdd_transformations_and_actions`](../notebooks/02_rdd/02_rdd_transformations_and_actions.ipynb) | Important | 2–3 h | RDD fundamentals | groupByKey vs reduceByKey shuffle volume | DE + Senior |
| [`03_rdd_partitions`](../notebooks/02_rdd/03_rdd_partitions.ipynb) | Important | 2–3 h | RDD transformations | Partition count vs runtime | DE + Senior |
| [`04_rdd_dependencies_and_lineage`](../notebooks/02_rdd/04_rdd_dependencies_and_lineage.ipynb) | Important | 2–3 h | RDD partitions | Read a lineage graph | DE + Senior |
| [`05_rdd_caching_and_persistence`](../notebooks/02_rdd/05_rdd_caching_and_persistence.ipynb) | Important | 2–3 h | RDD lineage, memory basics | Cache hit vs recomputation | DE + Senior |

### Module 03 — DataFrames and Spark SQL

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_dataframe_fundamentals`](../notebooks/03_dataframe_and_spark_sql/01_dataframe_fundamentals.ipynb) | Essential | 2–3 h | Lazy evaluation, basic Python | Logical plan vs physical plan | DE foundation |
| [`02_schemas_and_data_types`](../notebooks/03_dataframe_and_spark_sql/02_schemas_and_data_types.ipynb) | Essential | 2–3 h | DataFrame fundamentals | Inference vs explicit schema | DE foundation |
| [`03_dataframe_transformations`](../notebooks/03_dataframe_and_spark_sql/03_dataframe_transformations.ipynb) | Essential | 2–3 h | DataFrame fundamentals, schemas | Filter placement | DE foundation |
| [`04_spark_sql`](../notebooks/03_dataframe_and_spark_sql/04_spark_sql.ipynb) | Essential | 2–3 h | DataFrames, schemas | View scope and catalog | DE foundation |
| [`05_joins_and_aggregations`](../notebooks/03_dataframe_and_spark_sql/05_joins_and_aggregations.ipynb) | Essential | 3–4 h | DataFrames, Spark SQL, partitions | Broadcast vs sort-merge plan | DE foundation |
| [`06_window_functions`](../notebooks/03_dataframe_and_spark_sql/06_window_functions.ipynb) | Essential | 3–4 h | Joins, aggregations, Spark SQL | row_number vs rank vs dense_rank | DE foundation |
| [`07_nulls_and_data_quality`](../notebooks/03_dataframe_and_spark_sql/07_nulls_and_data_quality.ipynb) | Essential | 2–3 h | DataFrames, SQL, joins | Null behavior in joins and aggregations | DE foundation |
| [`08_udfs_and_pandas_udfs`](../notebooks/03_dataframe_and_spark_sql/08_udfs_and_pandas_udfs.ipynb) | Important | 2–3 h | DataFrames, Python, Arrow basics | Built-in vs Python UDF vs pandas UDF | DE + Senior |

### Module 04 — Execution engine

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_lazy_evaluation`](../notebooks/04_execution_engine/01_lazy_evaluation.ipynb) | Essential | 1–2 h | DataFrames, basic execution model | Transformation vs action timing | DE foundation |
| [`02_dag_and_execution_plans`](../notebooks/04_execution_engine/02_dag_and_execution_plans.ipynb) | Important | 2–3 h | Lazy evaluation, Catalyst basics | Read a formatted plan | DE + Senior |
| [`03_dag_scheduler`](../notebooks/04_execution_engine/03_dag_scheduler.ipynb) | Advanced | 2–3 h | DAG/plans, jobs/stages/tasks | Stage attempts after an executor failure | Senior + Specialist |
| [`04_task_scheduler`](../notebooks/04_execution_engine/04_task_scheduler.ipynb) | Advanced | 2–3 h | DAGScheduler, cluster architecture | Locality levels and wait time | Senior + Specialist |
| [`05_jobs_stages_and_tasks`](../notebooks/04_execution_engine/05_jobs_stages_and_tasks.ipynb) | Essential | 2–3 h | Lazy evaluation, DAG basics | Count jobs, stages, and tasks | DE foundation |
| [`06_narrow_and_wide_transformations`](../notebooks/04_execution_engine/06_narrow_and_wide_transformations.ipynb) | Essential | 2–3 h | Jobs/stages/tasks, partitions | Classify and verify | DE foundation |
| [`07_dependencies_and_stage_boundaries`](../notebooks/04_execution_engine/07_dependencies_and_stage_boundaries.ipynb) | Advanced | 2–3 h | Narrow/wide transformations, DAGScheduler | Derive stages from a dependency graph | Senior + Specialist |

### Module 05 — Partitions and shuffle

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`02_repartition_and_coalesce`](../notebooks/05_partitions_and_shuffle/02_repartition_and_coalesce.ipynb) | Essential | 2–3 h | Partitioning fundamentals, lazy evaluation | Measure the cost of repartition versus coalesce | DE foundation |
| [`03_shuffle_internals`](../notebooks/05_partitions_and_shuffle/03_shuffle_internals.ipynb) | Advanced | 3–4 h | Partitioning, narrow/wide transformations, Spark UI | Trace one shuffle end to end | Senior + Specialist |
| [`04_shuffle_operations`](../notebooks/05_partitions_and_shuffle/04_shuffle_operations.ipynb) | Essential | 2–3 h | Shuffle internals, DataFrame transformations | Classify operators by shuffle behavior | DE foundation |
| [`05_partitioning_strategies`](../notebooks/05_partitions_and_shuffle/05_partitioning_strategies.ipynb) | Important | 2–3 h | Shuffle operations, file formats, data lakes | Compare partition layouts by scan bytes | DE + Senior |
| [`06_data_skew_and_solutions`](../notebooks/05_partitions_and_shuffle/06_data_skew_and_solutions.ipynb) | Essential | 3–4 h | Shuffle internals, joins, aggregations, Spark UI | Detect and fix a hot key | DE foundation |
| [`01_partitioning_fundamentals`](../notebooks/05_partitions_and_shuffle/01_partitioning_fundamentals.ipynb) | Essential | 2–3 h | RDD partitions, lazy evaluation, cluster architecture | Find the parallelism knee | DE foundation |

### Module 06 — Memory and storage

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_spark_memory_architecture`](../notebooks/06_memory_and_storage/01_spark_memory_architecture.ipynb) | Important | 2–3 h | JVM basics, executors, partitioning | Watch the unified pool under pressure | DE + Senior |
| [`02_executor_memory`](../notebooks/06_memory_and_storage/02_executor_memory.ipynb) | Essential | 2–3 h | Spark memory architecture, container basics | Calculate a safe container budget | DE foundation |
| [`03_storage_and_execution_memory`](../notebooks/06_memory_and_storage/03_storage_and_execution_memory.ipynb) | Advanced | 2–3 h | Unified memory model, cache/persist | Force cache eviction | Senior + Specialist |
| [`04_cache_and_persist`](../notebooks/06_memory_and_storage/04_cache_and_persist.ipynb) | Essential | 2–3 h | Storage/execution memory, lazy evaluation | Prove that a cache pays off | DE foundation |
| [`05_spill_to_disk`](../notebooks/06_memory_and_storage/05_spill_to_disk.ipynb) | Important | 2–3 h | Memory architecture, shuffle internals | Reduce spill by projecting columns | DE + Senior |
| [`06_broadcast_variables`](../notebooks/06_memory_and_storage/06_broadcast_variables.ipynb) | Important | 2–3 h | Executors, serialization, memory model | Compare closure capture with broadcast | DE + Senior |
| [`07_broadcast_join`](../notebooks/06_memory_and_storage/07_broadcast_join.ipynb) | Essential | 2–3 h | Joins, broadcast variables, AQE | Find the broadcast threshold | DE foundation |

### Module 07 — Spark SQL internals

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_catalyst_optimizer`](../notebooks/07_spark_sql_internals/01_catalyst_optimizer.ipynb) | Advanced | 3–4 h | DataFrames, Spark SQL, logical plans | Watch a predicate move | Senior + Specialist |
| [`02_logical_and_physical_plans`](../notebooks/07_spark_sql_internals/02_logical_and_physical_plans.ipynb) | Advanced | 2–3 h | Catalyst basics, DataFrame API | Diff two plans | Senior + Specialist |
| [`03_tungsten_and_whole_stage_codegen`](../notebooks/07_spark_sql_internals/03_tungsten_and_whole_stage_codegen.ipynb) | Advanced | 3–4 h | JVM, physical plans, serialization | Find a codegen fallback | Senior + Specialist |
| [`04_join_strategies`](../notebooks/07_spark_sql_internals/04_join_strategies.ipynb) | Essential | 3–4 h | Physical plans, broadcast joins, partitioning | Compare join strategies | DE foundation |
| [`05_adaptive_query_execution`](../notebooks/07_spark_sql_internals/05_adaptive_query_execution.ipynb) | Important | 3–4 h | Join strategies, shuffle, physical plans | See AQE change the plan | DE + Senior |

### Module 08 — Data sources and formats

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_csv_and_json`](../notebooks/08_data_sources_and_formats/01_csv_and_json.ipynb) | Essential | 2–3 h | DataFrame basics, schemas, file paths | Compare inference with an explicit schema | DE foundation |
| [`02_parquet_and_orc`](../notebooks/08_data_sources_and_formats/02_parquet_and_orc.ipynb) | Essential | 2–3 h | CSV/JSON, schemas, columnar storage basics | Measure column pruning | DE foundation |
| [`03_partitioned_data_lake`](../notebooks/08_data_sources_and_formats/03_partitioned_data_lake.ipynb) | Important | 3–4 h | Parquet/ORC, partitioning strategies | Prove partition pruning | DE + Senior |
| [`04_schema_evolution`](../notebooks/08_data_sources_and_formats/04_schema_evolution.ipynb) | Important | 2–3 h | Parquet/ORC, partitioned lakes, schema basics | Read old and new files together | DE + Senior |
| [`05_reading_and_writing_data`](../notebooks/08_data_sources_and_formats/05_reading_and_writing_data.ipynb) | Essential | 2–3 h | Data sources, schemas, partitioning | Make a write idempotent | DE foundation |

### Module 09 — Streaming

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_streaming_fundamentals`](../notebooks/09_streaming/01_streaming_fundamentals.ipynb) | Important | 2–3 h | DataFrames, sources/sinks, basic architecture | Observe micro-batch progress | DE + Senior |
| [`02_structured_streaming`](../notebooks/09_streaming/02_structured_streaming.ipynb) | Essential | 2–3 h | Streaming fundamentals, DataFrame API | Stream a landing directory | DE foundation |
| [`03_triggers_and_output_modes`](../notebooks/09_streaming/03_triggers_and_output_modes.ipynb) | Essential | 2–3 h | Structured Streaming basics, DataFrames | Compare output modes | DE foundation |
| [`04_watermarks_and_late_data`](../notebooks/09_streaming/04_watermarks_and_late_data.ipynb) | Important | 3–4 h | Streaming aggregations, event time, output modes | Measure late-data loss | DE + Senior |
| [`05_checkpoints_and_fault_tolerance`](../notebooks/09_streaming/05_checkpoints_and_fault_tolerance.ipynb) | Advanced | 3–4 h | Streaming queries, sources/sinks, delivery semantics | Simulate a failure between sink and commit | Senior + Specialist |
| [`06_kafka_integration`](../notebooks/09_streaming/06_kafka_integration.ipynb) | Advanced | 3–4 h | Streaming fundamentals, checkpoints, Kafka basics | Read Kafka with bounded batches | Senior + Specialist |

### Module 10 — Performance and optimization

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_spark_ui_analysis`](../notebooks/10_performance_and_optimization/01_spark_ui_analysis.ipynb) | Essential | 2–3 h | Jobs/stages/tasks, shuffle, memory basics | Find the dominant stage | DE foundation |
| [`02_explain_and_execution_plans`](../notebooks/10_performance_and_optimization/02_explain_and_execution_plans.ipynb) | Important | 2–3 h | Logical/physical plans, Spark UI | Read a plan bottom-up | DE + Senior |
| [`03_query_optimization`](../notebooks/10_performance_and_optimization/03_query_optimization.ipynb) | Essential | 3–4 h | SQL, joins, partitions, plans | Optimize one query at a time | DE foundation |
| [`04_shuffle_optimization`](../notebooks/10_performance_and_optimization/04_shuffle_optimization.ipynb) | Important | 3–4 h | Shuffle internals, partitioning, joins | Cut shuffle bytes with projection | DE + Senior |
| [`05_memory_optimization`](../notebooks/10_performance_and_optimization/05_memory_optimization.ipynb) | Advanced | 3–4 h | Memory architecture, serialization, Spark UI | Compare serialized and deserialized cache | Senior + Specialist |
| [`06_data_skew_optimization`](../notebooks/10_performance_and_optimization/06_data_skew_optimization.ipynb) | Essential | 3–4 h | Data skew basics, joins, AQE | Compare skew remedies | DE foundation |
| [`07_performance_benchmarking`](../notebooks/10_performance_and_optimization/07_performance_benchmarking.ipynb) | Important | 2–3 h | Spark UI, plans, statistics | Run a controlled comparison | DE + Senior |

### Module 11 — Fault tolerance and reliability

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_lineage_and_recomputation`](../notebooks/11_fault_tolerance_and_reliability/01_lineage_and_recomputation.ipynb) | Important | 2–3 h | RDD dependencies, shuffle, stages | Observe partition recomputation | DE + Senior |
| [`02_executor_failure`](../notebooks/11_fault_tolerance_and_reliability/02_executor_failure.ipynb) | Important | 2–3 h | Cluster architecture, lineage, shuffle | Observe executor loss | DE + Senior |
| [`03_task_retries`](../notebooks/11_fault_tolerance_and_reliability/03_task_retries.ipynb) | Essential | 2–3 h | Task scheduling, executor failure, Spark UI | Force a task failure | DE foundation |
| [`04_checkpointing`](../notebooks/11_fault_tolerance_and_reliability/04_checkpointing.ipynb) | Advanced | 3–4 h | Lineage, persistence, reliable storage | Compare cache and checkpoint | Senior + Specialist |
| [`05_fault_tolerance_experiments`](../notebooks/11_fault_tolerance_and_reliability/05_fault_tolerance_experiments.ipynb) | Advanced | 3–4 h | Executor failure, task retries, checkpointing | Kill an executor and verify output | Senior + Specialist |

### Module 12 — Advanced topics

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_spark_on_yarn`](../notebooks/12_advanced_topics/01_spark_on_yarn.ipynb) | Advanced | 3–4 h | Cluster architecture, resource managers, deployment modes | Trace a YARN submission | Senior + Specialist |
| [`02_spark_on_kubernetes`](../notebooks/12_advanced_topics/02_spark_on_kubernetes.ipynb) | Advanced | 3–4 h | Containers, Kubernetes basics, Spark architecture | Inspect the pod lifecycle | Senior + Specialist |
| [`03_cluster_vs_client_mode`](../notebooks/12_advanced_topics/03_cluster_vs_client_mode.ipynb) | Important | 2–3 h | Deployment modes, cluster architecture, networking | Compare driver locations | DE + Senior |
| [`04_resource_allocation`](../notebooks/12_advanced_topics/04_resource_allocation.ipynb) | Important | 3–4 h | Executor sizing, cluster resources, memory overhead | Compare executor shapes | DE + Senior |
| [`05_spark_connect`](../notebooks/12_advanced_topics/05_spark_connect.ipynb) | Advanced | 2–3 h | SparkSession, client/server architecture, gRPC | Connect to a remote server | Senior + Specialist |
| [`06_pyspark_internals`](../notebooks/12_advanced_topics/06_pyspark_internals.ipynb) | Advanced | 3–4 h | PySpark basics, JVM, serialization | Compare UDF styles | Senior + Specialist |

### Module 13 — End-to-end projects

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_batch_etl_pipeline`](../notebooks/13_end_to_end_projects/01_batch_etl_pipeline.ipynb) | Important | 4–6 h | DataFrames, data sources, partitioning, writes | Run the pipeline twice | DE + Senior |
| [`02_large_scale_sales_analytics`](../notebooks/13_end_to_end_projects/02_large_scale_sales_analytics.ipynb) | Important | 4–6 h | Joins, windows, partitioning, query optimization | Validate a sales mart | DE + Senior |
| [`03_data_quality_pipeline`](../notebooks/13_end_to_end_projects/03_data_quality_pipeline.ipynb) | Important | 4–6 h | Data validation, SQL, pipeline design | Add a quality gate | DE + Senior |
| [`04_streaming_analytics_pipeline`](../notebooks/13_end_to_end_projects/04_streaming_analytics_pipeline.ipynb) | Advanced | 4–6 h | Structured Streaming, watermarks, checkpoints, sinks | Test late and duplicate events | Senior + Specialist |

### Module 14 — Business case studies

These are integrative cases, not new syntax topics. Complete at least Modules 03, 05, 06,
08, and 10 before attempting the fraud case; the others are approachable after Module 03
and Module 08.

| Notebook | Priority | Effort | Prerequisites | Experiment | Role focus |
|---|---|---|---|---|---|
| [`01_ecommerce_analytics`](../notebooks/14_business_case_studies/01_ecommerce_analytics.ipynb) | Important | 3–4 h | Joins, windows, broadcast joins, partitioned writes | Change the RFM window and explain segment movement | DE + Senior |
| [`02_telecom_network_analytics`](../notebooks/14_business_case_studies/02_telecom_network_analytics.ipynb) | Important | 3–4 h | Aggregations, percentiles, windows, event time | Compare average and p95 latency rankings | DE + Senior |
| [`03_financial_fraud_patterns`](../notebooks/14_business_case_studies/03_financial_fraud_patterns.ipynb) | Advanced | 3–4 h | Window functions, event time, precision/recall | Move the threshold and quantify the review load | Senior + Specialist |
| [`04_logistics_delivery_analytics`](../notebooks/14_business_case_studies/04_logistics_delivery_analytics.ipynb) | Important | 3–4 h | Date/time handling, joins, windows, SLA design | Change the on-time denominator and compare carriers | DE + Senior |

See [CASE_STUDIES.md](CASE_STUDIES.md) for business context, schemas, expected results, and
validation status.

---

## Topic Dependency Graph

The following diagram shows how topics build upon each other. Always complete
prerequisites before moving to dependent topics.

```
                    ┌─────────────────────┐
                    │  1. Cluster Setup   │
                    │     & Docker        │
                    └─────────┬───────────┘
                              │
                    ┌─────────▼───────────┐
                    │  2. Spark           │
                    │     Architecture    │
                    └────┬────────────┬───┘
                         │            │
              ┌──────────▼──┐   ┌─────▼──────────┐
              │ 3. Spark    │   │ 41. Dynamic     │
              │    Session  │   │     Resource    │
              │    & Config │   │     Allocation  │
              └──────┬──────┘   └────────────────┘
                     │
          ┌──────────┼──────────────┐
          │          │              │
   ┌──────▼──┐  ┌───▼──────┐  ┌───▼────────────┐
   │ 4. Trans│  │ 5. Data  │  │ 29. File       │
   │  & Acts │  │   Frame  │  │     Formats    │
   └──┬──┬───┘  │   Basics │  └──┬──────┬──────┘
      │  │      └─┬──┬──┬──┘     │      │
      │  │        │  │  │        │      │
      │  │   ┌────┘  │  └────┐   │    ┌─▼──────────┐
      │  │   │       │       │   │    │ 30. Schema  │
      │  │   │       │       │   │    │   Evolution │
      │  │   │       │       │   │    └─────────────┘
      │  │   │       │       │   │
   ┌──▼──▼─┐ │  ┌────▼──┐  ┌▼───▼──┐  ┌────────────┐
   │ 6. RDD│ │  │ 9.    │  │10. Col│  │31. Partition│
   │   API │ │  │  Spark│  │   Func│  │   Discovery │
   └─┬──┬──┘ │  │  SQL  │  └─┬──┬──┘  └────────────┘
     │  │    │  └───┬───┘    │  │
     │  │    │      │        │  │
   ┌─▼──┘    │   ┌──▼────────▼┐ │
   │ 7.Pair  │   │ 11. Joins  │ │
   │   RDDs  │   └─┬──────┬──┘ │
   └─────┘   │     │      │    │
     │       │     │      │    │
   ┌─▼──────┐│  ┌──▼───┐  │  ┌─▼──────────┐
   │ 8.Bcast││  │12.Win│  │  │13. Agg     │
   │  &Accum││  │  Func│  │  │   regations│
   └────────┘│  └──────┘  │  └────────────┘
             │            │
   ┌─────────▼──────┐  ┌──▼────────┐
   │ 15. DAG & Job  │  │14. UDFs   │
   │    Execution   │  └───────────┘
   └──┬──┬──┬───────┘
      │  │  │
      │  │  └──────────────────────┐
      │  │                         │
   ┌──▼──▼──────┐  ┌──────────┐  ┌▼───────────┐
   │ 16. Spark  │  │17. Part- │  │21. Memory  │
   │     UI     │  │  itions  │  │   Mgmt     │
   └──┬─────────┘  └──┬───┬──┘  └──┬──┬──────┘
      │               │   │        │  │
      │            ┌──▼┐  ▼──┐  ┌──▼┐ └───────┐
      │            │18.│ │20.│  │22.│  ┌──────▼──┐
      │            │Rep│ │H/R│  │Ca-│  │23.Off   │
      │            │art│ │Par│  │che│  │   Heap  │
      │            └───┘ └───┘  └───┘  └─────────┘
      │               │
      │         ┌─────▼────────┐
      │         │ 19. Shuffle  │
      │         │   Deep-Dive  │
      │         └──┬───────┬───┘
      │            │       │
      │    ┌───────▼──┐  ┌─▼───────────┐
      │    │39. Data  │  │24. Catalyst │
      │    │   Skew   │  │   Optimizer │
      │    └──────────┘  └──┬──┬───┬───┘
      │                     │  │   │
      │              ┌──────▼┐ │ ┌─▼──────────┐
      │              │25.Pred│ │ │27.Whole-Stg │
      │              │  Push │ │ │  Codegen    │
      │              └───────┘ │ └─────────────┘
      │                     ┌──▼──────────┐
      ▼                     │26. CBO      │
   ┌──────────┐             └─────────────┘
   │42.History│        ┌──────────────┐
   │  Server  │        │28. AQE       │
   └──────────┘        └──────────────┘
```

> [!IMPORTANT]
> Always verify you've completed the prerequisites before starting a new topic.
> Skipping foundations leads to shallow understanding that collapses under
> production pressure.

---

## Role-Based Learning Paths

Not everyone needs to learn everything. Choose the path that matches your role,
then expand from there.

### Path A: Data Engineer

**Focus:** Build reliable ETL/ELT pipelines, manage data quality, optimize data layouts.

**Duration:** 8 weeks

| Week | Topics (by #) | Focus Area |
|------|--------------|------------|
| 1 | 1, 2, 3 | Environment & Architecture |
| 2 | 4, 5, 29 | Core APIs & File Formats |
| 3 | 9, 10, 11 | SQL, Joins |
| 4 | 12, 13, 30, 31 | Window Functions, Aggregations, Schema Evolution |
| 5 | 15, 16, 17, 18 | Execution Model, Partitions |
| 6 | 19, 22, 38 | Shuffle, Caching, Broadcast Joins |
| 7 | 39, 40, 35, 36 | Skew, Bucketing, Fault Tolerance |
| 8 | 43 | End-to-End ETL Project |

**Must-Know Topics:** 1–5, 9–13, 15–19, 22, 29, 38, 43
**Skip or Defer:** 23, 26, 27, 37, 41, 44

---

### Path B: Senior Data Engineer

**Focus:** Everything in Path A plus deep performance tuning, Catalyst internals,
and production monitoring.

**Duration:** 10 weeks (Path A + 2 weeks of advanced topics)

| Week | Topics (by #) | Focus Area |
|------|--------------|------------|
| 1–8 | (Same as Path A) | Core Data Engineering |
| 9 | 21, 24, 25, 28 | Memory Management, Catalyst, AQE |
| 10 | 26, 27, 41, 42, 45 | CBO, Codegen, Dynamic Allocation, History Server |

**Additional Must-Know Topics:** 21, 24, 25, 28
**Should-Know Topics:** 26, 27, 41, 42, 45

---

### Path C: Analytics Engineer

**Focus:** SQL-centric analysis, window functions, data exploration, reporting pipelines.

**Duration:** 6 weeks

| Week | Topics (by #) | Focus Area |
|------|--------------|------------|
| 1 | 1, 2, 3, 5 | Setup & DataFrames |
| 2 | 9, 10, 29 | SQL & File Formats |
| 3 | 11, 12, 13 | Joins, Windows, Aggregations |
| 4 | 14, 15, 16 | UDFs, Execution Model basics |
| 5 | 17, 22, 38 | Partitions, Caching, Broadcast Joins |
| 6 | 43 | End-to-End Project |

**Must-Know Topics:** 1–3, 5, 9–13, 29
**Skip or Defer:** 6–8, 19–21, 23–28, 32–37, 41, 44, 45

---

### Path D: Platform / Big Data Engineer

**Focus:** Cluster tuning, resource management, monitoring, fault tolerance,
infrastructure-level optimization.

**Duration:** 10 weeks

| Week | Topics (by #) | Focus Area |
|------|--------------|------------|
| 1 | 1, 2, 3 | Cluster Setup, Architecture |
| 2 | 4, 5, 15, 16 | Core APIs, DAG, Spark UI |
| 3 | 17, 18, 19, 20 | Partitions & Shuffle |
| 4 | 21, 22, 23 | Memory Management (all levels) |
| 5 | 24, 25, 26, 27 | Catalyst & Tungsten |
| 6 | 28, 37, 41 | AQE, Speculative Exec, Dynamic Allocation |
| 7 | 35, 36, 42 | Fault Tolerance, History Server |
| 8 | 39, 40, 45 | Skew, Bucketing, Advanced Config |
| 9 | 32, 33 | Streaming Basics |
| 10 | 43, 44 | End-to-End Projects |

**Must-Know Topics:** 1–3, 15–23, 41, 42
**Unique to this path:** 23 (Off-Heap), 37 (Speculative), 41 (Dynamic Allocation), 45 (Advanced Config)

---

### Path Comparison at a Glance

```
                         Data Eng  Sr. Data Eng  Analytics Eng  Platform Eng
                         ────────  ────────────  ─────────────  ────────────
Environment & Setup         ██████    ██████████    ██████████    ██████████
RDD API                     ████████  ██████████    ░░░░░░░░░░    ████░░░░░░
DataFrame & SQL             ██████████ ██████████   ██████████    ████████░░
Execution Engine            ████████  ██████████    ████░░░░░░    ██████████
Partitions & Shuffle        ████████  ██████████    ████░░░░░░    ██████████
Memory Management           ████░░░░  ████████░░    ░░░░░░░░░░    ██████████
Catalyst & Internals        ░░░░░░░░  ████████░░    ░░░░░░░░░░    ██████████
Streaming                   ░░░░░░░░  ░░░░░░░░░░    ░░░░░░░░░░    ████████░░
Fault Tolerance             ████████  ████████░░    ░░░░░░░░░░    ██████████
Performance & Tuning        ████████  ██████████    ████░░░░░░    ██████████

██ = Core Focus   ░░ = Optional / Skip
```

---

## Study Tips & Learning Strategies

### 1. How to Use the Notebooks Effectively

**The Three-Pass Method:**

| Pass | Goal | What to Do |
|------|------|------------|
| **First Pass** | Orientation | Read all markdown cells. Skim code. Understand the *what* and *why*. |
| **Second Pass** | Active Learning | Run each cell. Before executing, predict the output. Note surprises. |
| **Third Pass** | Experimentation | Modify parameters. Break things on purpose. Answer the"what if?"questions. |

> [!TIP]
> **The Predict → Run → Explain cycle** is the most effective learning technique.
> Before running a cell, write down what you think will happen. After running,
> explain *why* the result matches or differs from your prediction.

### 2. When to Read vs When to Code

| Situation | Approach |
|-----------|----------|
| New concept you've never seen | Read first, then code |
| Concept you've read about but never used | Code first, read when stuck |
| Debugging a specific issue | Code + Spark UI together |
| Preparing for a design review | Read + draw diagrams |
| Optimizing an existing pipeline | Profile in Spark UI first, then code changes |

### 3. How to Use Spark UI for Learning

The Spark UI is your most powerful learning instrument. Here's a systematic approach:

**After every action (collect, show, write, count):**

1. **Jobs Tab** — How many jobs were created? Why?
2. **Stages Tab** — How many stages? Where are the shuffle boundaries?
3. **Stage Detail** — Click into a stage:
   - How many tasks? (Should match partition count)
   - Any skew? (Compare task duration min/median/max)
   - Any shuffle read/write? (How much data moved?)
4. **SQL Tab** — View the query plan with real metrics
5. **Storage Tab** — Is anything cached? How much memory used?

```
Learning Loop:
                    ┌──────────┐
                    │ Write    │
                    │ Code     │
                    └────┬─────┘
                         │
                    ┌────▼─────┐
                    │ Run      │
                    │ Action   │
                    └────┬─────┘
                         │
              ┌──────────▼──────────┐
              │ Inspect Spark UI    │
              │ (Jobs/Stages/Tasks) │
              └──────────┬──────────┘
                         │
              ┌──────────▼──────────┐
              │ Ask:"Is this what  │
              │  I expected?"│
              └─────┬─────────┬─────┘
                    │ No      │ Yes
              ┌─────▼───┐  ┌──▼────┐
              │Investiga-│  │Move   │
              │te why    │  │to next│
              └─────┬────┘  │topic  │
                    │       └───────┘
                    │
              ┌─────▼────┐
              │ Modify   │
              │ & re-run │
              └──────────┘
```

### 4. How to Verify Understanding

**Level 1 — Recognition:** Can you identify the concept when you see it?
- Read an `explain()` output and name each operator
- Look at Spark UI and identify shuffle stages

**Level 2 — Explanation:** Can you explain it to someone else?
- Write a 3-sentence explanation of each concept in your own words
- Draw the DAG for a given query on paper

**Level 3 — Application:** Can you use it to solve a new problem?
- Given a slow query, diagnose the bottleneck and fix it
- Design a partitioning strategy for a new dataset

**Level 4 — Evaluation:** Can you compare trade-offs and make recommendations?
- Choose between `repartition` and `coalesce` with justification
- Decide whether to cache a DataFrame with cost/benefit analysis
- Compare broadcast join vs sort-merge join for a given scenario

> [!IMPORTANT]
> You haven't truly learned a topic until you reach Level 3. Aim for Level 4
> on all"Essential"topics.

### 5. Common Pitfalls to Avoid

| Pitfall | Why It's a Problem | What to Do Instead |
|---------|-------------------|-------------------|
| Reading without running | No muscle memory, false confidence | Run every cell, modify parameters |
| Skipping RDDs entirely | Can't debug DataFrame plans at the RDD level | At minimum complete Topics 6–7 |
| Ignoring Spark UI | You'll never develop intuition for performance | Open Spark UI after every action |
| Memorizing configs | Configs change between versions, context matters | Understand *why* a config exists, look up the value |
| Jumping to optimization | Can't optimize what you don't understand | Master Phase 1–3 before Phase 4 |
| Never breaking things | You won't learn error patterns | Deliberately cause OOM, skew, shuffle explosions |

### 6. Recommended Resources Beyond This Lab

| Resource | Type | Best For |
|----------|------|----------|
| [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/) | Book | Comprehensive DataFrame/SQL reference |
| [Learning Spark, 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/) | Book | Modern Spark with Structured APIs |
| [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/) | Book | Deep performance tuning |
| [Spark Official Docs](https://spark.apache.org/docs/latest/) | Web | API reference, configuration keys |
| [Databricks Blog](https://www.databricks.com/blog) | Web | Latest features, best practices |
| [Jacek Laskowski's Internals](https://books.japila.pl/apache-spark-internals/) | Web | Deep internals, source code analysis |

---

## Self-Assessment Checklists

Use these to gauge your readiness at each phase transition.

### After Phase 1 (Foundation)

- [ ] I can explain the Spark architecture (Driver, Executors, Cluster Manager)
- [ ] I can create a SparkSession with custom configurations
- [ ] I know the difference between transformations and actions
- [ ] I can classify transformations as narrow or wide
- [ ] I can navigate the Spark Master UI and find running applications
- [ ] I can create DataFrames from multiple sources (list, CSV, JSON)

### After Phase 2 (Core Data Processing)

- [ ] I can write RDD transformations and explain when RDDs are still useful
- [ ] I understand `reduceByKey` vs `groupByKey` performance difference
- [ ] I can write complex SQL queries using Spark SQL
- [ ] I can implement all 7 join types and explain when to use each
- [ ] I can write window functions for ranking, running totals, and lead/lag
- [ ] I can read a basic `explain()` output

### After Phase 3 (Execution Engine)

- [ ] I can trace how code becomes Jobs → Stages → Tasks
- [ ] I can predict the number of stages from a query plan
- [ ] I can identify shuffle boundaries in a DAG
- [ ] I can choose between `repartition` and `coalesce` with justification
- [ ] I can use the Spark UI to find slow tasks and identify skew
- [ ] I can set `spark.sql.shuffle.partitions` appropriately

### After Phase 4 (Performance & Internals)

- [ ] I can diagram the executor memory layout with correct proportions
- [ ] I can decide when to cache vs not cache a DataFrame
- [ ] I can explain the 4 phases of Catalyst optimization
- [ ] I can read `explain(mode="formatted")` and identify applied optimizations
- [ ] I know what AQE does and when it activates
- [ ] I understand predicate pushdown and column pruning

### After Phase 5 (Advanced Topics)

- [ ] I can choose the right file format for a workload (Parquet vs ORC vs CSV)
- [ ] I can set up a basic Structured Streaming pipeline
- [ ] I can configure watermarking for late data handling
- [ ] I can explain how Spark recovers from executor failures
- [ ] I know when to use checkpointing vs caching

### After Phase 6 (Production & Projects)

- [ ] I can design and build a complete ETL pipeline
- [ ] I can diagnose a slow Spark job and apply ≥3 optimizations
- [ ] I can handle data skew using salting or AQE
- [ ] I can set up monitoring with the Spark History Server
- [ ] I can conduct a meaningful code review of Spark code
- [ ] I can explain trade-offs in my design decisions

---

## Quick Reference: Notebook Directory Map

```
notebooks/
├── 00_environment_and_spark_setup/     ← Phase 1, Week 1
├── 01_spark_fundamentals/              ← Phase 1, Week 2
├── 02_rdd/                             ← Phase 2, Week 3
├── 03_dataframe_and_spark_sql/         ← Phase 2, Week 4
├── 04_execution_engine/                ← Phase 3, Week 5
├── 05_partitions_and_shuffle/          ← Phase 3, Week 6
├── 06_memory_and_storage/              ← Phase 4, Week 7
├── 07_spark_sql_internals/             ← Phase 4, Week 8
├── 08_data_sources_and_formats/        ← Phase 5, Week 9
├── 09_streaming/                       ← Phase 5, Week 9
├── 10_performance_and_optimization/    ← Phase 6, Week 11
├── 11_fault_tolerance_and_reliability/ ← Phase 5, Week 10
├── 12_advanced_topics/                 ← Phase 6 (Platform path)
└── 13_end_to_end_projects/             ← Phase 6, Week 12
```

---

## Final Words

Learning Spark is a journey of layers. Each pass through the material reveals
depth you didn't see before. After completing this 12-week roadmap:

1. **Revisit Phase 3–4** — Execution engine and optimization are topics where
   second-pass learning is dramatically more effective.
2. **Build something real** — The projects in Week 12 are starting points.
   Apply Spark to your own domain data.
3. **Teach someone else** — The best way to solidify understanding is to
   explain it. Write a blog post or present to your team.
4. **Read source code** — For topics at the Advanced level, reading Spark's
   Scala source reveals implementation truths that no documentation captures.

> [!TIP]
> **The 80/20 rule applies:** 80% of production Spark work uses 20% of its features.
> Master the  Essential topics first. They cover that critical 20%.

---

*Last updated: 2026-11-09 | Spark Version: 4.2.1 | PySpark: 4.2.1*
