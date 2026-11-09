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
|  | **Must Know** — Cannot function as a Spark engineer without this |
|  | **Important** — Required for intermediate-level work |
|  | **Advanced** — Needed for senior / performance-critical work |
|  | **Optional** — Nice-to-have, niche, or deeply specialized |

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
| 1 | Docker Compose cluster setup |  Must Know | 1 h | `00_environment_and_spark_setup/` |
| 2 | Spark Master, Workers, Executors |  Must Know | 1.5 h | `00_environment_and_spark_setup/` |
| 3 | SparkSession creation & configuration |  Must Know | 1 h | `00_environment_and_spark_setup/` |
| 4 | Spark application lifecycle |  Must Know | 1 h | `01_spark_fundamentals/` |
| 5 | Navigating Spark UI (Master, Worker, Driver) |  Must Know | 1.5 h | `01_spark_fundamentals/` |

**Key Concepts to Master:**
- Driver vs Executor vs Cluster Manager responsibilities
- SparkContext vs SparkSession — when you need each
- How `spark-submit` parameters map to cluster resources
- Reading the Spark Master UI at `http://localhost:8080`

**Checkpoint:** You should be able to:
- [] Start the cluster and confirm 2 workers are registered
- [] Create a SparkSession with custom app name and configurations
- [] Explain the role of Driver, Executor, and Cluster Manager from memory
- [] Find a running application in the Spark Master UI

### Week 2: Spark Programming Model Basics

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Immutability & distributed collections |  Must Know | 1 h | `01_spark_fundamentals/` |
| 2 | Transformations vs Actions |  Must Know | 1.5 h | `01_spark_fundamentals/` |
| 3 | Narrow vs Wide transformations |  Must Know | 1.5 h | `01_spark_fundamentals/` |
| 4 | Basic DataFrame operations (select, filter, withColumn) |  Must Know | 1.5 h | `01_spark_fundamentals/` |
| 5 | Review & practice mini-project |  Must Know | 2 h | `01_spark_fundamentals/` |

**Key Concepts to Master:**
- Why transformations are lazy and what triggers execution
- The difference between `map` (narrow) and `groupBy` (wide)
- How Spark represents data as distributed partitions
- Schema inference vs explicit schema definition

**Checkpoint:** You should be able to:
- [] Explain lazy evaluation and give 3 examples of actions
- [] Classify any transformation as narrow or wide
- [] Create a DataFrame from a list, CSV, and JSON source
- [] Chain 5+ transformations and trigger with a single action

---

## Phase 2: Core Data Processing (Weeks 3–4)

**Goal:** Become fluent in Spark's primary APIs — RDD, DataFrame, and Spark SQL.

### Week 3: RDDs & DataFrame Deep-Dive

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | RDD creation (parallelize, textFile) |  Important | 1.5 h | `02_rdd/` |
| 2 | RDD transformations (map, flatMap, filter, reduceByKey) |  Important | 1.5 h | `02_rdd/` |
| 3 | RDD actions (collect, count, take, saveAsTextFile) |  Important | 1 h | `02_rdd/` |
| 4 | Pair RDDs & key-value operations |  Important | 1.5 h | `02_rdd/` |
| 5 | When to use RDD vs DataFrame |  Must Know | 1 h | `02_rdd/` |

> [!NOTE]
> While DataFrames are the preferred API in modern Spark, understanding RDDs is essential
> because DataFrames compile down to RDD operations internally. Debugging production issues
> often requires reading RDD-level execution plans.

**Key Concepts to Master:**
- RDD lineage and how it enables fault tolerance
- `reduceByKey` vs `groupByKey` — why it matters for performance
- Accumulators and Broadcast variables
- Why DataFrames outperform RDDs in most cases (Catalyst + Tungsten)

### Week 4: Spark SQL, Joins & Aggregations

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Spark SQL basics (createOrReplaceTempView, spark.sql) |  Must Know | 1.5 h | `03_dataframe_and_spark_sql/` |
| 2 | Column expressions & built-in functions |  Must Know | 1.5 h | `03_dataframe_and_spark_sql/` |
| 3 | Join types (inner, left, right, full, cross, semi, anti) |  Must Know | 2 h | `03_dataframe_and_spark_sql/` |
| 4 | Aggregations, groupBy, window functions |  Must Know | 2 h | `03_dataframe_and_spark_sql/` |
| 5 | UDFs and Pandas UDFs |  Important | 1.5 h | `03_dataframe_and_spark_sql/` |

**Key Concepts to Master:**
- How Spark SQL compiles queries to the same execution plan as DataFrame API
- Join strategies: Broadcast Hash Join vs Sort-Merge Join vs Shuffle Hash Join
- Window functions: `row_number`, `rank`, `lead`, `lag`, running totals
- Why scalar UDFs are slow and when Pandas UDFs solve the problem

**Checkpoint:** You should be able to:
- [] Write a multi-table join with mixed join types
- [] Implement a running total using window functions
- [] Explain why `select` + `filter` is equivalent to SQL `SELECT ... WHERE`
- [] Read an `explain()` output and identify the join strategy

---

## Phase 3: Execution Engine (Weeks 5–6)

**Goal:** Understand *how* Spark executes your code — the DAG, stages, tasks, and shuffle.

### Week 5: Lazy Evaluation, DAG & Job Execution

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Lazy evaluation internals — logical plan building |  Must Know | 1.5 h | `04_execution_engine/` |
| 2 | DAG construction and visualization |  Must Know | 1.5 h | `04_execution_engine/` |
| 3 | Jobs → Stages → Tasks hierarchy |  Must Know | 2 h | `04_execution_engine/` |
| 4 | Stage boundaries and shuffle dependencies |  Must Know | 1.5 h | `04_execution_engine/` |
| 5 | Reading the Spark UI: Jobs, Stages, Tasks tabs |  Must Know | 2 h | `04_execution_engine/` |

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
| 1 | What are partitions — size, count, distribution |  Must Know | 1.5 h | `05_partitions_and_shuffle/` |
| 2 | Hash partitioning vs Range partitioning |  Important | 1.5 h | `05_partitions_and_shuffle/` |
| 3 | Repartition vs Coalesce — when to use each |  Must Know | 1 h | `05_partitions_and_shuffle/` |
| 4 | Shuffle deep-dive — map side, sort, reduce side |  Must Know | 2 h | `05_partitions_and_shuffle/` |
| 5 | Diagnosing shuffle issues in Spark UI |  Important | 2 h | `05_partitions_and_shuffle/` |

**Key Concepts to Master:**
- The relationship: `spark.sql.shuffle.partitions` → number of reduce tasks
- How data skew causes straggler tasks in shuffle stages
- Shuffle spill: when and why it happens (memory pressure)
- The 128 MB partition guideline and when to deviate

**Checkpoint:** You should be able to:
- [] Draw the DAG for a query with 2 joins and 1 aggregation
- [] Predict the number of stages from an `explain()` output
- [] Identify shuffle-heavy operations and propose alternatives
- [] Set optimal partition count for a given data size

---

## Phase 4: Performance & Internals (Weeks 7–8)

**Goal:** Master Spark's internal optimization engine and memory management.

### Week 7: Memory Management & Caching

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Unified Memory Manager — Storage vs Execution memory |  Important | 2 h | `06_memory_and_storage/` |
| 2 | On-heap vs Off-heap memory |  Advanced | 1.5 h | `06_memory_and_storage/` |
| 3 | Cache vs Persist — storage levels explained |  Must Know | 1.5 h | `06_memory_and_storage/` |
| 4 | When to cache (and when NOT to cache) |  Must Know | 1 h | `06_memory_and_storage/` |
| 5 | Memory tuning: `spark.executor.memory`, fractions, overhead |  Important | 2 h | `06_memory_and_storage/` |

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
| 1 | Catalyst: Unresolved → Analyzed → Optimized → Physical Plan |  Important | 2 h | `07_spark_sql_internals/` |
| 2 | Rule-based optimizations (predicate pushdown, constant folding, column pruning) |  Important | 1.5 h | `07_spark_sql_internals/` |
| 3 | Cost-based optimization (CBO) & statistics |  Advanced | 1.5 h | `07_spark_sql_internals/` |
| 4 | Tungsten: whole-stage code generation & memory layout |  Advanced | 2 h | `07_spark_sql_internals/` |
| 5 | Reading and interpreting `explain(mode="formatted")` |  Must Know | 1.5 h | `07_spark_sql_internals/` |

**Key Concepts to Master:**
- Why predicate pushdown matters for Parquet/ORC files
- How column pruning eliminates unnecessary I/O
- What the `*` prefix in physical plans means (whole-stage codegen)
- AQE: Adaptive Query Execution — runtime re-optimization

**Checkpoint:** You should be able to:
- [] Explain the 4 phases of Catalyst optimization
- [] Read an `explain(mode="formatted")` and identify optimizations applied
- [] Calculate executor memory breakdown given config parameters
- [] Decide whether to cache a DataFrame based on its usage pattern

---

## Phase 5: Advanced Topics (Weeks 9–10)

**Goal:** Extend knowledge to streaming, data formats, and fault tolerance.

### Week 9: Data Sources, Formats & Streaming

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Reading/writing Parquet, ORC, CSV, JSON |  Must Know | 1.5 h | `08_data_sources_and_formats/` |
| 2 | Schema evolution & partition discovery |  Important | 1.5 h | `08_data_sources_and_formats/` |
| 3 | Structured Streaming fundamentals |  Important | 2 h | `09_streaming/` |
| 4 | Triggers, output modes, watermarking |  Important | 2 h | `09_streaming/` |
| 5 | Streaming joins & stateful processing |  Advanced | 2 h | `09_streaming/` |

**Key Concepts to Master:**
- Why Parquet is the de facto standard (columnar, compressed, predicate pushdown)
- Partition discovery: `path/year=2024/month=01/` → automatic partition columns
- Structured Streaming micro-batch vs continuous processing
- Watermarking: handling late-arriving data

### Week 10: Fault Tolerance & Reliability

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | RDD lineage and recomputation |  Important | 1.5 h | `11_fault_tolerance_and_reliability/` |
| 2 | Checkpointing: reliable vs local |  Important | 1.5 h | `11_fault_tolerance_and_reliability/` |
| 3 | Speculative execution |  Advanced | 1 h | `11_fault_tolerance_and_reliability/` |
| 4 | Data deduplication & exactly-once semantics |  Advanced | 1.5 h | `11_fault_tolerance_and_reliability/` |
| 5 | Handling corrupt records & data quality |  Important | 1.5 h | `11_fault_tolerance_and_reliability/` |

**Key Concepts to Master:**
- How Spark recovers from executor failures without restarting the job
- When to checkpoint vs when to cache
- Write-ahead logs in Structured Streaming
- `failfast` / `permissive` / `dropMalformed` read modes

**Checkpoint:** You should be able to:
- [] Explain how Spark recovers when an executor is killed mid-stage
- [] Set up a Structured Streaming job with watermarking
- [] Choose the right file format for a given workload
- [] Configure checkpointing for a streaming application

---

## Phase 6: Production & Projects (Weeks 11–12)

**Goal:** Apply everything in realistic, end-to-end projects and production scenarios.

### Week 11: Optimization & Production Patterns

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Bucketing & Z-ordering for join optimization |  Important | 1.5 h | `10_performance_and_optimization/` |
| 2 | Broadcast joins & join hint strategies |  Must Know | 1.5 h | `10_performance_and_optimization/` |
| 3 | Handling data skew (salting, adaptive skew join) |  Important | 2 h | `10_performance_and_optimization/` |
| 4 | Dynamic resource allocation & executor tuning |  Advanced | 1.5 h | `10_performance_and_optimization/` |
| 5 | Monitoring with Spark History Server |  Important | 1.5 h | `10_performance_and_optimization/` |

### Week 12: End-to-End Projects

| Day | Topic | Priority | Time | Notebook |
|-----|-------|----------|------|----------|
| 1 | Project 1: ETL pipeline (read → transform → write) |  Must Know | 3 h | `13_end_to_end_projects/` |
| 2 | Project 2: Analytical queries on large datasets |  Must Know | 3 h | `13_end_to_end_projects/` |
| 3 | Project 3: Streaming pipeline |  Important | 3 h | `13_end_to_end_projects/` |
| 4 | Project 4: Performance diagnosis & tuning exercise |  Important | 3 h | `13_end_to_end_projects/` |
| 5 | Capstone: design review, code review, presentation |  Must Know | 3 h | `13_end_to_end_projects/` |

**Checkpoint:** You should be able to:
- [] Build an end-to-end ETL pipeline with proper partitioning and error handling
- [] Diagnose a slow query using Spark UI and apply ≥3 optimizations
- [] Set up a streaming pipeline with exactly-once guarantees
- [] Conduct a code review of Spark code and identify anti-patterns

---

## Topic Priority Matrix

The complete reference matrix — use this to look up any topic quickly.

| # | Topic | Priority | Est. Time | Prerequisites | Notebooks | Key Concepts |
|---|-------|----------|-----------|---------------|-----------|-------------|
| 1 | Cluster Setup & Docker |  Must Know | 1.5 h | None | `00_environment_and_spark_setup/` | Docker Compose, master/worker, ports |
| 2 | Spark Architecture |  Must Know | 2 h | #1 | `00_environment_and_spark_setup/` | Driver, Executor, Cluster Manager |
| 3 | SparkSession & Config |  Must Know | 1.5 h | #1 | `00_environment_and_spark_setup/` | SparkSession.builder, config keys |
| 4 | Transformations & Actions |  Must Know | 2 h | #3 | `01_spark_fundamentals/` | Lazy eval, narrow/wide, action triggers |
| 5 | DataFrame Basics |  Must Know | 2 h | #3 | `01_spark_fundamentals/` | select, filter, withColumn, schema |
| 6 | RDD API |  Important | 3 h | #4 | `02_rdd/` | parallelize, map, reduceByKey, lineage |
| 7 | Pair RDDs |  Important | 2 h | #6 | `02_rdd/` | groupByKey, reduceByKey, aggregateByKey |
| 8 | Broadcast & Accumulators |  Important | 1.5 h | #6 | `02_rdd/` | Shared variables, side effects |
| 9 | Spark SQL |  Must Know | 2 h | #5 | `03_dataframe_and_spark_sql/` | SQL queries, temp views, catalog |
| 10 | Column Functions |  Must Know | 2 h | #5 | `03_dataframe_and_spark_sql/` | F.col, F.lit, F.when, type casting |
| 11 | Join Operations |  Must Know | 3 h | #5, #9 | `03_dataframe_and_spark_sql/` | 7 join types, join conditions |
| 12 | Window Functions |  Must Know | 2.5 h | #10 | `03_dataframe_and_spark_sql/` | row_number, rank, lead/lag, frames |
| 13 | Aggregations |  Must Know | 2 h | #10 | `03_dataframe_and_spark_sql/` | groupBy, agg, pivot, cube, rollup |
| 14 | UDFs & Pandas UDFs |  Important | 2 h | #10 | `03_dataframe_and_spark_sql/` | Scalar UDF, Pandas UDF, Arrow |
| 15 | DAG & Job Execution |  Must Know | 2.5 h | #4 | `04_execution_engine/` | DAGScheduler, stages, task slots |
| 16 | Spark UI Navigation |  Must Know | 2 h | #15 | `04_execution_engine/` | Jobs, Stages, Tasks, SQL tabs |
| 17 | Partitions |  Must Know | 2 h | #4 | `05_partitions_and_shuffle/` | Partition count, size, distribution |
| 18 | Repartition & Coalesce |  Must Know | 1.5 h | #17 | `05_partitions_and_shuffle/` | Full shuffle vs merge partitions |
| 19 | Shuffle Deep-Dive |  Must Know | 2.5 h | #15, #17 | `05_partitions_and_shuffle/` | Map-side, reduce-side, sort |
| 20 | Hash vs Range Partitioning |  Important | 1.5 h | #17 | `05_partitions_and_shuffle/` | HashPartitioner, RangePartitioner |
| 21 | Memory Management |  Important | 2.5 h | #15 | `06_memory_and_storage/` | Unified memory, fractions, overhead |
| 22 | Cache & Persist |  Must Know | 2 h | #21 | `06_memory_and_storage/` | Storage levels, unpersist, when to cache |
| 23 | Off-Heap Memory |  Advanced | 1.5 h | #21 | `06_memory_and_storage/` | sun.misc.Unsafe, Tungsten memory |
| 24 | Catalyst Optimizer |  Important | 2.5 h | #15 | `07_spark_sql_internals/` | 4 phases, rule-based optimization |
| 25 | Predicate Pushdown |  Important | 1.5 h | #24 | `07_spark_sql_internals/` | Filter pushdown to data source |
| 26 | Cost-Based Optimization |  Advanced | 2 h | #24 | `07_spark_sql_internals/` | Table stats, ANALYZE TABLE, histograms |
| 27 | Whole-Stage Codegen |  Advanced | 2 h | #24 | `07_spark_sql_internals/` | Tungsten, Java bytecode, * prefix |
| 28 | AQE (Adaptive Query Execution) |  Important | 2 h | #24, #19 | `07_spark_sql_internals/` | Runtime re-optimization, skew handling |
| 29 | File Formats (Parquet/ORC/CSV/JSON) |  Must Know | 2 h | #5 | `08_data_sources_and_formats/` | Columnar vs row, compression |
| 30 | Schema Evolution |  Important | 1.5 h | #29 | `08_data_sources_and_formats/` | mergeSchema, column additions |
| 31 | Partition Discovery |  Important | 1 h | #29, #17 | `08_data_sources_and_formats/` | Hive-style partitioning, pruning |
| 32 | Structured Streaming |  Important | 3 h | #5, #9 | `09_streaming/` | Micro-batch, triggers, output modes |
| 33 | Watermarking |  Important | 2 h | #32 | `09_streaming/` | Late data, event-time processing |
| 34 | Streaming Joins |  Advanced | 2 h | #32, #11 | `09_streaming/` | Stream-stream, stream-static |
| 35 | RDD Lineage & Recomputation |  Important | 1.5 h | #6, #15 | `11_fault_tolerance_and_reliability/` | DAG replay, lost partitions |
| 36 | Checkpointing |  Important | 1.5 h | #35 | `11_fault_tolerance_and_reliability/` | Reliable vs local checkpointing |
| 37 | Speculative Execution |  Advanced | 1 h | #15 | `11_fault_tolerance_and_reliability/` | Straggler detection, duplicate tasks |
| 38 | Broadcast Joins & Hints |  Must Know | 2 h | #11 | `10_performance_and_optimization/` | Threshold, hint syntax, BHJ vs SMJ |
| 39 | Data Skew Handling |  Important | 2.5 h | #19, #17 | `10_performance_and_optimization/` | Salting, AQE skew join |
| 40 | Bucketing |  Important | 1.5 h | #11, #29 | `10_performance_and_optimization/` | Pre-shuffle, sort within buckets |
| 41 | Dynamic Resource Allocation |  Advanced | 1.5 h | #2 | `10_performance_and_optimization/` | Min/max executors, idle timeout |
| 42 | History Server |  Important | 1 h | #16 | `10_performance_and_optimization/` | Event logs, completed apps |
| 43 | End-to-End ETL Project |  Must Know | 3 h | #29, #11, #13 | `13_end_to_end_projects/` | Full pipeline design |
| 44 | End-to-End Streaming Project |  Important | 3 h | #32, #33 | `13_end_to_end_projects/` | Streaming pipeline design |
| 45 | Advanced Configurations |  Advanced | 2 h | #21, #41 | `12_advanced_topics/` | Tuning configs, serialization |

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
> on all"Must Know"topics.

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

- [] I can explain the Spark architecture (Driver, Executors, Cluster Manager)
- [] I can create a SparkSession with custom configurations
- [] I know the difference between transformations and actions
- [] I can classify transformations as narrow or wide
- [] I can navigate the Spark Master UI and find running applications
- [] I can create DataFrames from multiple sources (list, CSV, JSON)

### After Phase 2 (Core Data Processing)

- [] I can write RDD transformations and explain when RDDs are still useful
- [] I understand `reduceByKey` vs `groupByKey` performance difference
- [] I can write complex SQL queries using Spark SQL
- [] I can implement all 7 join types and explain when to use each
- [] I can write window functions for ranking, running totals, and lead/lag
- [] I can read a basic `explain()` output

### After Phase 3 (Execution Engine)

- [] I can trace how code becomes Jobs → Stages → Tasks
- [] I can predict the number of stages from a query plan
- [] I can identify shuffle boundaries in a DAG
- [] I can choose between `repartition` and `coalesce` with justification
- [] I can use the Spark UI to find slow tasks and identify skew
- [] I can set `spark.sql.shuffle.partitions` appropriately

### After Phase 4 (Performance & Internals)

- [] I can diagram the executor memory layout with correct proportions
- [] I can decide when to cache vs not cache a DataFrame
- [] I can explain the 4 phases of Catalyst optimization
- [] I can read `explain(mode="formatted")` and identify applied optimizations
- [] I know what AQE does and when it activates
- [] I understand predicate pushdown and column pruning

### After Phase 5 (Advanced Topics)

- [] I can choose the right file format for a workload (Parquet vs ORC vs CSV)
- [] I can set up a basic Structured Streaming pipeline
- [] I can configure watermarking for late data handling
- [] I can explain how Spark recovers from executor failures
- [] I know when to use checkpointing vs caching

### After Phase 6 (Production & Projects)

- [] I can design and build a complete ETL pipeline
- [] I can diagnose a slow Spark job and apply ≥3 optimizations
- [] I can handle data skew using salting or AQE
- [] I can set up monitoring with the Spark History Server
- [] I can conduct a meaningful code review of Spark code
- [] I can explain trade-offs in my design decisions

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
> Master the  Must Know topics first. They cover that critical 20%.

---

*Last updated: 2026-11-09 | Spark Version: 4.2.1 | PySpark: 4.2.1*
