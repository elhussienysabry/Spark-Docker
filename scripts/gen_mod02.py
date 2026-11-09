#!/usr/bin/env python3
"""
Generates remaining Module 02 (RDD) notebooks:
- 02_rdd/02_rdd_transformations_and_actions.ipynb
- 02_rdd/03_rdd_partitions.ipynb
- 02_rdd/04_rdd_dependencies_and_lineage.ipynb
- 02_rdd/05_rdd_caching_and_persistence.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/02_rdd"

def gen_02_02():
    nb = NotebookBuilder(
        title="RDD Transformations and Actions: From Map to ReduceByKey",
        module_name="02_rdd",
        notebook_name="02_rdd_transformations_and_actions.ipynb",
        app_name="RDD_Transformations_Lab"
    )
    nb.add_header({
        "what": "Complete practical catalog of RDD transformations (map, flatMap, filter, distinct, union, subtract) and actions (collect, count, take, reduce, aggregate, saveAsTextFile).",
        "why": "Understanding low-level functional primitives clarifies how data moves across partitions and why map-side combining matters.",
        "problem": "Using inefficient transformations like groupByKey causes cluster memory exhaustion.",
        "real_world": "Legacy pipeline maintenance, custom partitioning, and low-level data extraction."
    })
    nb.add_markdown("""## 2. Transformations vs Actions Categorization

```
+-------------------------------------------------------------------------------+
| RDD OPERATIONS                                                                |
+---------------------------------------+---------------------------------------+
| TRANSFORMATIONS (Lazy, Return RDD)    | ACTIONS (Eager, Return Value / Write) |
+---------------------------------------+---------------------------------------+
| Narrow:                               | Collection:                           |
|  - map(func)                          |  - collect()                          |
|  - flatMap(func)                      |  - take(n), first(), top(n)           |
|  - filter(func)                       | Aggregation:                          |
|  - union(otherRdd)                    |  - count()                            |
| Wide (Shuffle):                       |  - reduce(func)                       |
|  - distinct()                         |  - aggregate(zero, seqOp, combOp)     |
|  - reduceByKey(func)                  | Output:                               |
|  - groupByKey()                       |  - saveAsTextFile(path)               |
|  - sortByKey()                        |  - foreach(func)                      |
+---------------------------------------+---------------------------------------+
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Practical: Key Transformations in Action

Let us test standard transformations on customer data from `/opt/spark-data/retail/customers.csv`.""")
    nb.add_code("""sc = spark.sparkContext
raw_customers = sc.textFile("/opt/spark-data/retail/customers.csv")
header = raw_customers.first()

# Filter out header and parse CSV lines into tuples: (customer_id, country, region, segment, loyalty_tier)
customers_rdd = (
    raw_customers
    .filter(lambda row: row != header)
    .map(lambda row: row.split(","))
    .map(lambda c: (int(c[0]), c[4], c[5], c[6], c[8]))
)

print(f"Total parsed customers: {customers_rdd.count()}")
print("Sample parsed customer records:")
for cust in customers_rdd.take(3):
    print(" ", cust)
""")
    nb.add_markdown("""## 4. PairRDD Operations: `reduceByKey` vs `aggregateByKey`

PairRDDs enable distributed key-value computations.""")
    nb.add_code("""# Key by loyalty_tier -> count customers per tier using reduceByKey
tier_counts = (
    customers_rdd
    .map(lambda c: (c[4], 1))
    .reduceByKey(lambda a, b: a + b)
    .collect()
)

print("Customers per loyalty tier (reduceByKey):")
for tier, count in sorted(tier_counts, key=lambda x: x[1], reverse=True):
    print(f"  {tier:15}: {count}")
""")
    nb.add_experiment({
        "title": "reduceByKey (Map-side Combine) vs groupByKey Performance",
        "testing": "Compare execution and network shuffle volume between reduceByKey and groupByKey on PairRDDs.",
        "expect": "reduceByKey combines values locally before network shuffle; groupByKey transfers all values across network, generating far more shuffle data.",
        "code": """# Generate sample key-value dataset with repetitive keys
pairs = sc.parallelize([(f"Key_{i % 5}", i) for i in range(50000)], 4)

# Test reduceByKey
t0 = time.time()
r_res = pairs.reduceByKey(lambda a, b: a + b).collect()
t_reduce = time.time() - t0

# Test groupByKey + sum
t0 = time.time()
g_res = pairs.groupByKey().mapValues(sum).collect()
t_group = time.time() - t0

print(f"reduceByKey duration: {t_reduce:.4f}s")
print(f"groupByKey duration:  {t_group:.4f}s")
print("Results match:", sorted(r_res) == sorted(g_res))
""",
        "happened": "reduceByKey executed significantly faster and transferred orders of magnitude less data over network.",
        "why": "reduceByKey applies the combiner function locally in the mapper memory buffer prior to shuffle write.",
        "takeaway": "Never use groupByKey() to compute aggregations; always use reduceByKey() or aggregateByKey()."
    })
    nb.add_exercises({
        "b_title": "Count Customers per Segment",
        "b_desc": "Using PairRDD operations, count the total number of customers in each segment (Consumer, Corporate, SMB).",
        "b_input": "customers_rdd",
        "b_starter": "# Exercise 1: Segment counts\n",
        "b_sol": """segment_counts = customers_rdd.map(lambda c: (c[3], 1)).reduceByKey(lambda a, b: a + b).collect()
print("Segment breakdown:", segment_counts)""",
        "i_title": "Distinct Countries Count",
        "i_desc": "Extract all country values, deduplicate using distinct(), and count how many unique countries exist.",
        "i_input": "customers_rdd",
        "i_starter": "# Exercise 2: Unique countries\n",
        "i_sol": """unique_countries = customers_rdd.map(lambda c: c[1]).distinct()
print(f"Unique countries count: {unique_countries.count()}")
print("Sample countries:", unique_countries.take(5))""",
        "a_title": "Custom aggregateByKey for Average Calculation",
        "a_desc": "Compute the average customer_id value per segment using aggregateByKey without collecting intermediate lists.",
        "a_input": "customers_rdd",
        "a_starter": "# Exercise 3: aggregateByKey for average\n",
        "a_sol": """# (sum, count) accumulator
zero_val = (0, 0)
seq_op = lambda acc, val: (acc[0] + val, acc[1] + 1)
comb_op = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])

avg_per_segment = (
    customers_rdd
    .map(lambda c: (c[3], c[0]))
    .aggregateByKey(zero_val, seq_op, comb_op)
    .mapValues(lambda total_cnt: round(total_cnt[0] / total_cnt[1], 2))
    .collect()
)
print("Average ID per segment:", avg_per_segment)"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Transformation vs Action": "Transformations define the recipe; Actions bake the cake.",
            "Map-Side Combine": "reduceByKey combines rows locally on the mapper before network transmission.",
            "Lazy Evaluation": "Allows Spark to pipeline consecutive map/filter steps into a single pass."
        },
        "terms": {
            "PairRDD": "RDD containing key-value 2-tuples.",
            "Map-Side Combine": "Local pre-aggregation performed in map task memory.",
            "groupByKey": "Wide transformation that groups all values for each key without local aggregation."
        },
        "mistakes": {
            "Using groupByKey for sums/counts": "Causes out-of-memory errors by transferring unaggregated records.",
            "Calling collect() on big RDDs": "Crashes Driver JVM."
        },
        "questions": {
            "Why is reduceByKey better than groupByKey?": "reduceByKey performs partial aggregation on the map side before shuffling, minimizing network data transfer.",
            "What is flatMap used for?": "Splitting each element into 0, 1, or multiple elements (e.g. word tokenization)."
        },
        "next_steps": "Open 02_rdd/03_rdd_partitions.ipynb to master partition mechanics and distribution."
    })
    nb.save(os.path.join(BASE_DIR, "02_rdd_transformations_and_actions.ipynb"))

def gen_02_03():
    nb = NotebookBuilder(
        title="RDD Partitions, Partitioners, and Parallelism Inspection",
        module_name="02_rdd",
        notebook_name="02_rdd_partitions.ipynb",
        app_name="RDD_Partitions_Lab"
    )
    nb.add_header({
        "what": "How RDDs are sliced into partitions, inspecting partition distribution with glom(), and controlling parallelism.",
        "why": "Partitions are the atomic unit of parallelism in Spark. Misconfigured partitions lead to either idle cores or memory thrashing.",
        "problem": "Under-partitioning wastes cluster resources; over-partitioning causes scheduling bottleneck.",
        "real_world": "Optimizing batch ETL runtimes and tuning data ingestion from external file systems."
    })
    nb.add_markdown("""## 2. Partition Mechanics

```
RDD (Distributed Collection)
+---------------------------------------------------------------+
|  Partition 0  |  Partition 1  |  Partition 2  |  Partition 3  |
+---------------+---------------+---------------+---------------+
        |               |               |               |
        v               v               v               v
    Task 0          Task 1          Task 2          Task 3
 (Core 1, W1)    (Core 2, W1)    (Core 1, W2)    (Core 2, W2)
```
- Each partition is assigned to exactly **one Task** running on **one CPU Core**.
- Default partition count when reading files is determined by total input splits (default 128 MB blocks).
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Inspecting Partitions with `getNumPartitions()` and `glom()`""")
    nb.add_code("""sc = spark.sparkContext
numbers = list(range(1, 25))

# Create RDD with 4 explicit partitions
rdd_4 = sc.parallelize(numbers, 4)
print(f"Partition count: {rdd_4.getNumPartitions()}")

# glom() gathers all elements of each partition into a Python list
partition_contents = rdd_4.glom().collect()
for idx, p in enumerate(partition_contents):
    print(f"  Partition {idx} (size {len(p)}): {p}")
""")
    nb.add_markdown("""## 4. `mapPartitions` and `mapPartitionsWithIndex`

`mapPartitions` processes an entire partition iterator at once, enabling connection reuse and batch operations.""")
    nb.add_code("""def compute_partition_stats(index, iterator):
    items = list(iterator)
    if items:
        yield {
            "partition": index,
            "count": len(items),
            "min": min(items),
            "max": max(items),
            "sum": sum(items)
        }

stats = rdd_4.mapPartitionsWithIndex(compute_partition_stats).collect()
print("Partition Statistics:")
for s in stats:
    print(" ", s)
""")
    nb.add_experiment({
        "title": "Execution Latency vs Partition Count",
        "testing": "Benchmark how execution duration varies when running on 1 partition vs 4 partitions on a 4-core cluster.",
        "expect": "1 partition utilizes only 1 core (sequential); 4 partitions utilize all 4 cores concurrently (~4x speedup).",
        "code": """import math

def heavy_computation(x):
    # CPU-bound calculation
    return sum(math.sqrt(i) for i in range(1, 10000))

data = list(range(200))

# 1 Partition (Sequential)
t0 = time.time()
sc.parallelize(data, 1).map(heavy_computation).count()
t_1 = time.time() - t0

# 4 Partitions (Distributed across all 4 worker cores)
t0 = time.time()
sc.parallelize(data, 4).map(heavy_computation).count()
t_4 = time.time() - t0

print(f"1 Partition duration:  {t_1:.3f}s (1 core active, 3 idle)")
print(f"4 Partitions duration: {t_4:.3f}s (all 4 cores active)")
print(f"Speedup factor:        {t_1 / t_4:.2f}x")
""",
        "happened": "4 partitions ran nearly 3-4x faster than 1 partition.",
        "why": "Because work was distributed in parallel across all 4 available executor cores in our 2-worker cluster.",
        "takeaway": "Always ensure partition count is at least equal to or a multiple of total executor cores."
    })
    nb.add_exercises({
        "b_title": "Check Partitions of Retail Orders RDD",
        "b_desc": "Load /opt/spark-data/retail/orders.csv via textFile and print its default partition count.",
        "b_input": "sc.textFile('/opt/spark-data/retail/orders.csv')",
        "b_starter": "# Exercise 1: Orders partition count\n",
        "b_sol": """orders_rdd = sc.textFile("/opt/spark-data/retail/orders.csv")
print("Default partitions for orders.csv:", orders_rdd.getNumPartitions())""",
        "i_title": "Find Largest and Smallest Partition via glom()",
        "i_desc": "Use glom() and map() to calculate the minimum and maximum partition sizes for orders_rdd.",
        "i_input": "orders_rdd",
        "i_starter": "# Exercise 2: Min and max partition sizes\n",
        "i_sol": """sizes = orders_rdd.glom().map(len).collect()
print("Partition sizes:", sizes)
print(f"Min partition size: {min(sizes)}, Max partition size: {max(sizes)}")""",
        "a_title": "Custom Hash Partitioning with partitionBy()",
        "a_desc": "Create a PairRDD of (customer_id, order_id) and partition it into 4 partitions using HashPartitioner.",
        "a_input": "orders_rdd mapped to (customer_id, order_id)",
        "a_starter": "# Exercise 3: partitionBy\n",
        "a_sol": """header = orders_rdd.first()
kv_rdd = orders_rdd.filter(lambda r: r != header).map(lambda r: r.split(",")).map(lambda c: (int(c[1]), int(c[0])))
partitioned_rdd = kv_rdd.partitionBy(4)
print("New partition count:", partitioned_rdd.getNumPartitions())
print("Distribution per partition:", partitioned_rdd.glom().map(len).collect())"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Partitions = Parallelism": "1 task processes 1 partition. Max concurrency = total available executor cores.",
            "glom() Utility": "Useful for diagnostics; gathers partition items into lists.",
            "mapPartitions Efficiency": "Enables batch operations and connection reuse per partition."
        },
        "terms": {
            "Partition": "Atomic split of distributed data.",
            "glom()": "Collects elements of each partition into an array.",
            "mapPartitions": "Operates on an entire partition via an iterator."
        },
        "mistakes": {
            "Single Partition Data": "Processing large data in 1 partition serializes execution and causes OOM.",
            "Calling glom().collect() on Huge Partitions": "Brings entire partition lists into Driver memory, causing OOM."
        },
        "questions": {
            "What is the relationship between partitions and tasks?": "There is a strict 1:1 mapping: each stage creates exactly one task for every partition in its input RDD.",
            "Why use mapPartitions instead of map?": "To avoid per-row initialization overhead (e.g. database connections, regex compilation, heavy parsers)."
        },
        "next_steps": "Open 02_rdd/04_rdd_dependencies_and_lineage.ipynb to explore RDD fault tolerance and stage boundaries."
    })
    nb.save(os.path.join(BASE_DIR, "03_rdd_partitions.ipynb"))

def gen_02_04():
    nb = NotebookBuilder(
        title="RDD Dependencies, Lineage Graphs, and Fault Tolerance",
        module_name="02_rdd",
        notebook_name="04_rdd_dependencies_and_lineage.ipynb",
        app_name="RDD_Lineage_Lab"
    )
    nb.add_header({
        "what": "How Spark constructs RDD lineage graphs, the distinction between Narrow and Wide dependencies, and how lineage guarantees fault tolerance.",
        "why": "Lineage is the foundation of Spark's resilience. Understanding dependencies explains why shuffles exist and how lost data is reconstructed.",
        "problem": "Without understanding lineage, developers cannot predict stage boundaries or diagnose recomputation overhead.",
        "real_world": "Explaining lineage recomputation and dependency classifications is a mandatory component of Spark technical interviews."
    })
    nb.add_markdown("""## 2. Narrow vs Wide Dependencies in the Lineage Graph

```
NARROW DEPENDENCIES (Pipelined in 1 Stage, No Network Transfer):
Parent Partitions:   [ P0 ]    [ P1 ]    [ P2 ]
                       |         |         |  (e.g. map, filter)
                       v         v         v
Child Partitions:    [ C0 ]    [ C1 ]    [ C2 ]

WIDE DEPENDENCIES (Shuffle, Stage Boundary, Network Transfer):
Parent Partitions:   [ P0 ]    [ P1 ]    [ P2 ]
                        \   \  /   \  /   /   (e.g. groupByKey, join)
                         v   v v   v v   v
Child Partitions:       [ C0 ]     [ C1 ]
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Inspecting RDD Lineage with `toDebugString()`""")
    nb.add_code("""sc = spark.sparkContext

# Build a multi-step RDD pipeline
base_rdd = sc.textFile("/opt/spark-data/retail/orders.csv")
header = base_rdd.first()

cleaned = base_rdd.filter(lambda line: line != header)
mapped = cleaned.map(lambda line: line.split(","))
keyed = mapped.map(lambda cols: (cols[7], 1)) # key by order status
reduced = keyed.reduceByKey(lambda a, b: a + b) # WIDE transformation

print("=== RDD Lineage Graph (toDebugString) ===")
print(reduced.toDebugString().decode('utf-8'))
""")
    nb.add_markdown("""## 4. Analyzing Stage Boundaries in Lineage Output

Notice the `ShuffledRDD` entry above: that is where Spark places a **Stage Boundary**!""")
    nb.add_code("""# Inspect dependencies directly using Python introspection
print(f"reduced RDD dependencies count: {len(reduced.dependencies)}")
for dep in reduced.dependencies:
    print(f"  Dependency Type: {type(dep).__name__}")
    print(f"  Parent RDD:      {dep.rdd}")
""")
    nb.add_experiment({
        "title": "Verifying Partition Recomputation from Lineage",
        "testing": "Demonstrate that evaluating an action on an unpersisted RDD re-executes its entire upstream lineage from disk.",
        "expect": "Each action will re-execute the map operations independently.",
        "code": """call_counter = sc.accumulator(0)

def tracked_map(x):
    global call_counter
    call_counter.add(1)
    return x * 2

rdd_calc = sc.parallelize(range(10), 2).map(tracked_map)

# First Action
print("Action 1 count:", rdd_calc.count())
print(f"Accumulator after Action 1: {call_counter.value} (Expected 10)")

# Second Action on same unpersisted RDD
print("Action 2 sum:  ", rdd_calc.sum())
print(f"Accumulator after Action 2: {call_counter.value} (Expected 20 due to recomputation!)")
""",
        "happened": "The accumulator reached 20 instead of staying at 10.",
        "why": "Because rdd_calc was not cached, Spark recomputed its entire lineage graph from scratch for Action 2.",
        "takeaway": "Uncached RDDs/DataFrames re-evaluate their complete transformation graph on every single action."
    })
    nb.add_exercises({
        "b_title": "Identify OneToOneDependency",
        "b_desc": "Create an RDD with map and filter, and inspect its dependencies to identify OneToOneDependency.",
        "b_input": "sc.parallelize(range(10)).map(lambda x: x+1).filter(lambda x: x > 5)",
        "b_starter": "# Exercise 1: OneToOneDependency\n",
        "b_sol": """r = sc.parallelize(range(10)).map(lambda x: x+1).filter(lambda x: x > 5)
print("Dependencies of filter step:", [type(d).__name__ for d in r.dependencies])""",
        "i_title": "Trace Stage Count in toDebugString",
        "i_desc": "Write a pipeline that performs two distinct shuffles (e.g. reduceByKey followed by sortByKey) and count ShuffledRDD occurrences in toDebugString.",
        "i_input": "reduced RDD",
        "i_starter": "# Exercise 2: Count shuffles in lineage\n",
        "i_sol": """sorted_rdd = reduced.sortByKey()
debug_str = sorted_rdd.toDebugString().decode('utf-8')
print("Lines with ShuffledRDD:")
for line in debug_str.split("\\n"):
    if "ShuffledRDD" in line:
        print("  ->", line)""",
        "a_title": "Simulate Lineage Recomputation",
        "a_desc": "Build an RDD pipeline from retail/products.csv, take an action, and print lineage depth.",
        "a_input": "retail/products.csv",
        "a_starter": "# Exercise 3: Lineage depth\n",
        "a_sol": """prods = sc.textFile("/opt/spark-data/retail/products.csv").filter(lambda l: "Electronics" in l).map(lambda l: l.split(",")).map(lambda c: (c[2], float(c[3]))).reduceByKey(lambda a, b: a + b)
print(f"Lineage tree:\\n{prods.toDebugString().decode('utf-8')}")"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Lineage Graph": "Spark tracks the sequence of transformations so lost partitions can be recomputed deterministically.",
            "Narrow Dependency": "1:1 partition mapping; pipelined within a single stage.",
            "Wide Dependency": "Shuffle required; draws stage boundaries in the DAG."
        },
        "terms": {
            "Lineage": "Deterministic acyclic graph of parent RDD transformations.",
            "OneToOneDependency": "Narrow dependency where child partition depends only on 1 parent partition.",
            "ShuffleDependency": "Wide dependency where child partition depends on all parent partitions."
        },
        "mistakes": {
            "Assuming Spark Caches Automatically": "Spark does NOT cache intermediate RDDs; every action recomputes from source unless persisted.",
            "Ignoring Lineage Length": "Extremely long iterative lineage graphs can cause JVM StackOverflowError (fix with checkpointing)."
        },
        "questions": {
            "How does Spark achieve fault tolerance?": "Through RDD lineage recomputation. If an executor fails, Spark recomputes only the missing partition from its parent dependencies.",
            "What is toDebugString()?": "An RDD method that displays the full dependency lineage tree."
        },
        "next_steps": "Open 02_rdd/05_rdd_caching_and_persistence.ipynb to learn caching and memory storage levels."
    })
    nb.save(os.path.join(BASE_DIR, "04_rdd_dependencies_and_lineage.ipynb"))

def gen_02_05():
    nb = NotebookBuilder(
        title="RDD Caching and Persistence Strategies",
        module_name="02_rdd",
        notebook_name="05_rdd_caching_and_persistence.ipynb",
        app_name="RDD_Caching_Lab"
    )
    nb.add_header({
        "what": "How RDD caching and persistence work, comparing storage levels (MEMORY_ONLY, MEMORY_AND_DISK, MEMORY_ONLY_SER), and cache eviction.",
        "why": "Caching prevents redundant recomputation across multiple actions, turning 10-minute pipelines into 10-second queries.",
        "problem": "Blind caching wastes executor RAM, causes memory spill to disk, and chokes shuffle execution memory.",
        "real_world": "Iterative machine learning algorithms (K-Means, Gradient Descent) and branching ETL pipelines."
    })
    nb.add_markdown("""## 2. Storage Levels Comparison

| Storage Level | Space Used | CPU Time | In Memory | On Disk | Serialized | Off-Heap |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **MEMORY_ONLY** | High | Low | Yes | No | No | No |
| **MEMORY_ONLY_SER** | Low | High | Yes | No | Yes | No |
| **MEMORY_AND_DISK** | High | Medium | Some | Some | No | No |
| **MEMORY_AND_DISK_SER** | Low | High | Some | Some | Yes | No |
| **DISK_ONLY** | Low | High | No | Yes | Yes | No |

- `rdd.cache()` is an alias for `rdd.persist(StorageLevel.MEMORY_ONLY)`.
- Always call `rdd.unpersist()` when finished with the cached dataset.
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Hands-On: Caching an Expensive RDD Computation""")
    nb.add_code("""from pyspark import StorageLevel

sc = spark.sparkContext
orders_rdd = sc.textFile("/opt/spark-data/retail/orders.csv")

# Perform an expensive parsing step
parsed_orders = (
    orders_rdd
    .filter(lambda l: not l.startswith("order_id"))
    .map(lambda l: l.split(","))
    .map(lambda c: (c[7], float(c[5]) * int(c[4]))) # (status, total_price)
)

# Persist in memory
parsed_orders.persist(StorageLevel.MEMORY_ONLY)
print(f"Is RDD cached?: {parsed_orders.is_cached}")
print(f"Storage Level:  {parsed_orders.getStorageLevel()}")
""")
    nb.add_markdown("""## 4. Benchmarking Cached vs Uncached Performance

Watch the execution time drop dramatically on the second Action!""")
    nb.add_code("""# Action 1: First time evaluates lineage AND caches into BlockManager
t0 = time.time()
count_1 = parsed_orders.count()
t_action1 = time.time() - t0

# Action 2: Evaluates directly from in-memory BlockManager!
t0 = time.time()
count_2 = parsed_orders.count()
t_action2 = time.time() - t0

print(f"Action 1 (Lineage + Cache populate): {t_action1:.4f}s")
print(f"Action 2 (Directly from RAM cache):  {t_action2:.4f}s")
print(f"Speedup from caching:                 {t_action1 / t_action2:.2f}x")
""")
    nb.add_markdown("""## 5. Unpersisting to Free Memory

Always unpersist when your branching pipeline finishes!""")
    nb.add_code("""parsed_orders.unpersist()
print(f"Is RDD cached after unpersist?: {parsed_orders.is_cached}")
""")
    nb.add_experiment({
        "title": "Inspecting Cache in Spark UI Storage Tab",
        "testing": "Persist an RDD, trigger an action, and verify that cached partitions appear in the Spark UI Storage tab.",
        "expect": "Storage tab shows memory used and percentage cached.",
        "code": """rdd_sample = sc.parallelize(range(100000), 4).map(lambda x: x * 3)
rdd_sample.persist(StorageLevel.MEMORY_ONLY)
# Action to populate cache
rdd_sample.count()

print("Check http://localhost:4040/storage/ to see the cached RDD blocks!")
print(f"Storage level: {rdd_sample.getStorageLevel()}")
rdd_sample.unpersist()
""",
        "happened": "Cached blocks were populated in the BlockManager and visible in Spark UI.",
        "why": "Calling an action materializes the RDD partitions and stores them in the Storage Memory pool.",
        "takeaway": "Always monitor the Storage tab in Spark UI to ensure cached datasets fit in RAM without spilling."
    })
    nb.add_exercises({
        "b_title": "Persist with MEMORY_AND_DISK",
        "b_desc": "Persist parsed_orders using StorageLevel.MEMORY_AND_DISK, run count(), and verify getStorageLevel().",
        "b_input": "StorageLevel.MEMORY_AND_DISK",
        "b_starter": "# Exercise 1: MEMORY_AND_DISK persistence\n",
        "b_sol": """p = sc.parallelize(range(100)).persist(StorageLevel.MEMORY_AND_DISK)
p.count()
print("Storage level:", p.getStorageLevel())
p.unpersist()""",
        "i_title": "Compare Cached vs Uncached Branching Aggregations",
        "i_desc": "Execute sum() and max() on an unpersisted RDD vs cached RDD and compare total execution time.",
        "i_input": "sc.parallelize(range(1000000), 4).map(lambda x: x**2)",
        "i_starter": "# Exercise 2: Branching actions benchmark\n",
        "i_sol": """r = sc.parallelize(range(500000), 4).map(lambda x: x * 2).persist()
# Warm up cache with action
r.count()
t0 = time.time()
s_val = r.sum()
m_val = r.max()
print(f"Sum: {s_val}, Max: {m_val} executed in {time.time()-t0:.4f}s from cache")
r.unpersist()""",
        "a_title": "Inspect Serialized Storage Level Memory Footprint",
        "a_desc": "Persist an RDD with MEMORY_ONLY and MEMORY_ONLY_SER and compare behavior.",
        "a_input": "StorageLevel.MEMORY_ONLY_SER",
        "a_starter": "# Exercise 3: Serialized storage\n",
        "a_sol": """r_ser = sc.parallelize([f"Str_{i}" for i in range(50000)]).persist(StorageLevel.MEMORY_ONLY_SER)
r_ser.count()
print("Serialized level:", r_ser.getStorageLevel())
r_ser.unpersist()"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "When to Cache": "Cache only when an expensive RDD is reused across 2 or more actions.",
            "unpersist() Discipline": "Always release memory when finished to avoid choking execution pools.",
            "Storage Levels": "MEMORY_ONLY is fastest; MEMORY_AND_DISK prevents recomputation if RAM is full."
        },
        "terms": {
            "cache()": "Persist with default MEMORY_ONLY (RDD) or MEMORY_AND_DISK (DataFrame).",
            "StorageLevel": "Configuration defining memory, disk, serialization, and replication flags.",
            "unpersist()": "Eagerly removes cached blocks from the BlockManager."
        },
        "mistakes": {
            "Caching Single-Use RDDs": "Adds serialization and BlockManager overhead with zero reuse benefit.",
            "Forgetting to Unpersist": "Leads to memory eviction and disk spilling in subsequent jobs."
        },
        "questions": {
            "What is the difference between cache() and persist()?": "cache() uses the default storage level; persist() accepts a custom StorageLevel parameter.",
            "Why might caching make a job slower?": "If the data is only used once, caching incurs storage overhead without any recomputation savings."
        },
        "next_steps": "Module 02 complete! Proceed to Module 03: DataFrames and Spark SQL."
    })
    nb.save(os.path.join(BASE_DIR, "05_rdd_caching_and_persistence.ipynb"))

if __name__ == '__main__':
    gen_02_02()
    gen_02_03()
    gen_02_04()
    gen_02_05()
