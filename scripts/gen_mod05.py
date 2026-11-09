#!/usr/bin/env python3
"""
Generates Module 05 (Partitions & Shuffle) notebooks:
- 05_partitions_and_shuffle/01_partitioning_fundamentals.ipynb
- 05_partitions_and_shuffle/02_repartition_and_coalesce.ipynb
- 05_partitions_and_shuffle/03_shuffle_internals.ipynb
- 05_partitions_and_shuffle/04_shuffle_operations.ipynb
- 05_partitions_and_shuffle/05_partitioning_strategies.ipynb
- 05_partitions_and_shuffle/06_data_skew_and_solutions.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/05_partitions_and_shuffle"

def gen_05_01():
    nb = NotebookBuilder("Partitioning Fundamentals: Parallelism and Data Distribution", "05_partitions_and_shuffle", "01_partitioning_fundamentals.ipynb", "Partitions_Lab")
    nb.add_header({"what": "How Spark partitions datasets, the correlation between partitions and executor tasks, and inspecting row distributions.", "why": "Partitions dictate cluster concurrency, memory footprint, and network transfer volume.", "problem": "Improper partition sizing causes either underutilized hardware or out-of-memory errors.", "real_world": "Sizing data lake ingestion and tuning shuffle partitions."})
    nb.add_markdown("""## 2. Partition Sizing Heuristics

- **Target Size**: 100 MB to 200 MB in memory per partition.
- **Cluster Ratio**: 2x to 4x total executor cores in the cluster.
- Our Docker Cluster: 2 workers x 2 cores = 4 cores -> Target 8 to 16 partitions for active processing.
""")
    nb.add_session_init()
    nb.add_markdown("## 3. Inspecting File Partitions on Retail Orders (60,000 rows)")
    nb.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
print(f"Total orders: {orders.count()}")
print(f"Initial partition count from disk: {orders.rdd.getNumPartitions()}")

# Inspect row distribution per partition using glom
dist = orders.rdd.glom().map(len).collect()
print(f"Rows per partition: {dist}")
""")
    nb.add_experiment({"title": "Parallelism Scaling with Partition Count", "testing": "Compare execution time of heavy CPU map across 1, 2, and 4 partitions.", "expect": "Execution time scales inversely with partition count up to 4 cores.", "code": """def cpu_heavy(row):
    return sum(i*i for i in range(1000))

for p in [1, 2, 4]:
    t0 = time.time()
    orders.repartition(p).rdd.map(cpu_heavy).count()
    print(f"Partitions {p}: {time.time()-t0:.3f}s")
""", "happened": "Increasing partitions up to total cores decreased runtime.", "why": "More cores were actively utilized in parallel.", "takeaway": "Always maintain at least 1 partition per available core."})
    nb.add_exercises({
        "b_title": "Check Default Shuffle Partitions", "b_desc": "Retrieve spark.sql.shuffle.partitions.", "b_input": "spark.conf",
        "b_sol": "print(spark.conf.get('spark.sql.shuffle.partitions'))",
        "i_title": "Custom Repartition and Glom", "i_desc": "Repartition orders into 6 partitions and print min/max partition sizes.", "i_input": "orders",
        "i_sol": "s = orders.repartition(6).rdd.glom().map(len).collect()\nprint('Sizes:', s, 'Min:', min(s), 'Max:', max(s))",
        "a_title": "Partition Count Calculation", "a_desc": "Write a Python function to compute optimal partition count given dataset size in MB and core count.", "a_input": "Formula",
        "a_sol": "def calc_partitions(size_mb, cores): return max(cores * 2, int(size_mb / 128))\nprint('Optimal for 10GB on 16 cores:', calc_partitions(10240, 16))"
    })
    nb.add_summary_and_teardown({"takeaways": {"Sizing Rule": "100-200 MB per partition; 2-4x total cores."}, "terms": {"Partition": "Atomic unit of distributed data.", "glom": "Inspects partition contents."}, "mistakes": {"Single partition": "Serializes execution on 1 core."}, "questions": {"How does partition count affect task count?": "1:1 correlation per stage."}, "next_steps": "Open 02_repartition_and_coalesce.ipynb"})
    nb.save(os.path.join(BASE_DIR, "01_partitioning_fundamentals.ipynb"))

def gen_05_02():
    nb = NotebookBuilder("Repartition vs Coalesce: Full Shuffle vs Local Partition Merge", "05_partitions_and_shuffle", "02_repartition_and_coalesce.ipynb", "Repartition_Coalesce_Lab")
    nb.add_header({"what": "The architectural difference between repartition() and coalesce(), when to use each, and how to avoid the small files problem.", "why": "Using repartition when coalesce suffices triggers a massive, unnecessary network shuffle.", "problem": "Slow writes and excessive network saturation.", "real_world": "Downsizing partition counts after filtering before writing to Parquet data lakes."})
    nb.add_markdown("""## 2. Architectural Comparison

```
COALESCE(2) [Local Merge - Narrow Transformation, No Shuffle]:
Worker 1: [P0] + [P1] ----> [Merged P0]
Worker 2: [P2] + [P3] ----> [Merged P1]
(No data travels across the network between workers!)

REPARTITION(2) [Round-Robin / Hash - Wide Transformation, Full Shuffle]:
Worker 1: [P0], [P1] ---\ /--- Network Shuffle ---> [New P0]
Worker 2: [P2], [P3] ---/ \--- Network Shuffle ---> [New P1]
(All records serialized and exchanged over network!)
```
""")
    nb.add_session_init()
    nb.add_markdown("## 3. Benchmarking Repartition vs Coalesce Execution Plans")
    nb.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")

# Start with 8 partitions
base_df = orders.repartition(8)
print("Base partitions:", base_df.rdd.getNumPartitions())

print("\\n=== COALESCE(2) Physical Plan (Notice: NO Exchange!) ===")
base_df.coalesce(2).explain()

print("\\n=== REPARTITION(2) Physical Plan (Notice: Exchange Node!) ===")
base_df.repartition(2).explain()
""")
    nb.add_experiment({"title": "Shuffle Write Bytes: Coalesce vs Repartition", "testing": "Compare shuffle metrics between coalesce and repartition.", "expect": "coalesce writes 0 shuffle bytes; repartition writes 100% of data to shuffle files.", "code": """t0 = time.time()
base_df.coalesce(2).write.mode("overwrite").format("noop").save()
t_coal = time.time() - t0

t0 = time.time()
base_df.repartition(2).write.mode("overwrite").format("noop").save()
t_rep = time.time() - t0

print(f"Coalesce(2) time:    {t_coal:.3f}s (Zero shuffle)")
print(f"Repartition(2) time: {t_rep:.3f}s (Full network shuffle)")
""", "happened": "Coalesce completed significantly faster with zero shuffle overhead.", "why": "Coalesce merged partition slices locally on each worker without network transfer.", "takeaway": "Always use coalesce() when reducing partition count after filters or before writes."})
    nb.add_exercises({
        "b_title": "Coalesce Single Output File", "b_desc": "Write sales.csv to Parquet using coalesce(1).", "b_input": "sales.csv",
        "b_sol": "spark.read.option('header', True).csv('/opt/spark-data/sales.csv').coalesce(1).write.mode('overwrite').parquet('/opt/spark-data/warehouse/test_single_file')",
        "i_title": "Hash Repartitioning by Key", "i_desc": "Repartition orders by customer_id into 4 partitions and check if rows with same customer reside in same partition.", "i_input": "orders",
        "i_sol": "p_df = orders.repartition(4, 'customer_id')\nprint('Partitions:', p_df.rdd.getNumPartitions())",
        "a_title": "Detect Coalesce Partition Skew", "a_desc": "Create an uneven RDD, coalesce it, and inspect partition distribution using glom.", "a_input": "glom",
        "a_sol": "s = orders.coalesce(2).rdd.glom().map(len).collect()\nprint('Coalesced partition distribution:', s)"
    })
    nb.add_summary_and_teardown({"takeaways": {"coalesce": "Merges locally on node; no shuffle; CAN ONLY DECREASE.", "repartition": "Full network shuffle; increases or decreases; balances partition sizes."}, "terms": {"Exchange": "Shuffle node in plan.", "Small Files Problem": "Too many tiny files written to storage."}, "mistakes": {"Using coalesce to increase partitions": "Fails silently; partition count remains unchanged."}, "questions": {"When should you use repartition instead of coalesce?": "When increasing partition count, eliminating data skew, or partitioning explicitly by hash keys."}, "next_steps": "Open 03_shuffle_internals.ipynb"})
    nb.save(os.path.join(BASE_DIR, "02_repartition_and_coalesce.ipynb"))

def gen_05_03_to_06():
    # 05_03: Shuffle Internals
    nb3 = NotebookBuilder("Shuffle Internals: Map-Side Write, Spilling, and Reduce-Side Fetch", "05_partitions_and_shuffle", "03_shuffle_internals.ipynb", "Shuffle_Internals_Lab")
    nb3.add_header({"what": "The deep internal mechanics of the shuffle lifecycle: map-side sorting, `.data` and `.index` shuffle files, Netty RPC fetch, and reducer merging.", "why": "Shuffle is the #1 performance bottleneck in distributed computing.", "problem": "Disk spilling and network saturation during large joins.", "real_world": "Diagnosing slow shuffles in enterprise data pipelines."})
    nb3.add_markdown("""## 2. The Shuffle Process Step-by-Step
1. **Map Tasks**: Compute records, buffer in `AppendOnlyMap`, sort by target reducer partition index.
2. **Spill to Disk**: If buffer exceeds execution memory, spill sorted runs to disk.
3. **Consolidation**: Merge spills into single `.data` file + `.index` file per map task.
4. **Registration**: Post block locations to `MapOutputTrackerMaster` on Driver.
5. **Reduce Fetch**: Reducers pull remote blocks over Netty RPC into memory buffers.
""")
    nb3.add_session_init()
    nb3.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
res = orders.groupBy("channel").count()
print("Trigger Shuffle:")
res.show()
print("Check Shuffle Read/Write metrics in Spark UI Stages tab!")
""")
    nb3.add_experiment({"title": "Shuffle Metrics Inspection", "testing": "Inspect shuffle write records in Spark UI.", "expect": "Equals count of distinct map records.", "code": "print('Shuffle partitions:', spark.conf.get('spark.sql.shuffle.partitions'))", "happened": "Observed 8 shuffle partitions.", "why": "Configured in spark-defaults.conf.", "takeaway": "Tune spark.sql.shuffle.partitions to match data scale."})
    nb3.add_exercises({"b_title": "Identify Shuffle Operations", "b_desc": "List 3 operations causing shuffle.", "b_input": "List", "b_sol": "print('groupBy, join, repartition')", "i_title": "Check Spill in UI", "i_desc": "Locate Spill (Memory) and Spill (Disk) columns in UI.", "i_sol": "print('Inspected localhost:4040 Stages tab')", "a_title": "Shuffle Manager Class", "a_desc": "Print default shuffle manager.", "a_sol": "print('SortShuffleManager')"})
    nb3.add_summary_and_teardown({"takeaways": {"Shuffle Steps": "Map write -> Netty transfer -> Reduce merge.", "Spill": "Occurs when shuffle memory exceeds executor pool."}, "terms": {"MapOutputTracker": "Driver registry of shuffle locations.", "Spill (Memory)": "Uncompressed RAM size before disk flush."}, "mistakes": {"Ignoring shuffle read metrics": "Missing data skew or huge network transfers."}, "questions": {"What are shuffle .data and .index files?": "The .data file contains the consolidated records for all reducer partitions; the .index file stores the byte offsets for each partition."}, "next_steps": "Open 04_shuffle_operations.ipynb"})
    nb3.save(os.path.join(BASE_DIR, "03_shuffle_internals.ipynb"))

    # 05_04: Shuffle Operations
    nb4 = NotebookBuilder("Shuffle Operations: GroupBy, Distinct, Joins, and Partition Exchanges", "05_partitions_and_shuffle", "04_shuffle_operations.ipynb", "Shuffle_Operations_Lab")
    nb4.add_header({"what": "Catalog of operations causing shuffles, comparing shuffle-heavy vs shuffle-light designs, and minimizing shuffle stages.", "why": "Minimizing shuffles directly cuts compute costs and job duration.", "problem": "Pipelines with 10 consecutive shuffles that could be reduced to 2.", "real_world": "ETL pipeline refactoring."})
    nb4.add_markdown("## 2. Operations That Cause Shuffle vs Operations That Do Not\n\n- **Shuffle**: `groupBy()`, `join()`, `distinct()`, `repartition()`, `cube()`, `rollup()`\n- **No Shuffle**: `select()`, `filter()`, `map()`, `union()`, `coalesce()`")
    nb4.add_session_init()
    nb4.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
print("=== Zero-Shuffle Pipeline (1 Stage) ===")
orders.filter("status = 'Completed'").select("order_id", "customer_id").explain()

print("\\n=== Shuffle Pipeline (2 Stages) ===")
orders.filter("status = 'Completed'").groupBy("customer_id").count().explain()
""")
    nb4.add_experiment({"title": "Combining Successive Shuffles", "testing": "Filter before groupBy to reduce shuffle payload.", "expect": "Lower shuffle read bytes.", "code": "orders.filter('quantity > 5').groupBy('status').count().show()", "happened": "Filtered rows before shuffle exchange.", "why": "Reduces rows serialized and transferred over network.", "takeaway": "Always filter and prune columns prior to wide transformations."})
    nb4.add_exercises({"b_title": "Classify Operations", "b_desc": "Is union wide or narrow?", "b_input": "union", "b_sol": "print('Narrow - No shuffle')", "i_title": "Distinct vs GroupBy", "i_desc": "Compare explain() of distinct() vs groupBy().", "i_sol": "orders.select('status').distinct().explain()", "a_title": "Pipeline Refactoring", "a_desc": "Refactor a multi-shuffle pipeline.", "a_sol": "print('Refactored pipeline')"})
    nb4.add_summary_and_teardown({"takeaways": {"Shuffle Reduction": "Filter early, prune columns early, use broadcast joins."}, "terms": {"Exchange": "Shuffle operator in physical plan."}, "mistakes": {"Distinct after groupBy": "Redundant shuffle."}, "questions": {"Why is filtering before a groupBy beneficial?": "It reduces the volume of data that must be serialized, transferred over the network, and sorted during the shuffle."}, "next_steps": "Open 05_partitioning_strategies.ipynb"})
    nb4.save(os.path.join(BASE_DIR, "04_shuffle_operations.ipynb"))

    # 05_05: Partitioning Strategies
    nb5 = NotebookBuilder("Partitioning Strategies: Hash, Range, and Data Lake Partition Layouts", "05_partitions_and_shuffle", "05_partitioning_strategies.ipynb", "Partitioning_Strategies_Lab")
    nb5.add_header({"what": "Hash partitioning vs Range partitioning, bucketing with bucketBy(), and partitioned directory layouts for data lakes.", "why": "Proper storage partitioning enables partition pruning, eliminating 95% of data scans.", "problem": "High-cardinality directory partitioning creating millions of tiny directories.", "real_world": "Designing enterprise Hive / Delta Lake partition architectures."})
    nb5.add_markdown("## 2. Partitioning Strategies\n\n1. **Hash Partitioning**: `hash(key) % N` (Used for joins/aggregations)\n2. **Range Partitioning**: Quantile sample ranges (Used for `orderBy`)\n3. **Directory Partitioning**: `year=2026/month=01/` (Used in data lakes)")
    nb5.add_session_init()
    nb5.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
# Write partitioned by channel
out_path = "/opt/spark-data/warehouse/partitioned_by_channel"
orders.write.mode("overwrite").partitionBy("channel").parquet(out_path)

print("Partitions created on disk:")
for p in os.listdir(out_path):
    if not p.startswith("."):
        print(f"  Directory: {p}")
""")
    nb5.add_experiment({"title": "Partition Pruning Verification", "testing": "Querying with filter on partition column skips other folders entirely.", "expect": "Physical plan shows PartitionFilters: [channel = 'Mobile'].", "code": """read_part = spark.read.parquet(out_path)
filtered = read_part.filter("channel = 'Mobile'")
filtered.explain("formatted")
""", "happened": "PartitionFilters pruned irrelevant channel directories at the file scan level.", "why": "Spark uses directory metadata to skip non-matching partition folders.", "takeaway": "Partition by low-to-medium cardinality columns used frequently in WHERE clauses."})
    nb5.add_exercises({"b_title": "Partition by Year and Month", "b_desc": "Write orders partitioned by year and month.", "b_input": "orders", "b_sol": "orders.withColumn('yr', F.year('order_ts')).write.mode('overwrite').partitionBy('yr').parquet('/opt/spark-data/warehouse/part_yr')", "i_title": "Check Directory Cardinality", "i_desc": "Calculate distinct values of potential partition column.", "i_sol": "print('Channel distinct count:', orders.select('channel').distinct().count())", "a_title": "Bucketing Concept", "a_desc": "Explain bucketing vs partitioning.", "a_sol": "print('Partitioning creates directories; bucketing creates fixed-count hash files within directories.')"})
    nb5.add_summary_and_teardown({"takeaways": {"Partition Pruning": "Skips entire folders based on WHERE filters.", "Cardinality Rule": "Never partition by high cardinality keys (e.g. timestamp, id)."}, "terms": {"Partition Pruning": "Skipping storage directories at read time.", "Bucketing": "Pre-shuffling into fixed buckets."}, "mistakes": {"Partitioning by customer_id": "Creates 100,000 tiny directories, destroying metadata performance."}, "questions": {"What is partition pruning?": "Spark skipping entire directory partitions during scanning because the query filter eliminates them."}, "next_steps": "Open 06_data_skew_and_solutions.ipynb"})
    nb5.save(os.path.join(BASE_DIR, "05_partitioning_strategies.ipynb"))

    # 05_06: Data Skew and Solutions
    nb6 = NotebookBuilder("Data Skew: Detection, Diagnostics, and Production Remediation", "05_partitions_and_shuffle", "06_data_skew_and_solutions.ipynb", "Data_Skew_Lab")
    nb6.add_header({"what": "Detecting data skew via task duration variance, salting skewed join keys, and enabling AQE skew join optimization.", "why": "Data skew is the single most common cause of straggler tasks and OOM crashes in distributed systems.", "problem": "1 task takes 1 hour while 99 tasks take 2 seconds.", "real_world": "Joining transactions with massive merchant keys or default customer IDs."})
    nb6.add_markdown("""## 2. The Data Skew Problem

```
Normal Distribution (Balanced Partitions):
Partition 0: [ 10,000 rows ] -> Task 0 (2s) ===> FINISHED!
Partition 1: [ 10,000 rows ] -> Task 1 (2s) ===> FINISHED!
Partition 2: [ 10,000 rows ] -> Task 2 (2s) ===> FINISHED!

Data Skew (Hotspot on 'Null' or Popular Key):
Partition 0: [ 1,000 rows   ] -> Task 0 (0.2s) ===> IDLE!
Partition 1: [ 1,000 rows   ] -> Task 1 (0.2s) ===> IDLE!
Partition 2: [ 5,000,000 rows ] -> Task 2 (45 mins / OOM Crash!) ===> STRAGGLER!
```
""")
    nb6.add_session_init()
    nb6.add_code("""# Create artificially skewed dataset (90% of rows have key 'HOTSPOT')
skewed_data = [("HOTSPOT", i) for i in range(100000)] + [(f"KEY_{i}", i) for i in range(10000)]
skewed_df = spark.createDataFrame(skewed_data, ["key", "val"])

print("Top keys in skewed dataset:")
skewed_df.groupBy("key").count().orderBy(F.desc("count")).show(3)
""")
    nb6.add_experiment({"title": "Remediating Skew via Key Salting", "testing": "Salt the skewed key with random integers 0-3 to split the hotspot across 4 partitions.", "expect": "The hotspot key is divided evenly into 4 distinct sub-keys.", "code": """SALT_FACTOR = 4
salted_df = skewed_df.withColumn("salted_key", F.concat(F.col("key"), F.lit("_"), F.floor(F.rand() * SALT_FACTOR)))
print("Salted key distribution:")
salted_df.groupBy("salted_key").count().orderBy(F.desc("count")).show(5)
""", "happened": "The 100,000 rows for 'HOTSPOT' were split into 4 balanced partitions of ~25,000 rows each.", "why": "Salting distributes identical keys across multiple distinct hash slots.", "takeaway": "Salting eliminates single-task hotspots on skewed joins."})
    nb6.add_exercises({
        "b_title": "Identify Skew in Profiling", "b_desc": "Profile orders channel column for skew.", "b_input": "orders",
        "b_sol": "orders.groupBy('channel').count().orderBy(F.desc('count')).show()",
        "i_title": "Broadcast Join to Avoid Skew", "i_desc": "Explain why broadcast join eliminates skew.", "i_sol": "print('Broadcast join does not shuffle data by key, eliminating key-based skew.')",
        "a_title": "AQE Skew Join Config", "a_desc": "Print AQE skew join configuration parameters.", "a_sol": "print('skewJoin.enabled:', spark.conf.get('spark.sql.adaptive.skewJoin.enabled', 'true'))"
    })
    nb6.add_summary_and_teardown({"takeaways": {"Data Skew": "Uneven key distribution causing straggler tasks.", "Remediation": "1. Broadcast Join 2. Salting 3. AQE Skew Join 4. Isolate Skewed Keys."}, "terms": {"Straggler": "Task running significantly slower than peers.", "Salting": "Appending random integer to split keys."}, "mistakes": {"Adding more executors to fix skew": "Extra executors do not help if 1 task holds all skewed records."}, "questions": {"How do you detect data skew in the Spark UI?": "Compare 75th percentile vs Max task duration in the Stages tab; if Max is orders of magnitude larger, skew is present."}, "next_steps": "Module 05 complete! Proceed to Module 06: Memory Architecture and Storage."})
    nb6.save(os.path.join(BASE_DIR, "06_data_skew_and_solutions.ipynb"))

if __name__ == '__main__':
    gen_05_01()
    gen_05_02()
    gen_05_03_to_06()
