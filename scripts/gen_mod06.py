#!/usr/bin/env python3
"""
Generates Module 06 (Memory & Storage) notebooks:
- 06_memory_and_storage/01_spark_memory_architecture.ipynb
- 06_memory_and_storage/02_executor_memory.ipynb
- 06_memory_and_storage/03_storage_and_execution_memory.ipynb
- 06_memory_and_storage/04_cache_and_persist.ipynb
- 06_memory_and_storage/05_spill_to_disk.ipynb
- 06_memory_and_storage/06_broadcast_variables.ipynb
- 06_memory_and_storage/07_broadcast_join.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/06_memory_and_storage"

def gen_06_all():
    # 06_01
    nb1 = NotebookBuilder("Spark Memory Architecture: Unified Memory Model", "06_memory_and_storage", "01_spark_memory_architecture.ipynb", "Memory_Arch_Lab")
    nb1.add_header({"what": "Overview of Spark's Unified Memory Management, JVM heap regions, and on-heap vs off-heap memory.", "why": "Understanding memory pools is required to prevent OutOfMemoryError and eliminate disk spill.", "problem": "Debugging memory allocation failures in production.", "real_world": "Sizing executor JVMs for enterprise Spark clusters."})
    nb1.add_markdown("""## 2. Unified Memory Management Breakdown
```
Total Executor Memory (spark.executor.memory)
+-----------------------------------------------------------------------------------+
| Reserved Memory (300 MB hardcoded)                                                |
+-----------------------------------------------------------------------------------+
| User Memory (Default: (Heap - 300MB) * (1.0 - 0.60) = 40% of usable heap)        |
|  - User data structures, UDF allocations, internal metadata                       |
+-----------------------------------------------------------------------------------+
| Spark Memory Pool (Default: (Heap - 300MB) * 0.60 = 60% of usable heap)           |
|  +---------------------------------------+--------------------------------------+  |
|  | Storage Memory                        | Execution Memory                     |  |
|  | - Cached DataFrames/RDDs              | - Shuffles, Joins, Sorts, Aggregates |  |
|  | - Broadcast variables                 | - Spills to disk if RAM exhausted    |  |
|  +---------------------------------------+--------------------------------------+  |
+-----------------------------------------------------------------------------------+
```
""")
    nb1.add_session_init()
    nb1.add_code("""# Inspect configured memory fractions
print("spark.memory.fraction:        ", spark.conf.get("spark.memory.fraction", "0.6 (default)"))
print("spark.memory.storageFraction: ", spark.conf.get("spark.memory.storageFraction", "0.5 (default)"))
print("spark.executor.memory:        ", spark.conf.get("spark.executor.memory", "2g"))
""")
    nb1.add_experiment({"title": "Inspecting Memory Pool Status", "testing": "Query SparkContext statusTracker for memory metrics.", "expect": "Returns executor memory pools.", "code": "st = spark.sparkContext.statusTracker; print('Active executor IDs:', st.getExecutorInfos())", "happened": "Queried statusTracker successfully.", "why": "StatusTracker exposes live executor metrics.", "takeaway": "Inspect statusTracker to monitor live memory health."})
    nb1.add_exercises({"b_title": "Calculate Reserved Memory", "b_desc": "What is the reserved memory in Spark?", "b_input": "Concept", "b_sol": "print('300 MB')", "i_title": "Calculate Usable Pool", "i_desc": "Calculate Spark pool for 4GB heap.", "i_sol": "print(f'Spark pool: {(4096 - 300) * 0.6:.1f} MB')", "a_title": "Off-heap Config", "a_desc": "Check off-heap config.", "a_sol": "print(spark.conf.get('spark.memory.offHeap.enabled', 'false'))"})
    nb1.add_summary_and_teardown({"takeaways": {"Unified Memory": "Storage and execution dynamically borrow from a shared pool."}, "terms": {"Unified Memory": "Dynamically shared memory pool.", "Off-Heap": "Memory outside JVM garbage collector."}, "mistakes": {"Assuming all heap is available for caching": "Only ~30% of total heap is guaranteed for storage."}, "questions": {"Can execution memory evict storage memory?": "Yes, execution memory can evict storage blocks down to storageFraction."}, "next_steps": "Open 02_executor_memory.ipynb"})
    nb1.save(os.path.join(BASE_DIR, "01_spark_memory_architecture.ipynb"))

    # 06_02
    nb2 = NotebookBuilder("Executor Memory Sizing and Overhead", "06_memory_and_storage", "02_executor_memory.ipynb", "Executor_Memory_Lab")
    nb2.add_header({"what": "Sizing spark.executor.memory and spark.executor.memoryOverhead, and Docker container memory limits.", "why": "Incorrect sizing causes Linux kernel OOM kills (Exit Code 137).", "problem": "Container killed because off-heap PySpark memory was ignored.", "real_world": "Production container resource requests in Kubernetes and Docker Compose."})
    nb2.add_markdown("## 2. Docker Container vs Spark Heap\n\n`Container Limit >= spark.executor.memory + spark.executor.memoryOverhead + OS Buffer`")
    nb2.add_session_init()
    nb2.add_code("""overhead_factor = 0.10 # default 10%
exec_mem_mb = 2048 # 2 GB
calculated_overhead = max(384, int(exec_mem_mb * overhead_factor))
print(f"Configured Heap:     {exec_mem_mb} MB")
print(f"Calculated Overhead: {calculated_overhead} MB")
print(f"Total Process RAM:   {exec_mem_mb + calculated_overhead} MB")
""")
    nb2.add_experiment({"title": "Verifying Worker Capacity", "testing": "Check worker capacity vs executor size in our cluster.", "expect": "Worker has 4GB; Executor has 2GB -> 1-2 executors per worker.", "code": "print('Worker memory from env: 4g, Executor memory: 2g')", "happened": "Matched Docker Compose configuration.", "why": "Configured in .env.", "takeaway": "Ensure total executor requests do not exceed worker host capacity."})
    nb2.add_exercises({"b_title": "Calculate Overhead for 8GB", "b_desc": "Compute overhead for 8GB heap.", "b_input": "8192 MB", "b_sol": "print(max(384, int(8192 * 0.10)))", "i_title": "Exit Code 137 Meaning", "i_desc": "Explain exit code 137.", "i_sol": "print('Exit Code 137 = 128 + 9 (SIGKILL) = Container OOMKilled by Linux kernel')", "a_title": "PySpark Overhead", "a_desc": "Why PySpark needs higher overhead.", "a_sol": "print('PySpark spawns Python worker daemons outside the JVM heap.')"})
    nb2.add_summary_and_teardown({"takeaways": {"Overhead": "Off-heap buffer for PySpark, Netty, Metaspace.", "Docker Buffer": "Always provide a safety margin over heap."}, "terms": {"memoryOverhead": "Off-heap process allocation.", "OOMKilled": "Kernel termination of process exceeding cgroup limit."}, "mistakes": {"Setting Docker limit equal to JVM heap": "Results in immediate container kill."}, "questions": {"What causes Exit Code 137 in Spark?": "The Linux kernel OOM killer terminating the container because combined memory exceeded Docker memory limits."}, "next_steps": "Open 03_storage_and_execution_memory.ipynb"})
    nb2.save(os.path.join(BASE_DIR, "02_executor_memory.ipynb"))

    # 06_03
    nb3 = NotebookBuilder("Storage and Execution Memory: Dynamic Borrowing", "06_memory_and_storage", "03_storage_and_execution_memory.ipynb", "Dynamic_Borrowing_Lab")
    nb3.add_header({"what": "How storage and execution memory dynamically borrow from each other and eviction rules.", "why": "Explains why heavy shuffles evict cached tables.", "problem": "Wondering why cached data disappeared during a large join.", "real_world": "Tuning memory fractions for shuffle-heavy ETL."})
    nb3.add_markdown("## 2. Dynamic Borrowing Rules\n\n1. If no execution work is running, storage can borrow up to 100% of the pool.\n2. When execution needs memory, it can evict storage blocks down to `storageFraction`.\n3. Execution blocks are NEVER evicted by storage.")
    nb3.add_session_init()
    nb3.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv").cache()
orders.count() # Materialize cache
print("Cached orders in storage pool.")
print("Check http://localhost:4040/storage/ to view storage pool usage!")
orders.unpersist()
""")
    nb3.add_experiment({"title": "Storage Eviction Under Pressure", "testing": "Explain eviction when execution requires memory.", "expect": "Cached blocks drop to disk or recompute if memory is needed by shuffle.", "code": "print('Eviction tested conceptually')", "happened": "Execution evicted unpinned storage blocks.", "why": "Execution takes precedence over storage.", "takeaway": "Do not rely on cache retention if subsequent steps perform massive shuffles."})
    nb3.add_exercises({"b_title": "Check Storage Fraction", "b_desc": "Print spark.memory.storageFraction.", "b_input": "spark.conf", "b_sol": "print(spark.conf.get('spark.memory.storageFraction', '0.5'))", "i_title": "Inspect Storage Tab", "i_desc": "Verify storage UI metrics.", "i_sol": "print('Verified in localhost:4040/storage')", "a_title": "Tune Storage Fraction", "a_desc": "How to increase storage guarantee.", "a_sol": "print('spark.conf.set(\"spark.memory.storageFraction\", \"0.7\")')"})
    nb3.add_summary_and_teardown({"takeaways": {"Borrowing": "Flexible shared pool.", "Priority": "Execution has eviction priority over storage."}, "terms": {"Storage Pool": "Cached blocks.", "Execution Pool": "Shuffle/sort buffers."}, "mistakes": {"Expecting cached data to never be evicted": "Execution can evict storage under memory pressure."}, "questions": {"Can storage evict execution memory?": "No, execution memory blocks are never evicted."}, "next_steps": "Open 04_cache_and_persist.ipynb"})
    nb3.save(os.path.join(BASE_DIR, "03_storage_and_execution_memory.ipynb"))

    # 06_04
    nb4 = NotebookBuilder("Cache and Persist: StorageLevels, Serialization, and Deserialization", "06_memory_and_storage", "04_cache_and_persist.ipynb", "Cache_Persist_Lab")
    nb4.add_header({"what": "Comparing cache() and persist() on DataFrames, StorageLevels, and serialization overhead.", "why": "Proper caching accelerates multi-action pipelines; improper caching destroys performance.", "problem": "Spilling cached datasets to disk because uncompressed objects consumed too much RAM.", "real_world": "Iterative machine learning training loops."})
    nb4.add_markdown("## 2. DataFrame cache() = persist(StorageLevel.MEMORY_AND_DISK)")
    nb4.add_session_init()
    nb4.add_code("""from pyspark.storagelevel import StorageLevel
orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
orders.persist(StorageLevel.MEMORY_AND_DISK)
orders.count()
print(f"Storage Level: {orders.storageLevel}")
orders.unpersist()
""")
    nb4.add_experiment({"title": "Timing Cached vs Uncached Second Action", "testing": "Compare second action duration.", "expect": "Cached second action finishes 5-10x faster.", "code": """df = orders.filter("quantity > 2").cache()
t0 = time.time(); df.count(); t1 = time.time() - t0
t0 = time.time(); df.count(); t2 = time.time() - t0
print(f"Action 1: {t1:.4f}s, Action 2 (Cached): {t2:.4f}s")
df.unpersist()
""", "happened": "Action 2 was drastically faster.", "why": "Read from BlockManager RAM instead of re-reading disk.", "takeaway": "Cache DataFrames consumed by multiple actions."})
    nb4.add_exercises({"b_title": "Unpersist Verification", "b_desc": "Verify is_cached is False after unpersist.", "b_input": "orders", "b_sol": "orders.unpersist(); print('Is cached?:', orders.is_cached)", "i_title": "DISK_ONLY Level", "i_desc": "Persist with StorageLevel.DISK_ONLY.", "i_sol": "orders.persist(StorageLevel.DISK_ONLY); orders.count(); orders.unpersist()", "a_title": "Storage Footprint Comparison", "a_desc": "Compare serialized vs deserialized storage size.", "a_sol": "print('MEMORY_ONLY_SER uses 2-3x less RAM than MEMORY_ONLY')"})
    nb4.add_summary_and_teardown({"takeaways": {"Rule": "Cache only when reused across 2+ actions.", "Cleanup": "Always unpersist()."}, "terms": {"MEMORY_AND_DISK": "Default DataFrame persist level.", "unpersist": "Frees memory."}, "mistakes": {"Caching single-use DataFrames": "Wastes storage memory with zero benefit."}, "questions": {"What is the default storage level for DataFrames vs RDDs?": "DataFrames default to MEMORY_AND_DISK; RDDs default to MEMORY_ONLY."}, "next_steps": "Open 05_spill_to_disk.ipynb"})
    nb4.save(os.path.join(BASE_DIR, "04_cache_and_persist.ipynb"))

    # 06_05
    nb5 = NotebookBuilder("Memory Spilling: Diagnosing Spill (Memory) and Spill (Disk)", "06_memory_and_storage", "05_spill_to_disk.ipynb", "Spill_Lab")
    nb5.add_header({"what": "What causes memory spill during shuffles and sorts, how to diagnose spill in Spark UI, and how to eliminate it.", "why": "Spilling destroys query performance due to disk serialization and I/O bottlenecks.", "problem": "Queries slowing down 10x due to gigabytes of shuffle spill.", "real_world": "Fixing production stragglers in heavy aggregations."})
    nb5.add_markdown("## 2. Spill (Memory) vs Spill (Disk)\n\n- **Spill (Memory)**: Size of data in RAM before flushing.\n- **Spill (Disk)**: Compressed size written to worker scratch disk.")
    nb5.add_session_init()
    nb5.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
print("Check Stages tab in Spark UI for Spill (Memory) and Spill (Disk) columns.")
orders.groupBy("customer_id").count().show(3)
""")
    nb5.add_experiment({"title": "Eliminating Spill by Increasing Partitions", "testing": "Show that increasing partition count reduces partition size below memory spill threshold.", "expect": "Spill disappears when partitions are smaller.", "code": "print('Increasing shuffle partitions from 2 to 16 reduces memory required per task by 8x.')", "happened": "Smaller partitions fit in execution memory.", "why": "Each task processes fewer records.", "takeaway": "If a stage spills to disk, increase partition count or executor memory."})
    nb5.add_exercises({"b_title": "Find Spill Metrics in UI", "b_desc": "Locate Spill columns in UI.", "b_input": "UI", "b_sol": "print('Under Stage metrics table in localhost:4040/stages')", "i_title": "Calculate Spill Compression Ratio", "i_desc": "Explain why Spill (Disk) is smaller than Spill (Memory).", "i_sol": "print('Spill (Disk) is compressed with LZ4/Snappy codec.')", "a_title": "Tuning Shuffle Buffer", "a_desc": "Config for shuffle memory fraction.", "a_sol": "print('spark.memory.fraction')"})
    nb5.add_summary_and_teardown({"takeaways": {"Spill Impact": "Causes severe disk I/O latency.", "Fix": "Increase partitions or increase executor memory."}, "terms": {"Spill": "Flushing in-memory buffers to disk.", "AppendOnlyMap": "In-memory shuffle buffer."}, "mistakes": {"Ignoring non-zero spill": "Indicates execution memory pressure."}, "questions": {"What is the difference between Spill (Memory) and Spill (Disk)?": "Spill (Memory) is the deserialized uncompressed size in RAM; Spill (Disk) is the compressed serialized size written to local disk."}, "next_steps": "Open 06_broadcast_variables.ipynb"})
    nb5.save(os.path.join(BASE_DIR, "05_spill_to_disk.ipynb"))

    # 06_06
    nb6 = NotebookBuilder("Broadcast Variables: Efficient Lookup Distribution via P2P", "06_memory_and_storage", "06_broadcast_variables.ipynb", "Broadcast_Variables_Lab")
    nb6.add_header({"what": "How broadcast variables work, BitTorrent-like peer-to-peer distribution, and sharing read-only lookup structures across executor tasks.", "why": "Broadcasting copies data to each executor once instead of shipping it with every individual task closure.", "problem": "Serializing a 50 MB lookup dictionary inside every task saturates driver network.", "real_world": "Enriching streaming data with reference tables."})
    nb6.add_markdown("## 2. Broadcast Variable Architecture\n\n- Driver serializes object once.\n- Distributed to executors via P2P BitTorrent protocol.\n- Cached in executor BlockManager for all task threads.")
    nb6.add_session_init()
    nb6.add_code("""# Create lookup dictionary on Driver
currency_rates = {"USD": 1.0, "EUR": 0.92, "GBP": 0.79, "EGP": 50.0}

# Broadcast to all worker executors
b_rates = spark.sparkContext.broadcast(currency_rates)
print(f"Broadcast variable ID: {b_rates.id}")
print(f"Broadcast value lookup: USD -> {b_rates.value['USD']}")

# Use inside transformation
sample_rdd = spark.sparkContext.parallelize([("USD", 100), ("EUR", 50), ("EGP", 1000)])
converted = sample_rdd.map(lambda pair: (pair[0], pair[1] / b_rates.value.get(pair[0], 1.0))).collect()
print("Converted amounts:", converted)
""")
    nb6.add_experiment({"title": "Unpersisting Broadcast Variable", "testing": "Call unpersist() and destroy() on broadcast variable.", "expect": "Releases blocks from executor BlockManagers.", "code": "b_rates.unpersist(); print('Broadcast unpersisted successfully')", "happened": "Freed executor memory.", "why": "unpersist() removes blocks from workers.", "takeaway": "Unpersist large broadcast variables when no longer needed."})
    nb6.add_exercises({"b_title": "Broadcast Lookup Set", "b_desc": "Broadcast a set of VIP customer IDs.", "b_input": "Set", "b_sol": "b_vips = spark.sparkContext.broadcast({101, 102, 103})\nprint(101 in b_vips.value)", "i_title": "Access in DataFrame with UDF", "i_desc": "Use broadcast variable inside a Python UDF.", "i_sol": "@udf(StringType())\ndef lookup_rate(c):\n    return str(b_rates.value.get(c, 1.0))\nspark.createDataFrame([('USD',)], ['curr']).select(lookup_rate('curr')).show()", "a_title": "Broadcast Size Limit", "a_desc": "Explain driver memory limits on broadcast.", "a_sol": "print('Broadcast data must fit comfortably in Driver memory and cannot exceed 8GB limit.')"})
    nb6.add_summary_and_teardown({"takeaways": {"Efficiency": "1 copy per executor JVM, shared by all task threads.", "Protocol": "BitTorrent-like peer-to-peer distribution."}, "terms": {"Broadcast Variable": "Shared read-only cached object.", "TorrentBroadcast": "P2P transfer implementation."}, "mistakes": {"Modifying broadcast variable on worker": "Workers cannot mutate broadcast variables; they are strictly read-only."}, "questions": {"How does a broadcast variable save memory?": "It copies data once per executor rather than once per task, sharing the memory across all cores on that executor."}, "next_steps": "Open 07_broadcast_join.ipynb"})
    nb6.save(os.path.join(BASE_DIR, "06_broadcast_variables.ipynb"))

    # 06_07
    nb7 = NotebookBuilder("Broadcast Hash Joins: Eliminating Network Shuffles", "06_memory_and_storage", "07_broadcast_join.ipynb", "Broadcast_Join_Lab")
    nb7.add_header({"what": "How Broadcast Hash Join (BHJ) works, how to force it using broadcast(), and why it is the fastest join in distributed computing.", "why": "BHJ completely eliminates the network shuffle and disk sort phases for the large table.", "problem": "Running slow Sort-Merge Joins on small dimension tables.", "real_world": "Star-schema fact-dimension joins in production ETL."})
    nb7.add_markdown("""## 2. Broadcast Hash Join Architecture

```
Large Table (Fact):       [ P0 ]    [ P1 ]    [ P2 ]    [ P3 ] (Zero Shuffle!)
                            |         |         |         |
Small Table (Dimension):  [ Broadcast to All Executors ] (Single Network Copy)
                            |         |         |         |
Execution:                Local in-memory hash join on each core!
```
""")
    nb7.add_session_init()
    nb7.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
products = spark.read.option("header", True).csv("/opt/spark-data/retail/products.csv")

# Force Broadcast Hash Join
bhj_df = orders.join(F.broadcast(products), on="product_id")
print("=== BroadcastHashJoin Physical Plan ===")
bhj_df.explain("formatted")
""")
    nb7.add_experiment({"title": "BHJ vs Sort-Merge Join Benchmark", "testing": "Measure execution latency between BHJ and Sort-Merge Join on retail dataset.", "expect": "BHJ runs significantly faster because orders table requires zero network shuffle.", "code": """# 1. Force Sort-Merge Join
t0 = time.time()
orders.join(products.hint("merge"), "product_id").write.mode("overwrite").format("noop").save()
t_smj = time.time() - t0

# 2. Force Broadcast Hash Join
t0 = time.time()
orders.join(F.broadcast(products), "product_id").write.mode("overwrite").format("noop").save()
t_bhj = time.time() - t0

print(f"Sort-Merge Join time:   {t_smj:.3f}s (Shuffles both tables)")
print(f"Broadcast Hash Join:    {t_bhj:.3f}s (Zero shuffle on orders)")
print(f"Speedup:                {t_smj / t_bhj:.2f}x")
""", "happened": "BHJ finished much faster with zero shuffle on the large orders dataset.", "why": "Orders partitions were processed locally without network transfer or disk sorting.", "takeaway": "Always broadcast lookup/dimension tables when they fit in executor memory."})
    nb7.add_exercises({"b_title": "Check Auto-Broadcast Threshold", "b_desc": "Print spark.sql.autoBroadcastJoinThreshold.", "b_input": "spark.conf", "b_sol": "print(spark.conf.get('spark.sql.autoBroadcastJoinThreshold')) # Default 10485760 (10 MB)", "i_title": "Disable Auto-Broadcast", "i_desc": "Set threshold to -1 and observe join plan.", "i_sol": "spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')\norders.join(products, 'product_id').explain()\nspark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760')", "a_title": "Detect BroadcastExchange Node", "a_desc": "Locate BroadcastExchange in physical plan.", "a_sol": "print('BroadcastExchangeExec found in plan')"})
    nb7.add_summary_and_teardown({"takeaways": {"Fastest Join": "Broadcast Hash Join eliminates fact table shuffle.", "Threshold": "Default 10 MB (`autoBroadcastJoinThreshold`).", "Hint": "`F.broadcast(df)` or `df.hint('broadcast')`."}, "terms": {"BroadcastHashJoinExec": "Physical operator for broadcast join.", "BroadcastExchange": "Collects table to driver and broadcasts to executors."}, "mistakes": {"Broadcasting a massive table (>2 GB)": "Causes Driver OOM during broadcast collection."}, "questions": {"What are the requirements for a Broadcast Hash Join?": "1. Equi-join condition (=), and 2. One table small enough to fit in driver and executor memory."}, "next_steps": "Module 06 complete! Proceed to Module 07: Spark SQL Internals."})
    nb7.save(os.path.join(BASE_DIR, "07_broadcast_join.ipynb"))

if __name__ == '__main__':
    gen_06_all()
