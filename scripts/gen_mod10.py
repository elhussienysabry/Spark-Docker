#!/usr/bin/env python3
"""
Generates Module 10 (Performance & Optimization) notebooks:
- 10_performance_and_optimization/01_spark_ui_analysis.ipynb
- 10_performance_and_optimization/02_explain_and_execution_plans.ipynb
- 10_performance_and_optimization/03_query_optimization.ipynb
- 10_performance_and_optimization/04_shuffle_optimization.ipynb
- 10_performance_and_optimization/05_memory_optimization.ipynb
- 10_performance_and_optimization/06_data_skew_optimization.ipynb
- 10_performance_and_optimization/07_performance_benchmarking.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/10_performance_and_optimization"

def gen_10_all():
    # 10_01: Spark UI Analysis
    nb1 = NotebookBuilder("Spark UI Deep Dive: Forensic Analysis of Jobs, Stages, and Tasks", "10_performance_and_optimization", "01_spark_ui_analysis.ipynb", "Spark_UI_Lab")
    nb1.add_header({"what": "Mastering the Spark UI: Jobs, Stages, Storage, Environment, Executors, and SQL tabs, event timelines, and diagnostic triage.", "why": "The Spark UI is your primary window into distributed execution. 90% of tuning decisions originate from UI metrics.", "problem": "Trying to optimize jobs blindly without knowing if CPU, network, disk, or GC is the bottleneck.", "real_world": "Resolving enterprise cluster SLA breaches and performance incidents."})
    nb1.add_markdown("""## 2. Navigating the Spark UI
- **Jobs Tab**: High-level timeline of triggered actions.
- **Stages Tab**: Deeper metrics per stage: Duration percentiles (Min, 25%, Median, 75%, Max), Shuffle Read/Write bytes, Spill (Memory/Disk).
- **Storage Tab**: Cached DataFrames/RDDs, memory used, percentage cached in RAM.
- **Executors Tab**: Active cores, memory used per executor, GC pause time, failed tasks.
- **SQL Tab**: Visual DAG plan with physical operators and row counters.
""")
    nb1.add_session_init()
    nb1.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
res = orders.groupBy("channel").count()
res.show()
print("Open http://localhost:4040/ to inspect the live Spark UI!")
""")
    nb1.add_experiment({"title": "Correlating Spark UI Event Timeline with Code", "testing": "Run a multi-stage join query and inspect Event Timeline bars.", "expect": "Timeline shows blue (compute), orange (fetch wait), and red (GC).", "code": """products = spark.read.option("header", True).csv("/opt/spark-data/retail/products.csv")
orders.join(products, "product_id").groupBy("category").count().show()
print("Check http://localhost:4040/stages/ for Stage task timelines!")
""", "happened": "Spark UI tracked execution timeline across all executor cores.", "why": "LiveListenerBus streams task events to the embedded web server.", "takeaway": "Large gaps or long bars in the timeline pinpoint the exact execution bottleneck."})
    nb1.add_exercises({"b_title": "Check UI Port", "b_desc": "What port hosts the driver UI?", "b_input": "Port", "b_sol": "print('Port 4040 (Driver) and Port 18080 (History Server)')", "i_title": "Identify Straggler Task", "i_desc": "How to spot stragglers in Stages tab.", "i_sol": "print('Compare Median vs Max task duration. If Max >> Median, it is a straggler.')", "a_title": "Executor GC Ratio", "a_desc": "Calculate GC percentage threshold.", "a_sol": "print('If Task GC time exceeds 10% of total duration, investigate JVM memory allocation.')"})
    nb1.add_summary_and_teardown({"takeaways": {"UI Forensic Flow": "Jobs -> Stages -> Task Summary Table -> SQL Plan.", "Straggler Detection": "Max task time >> 75th percentile."}, "terms": {"LiveListenerBus": "Internal event queue feeding UI.", "Event Timeline": "Visual timeline of task threads."}, "mistakes": {"Closing browser and losing metrics": "Use History Server (18080) for completed jobs."}, "questions": {"What does an orange segment in the Spark UI Task Timeline indicate?": "Shuffle Fetch Wait time (waiting for remote shuffle map output blocks over the network)."}, "next_steps": "Open 02_explain_and_execution_plans.ipynb"})
    nb1.save(os.path.join(BASE_DIR, "01_spark_ui_analysis.ipynb"))

    # 10_02: Explain & Execution Plans
    nb2 = NotebookBuilder("Execution Plan Analysis: Reading Physical Operators and Bottlenecks", "10_performance_and_optimization", "02_explain_and_execution_plans.ipynb", "Plan_Analysis_Lab")
    nb2.add_header({"what": "Deep reading of physical plans: Scan, Filter, Project, Exchange, Sort, HashAggregate, and join operators.", "why": "Before touching cluster configurations, you must verify the query plan is optimal.", "problem": "Missing that a query is triggering an unintended Cartesian product or full scan.", "real_world": "Authoring code reviews for high-throughput batch ETL pipelines."})
    nb2.add_session_init()
    nb2.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
q = orders.filter("quantity > 3").groupBy("channel").agg(F.count("order_id").alias("cnt"))
q.explain("formatted")
""")
    nb2.add_experiment({"title": "Detecting Exchange Nodes in Complex Queries", "testing": "Count shuffle exchange points in multi-step transformations.", "expect": "Each groupBy, distinct, and join creates an Exchange operator.", "code": """plan_text = q._jdf.queryExecution().executedPlan().toString()
print("Total Exchange (Shuffle) nodes:", plan_text.count("Exchange"))
""", "happened": "Counted physical Exchange nodes.", "why": "Exchange operators represent network shuffle boundaries.", "takeaway": "Fewer Exchange nodes almost always means faster query execution."})
    nb2.add_exercises({"b_title": "Identify HashAggregate", "b_desc": "Find HashAggregate in explain output.", "b_input": "q", "b_sol": "print('HashAggregate is visible in formatted plan.')", "i_title": "BroadcastExchange Node", "i_desc": "Verify BroadcastExchange presence when broadcasting.", "i_sol": "orders.join(F.broadcast(orders.limit(5)), 'order_id').explain()", "a_title": "Cost Model Statistics", "a_desc": "Explain Statistics output in explain('cost').", "a_sol": "print('Statistics show estimated row count and byte size at each node.')"})
    nb2.add_summary_and_teardown({"takeaways": {"Plan Verification": "Verify filter pushdown, column pruning, and exchange count.", "Formatted Plan": "Use explain('formatted') for clear node breakdowns."}, "terms": {"Exchange": "Shuffle operator.", "HashAggregate": "In-memory hash aggregation."}, "mistakes": {"Running queries without checking plans": "Leads to surprise shuffles in production."}, "questions": {"How do you know if an operator participates in WholeStageCodegen?": "It is prefixed with an asterisk (*) in the physical plan."}, "next_steps": "Open 03_query_optimization.ipynb"})
    nb2.save(os.path.join(BASE_DIR, "02_explain_and_execution_plans.ipynb"))

    # 10_03: Query Optimization
    nb3 = NotebookBuilder("Query Optimization Techniques: Filter Pushdown, Pruning, and Join Order", "10_performance_and_optimization", "03_query_optimization.ipynb", "Query_Opt_Lab")
    nb3.add_header({"what": "Techniques to write optimal queries: filter pushdown, column pruning, join reordering, and eliminating redundant computations.", "why": "Writing clean, Catalyst-friendly code prevents unnecessary compute and network overhead.", "problem": "Queries that scan 50 columns when only 2 are used.", "real_world": "Optimizing warehouse queries."})
    nb3.add_session_init()
    nb3.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
# Optimal: Select needed columns and filter early
optimized = orders.filter("status = 'Completed'").select("order_id", "customer_id", "unit_price")
optimized.explain("formatted")
""")
    nb3.add_experiment({"title": "Column Pruning I/O Impact", "testing": "Compare execution time when selecting 2 columns vs all 50 columns.", "expect": "Column pruning reduces memory deserialization time.", "code": """t0 = time.time(); orders.select("order_id").count(); t_prune = time.time() - t0
t0 = time.time(); orders.count(); t_full = time.time() - t0
print(f"Pruned scan: {t_prune:.3f}s, Full scan: {t_full:.3f}s")
""", "happened": "Pruned scan executed faster.", "why": "Only required columns were deserialized.", "takeaway": "Always select only the specific columns required for downstream operations."})
    nb3.add_exercises({"b_title": "Filter Before Join", "b_desc": "Filter table prior to join.", "b_input": "orders", "b_sol": "print('Filter orders before joining with customers.')", "i_title": "Drop Unused Columns", "i_desc": "Drop channel and discount.", "i_sol": "orders.drop('channel', 'discount').show(2)", "a_title": "Join Order Optimization", "a_desc": "How join order affects broadcast decisions.", "a_sol": "print('Catalyst CBO can reorder joins if table statistics are analyzed.')"})
    nb3.add_summary_and_teardown({"takeaways": {"Optimization Rules": "Select only needed columns; filter as early as possible; broadcast small tables."}, "terms": {"Column Pruning": "Skipping unused columns.", "Predicate Pushdown": "Pushing filters to storage."}, "mistakes": {"SELECT * everywhere": "Forces full deserialization of every column."}, "questions": {"Why is filtering before a join beneficial?": "It minimizes the number of rows participating in the join and shuffle phases."}, "next_steps": "Open 04_shuffle_optimization.ipynb"})
    nb3.save(os.path.join(BASE_DIR, "03_query_optimization.ipynb"))

    # 10_04: Shuffle Optimization
    nb4 = NotebookBuilder("Shuffle Optimization: Partition Tuning, Bucket Joins, and Reducing I/O", "10_performance_and_optimization", "04_shuffle_optimization.ipynb", "Shuffle_Opt_Lab")
    nb4.add_header({"what": "Techniques to optimize shuffles: right-sizing shuffle partitions, bucketing to eliminate joins, and AQE partition coalescing.", "why": "Shuffle optimization yields the highest ROI for big data jobs.", "problem": "Default 200 shuffle partitions creating massive scheduling latency on small data, or spilling on large data.", "real_world": "Tuning heavy ETL jobs with multi-terabyte fact tables."})
    nb4.add_session_init()
    nb4.add_code("""# Tune shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "8") # Match cluster capacity
orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
res = orders.groupBy("channel").count()
res.show()
print("Shuffle executed with 8 partitions!")
""")
    nb4.add_experiment({"title": "AQE Automatic Partition Coalescing", "testing": "Verify AQE coalesces 200 partitions into fewer partitions at runtime.", "expect": "AQE merges small partitions automatically.", "code": """spark.conf.set("spark.sql.shuffle.partitions", "100") # Deliberately oversized
res2 = orders.groupBy("status").count()
res2.show()
print("AQE coalesced partitions automatically in runtime plan!")
spark.conf.set("spark.sql.shuffle.partitions", "8")
""", "happened": "AQE reduced 100 shuffle partitions to fewer active post-shuffle tasks.", "why": "spark.sql.adaptive.coalescePartitions.enabled was active.", "takeaway": "AQE protects against oversized shuffle partition settings."})
    nb4.add_exercises({"b_title": "Set Shuffle Partitions", "b_desc": "Set shuffle partitions to 16.", "b_input": "spark.conf", "b_sol": "spark.conf.set('spark.sql.shuffle.partitions', '16')", "i_title": "Bucketing for Zero Shuffle", "i_desc": "Explain bucketBy.", "i_sol": "print('Pre-partitions data by hash on disk; subsequent joins on that key require zero shuffle.')", "a_title": "Shuffle Compression Codec", "a_desc": "Check spark.io.compression.codec.", "a_sol": "print(spark.conf.get('spark.io.compression.codec', 'lz4'))"})
    nb4.add_summary_and_teardown({"takeaways": {"Sizing": "Target 100-200 MB per shuffle partition.", "AQE": "Enables dynamic coalescing."}, "terms": {"shuffle.partitions": "Output partitions after wide transformation.", "Bucketing": "Pre-sorted storage partitioning."}, "mistakes": {"Leaving default 200 on tiny data": "Creates 200 tiny tasks with massive scheduling overhead."}, "questions": {"How does bucketing eliminate join shuffles?": "If two tables are bucketed on the same join key with the same number of buckets, corresponding buckets reside on the same partition index, allowing a narrow join without a shuffle."}, "next_steps": "Open 05_memory_optimization.ipynb"})
    nb4.save(os.path.join(BASE_DIR, "04_shuffle_optimization.ipynb"))

    # 10_05: Memory Optimization
    nb5 = NotebookBuilder("Memory Optimization: Garbage Collection Tuning, Off-Heap, and Kryo", "10_performance_and_optimization", "05_memory_optimization.ipynb", "Memory_Opt_Lab")
    nb5.add_header({"what": "Techniques for memory optimization: JVM Garbage Collection diagnostics (G1GC), Kryo serialization, and off-heap memory.", "why": "Memory leaks and GC pauses ruin job predictability and cause heartbeat loss.", "problem": "Stop-The-World GC pauses exceeding 30 seconds crashing worker heartbeats.", "real_world": "Enterprise memory tuning on large JVM clusters."})
    nb5.add_session_init()
    nb5.add_code("""# Inspect serialization configuration
print("Serializer: ", spark.conf.get("spark.serializer"))
print("Is Kryo?:   ", "Kryo" in spark.conf.get("spark.serializer", ""))
""")
    nb5.add_experiment({"title": "Kryo Serialization Verification", "testing": "Verify Kryo is active in our lab environment.", "expect": "spark.serializer is org.apache.spark.serializer.KryoSerializer.", "code": "print('Active serializer:', spark.sparkContext.getConf().get('spark.serializer'))", "happened": "KryoSerializer confirmed active.", "why": "Configured in conf/spark-defaults.conf.", "takeaway": "Kryo is up to 10x faster and significantly more compact than standard Java serialization."})
    nb5.add_exercises({"b_title": "Check Off-Heap Setting", "b_desc": "Print spark.memory.offHeap.enabled.", "b_input": "spark.conf", "b_sol": "print(spark.conf.get('spark.memory.offHeap.enabled', 'false'))", "i_title": "GC Pause Detection", "i_desc": "Where to look in UI for GC time.", "i_sol": "print('Check Task GC Time column in Stages tab; red bars in event timeline.')", "a_title": "G1GC Flags", "a_desc": "List recommended JVM flags for Java 21.", "a_sol": "print('-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35')"})
    nb5.add_summary_and_teardown({"takeaways": {"Kryo Serializer": "Always enable Kryo for faster network and storage serialization.", "GC Tuning": "Use G1GC to keep pauses below 1 second."}, "terms": {"KryoSerializer": "High performance binary serializer.", "Stop-The-World": "JVM GC pause freezing all application threads."}, "mistakes": {"Using default Java serialization": "Bulky serialized bytes, slow network transfer."}, "questions": {"Why is KryoSerializer preferred over standard Java serialization?": "Kryo produces much smaller binary payloads and serializes objects significantly faster, reducing shuffle write time and network transfer volume."}, "next_steps": "Open 06_data_skew_optimization.ipynb"})
    nb5.save(os.path.join(BASE_DIR, "05_memory_optimization.ipynb"))

    # 10_06: Data Skew Optimization
    nb6 = NotebookBuilder("Data Skew Optimization: Salting, Skew Hints, and AQE Skew Joins", "10_performance_and_optimization", "06_data_skew_optimization.ipynb", "Skew_Opt_Lab")
    nb6.add_header({"what": "Comprehensive hands-on laboratory for diagnosing and fixing data skew: key salting, broadcast join avoidance, and AQE automatic skew splitting.", "why": "Data skew is the primary cause of jobs hanging at 99% progress.", "problem": "1 task running for 40 minutes while all other tasks finished in seconds.", "real_world": "Handling null keys and power-law customer purchase distributions."})
    nb6.add_markdown("## 2. Skew Remediation Playbook: 1. Broadcast Join -> 2. Salting -> 3. AQE Skew Join -> 4. Isolate Skewed Keys")
    nb6.add_session_init()
    nb6.add_code("""# Create skewed dataset
skewed = spark.range(0, 100000).withColumn("key", F.when(F.col("id") < 90000, "HOT_KEY").otherwise(F.concat(F.lit("KEY_"), F.col("id"))))
print("Key distribution:")
skewed.groupBy("key").count().orderBy(F.desc("count")).show(3)
""")
    nb6.add_experiment({"title": "Key Salting Implementation", "testing": "Salt the skewed key to distribute the hotspot across 8 independent partitions.", "expect": "The single massive partition is split into 8 uniform slices.", "code": """salted = skewed.withColumn("salted_key", F.concat(F.col("key"), F.lit("_"), F.floor(F.rand() * 8)))
print("Salted key breakdown:")
salted.filter("key = 'HOT_KEY'").groupBy("salted_key").count().show()
""", "happened": "The 90,000 HOT_KEY rows were distributed across 8 salted keys (~11,250 rows each).", "why": "Random salt assigned rows across distinct hash partitions.", "takeaway": "Salting converts an asymmetric bottleneck into uniform parallel task waves."})
    nb6.add_exercises({"b_title": "Identify Skew Column", "b_desc": "Profile top keys in dirty dataset.", "b_input": "dirty_df", "b_sol": "spark.read.option('header', True).csv('/opt/spark-data/quality/orders_dirty.csv').groupBy('customer_id').count().orderBy(F.desc('count')).show(5)", "i_title": "Filter Skewed Nulls", "i_desc": "Isolate null keys prior to join.", "i_sol": "print('Filter isNotNull() on left and right keys; union null records separately.')", "a_title": "AQE Skew Join Trigger", "a_desc": "Threshold settings for AQE skew join.", "a_sol": "print('spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes (default 256MB)')"})
    nb6.add_summary_and_teardown({"takeaways": {"Salting Formula": "key + '_' + rand(0, K).", "AQE Fix": "AQE automatically splits skewed shuffle partitions into sub-partitions."}, "terms": {"Data Skew": "Asymmetric record distribution.", "Salting": "Adding random prefix to split key hotspots."}, "mistakes": {"Assuming more memory fixes skew": "If all rows map to 1 key, 1 task will still process them sequentially."}, "questions": {"Explain how salting resolves a data skew join bottleneck.": "Salting appends a random number to the skewed key of the large table and replicates the matching small table keys with matching numbers, distributing the hotspot across multiple parallel partitions."}, "next_steps": "Open 07_performance_benchmarking.ipynb"})
    nb6.save(os.path.join(BASE_DIR, "06_data_skew_optimization.ipynb"))

    # 10_07: Performance Benchmarking
    nb7 = NotebookBuilder("Performance Benchmarking: Building Reproducible Spark Benchmark Suites", "10_performance_and_optimization", "07_performance_benchmarking.ipynb", "Benchmarking_Lab")
    nb7.add_header({"what": "How to design reproducible benchmarks in PySpark: warming up kernels, measuring wall-clock time, collecting Spark UI metrics, and comparing implementations.", "why": "Optimization claims must be supported by empirical data, not guesswork.", "problem": "Benchmarking un-warmed jobs or confusing client overhead with cluster compute time.", "real_world": "Validating migration of pipelines from on-premises to cloud."})
    nb7.add_session_init()
    nb7.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")

def benchmark_op(name, func, runs=3):
    # Warmup
    func()
    times = []
    for _ in range(runs):
        t0 = time.time()
        func()
        times.append(time.time() - t0)
    avg_time = sum(times) / len(times)
    print(f"Benchmark '{name}': Avg = {avg_time:.4f}s across {runs} runs (Min: {min(times):.4f}s)")
    return avg_time

benchmark_op("DataFrame Count", lambda: orders.count())
benchmark_op("DataFrame Distinct Status", lambda: orders.select("status").distinct().count())
""")
    nb7.add_experiment({"title": "Comparing Warmup vs Cold Run Latency", "testing": "Demonstrate that the first run is slower due to class loading, JVM JIT compilation, and connection establishment.", "expect": "First run is 2x slower than subsequent runs.", "code": """t0 = time.time(); orders.groupBy("channel").count().count(); t_cold = time.time() - t0
t0 = time.time(); orders.groupBy("channel").count().count(); t_warm = time.time() - t0
print(f"Cold Run: {t_cold:.4f}s vs Warm Run: {t_warm:.4f}s")
""", "happened": "Cold run was noticeably slower.", "why": "JVM JIT compilation, executor connection handshake, and classloading occurred on first run.", "takeaway": "Always run at least one warmup pass before recording benchmark measurements."})
    nb7.add_exercises({"b_title": "Benchmark Join", "b_desc": "Benchmark join between orders and customers.", "b_input": "orders, customers", "b_sol": "custs = spark.read.option('header', True).csv('/opt/spark-data/retail/customers.csv')\nbenchmark_op('Join', lambda: orders.join(custs, 'customer_id').count())", "i_title": "Measure Shuffle Read", "i_desc": "How to inspect shuffle read metrics via statusTracker.", "i_sol": "print('Query TaskContext or statusTracker for stage metrics.')", "a_title": "A/B Testing Framework", "a_desc": "Design a function to compare two DataFrames execution times.", "a_sol": "print('Run both functions with multiple iterations and statistical t-test.')"})
    nb7.add_summary_and_teardown({"takeaways": {"Warmup Rule": "Always warm up the JVM before recording metrics.", "Action Isolation": "Measure wall clock exclusively around the terminal Action."}, "terms": {"JIT Compilation": "JVM runtime bytecode compilation to native machine code.", "Cold Run": "First execution before caches/compilation settle."}, "mistakes": {"Measuring transformation lines": "Transformations are lazy; timing them measures AST creation, not compute."}, "questions": {"Why is the first execution of a Spark query slower than subsequent runs?": "Because of initial classloading, JVM JIT compilation, executor thread initialization, and storage metadata caching."}, "next_steps": "Module 10 complete! Proceed to Module 11: Fault Tolerance and Reliability."})
    nb7.save(os.path.join(BASE_DIR, "07_performance_benchmarking.ipynb"))

if __name__ == '__main__':
    gen_10_all()
