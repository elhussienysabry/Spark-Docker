#!/usr/bin/env python3
"""
Generates Module 00 and Module 01 notebooks:
- 00_environment_and_spark_setup/00_spark_environment.ipynb
- 01_spark_fundamentals/01_introduction_to_spark.ipynb
- 01_spark_fundamentals/02_spark_architecture.ipynb
- 01_spark_fundamentals/03_spark_session_and_spark_context.ipynb
- 01_spark_fundamentals/04_spark_cluster_execution.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks"

def gen_00_00():
    nb = NotebookBuilder(
        title="Spark 4.2.1 Cluster Environment Verification & Architecture Inspection",
        module_name="00_environment_and_spark_setup",
        notebook_name="00_spark_environment.ipynb",
        app_name="Environment_Verification_Lab"
    )
    nb.add_header({
        "what": "How our 5-container Docker Spark Standalone cluster is organized, how to connect from JupyterLab, and how to verify driver-to-executor communication.",
        "why": "Before executing distributed algorithms, you must verify network reachability, worker registration, event logging, and cluster resource quotas.",
        "problem": "Prevents silent misconfigurations like wrong driver host IP, missing event logging volumes, or inactive worker executors.",
        "real_world": "Every production Spark engineer must validate cluster topology, driver binding, and memory quotas before running production workloads."
    })
    nb.add_markdown("""## 2. Theoretical Architecture of Our Docker Cluster

In our environment, Docker Compose coordinates five synchronized containers:

```
+---------------------------------------------------------------------------------------+
| Docker Host Network (127.0.0.1)                                                       |
|                                                                                       |
|   :8888  JupyterLab (Driver)  --------------------+                                   |
|   :4040  Driver Web UI                            |                                   |
|                                                   v RPC: 7077                         |
|   :8080  Spark Master <==================> Workers:                                   |
|                                            - Worker 1 (Port :8081, 2 cores, 4GB)      |
|                                            - Worker 2 (Port :8082, 2 cores, 4GB)      |
|                                                   |                                   |
|                                                   v Event Logs                        |
|   :18080 Spark History Server <=========== ./events (Shared Host Volume)             |
|                                                                                       |
|   Shared Volumes: ./data -> /opt/spark-data | ./events -> /opt/spark-events          |
+---------------------------------------------------------------------------------------+
```

### Key Service Endpoints:
- **JupyterLab (Driver)**: `http://localhost:8888/?token=spark`
- **Spark Master UI**: `http://localhost:8080` (Worker status, memory, cores)
- **Spark Worker 1 UI**: `http://localhost:8081`
- **Spark Worker 2 UI**: `http://localhost:8082`
- **Spark History Server**: `http://localhost:18080` (Completed job event logs)
- **Driver Web UI**: `http://localhost:4040` (Active while SparkSession is running)
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Practical Environment Inspection

Let us inspect the Python, Java, and Spark cluster runtime versions directly from code.""")
    nb.add_code("""import sys
import platform
import os

print(f"Python Executable: {sys.executable}")
print(f"Python Version:    {sys.version}")
print(f"Platform:          {platform.platform()}")

# Verify SparkContext environment
sc = spark.sparkContext
print(f"Spark App ID:      {sc.applicationId}")
print(f"Spark User:        {sc.sparkUser()}")
print(f"Default Parallelism: {sc.defaultParallelism}")
""")
    nb.add_markdown("""## 4. Internal Investigation: Cluster Cores and Workers

Let us check how many worker cores are registered with our driver by running a distributed RDD operation across all workers.""")
    nb.add_code("""# Run an RDD parallelize across default parallelism to check task execution
rdd = sc.parallelize(range(100), sc.defaultParallelism)
host_distribution = rdd.mapPartitions(lambda it: [os.uname().nodename]).collect()

print("Partition distribution across nodes:")
for idx, host in enumerate(host_distribution):
    print(f"  Partition {idx} ran on container: {host}")
""")
    nb.add_markdown("""## 5. Verifying Available Datasets in `/opt/spark-data`

Our lab includes several realistic datasets mounted from `./data` on the host.""")
    nb.add_code("""import glob

data_files = glob.glob("/opt/spark-data/**", recursive=True)
print("Available datasets mounted in /opt/spark-data:")
for f in sorted(data_files):
    if os.path.isfile(f):
        print(f"  {f} ({os.path.getsize(f):,} bytes)")
""")
    nb.add_experiment({
        "title": "Verifying Event Logging to Persistent Storage",
        "testing": "Confirm that running a Spark job writes an event log to /opt/spark-events so the History Server can render it.",
        "expect": "Event log files will appear in /opt/spark-events, visible to History Server on port 18080.",
        "code": """# Run a sample job to trigger event log write
sample_df = spark.range(0, 10000).filter("id % 2 == 0")
print(f"Sample even count: {sample_df.count()}")

event_files = os.listdir("/opt/spark-events")
print(f"Event logs found in /opt/spark-events: {len(event_files)} entries")
for ef in event_files[:5]:
    print(f"  - {ef}")
""",
        "happened": "A new event log directory was created under /opt/spark-events for this application ID.",
        "why": "Because spark.eventLog.enabled=true and spark.eventLog.dir=file:///opt/spark-events are configured in the builder.",
        "takeaway": "Persistent event logging allows post-mortem diagnostics on port 18080 even after Jupyter kernel restarts."
    })
    nb.add_exercises({
        "b_title": "Check Active Spark Configurations",
        "b_desc": "Write code to retrieve and print spark.sql.shuffle.partitions and spark.driver.host from spark.conf.",
        "b_input": "The active spark session.",
        "b_starter": "# Exercise 1: Print active configs\n",
        "b_sol": """print("spark.sql.shuffle.partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
print("spark.driver.host:", spark.conf.get("spark.driver.host"))""",
        "i_title": "Inspect Executor Hostname Distribution",
        "i_desc": "Create an RDD with 8 partitions and collect the unique container hostnames executing the tasks.",
        "i_input": "sc.parallelize(range(100), 8)",
        "i_starter": "# Exercise 2: Unique executor hostnames\n",
        "i_sol": """hosts = sc.parallelize(range(100), 8).mapPartitions(lambda it: [os.uname().nodename]).distinct().collect()
print("Executors running on:", hosts)""",
        "a_title": "Read and Verify Retail Orders Dataset",
        "a_desc": "Read /opt/spark-data/retail/orders.csv, count total rows, and display schema.",
        "a_input": "/opt/spark-data/retail/orders.csv",
        "a_starter": "# Exercise 3: Read retail orders\n",
        "a_sol": """df_orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
df_orders.printSchema()
print("Total orders count:", df_orders.count())"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Cluster Setup": "Our cluster has 1 Master (7077), 2 Workers (8081, 8082), 1 History Server (18080), and JupyterLab (8888).",
            "Driver Host": "spark.driver.host=jupyter is required so executors in worker containers can route RPC packets back to the driver.",
            "Event Logs": "Event logging persists metrics to /opt/spark-events for History Server post-mortem analysis."
        },
        "terms": {
            "SparkContext": "Low-level connection object to the Spark cluster.",
            "SparkSession": "Unified high-level entry point introduced in Spark 2.0+.",
            "Default Parallelism": "Default partition count for RDD operations (equals total available cores)."
        },
        "mistakes": {
            "Driver set to localhost": "Workers cannot reach localhost because localhost resolves to the worker container itself.",
            "Missing event log volume": "If ./events is not mounted, the History Server cannot view past runs."
        },
        "questions": {
            "Why is spark.driver.host necessary in Docker?": "In containerized networks, containers have individual IPs and hostnames. Setting the driver host tells workers where to send RPC heartbeats.",
            "What port does the active Driver UI run on?": "Port 4040."
        },
        "next_steps": "Open 01_spark_fundamentals/01_introduction_to_spark.ipynb to explore core Spark concepts and primitives."
    })
    nb.save(os.path.join(BASE_DIR, "00_environment_and_spark_setup/00_spark_environment.ipynb"))

def gen_01_01():
    nb = NotebookBuilder(
        title="Introduction to Apache Spark: Philosophy, Ecosystem & Primitives",
        module_name="01_spark_fundamentals",
        notebook_name="01_introduction_to_spark.ipynb",
        app_name="Spark_Introduction_Lab"
    )
    nb.add_header({
        "what": "The foundational concepts of Apache Spark, why it replaced Hadoop MapReduce, and its unified computational engine.",
        "why": "Understanding Spark's design philosophy clarifies why DataFrames are faster than Python scripts and how distributed execution works.",
        "problem": "Traditional tools (Pandas, raw Python, MapReduce) fail to scale beyond single-machine RAM or incur massive disk serialization bottlenecks.",
        "real_world": "Used at Netflix, Uber, Apple, and banks for petabyte-scale ETL, fraud detection, and recommendation pipelines."
    })
    nb.add_markdown("""## 2. The Evolution of Big Data Processing: MapReduce vs Spark

```
Hadoop MapReduce (Disk-Bound Iteration):
[Input] -> [Map] -> [Disk Spill] -> [Network Shuffle] -> [Reduce] -> [Disk Write]
            |                                                           |
            +----------------- Repeat for Next Job <--------------------+
            (Massive Disk I/O Bottleneck at Every Iteration)

Apache Spark (In-Memory DAG Pipeline):
[Input] -> [Transform 1] -> [Transform 2] -> [In-Memory Cache] -> [Action Output]
            \__________________________/
              Fused into single Stage
               (10x - 100x Faster)
```

Spark's core innovations:
1. **In-Memory Computing**: Retains intermediate state in RAM across operations.
2. **DAG (Directed Acyclic Graph)**: Plans multi-step pipelines holistically before running.
3. **Unified Engine**: One unified API for SQL, streaming, machine learning, and graphs.
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Hands-On: Classic Distributed Word Count

Let us run the classic distributed word count on `/opt/spark-data/words.txt`.""")
    nb.add_code("""# Read text file as RDD
lines_rdd = spark.sparkContext.textFile("/opt/spark-data/words.txt")
print(f"Total lines in words.txt: {lines_rdd.count()}")

# Tokenize, map, and reduce
word_counts = (
    lines_rdd
    .flatMap(lambda line: line.lower().split())
    .map(lambda word: (word.strip(".,!?\\\"'"), 1))
    .filter(lambda pair: len(pair[0]) > 0)
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda pair: pair[1], ascending=False)
)

print("Top 10 most frequent words:")
for word, count in word_counts.take(10):
    print(f"  {word:15}: {count}")
""")
    nb.add_markdown("""## 4. The Same Problem in Spark SQL / DataFrame API

Now let us execute the exact same analysis using the DataFrame API and compare readability and optimization.""")
    nb.add_code("""from pyspark.sql import functions as F

words_df = spark.read.text("/opt/spark-data/words.txt")

df_counts = (
    words_df
    .select(F.explode(F.split(F.lower(F.col("value")), "\\\\s+")).alias("raw_word"))
    .select(F.regexp_replace(F.col("raw_word"), "[^a-zA-Z]", "").alias("word"))
    .filter(F.length(F.col("word")) > 0)
    .groupBy("word")
    .count()
    .orderBy(F.desc("count"))
)

print("DataFrame Word Count Output:")
df_counts.show(10)
""")
    nb.add_experiment({
        "title": "RDD vs DataFrame Execution Plan Comparison",
        "testing": "Compare how Spark optimizes DataFrame operations vs low-level RDD transformations.",
        "expect": "DataFrame plan shows Catalyst optimization (WholeStageCodegen, HashAggregate), while RDD plan is opaque.",
        "code": """print("=== DataFrame Physical Plan ===")
df_counts.explain()

print("\\n=== RDD Lineage Debug String ===")
print(word_counts.toDebugString().decode('utf-8'))
""",
        "happened": "DataFrame execution plan shows optimized operators (*HashAggregate, *Project) whereas RDD shows raw stage steps.",
        "why": "Catalyst optimizes DataFrame operations because it understands relations and column semantics.",
        "takeaway": "DataFrames allow Catalyst and Tungsten to optimize execution, whereas RDDs treat user functions as opaque black boxes."
    })
    nb.add_exercises({
        "b_title": "Filter High-Frequency Words",
        "b_desc": "Filter df_counts to keep only words appearing 2 or more times and print the count of distinct words.",
        "b_input": "df_counts DataFrame from earlier.",
        "b_starter": "# Exercise 1: Filter words with count >= 2\n",
        "b_sol": """frequent_words = df_counts.filter(F.col("count") >= 2)
print("Count of frequent words:", frequent_words.count())
frequent_words.show()""",
        "i_title": "Calculate Average Word Length",
        "i_desc": "Using DataFrame API, calculate the average length of all distinct words in words.txt.",
        "i_input": "df_counts DataFrame",
        "i_starter": "# Exercise 2: Average word length\n",
        "i_sol": """avg_len = df_counts.select(F.avg(F.length(F.col("word"))).alias("avg_length")).collect()[0][0]
print(f"Average distinct word length: {avg_len:.2f}")""",
        "a_title": "Monte Carlo Pi Estimation with Parallelize",
        "a_desc": "Estimate Pi using 1,000,000 random points distributed across RDD partitions.",
        "a_input": "Random sampling with math.hypot(x, y) <= 1.0",
        "a_starter": "# Exercise 3: Distributed Pi calculation\n",
        "a_sol": """import random

def sample_point(_):
    x, y = random.random(), random.random()
    return 1 if (x*x + y*y) <= 1.0 else 0

SAMPLES = 500000
count_inside = spark.sparkContext.parallelize(range(SAMPLES), 8).map(sample_point).reduce(lambda a, b: a + b)
pi_estimate = 4.0 * count_inside / SAMPLES
print(f"Estimated Pi: {pi_estimate:.5f} (True Pi: 3.14159)")"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Spark Paradigm": "Spark eliminates disk bottlenecks by pipelining transformations in memory using DAG scheduling.",
            "Unified API": "DataFrames provide a single high-level abstraction optimized by Catalyst across batch, streaming, and SQL.",
            "RDD vs DataFrame": "Prefer DataFrames in production; use RDDs only for custom unstructured data manipulation."
        },
        "terms": {
            "RDD": "Resilient Distributed Dataset (immutable, distributed collection of objects).",
            "DAG": "Directed Acyclic Graph representing the dependency graph of transformations.",
            "Action": "Terminal operation triggering execution across the cluster."
        },
        "mistakes": {
            "Using RDDs for Tabular Data": "Misses Catalyst predicate pushdown, column pruning, and Tungsten code generation.",
            "Imperative Python Loops": "Iterating over DataFrame rows with Python `for` loops bypasses distributed execution."
        },
        "questions": {
            "Why is Spark faster than MapReduce?": "Spark retains intermediate data in memory across stages and fuses narrow transformations into single tasks using DAG pipelining.",
            "What is an RDD?": "An immutable, lazily evaluated, partitioned collection of records that can be recomputed upon node failure via its lineage."
        },
        "next_steps": "Open 01_spark_fundamentals/02_spark_architecture.ipynb to dissect the Driver, Master, Workers, and Executors."
    })
    nb.save(os.path.join(BASE_DIR, "01_spark_fundamentals/01_introduction_to_spark.ipynb"))

def gen_01_02():
    nb = NotebookBuilder(
        title="Deep Dive into Apache Spark Cluster Architecture",
        module_name="01_spark_fundamentals",
        notebook_name="02_spark_architecture.ipynb",
        app_name="Spark_Architecture_Lab"
    )
    nb.add_header({
        "what": "The deep internal architecture of Spark: Driver, Cluster Manager, Worker nodes, and Executors.",
        "why": "To write scalable distributed code, you must understand where your code runs: on the driver thread or distributed across executor threads.",
        "problem": "Developers often write driver-side bottlenecks (e.g. collecting massive datasets, serializing non-serializable objects) without realizing it.",
        "real_world": "Diagnosing production cluster crashes, OutOfMemory errors, and cluster sizing require architectural mastery."
    })
    nb.add_markdown("""## 2. Detailed Architecture Walkthrough

```
+-----------------------------------------------------------------------------------+
| DRIVER (Jupyter Container)                                                        |
|  - SparkContext & SparkSession                                                    |
|  - DAGScheduler (Creates Stages)                                                  |
|  - TaskScheduler (Dispatches Tasks)                                               |
|  - Web UI (Port 4040)                                                             |
+-------------------------+---------------------------------------------------------+
                          |
                          v RPC (spark://spark-master:7077)
+-----------------------------------------------------------------------------------+
| CLUSTER MANAGER (Standalone Master)                                               |
|  - Tracks Worker capacity (Cores & RAM)                                           |
|  - Allocates Executor slots to applications                                       |
+-------------------------+---------------------------------------------------------+
                          | Launches Executors
             +------------+------------+
             |                         |
             v                         v
+------------------------+  +------------------------+
| WORKER 1 (Port 8081)   |  | WORKER 2 (Port 8082)   |
|  Worker Daemon         |  |  Worker Daemon         |
|  +------------------+  |  |  +------------------+  |
|  | Executor JVM     |  |  |  | Executor JVM     |  |
|  | - 2 Cores        |  |  |  | - 2 Cores        |  |
|  | - 4 GB RAM       |  |  |  | - 4 GB RAM       |  |
|  | - BlockManager   |  |  |  | - BlockManager   |  |
|  | - Task Threads   |  |  |  | - Task Threads   |  |
|  +------------------+  |  |  +------------------+  |
+------------------------+  +------------------------+
```

### Driver vs Executor Code Execution:
- Code written outside transformations executes **once on the Driver**.
- Code inside `map`, `filter`, `foreachPartition` executes **concurrently inside Executor JVM task threads**.
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Investigating the Driver & SparkContext Configuration

Let us inspect the live properties configured for this cluster application.""")
    nb.add_code("""conf = spark.sparkContext.getConf()
print("=== Key Cluster Configurations ===")
for key in [
    "spark.master",
    "spark.app.name",
    "spark.driver.host",
    "spark.driver.port",
    "spark.executor.memory",
    "spark.sql.shuffle.partitions",
    "spark.eventLog.enabled"
]:
    print(f"  {key:30}: {conf.get(key, 'NOT SET')}")
""")
    nb.add_markdown("""## 4. Tracing Driver vs Executor Execution Contexts

Watch what prints on the Driver vs what prints in Executor worker logs!""")
    nb.add_code("""import os

print(f" DRIVER is running on container: {os.uname().nodename}")

def log_worker_info(partition_index, iterator):
    worker_host = os.uname().nodename
    yield f"Task for partition {partition_index} executed on executor container: {worker_host}"

results = (
    spark.sparkContext
    .parallelize(range(10), numSlices=4)
    .mapPartitionsWithIndex(log_worker_info)
    .collect()
)

print("\\n EXECUTOR execution locations:")
for r in results:
    print(f"  - {r}")
""")
    nb.add_experiment({
        "title": "Non-Serializable Object Driver-to-Executor Passing",
        "testing": "Observe what happens when passing an un-serializable Python object into an executor task.",
        "expect": "Spark raises a PickleError / Py4J serialization error because task closures must be serialized over the network.",
        "code": """import socket

# socket objects cannot be pickled or serialized across network
sock = socket.socket()

try:
    spark.sparkContext.parallelize([1, 2, 3]).map(lambda x: (x, str(sock))).collect()
    print("Execution succeeded.")
except Exception as e:
    print(f"Caught expected serialization error: {type(e).__name__}")
finally:
    sock.close()
""",
        "happened": "Spark threw an exception during task closure serialization.",
        "why": "Every variable referenced inside a transformation lambda must be serialized via pickle to executor processes.",
        "takeaway": "Instantiate connection objects (DB connections, sockets, files) INSIDE `mapPartitions` on the executor, never on the driver."
    })
    nb.add_exercises({
        "b_title": "Inspect Active Application ID and Web UI URL",
        "b_desc": "Retrieve the applicationId and webUrl from sparkContext and print them.",
        "b_input": "spark.sparkContext",
        "b_starter": "# Exercise 1: App ID and UI URL\n",
        "b_sol": """print("Application ID:", spark.sparkContext.applicationId)
print("Driver Web UI:  ", spark.sparkContext.uiWebUrl)""",
        "i_title": "Count Cores Used Across Worker Nodes",
        "i_desc": "Run a job with 8 partitions and verify how many unique container hostnames contributed.",
        "i_input": "sc.parallelize(range(80), 8)",
        "i_starter": "# Exercise 2: Count active worker nodes\n",
        "i_sol": """nodes = spark.sparkContext.parallelize(range(80), 8).map(lambda _: os.uname().nodename).distinct().collect()
print(f"Total distinct nodes executing work: {len(nodes)} -> {nodes}")""",
        "a_title": "Demonstrate mapPartitions DB Connection Pattern",
        "a_desc": "Use mapPartitions to simulate opening a database connection once per partition rather than once per row.",
        "a_input": "RDD of 100 elements across 4 partitions",
        "a_starter": "# Exercise 3: mapPartitions connection pattern\n",
        "a_sol": """def process_partition_with_mock_conn(iterator):
    # Opened once per partition on the executor JVM/Python worker
    conn = f"MockDBConn_{os.uname().nodename}"
    processed = []
    for item in iterator:
        processed.append(f"{item}_saved_via_{conn}")
    return iter(processed)

res = spark.sparkContext.parallelize(range(8), 2).mapPartitions(process_partition_with_mock_conn).collect()
print("Sample results from mapPartitions:", res[:4])"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Driver Role": "Coordinates DAG, breaks jobs into stages, dispatches tasks, hosts Web UI.",
            "Executor Role": "Executes task threads, caches data blocks, serves shuffle data.",
            "Serialization": "All code referenced in transformations must be serializable."
        },
        "terms": {
            "Driver": "The orchestrating JVM process running the Spark application.",
            "Executor": "Worker JVM process running task slots.",
            "Closure": "Variables and functions packaged to be sent over network to executors."
        },
        "mistakes": {
            "Initializing heavy resources on Driver": "Opening DB connections on driver and passing to lambdas fails with serialization errors.",
            "Calling collect() on large outputs": "Causes Driver OOM."
        },
        "questions": {
            "What is the difference between a Worker and an Executor?": "A Worker is a long-lived machine daemon; an Executor is an application-specific JVM spawned by the worker.",
            "Where does the Spark UI run?": "Embedded inside the Driver process on port 4040."
        },
        "next_steps": "Open 01_spark_fundamentals/03_spark_session_and_spark_context.ipynb to master session management."
    })
    nb.save(os.path.join(BASE_DIR, "01_spark_fundamentals/02_spark_architecture.ipynb"))

def gen_01_03():
    nb = NotebookBuilder(
        title="SparkSession, SparkContext, and Configuration Management",
        module_name="01_spark_fundamentals",
        notebook_name="03_spark_session_and_spark_context.ipynb",
        app_name="SparkSession_DeepDive"
    )
    nb.add_header({
        "what": "How SparkSession acts as the unified gateway, how it encapsulates SparkContext, SQLContext, and HiveContext, and how to tune runtime configurations.",
        "why": "In modern Spark, SparkSession is your primary interface. Mastering builder patterns, catalogs, and dynamic configuration is required for production engineering.",
        "problem": "Prior to Spark 2.0, developers juggled multiple contexts (SQLContext, HiveContext, StreamingContext, SparkContext) causing confusion.",
        "real_world": "Setting up multi-tenant applications, isolated session views, and dynamic partition overwrite parameters."
    })
    nb.add_markdown("""## 2. SparkSession Architecture

```
+---------------------------------------------------------------+
|                        SparkSession                           |
|  - Unified entry point (Spark 2.0+)                           |
|  - Builder pattern: SparkSession.builder.getOrCreate()        |
+-------------------------------+-------------------------------+
                                |
        +-----------------------+-----------------------+
        |                       |                       |
        v                       v                       v
+---------------+       +---------------+       +---------------+
|  SparkContext |       |  Catalog API  |       | Conf Manager  |
|  - Low-level  |       |  - Databases  |       | - spark.conf  |
|  - RDD engine |       |  - Tables     |       | - Runtime     |
|  - Hardware   |       |  - Functions  |       |   parameters  |
+---------------+       +---------------+       +---------------+
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Exploring the Catalog API

The Catalog allows programmatically inspecting registered databases, tables, views, and built-in functions.""")
    nb.add_code("""# Create a temporary view from sales.csv
sales_df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/sales.csv")
sales_df.createOrReplaceTempView("sales_view")

# Query the Catalog
tables = spark.catalog.listTables()
print("Registered Tables and Views in Catalog:")
for t in tables:
    print(f"  Name: {t.name:20} IsTemporary: {t.isTemporary}")

# List available built-in mathematical/string functions in Catalog
funcs = spark.catalog.listFunctions()
print(f"\\nTotal built-in functions registered in Catalog: {len(funcs)}")
print("Sample functions:", [f.name for f in funcs[:8]])
""")
    nb.add_markdown("""## 4. Dynamic Runtime Configuration via `spark.conf`

You can inspect and modify runtime configurations dynamically without restarting the cluster.""")
    nb.add_code("""# Read current shuffle partitions
current_shuffle = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Current shuffle partitions: {current_shuffle}")

# Dynamically change shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "16")
print(f"Updated shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Reset back to lab default
spark.conf.set("spark.sql.shuffle.partitions", "8")
""")
    nb.add_experiment({
        "title": "Isolated Cloned Sessions with `newSession()`",
        "testing": "Demonstrate that spark.newSession() creates an isolated session sharing the SparkContext but with independent SQL views and configs.",
        "expect": "Temporary views in session A are NOT visible in session B unless registered globally.",
        "code": """# Session A creates a local temp view
sales_df.createOrReplaceTempView("view_session_a")

# Create Session B
session_b = spark.newSession()

print("Checking 'view_session_a' from Session A:")
print("  Found in Session A:", spark.catalog.tableExists("view_session_a"))

print("Checking 'view_session_a' from Session B:")
print("  Found in Session B:", session_b.catalog.tableExists("view_session_a"))
""",
        "happened": "view_session_a existed in Session A, but tableExists returned False in Session B.",
        "why": "Temporary views are scoped to individual SparkSession instances.",
        "takeaway": "Use newSession() for multi-tenant applications or to prevent temporary view naming collisions."
    })
    nb.add_exercises({
        "b_title": "Check if a Table Exists in Catalog",
        "b_desc": "Write code using spark.catalog.tableExists() to check if 'orders_test' exists.",
        "b_input": "spark.catalog",
        "b_starter": "# Exercise 1: Check table existence\n",
        "b_sol": """exists = spark.catalog.tableExists("orders_test")
print("Does 'orders_test' exist?:", exists)""",
        "i_title": "Drop a Temporary View Safely",
        "i_desc": "Register a temp view 'dummy_view', verify it exists, then drop it using spark.catalog.dropTempView().",
        "i_input": "spark.catalog.dropTempView",
        "i_starter": "# Exercise 2: Drop temporary view\n",
        "i_sol": """spark.range(5).createOrReplaceTempView("dummy_view")
print("Exists before drop:", spark.catalog.tableExists("dummy_view"))
spark.catalog.dropTempView("dummy_view")
print("Exists after drop: ", spark.catalog.tableExists("dummy_view"))""",
        "a_title": "Global Temporary Views Across Sessions",
        "a_desc": "Register a Global Temporary View and verify that it IS accessible from session_b.",
        "a_input": "createGlobalTempView('global_sales')",
        "a_starter": "# Exercise 3: Global Temp Views\n",
        "a_sol": """sales_df.createOrReplaceGlobalTempView("global_sales")
session_b = spark.newSession()
print("Can Session B query global_temp.global_sales?:")
session_b.sql("SELECT COUNT(*) FROM global_temp.global_sales").show()"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "SparkSession Purpose": "Unified interface managing Catalog, SQL, and underlying SparkContext.",
            "Dynamic Configs": "spark.conf allows inspecting and overriding runtime SQL parameters.",
            "Session Scoping": "Temporary views are private to a SparkSession; Global views reside in global_temp database."
        },
        "terms": {
            "SparkSession": "Unified client entry point.",
            "Catalog": "Internal metastore interface for tables, databases, and functions.",
            "newSession()": "Creates an isolated session sharing the cluster connection."
        },
        "mistakes": {
            "Creating Multiple SparkContexts": "A JVM process can only host ONE active SparkContext at a time.",
            "Assuming Temp Views are Global": "Temp views disappear across different sessions unless made global."
        },
        "questions": {
            "What is the difference between SparkSession and SparkContext?": "SparkContext is the low-level cluster connection for RDDs; SparkSession wraps SparkContext and adds SQL, DataFrames, and Catalogs.",
            "How do you share views across multiple SparkSessions?": "Use createOrReplaceGlobalTempView() and query via global_temp.view_name."
        },
        "next_steps": "Open 01_spark_fundamentals/04_spark_cluster_execution.ipynb to trace execution from action to tasks."
    })
    nb.save(os.path.join(BASE_DIR, "01_spark_fundamentals/03_spark_session_and_spark_context.ipynb"))

def gen_01_04():
    nb = NotebookBuilder(
        title="End-to-End Cluster Execution Flow: From Code to Tasks",
        module_name="01_spark_fundamentals",
        notebook_name="04_spark_cluster_execution.ipynb",
        app_name="Execution_Flow_Lab"
    )
    nb.add_header({
        "what": "The step-by-step lifecycle of an Apache Spark job: from code declaration to DAG construction, stage splitting, task scheduling, and result return.",
        "why": "To diagnose bottlenecks, you must be able to trace exactly where a query is spending time: DAGScheduler, TaskScheduler, or network shuffle.",
        "problem": "Without understanding execution hierarchy (Action -> Job -> Stage -> Task), developers cannot decipher the Spark UI.",
        "real_world": "Explaining execution flow and debugging stuck stages are top-tier Senior Data Engineering interview topics."
    })
    nb.add_markdown("""## 2. Complete Execution Hierarchy

```
[User Action: df.count()]
        |
        v
+-----------------------------------------------------------+
| JOB (1 per Action)                                        |
|  Identified by Job ID (e.g. Job 0, Job 1)                 |
+-----------------------------+-----------------------------+
                              |
                              v DAGScheduler splits at Shuffles
+-----------------------------------------------------------+
| STAGES (Bounded by Wide Dependencies)                     |
|  - Stage 0: ShuffleMapStage (reads, filters, map output)  |
|  - Stage 1: ResultStage (fetches shuffle, aggregates)     |
+-----------------------------+-----------------------------+
                              |
                              v TaskScheduler assigns to Cores
+-----------------------------------------------------------+
| TASKS (1 per Partition in Stage)                          |
|  - Task 0 -> Core 1 on Worker 1                           |
|  - Task 1 -> Core 2 on Worker 1                           |
|  - Task 2 -> Core 1 on Worker 2                           |
|  - Task 3 -> Core 2 on Worker 2                           |
+-----------------------------------------------------------+
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Practical: Generating a Multi-Stage Job

Let us run an operation with an explicit wide dependency (shuffle) and trace its job and stage breakdown.""")
    nb.add_code("""orders_df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")

# A 2-stage execution: Filter (narrow) + GroupBy (wide)
agg_df = (
    orders_df
    .filter("status = 'Completed'")
    .groupBy("channel")
    .agg(
        F.count("order_id").alias("order_count"),
        F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("total_revenue")
    )
)

print("=== Execution Plan ===")
agg_df.explain("formatted")
""")
    nb.add_markdown("""## 4. Triggering the Action and Checking the Spark UI

Now we trigger the Action (`show()`) and inspect the generated Job in the Spark UI (`http://localhost:4040`).""")
    nb.add_code("""start_time = time.time()
agg_df.show()
elapsed = time.time() - start_time
print(f"Action executed in: {elapsed:.3f} seconds")
print("Check http://localhost:4040/jobs/ to inspect Job, Stages, and Tasks metrics!")
""")
    nb.add_experiment({
        "title": "Predicting and Verifying Stage Count",
        "testing": "Verify that a query with 2 wide transformations (groupBy + repartition) produces exactly 3 stages.",
        "expect": "The DAG will have 3 stages separated by 2 shuffle exchange boundaries.",
        "code": """# Query with 2 shuffles: groupBy followed by repartition
multi_shuffle = (
    orders_df
    .groupBy("customer_id")
    .count()
    .repartition(4)
)

# Observe Exchange nodes in plan
multi_shuffle.explain()
print(f"Result count: {multi_shuffle.count()}")
""",
        "happened": "Spark UI shows 3 stages for this job (Scan & partial group, shuffle merge & recount, repartition exchange).",
        "why": "Each wide dependency (Exchange node) creates a new Stage boundary.",
        "takeaway": "Counting Exchange nodes in explain() allows you to predict the exact number of stages in the Spark UI."
    })
    nb.add_exercises({
        "b_title": "Identify Narrow Transformation Chain",
        "b_desc": "Write a pipeline chaining filter, withColumn, and select. How many stages will count() produce?",
        "b_input": "orders_df",
        "b_starter": "# Exercise 1: Count stages for narrow pipeline\n",
        "b_sol": """narrow_df = orders_df.filter("quantity > 1").withColumn("tax", F.col("unit_price")*0.1).select("order_id", "tax")
# Has ZERO Exchange nodes -> exactly 1 Stage!
narrow_df.explain()
print("Count:", narrow_df.count())""",
        "i_title": "Trace Task Count from Partition Count",
        "i_desc": "Set shuffle partitions to 5, execute a groupBy count, and check the task count in the final stage.",
        "i_input": "spark.conf.set('spark.sql.shuffle.partitions', '5')",
        "i_starter": "# Exercise 2: Tune and verify task count\n",
        "i_sol": """spark.conf.set("spark.sql.shuffle.partitions", "5")
res = orders_df.groupBy("status").count()
print("Number of partitions in aggregated DF:", res.rdd.getNumPartitions())
res.show()
spark.conf.set("spark.sql.shuffle.partitions", "8")""",
        "a_title": "End-to-End Latency Breakdown",
        "a_desc": "Measure wall-clock latency of reading 60k orders, performing aggregation, and writing to Parquet.",
        "a_input": "retail/orders.csv",
        "a_starter": "# Exercise 3: Full pipeline timing\n",
        "a_sol": """out_path = "/opt/spark-data/warehouse/test_perf_out"
t0 = time.time()
(
    orders_df
    .filter("status = 'Completed'")
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_qty"))
    .coalesce(1)
    .write.mode("overwrite").parquet(out_path)
)
print(f"Full pipeline executed and written to Parquet in {time.time()-t0:.2f}s")"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Hierarchy": "Action triggers Job -> Job splits into Stages at shuffles -> Stages split into Tasks per partition.",
            "Pipelining": "Narrow operations execute together in a single stage without intermediate disk or network hops.",
            "Task Execution": "Each task runs on exactly one partition on an executor thread."
        },
        "terms": {
            "Job": "Execution triggered by one Action.",
            "Stage": "Pipelined set of tasks separated by shuffle dependencies.",
            "Task": "Individual thread unit processing a single partition."
        },
        "mistakes": {
            "Expecting Stages for Every Operation": "Only wide transformations (shuffles) create stage boundaries; narrow operations are fused.",
            "Ignoring Task Count": "Too few tasks leaves cores idle; too many tasks floods scheduler."
        },
        "questions": {
            "What determines the number of tasks in a stage?": "The number of partitions in the RDD/DataFrame entering that stage.",
            "What causes a new stage to be created?": "A shuffle dependency (wide transformation) such as groupBy, join, or repartition."
        },
        "next_steps": "Module 01 complete! Proceed to Module 02: Resilient Distributed Datasets (RDDs)."
    })
    nb.save(os.path.join(BASE_DIR, "01_spark_fundamentals/04_spark_cluster_execution.ipynb"))

if __name__ == '__main__':
    gen_00_00()
    gen_01_01()
    gen_01_02()
    gen_01_03()
    gen_01_04()
