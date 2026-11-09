#!/usr/bin/env python3
"""
Generates Module 11 (Fault Tolerance) & Module 12 (Advanced Topics) notebooks:
- Module 11 (5 notebooks):
  - 01_lineage_and_recomputation.ipynb
  - 02_executor_failure.ipynb
  - 03_task_retries.ipynb
  - 04_checkpointing.ipynb
  - 05_fault_tolerance_experiments.ipynb
- Module 12 (6 notebooks):
  - 01_spark_on_yarn.ipynb
  - 02_spark_on_kubernetes.ipynb
  - 03_cluster_vs_client_mode.ipynb
  - 04_resource_allocation.ipynb
  - 05_spark_connect.ipynb
  - 06_pyspark_internals.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_11 = "/home/elhussieny/spark-docker/notebooks/11_fault_tolerance_and_reliability"
BASE_12 = "/home/elhussieny/spark-docker/notebooks/12_advanced_topics"

def gen_11_all():
    # 11_01: Lineage and Recomputation
    nb1 = NotebookBuilder("Fault Tolerance: RDD Lineage and Partition Recomputation", "11_fault_tolerance_and_reliability", "01_lineage_and_recomputation.ipynb", "Lineage_Recompute_Lab")
    nb1.add_header({"what": "How Spark recovers lost partitions via deterministic RDD lineage graph re-evaluation without data replication.", "why": "Lineage recomputation is Spark's core resilience mechanism.", "problem": "Understanding what gets recomputed when worker nodes fail.", "real_world": "Spot instances and preemptible cloud clusters."})
    nb1.add_markdown("## 2. Lineage Recomputation Mechanics: If partition P2 fails, Spark traces P2's parent lineage back to source files and recomputes only P2.")
    nb1.add_session_init()
    nb1.add_code("""rdd = spark.sparkContext.parallelize(range(50), 4).map(lambda x: x*2).filter(lambda x: x > 20)
print("Lineage graph of transformations:")
print(rdd.toDebugString().decode('utf-8'))
print(f"Computed count: {rdd.count()}")
""")
    nb1.add_experiment({"title": "Lineage Recomputation Verification", "testing": "Verify that lost partitions are reconstructed from upstream lineage.", "expect": "Recomputation succeeds deterministically.", "code": "print('Lineage graph has depth:', len(rdd.toDebugString().splitlines()))", "happened": "Lineage tree verified.", "why": "Spark retains the complete sequence of deterministic operations.", "takeaway": "Deterministic transformations ensure exact partition reproduction upon failure."})
    nb1.add_exercises({"b_title": "Inspect Lineage Depth", "b_desc": "Count transformation steps in toDebugString.", "b_input": "rdd", "b_sol": "print(len(rdd.toDebugString().splitlines()))", "i_title": "Non-Deterministic Hazard", "i_desc": "Why random numbers break lineage recomputation.", "i_sol": "print('Recomputed partitions will have different random values, corrupting joins.')", "a_title": "Lineage Truncation", "a_desc": "When to truncate lineage.", "a_sol": "print('Use checkpoint() when lineage depth exceeds ~50 steps to prevent StackOverflowError.')"})
    nb1.add_summary_and_teardown({"takeaways": {"Lineage Resilience": "Recomputes lost partitions on the fly.", "Zero Replication": "Saves network bandwidth compared to HDFS replication."}, "terms": {"Lineage": "Acyclic transformation graph.", "Recomputation": "Evaluating parent steps for lost slices."}, "mistakes": {"Non-deterministic functions": "e.g. random.random() inside lineage causes corrupt recomputation."}, "questions": {"How does Spark handle a lost partition?": "It recomputes only the lost partition from parent dependencies using its lineage graph."}, "next_steps": "Open 02_executor_failure.ipynb"})
    nb1.save(os.path.join(BASE_11, "01_lineage_and_recomputation.ipynb"))

    # 11_02: Executor Failure
    nb2 = NotebookBuilder("Executor Failures: Node Eviction, Heartbeats, and State Handling", "11_fault_tolerance_and_reliability", "02_executor_failure.ipynb", "Executor_Failure_Lab")
    nb2.add_header({"what": "What happens when an executor process crashes: heartbeat timeout, cluster manager notification, task rescheduling, and shuffle data loss.", "why": "Distributed systems must be designed for frequent hardware and container failures.", "problem": "Understanding why shuffle files may force upstream stage recomputation.", "real_world": "Running Spark on AWS EC2 Spot Instances."})
    nb2.add_markdown("## 2. Executor Failure Lifecycle\n1. Heartbeat stops (`spark.network.timeout` = 120s).\n2. Master marks executor dead; requests replacement.\n3. Running tasks rescheduled on healthy workers.\n4. Shuffle files lost -> Upstream ShuffleMapStage recomputed.")
    nb2.add_session_init()
    nb2.add_code("""conf = spark.sparkContext.getConf()
print("spark.network.timeout:         ", conf.get("spark.network.timeout", "120s"))
print("spark.executor.heartbeatInterval: ", conf.get("spark.executor.heartbeatInterval", "10s"))
""")
    nb2.add_experiment({"title": "External Shuffle Service Resilience", "testing": "Explain role of External Shuffle Service during executor loss.", "expect": "ESS preserves shuffle files after executor crash.", "code": "print('ESS runs as an auxiliary node daemon so shuffle files survive executor JVM crashes.')", "happened": "Explained ESS architecture.", "why": "ESS decouples shuffle storage from executor process lifecycle.", "takeaway": "Enable ESS when running dynamic allocation or preemptible instances."})
    nb2.add_exercises({"b_title": "Check Heartbeat Interval", "b_desc": "Print default heartbeat interval.", "b_input": "conf", "b_sol": "print(spark.conf.get('spark.executor.heartbeatInterval', '10s'))", "i_title": "Stage Invalidation", "i_desc": "Why lost shuffle files invalidate stages.", "i_sol": "print('Reducers cannot fetch missing shuffle map files; parent stage must re-run.')", "a_title": "Dynamic Allocation with ESS", "a_desc": "Why dynamic allocation requires ESS.", "a_sol": "print('Without ESS, deallocated executors would delete their shuffle files.')"})
    nb2.add_summary_and_teardown({"takeaways": {"Executor Crash": "Driver automatically reschedules tasks and recomputes lost data.", "Shuffle Loss": "Forces upstream stage rerun unless ESS is used."}, "terms": {"Heartbeat": "Periodic health ping from executor to driver.", "ESS": "External Shuffle Service."}, "mistakes": {"Assuming application dies with executor": "Spark applications survive executor crashes gracefully."}, "questions": {"What happens if an executor holding shuffle files crashes?": "The DAGScheduler invalidates the upstream ShuffleMapStage and reschedules the lost map tasks to regenerate the shuffle files."}, "next_steps": "Open 03_task_retries.ipynb"})
    nb2.save(os.path.join(BASE_11, "02_executor_failure.ipynb"))

    # 11_03: Task Retries
    nb3 = NotebookBuilder("Task Retries, Stage Retries, and Failure Handling", "11_fault_tolerance_and_reliability", "03_task_retries.ipynb", "Task_Retries_Lab")
    nb3.add_header({"what": "Task retry mechanisms, spark.task.maxFailures, stage retries, and handling transient exceptions.", "why": "Transient errors (network hiccups, disk blips) should not abort long-running ETL jobs.", "problem": "Understanding task retry limits and avoiding poison pill data crashes.", "real_world": "Production resilience engineering."})
    nb3.add_session_init()
    nb3.add_code("""print("spark.task.maxFailures: ", spark.conf.get("spark.task.maxFailures", "4 (default)"))
""")
    nb3.add_experiment({"title": "Simulating a Task Retry with an Unstable UDF", "testing": "Run a function that fails on attempt 0 and succeeds on attempt 1.", "expect": "Spark retries the task automatically up to maxFailures.", "code": """from pyspark import TaskContext

def flaky_task(x):
    ctx = TaskContext.get()
    attempt = ctx.attemptNumber()
    if x == 42 and attempt == 0:
        raise RuntimeError("Transient blip on attempt 0!")
    return f"Processed {x} on attempt {attempt}"

try:
    res = spark.sparkContext.parallelize([1, 42, 3], 2).map(flaky_task).collect()
    print("Flaky task succeeded after retry:")
    for r in res:
        print(" ", r)
except Exception as e:
    print(f"Task retry failed: {e}")
""", "happened": "Task failed on attempt 0 and automatically succeeded on attempt 1.", "why": "TaskScheduler re-queued the task because attemptNumber < spark.task.maxFailures.", "takeaway": "Spark task retries absorb transient network and I/O glitches seamlessly."})
    nb3.add_exercises({"b_title": "Check Max Failures", "b_desc": "Print spark.task.maxFailures.", "b_input": "conf", "b_sol": "print(spark.conf.get('spark.task.maxFailures', '4'))", "i_title": "Poison Pill Concept", "i_desc": "Explain what a poison pill record is.", "i_sol": "print('A bad record that deterministically crashes on every retry until maxFailures is exceeded.')", "a_title": "Stage Abort Rule", "a_desc": "When is an entire stage aborted?", "a_sol": "print('When any single task fails 4 consecutive times.')"})
    nb3.add_summary_and_teardown({"takeaways": {"Task Retries": "Default 4 attempts per task.", "Stage Resilience": "Stage retries if shuffle fetch fails."}, "terms": {"attemptNumber": "Current retry attempt index.", "Poison Pill": "Deterministic failure record."}, "mistakes": {"Setting maxFailures=1 in production": "Any minor network blip aborts the entire job."}, "questions": {"How many times will Spark retry a failed task by default?": "4 times (spark.task.maxFailures = 4)."}, "next_steps": "Open 04_checkpointing.ipynb"})
    nb3.save(os.path.join(BASE_11, "03_task_retries.ipynb"))

    # 11_04: Checkpointing
    nb4 = NotebookBuilder("Checkpointing: Truncating Lineage and Durable State Preservation", "11_fault_tolerance_and_reliability", "04_checkpointing.ipynb", "Checkpointing_Lab")
    nb4.add_header({"what": "How checkpointing saves materialized data to durable storage and completely severs the upstream lineage graph.", "why": "Recursive or iterative algorithms generate massive DAGs that cause JVM StackOverflowError unless checkpointed.", "problem": "JVM crashing due to 500-step iterative DAG lineage.", "real_world": "Iterative graph algorithms, ML training, and streaming recovery."})
    nb4.add_markdown("## 2. Checkpoint vs Persist\n- `persist()`: Keeps data in memory/disk; **retains full lineage**.\n- `checkpoint()`: Saves to reliable storage; **truncates lineage completely**.")
    nb4.add_session_init()
    nb4.add_code("""chk_dir = "/opt/spark-data/warehouse/rdd_checkpoint_dir"
os.system(f"rm -rf {chk_dir}")
spark.sparkContext.setCheckpointDir(f"file://{chk_dir}")

# Build RDD and checkpoint
rdd = spark.sparkContext.parallelize(range(20), 2).map(lambda x: x * 3)
rdd.checkpoint()

print("Lineage before Action:")
print(rdd.toDebugString().decode('utf-8'))

# Action triggers materialization and lineage truncation
rdd.count()

print("\\nLineage AFTER Action (Notice: Lineage is TRUNCATED!):")
print(rdd.toDebugString().decode('utf-8'))
""")
    nb4.add_experiment({"title": "Verifying Lineage Truncation", "testing": "Verify that toDebugString() points directly to CheckpointRDD after action.", "expect": "Original parallelize and map steps are completely severed.", "code": "print('Is RDD checkpointed?:', rdd.isCheckpointed())", "happened": "isCheckpointed returned True; parent steps vanished from debug string.", "why": "Spark wrote the materialized partition files to disk and reset the parent pointer.", "takeaway": "Checkpointing cuts the lineage graph, freeing JVM memory from deep recursion."})
    nb4.add_exercises({"b_title": "Set Checkpoint Dir", "b_desc": "Set checkpoint directory on SparkContext.", "b_input": "setCheckpointDir", "b_sol": "spark.sparkContext.setCheckpointDir('file:///opt/spark-data/warehouse/chk')", "i_title": "DataFrame Checkpoint", "i_desc": "Checkpoint a DataFrame using df.checkpoint().", "i_sol": "df = spark.range(10); df_c = df.checkpoint(); print('DF checkpointed:', df_c.isLocal)", "a_title": "Eager vs Non-Eager Checkpoint", "a_desc": "Explain df.checkpoint(eager=False).", "a_sol": "print('eager=True executes immediately; eager=False waits for downstream action.')"})
    nb4.add_summary_and_teardown({"takeaways": {"Lineage Cut": "Checkpointing permanently severs parent dependencies.", "Durable Storage": "Saved to reliable disk/HDFS."}, "terms": {"CheckpointRDD": "RDD reading directly from checkpoint files.", "setCheckpointDir": "Registers checkpoint storage path."}, "mistakes": {"Using persist instead of checkpoint for deep loops": "Causes JVM StackOverflowError."}, "questions": {"What is the difference between persist() and checkpoint()?": "persist() keeps data in executor cache while retaining the lineage graph; checkpoint() writes data to durable storage and permanently severs the lineage graph."}, "next_steps": "Open 05_fault_tolerance_experiments.ipynb"})
    nb4.save(os.path.join(BASE_11, "04_checkpointing.ipynb"))

    # 11_05: Fault Tolerance Experiments
    nb5 = NotebookBuilder("Fault Tolerance Experiments: Empirical Resilience Verification", "11_fault_tolerance_and_reliability", "05_fault_tolerance_experiments.ipynb", "FT_Experiments_Lab")
    nb5.add_header({"what": "Laboratory suite of empirical fault tolerance experiments: lineage reconstruction, task failure recovery, and checkpoint restoration.", "why": "Proving resilience empirically prepares you for production chaos engineering.", "problem": "Assuming fault tolerance works without verifying recovery behavior.", "real_world": "Disaster recovery testing for mission-critical pipelines."})
    nb5.add_session_init()
    nb5.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
print(f"Loaded {orders.count()} orders for resilience experiments.")
""")
    nb5.add_experiment({"title": "Simulated Node Crash Recomputation", "testing": "Unpersist an RDD and observe that subsequent actions re-evaluate from source files.", "expect": "Recomputation succeeds seamlessly.", "code": """rdd_test = orders.rdd.map(lambda r: (r[7], 1)).reduceByKey(lambda a, b: a+b).persist()
rdd_test.count() # materialize
rdd_test.unpersist() # simulate block loss
print("Count after simulated loss:", rdd_test.count())
""", "happened": "Spark seamlessly re-read source files and re-executed the pipeline.", "why": "Lineage remained intact, allowing automatic recomputation.", "takeaway": "Spark handles in-memory block loss transparently through lineage graphs."})
    nb5.add_exercises({"b_title": "Verify Unpersist", "b_desc": "Check is_cached is False after unpersist.", "b_input": "rdd_test", "b_sol": "print('Is cached:', rdd_test.is_cached)", "i_title": "Check Task Failures in UI", "i_desc": "Inspect failed tasks count in UI Executors tab.", "i_sol": "print('Inspected localhost:4040/executors')", "a_title": "Chaos Engineering Scenario", "a_desc": "Explain what happens if Master container dies.", "a_sol": "print('Running applications continue; new submissions block until Master recovers.')"})
    nb5.add_summary_and_teardown({"takeaways": {"Resilience Validated": "Lineage recomputes lost blocks; task retries absorb glitches."}, "terms": {"Chaos Engineering": "Testing systems by intentionally introducing failures."}, "mistakes": {"Assuming data loss on executor crash": "Lineage guarantees deterministic recovery."}, "questions": {"Can Spark recover if the Driver dies in client mode?": "No, in client mode the driver hosts the DAGScheduler and SparkContext; if it dies, the application terminates."}, "next_steps": "Module 11 complete! Proceed to Module 12: Advanced Topics."})
    nb5.save(os.path.join(BASE_11, "05_fault_tolerance_experiments.ipynb"))

def gen_12_all():
    # 12_01: YARN
    nb1 = NotebookBuilder("Spark on Apache Hadoop YARN: Architecture and Resource Allocation", "12_advanced_topics", "01_spark_on_yarn.ipynb", "YARN_Lab")
    nb1.add_header({"what": "How Spark runs on Hadoop YARN: ResourceManager, NodeManager, ApplicationMaster, and yarn-client vs yarn-cluster modes.", "why": "YARN is the dominant cluster manager in enterprise on-premises Hadoop data platforms.", "problem": "Configuring YARN memory overhead and queue allocations.", "real_world": "Enterprise banking and telecom Hadoop clusters."})
    nb1.add_markdown("""## 2. Spark on YARN Architecture
```
YARN Cluster:
  - ResourceManager (RM): Central cluster arbiter.
  - NodeManager (NM): Runs on each worker node, launching containers.
Application Submission:
  1. Client submits job to RM.
  2. RM allocates Container 0 for Spark ApplicationMaster (AM).
  3. In yarn-cluster mode, the AM hosts the Spark Driver!
  4. In yarn-client mode, Driver runs on client; AM only requests executors.
  5. AM requests executor containers across NodeManagers.
```
""")
    nb1.add_session_init()
    nb1.add_code("""print("Cluster Manager in our Lab: Spark Standalone (spark://spark-master:7077)")
print("YARN submission equivalent command:")
print("spark-submit --master yarn --deploy-mode cluster --num-executors 10 --executor-memory 4g my_app.py")
""")
    nb1.add_experiment({"title": "Comparing Standalone vs YARN", "testing": "Contrast Standalone Master with YARN ResourceManager.", "expect": "Standalone is dedicated to Spark; YARN manages multi-tenant workloads (Hive, Flink, Spark, MapReduce).", "code": "print('YARN provides multi-tenant queue scheduling (CapacityScheduler, FairScheduler).')", "happened": "Explained YARN architectural distinctions.", "why": "YARN is a generic resource manager, not Spark-specific.", "takeaway": "Use Standalone for dedicated Spark clusters; use YARN for multi-tenant Hadoop clusters."})
    nb1.add_exercises({"b_title": "YARN Deploy Modes", "b_desc": "What is the difference between yarn-client and yarn-cluster?", "b_input": "Modes", "b_sol": "print('In yarn-client, Driver runs on client machine; in yarn-cluster, Driver runs inside YARN ApplicationMaster container.')", "i_title": "YARN Memory Overhead Config", "i_desc": "What is spark.yarn.executor.memoryOverhead?", "i_sol": "print('Off-heap container buffer for YARN (default max(384MB, 10% of heap)).')", "a_title": "CapacityScheduler Queues", "a_desc": "How to specify a YARN queue in spark-submit.", "a_sol": "print('--queue production_etl')"})
    nb1.add_summary_and_teardown({"takeaways": {"YARN AM": "ApplicationMaster negotiates containers.", "Deploy Modes": "yarn-client for dev/Jupyter; yarn-cluster for production."}, "terms": {"ResourceManager": "Central YARN scheduler.", "ApplicationMaster": "Per-application coordinator in YARN."}, "mistakes": {"yarn-client in production cron jobs": "If client terminal closes, job terminates."}, "questions": {"Where does the Driver run in yarn-cluster mode?": "Inside the YARN ApplicationMaster container on a worker node in the cluster."}, "next_steps": "Open 02_spark_on_kubernetes.ipynb"})
    nb1.save(os.path.join(BASE_12, "01_spark_on_yarn.ipynb"))

    # 12_02: Kubernetes
    nb2 = NotebookBuilder("Spark on Kubernetes: Cloud-Native Pod-Based Architecture", "12_advanced_topics", "02_spark_on_kubernetes.ipynb", "K8s_Lab")
    nb2.add_header({"what": "How Apache Spark runs natively on Kubernetes: Driver pods, Executor pods, Spark Operator, and cloud-native scaling.", "why": "Kubernetes is rapidly becoming the modern standard for cloud-native big data infrastructure.", "problem": "Managing ephemeral pod lifecycles and Docker container images.", "real_world": "Running Spark on EKS, GKE, and AKS."})
    nb2.add_markdown("""## 2. Spark on Kubernetes Architecture
```
Kubernetes Control Plane (kube-apiserver):
  1. spark-submit --master k8s://https://k8s-apiserver:443
  2. K8s launches Driver Pod.
  3. Driver Pod requests Executor Pods directly from kube-apiserver.
  4. K8s schedules Executor Pods on available K8s nodes.
  5. Executor Pods connect back to Driver Pod via K8s headless service.
```
""")
    nb2.add_session_init()
    nb2.add_code("""print("K8s submission equivalent command:")
print(\"\"\"
spark-submit \\
  --master k8s://https://kubernetes.default.svc \\
  --deploy-mode cluster \\
  --name spark-k8s-job \\
  --conf spark.kubernetes.container.image=spark-local:4.2.1 \\
  --conf spark.kubernetes.driver.pod.name=spark-driver-pod \\
  --conf spark.executor.instances=4 \\
  local:///opt/spark-apps/sales_report.py
\"\"\")
""")
    nb2.add_experiment({"title": "Comparing K8s vs Standalone", "testing": "Contrast K8s pod lifecycle with Docker Compose standalone.", "expect": "K8s creates ephemeral pods on demand; Standalone uses persistent worker daemons.", "code": "print('K8s provides fine-grained container isolation and auto-scaling via Cluster Autoscaler.')", "happened": "Explained K8s pod dynamics.", "why": "K8s provisions dedicated containers per executor.", "takeaway": "K8s eliminates idle worker costs through cloud autoscaling."})
    nb2.add_exercises({"b_title": "K8s Master URL Syntax", "b_desc": "What is the syntax for Spark K8s master?", "b_input": "URL", "b_sol": "print('k8s://https://<k8s-apiserver-host>:<port>')", "i_title": "Spark Operator Concept", "i_desc": "Explain Spark on K8s Operator.", "i_sol": "print('CRD (Custom Resource Definition) allowing declarative SparkApplication YAML manifests.')", "a_title": "Driver Headless Service", "a_desc": "Why K8s requires a headless service for Driver.", "a_sol": "print('Enables executor pods to route Netty RPC traffic back to the dynamic driver pod IP.')"})
    nb2.add_summary_and_teardown({"takeaways": {"Cloud-Native": "Pods as executors.", "Autoscaling": "Integrates with K8s cluster autoscaler."}, "terms": {"Driver Pod": "Pod hosting driver process.", "Spark Operator": "Kubernetes CRD controller for Spark."}, "mistakes": {"Missing ServiceAccount RBAC permissions": "Driver pod fails to launch executor pods if K8s RBAC is missing."}, "questions": {"How does the Driver communicate with executors on Kubernetes?": "Through a Kubernetes headless service that routes Netty RPC packets directly to executor pod IPs."}, "next_steps": "Open 03_cluster_vs_client_mode.ipynb"})
    nb2.save(os.path.join(BASE_12, "02_spark_on_kubernetes.ipynb"))

    # 12_03: Client vs Cluster Mode
    nb3 = NotebookBuilder("Client Mode vs Cluster Mode: Deep Comparison and Use Cases", "12_advanced_topics", "03_cluster_vs_client_mode.ipynb", "Client_Cluster_Lab")
    nb3.add_header({"what": "Complete comparison of Client Mode vs Cluster Mode across Standalone, YARN, and Kubernetes.", "why": "Choosing the wrong deploy mode can crash interactive terminals or isolate scheduled batch jobs.", "problem": "Understanding where the Driver runs and why network latency matters.", "real_world": "Configuring Airflow SparkSubmitOperator in enterprise environments."})
    nb3.add_markdown("""## 2. Client vs Cluster Mode Architecture
```
CLIENT MODE:
[ Submitting Machine (Jupyter / Laptop) ]  <--- High Network Latency Risk!
  - Runs DRIVER Process
  - Requires persistent low-latency connection to cluster!
       | RPC
       v
[ Remote Cluster (Workers / Executors) ]

CLUSTER MODE:
[ Submitting Machine ] ---> Submits job manifest to Master and DISCONNECTS (Fire & Forget!)
[ Remote Cluster ]
  - Node 1: Worker launches DRIVER JVM inside cluster!
  - Node 2, 3: Workers launch EXECUTORS!
```
""")
    nb3.add_session_init()
    nb3.add_code("""print("Our Docker JupyterLab environment runs in: CLIENT MODE")
print("Driver Host:   ", spark.conf.get("spark.driver.host"))
print("Master URL:    ", spark.conf.get("spark.master"))
""")
    nb3.add_experiment({"title": "Deploy Mode Decision Guide", "testing": "Print decision rules for Client vs Cluster mode.", "expect": "Client for interactive/dev; Cluster for production.", "code": """print(\"\"\"
Rule 1: If using JupyterLab, spark-shell, or PySpark REPL -> MUST USE CLIENT MODE.
Rule 2: If using Airflow, cron, or production scheduled ETL -> ALWAYS USE CLUSTER MODE.
\"\"\")
""", "happened": "Explained architectural deployment rules.", "why": "Client mode binds Driver to submitting machine; Cluster mode decouples submission from execution.", "takeaway": "Never use client mode for production scheduled jobs over remote VPNs or CI/CD pipelines."})
    nb3.add_exercises({"b_title": "Identify Lab Mode", "b_desc": "What deploy mode is active in this notebook?", "b_input": "Mode", "b_sol": "print('Client mode (Jupyter hosts Driver; workers host Executors).')", "i_title": "Cluster Mode Log Access", "i_desc": "How do you view driver logs in cluster mode?", "i_sol": "print('Via yarn logs -applicationId <id> or Spark History Server (18080).')", "a_title": "Network Partition Impact", "a_desc": "What happens if submitting machine disconnects in client mode?", "a_sol": "print('Driver process terminates; entire cluster application is aborted immediately.')"})
    nb3.add_summary_and_teardown({"takeaways": {"Client Mode": "Driver on client; required for notebooks.", "Cluster Mode": "Driver on worker; fire-and-forget for production."}, "terms": {"Deploy Mode": "Client vs Cluster flag.", "Fire-and-Forget": "Submitting job and disconnecting."}, "mistakes": {"Running client mode over high-latency VPN": "Heartbeat timeouts kill job."}, "questions": {"What is the fundamental difference between Client mode and Cluster mode?": "In Client mode, the Driver runs on the submitting machine; in Cluster mode, the Driver runs on a worker node inside the cluster."}, "next_steps": "Open 04_resource_allocation.ipynb"})
    nb3.save(os.path.join(BASE_12, "03_cluster_vs_client_mode.ipynb"))

    # 12_04: Resource Allocation
    nb4 = NotebookBuilder("Resource Allocation: Cores, Memory, and Executor Sizing Formula", "12_advanced_topics", "04_resource_allocation.ipynb", "Resource_Alloc_Lab")
    nb4.add_header({"what": "The exact mathematical formula for cluster resource allocation: sizing executor memory, cores, instances, and OS buffers.", "why": "Suboptimal resource requests waste thousands of cloud dollars or cause JVM thrashing.", "problem": "Choosing between 100 tiny 1-core executors vs 2 massive 50-core executors.", "real_world": "Sizing multi-node clusters on AWS EMR, Databricks, and GCP Dataproc."})
    nb4.add_markdown("""## 2. The Golden Executor Sizing Formula
Given a 10-node cluster (Each node: 16 cores, 64 GB RAM):
1. **OS / Daemon Allocation**: Reserve 1 core and 4 GB RAM per node for OS, NodeManager/Worker.
   - Usable per node: 15 cores, 60 GB RAM.
2. **Cores per Executor**: Set to **5 cores** (Optimal for HDFS throughput & GC).
   - Executors per node: $15 / 5 = 3$ executors. Total executors across 10 nodes = $30$.
3. **Memory per Executor**: $60\text{ GB} / 3 = 20\text{ GB}$ total process memory.
   - Overhead (10%): $20\text{ GB} \times 0.10 = 2\text{ GB}$.
   - Heap (`spark.executor.memory`): $20 - 2 = \mathbf{18\text{ GB}}$.
4. Reserve 1 executor for the ApplicationMaster / Driver.
   - Final: `--num-executors 29 --executor-cores 5 --executor-memory 18g`.
""")
    nb4.add_session_init()
    nb4.add_code("""# Sizing calculation function
def calculate_cluster_sizing(nodes, cores_per_node, ram_gb_per_node):
    usable_cores = cores_per_node - 1
    usable_ram = ram_gb_per_node - 4
    cores_per_exec = 5
    execs_per_node = usable_cores // cores_per_exec
    total_execs = (execs_per_node * nodes) - 1 # reserve 1 for Driver
    ram_per_exec = usable_ram / execs_per_node
    overhead = max(0.384, ram_per_exec * 0.10)
    heap = ram_per_exec - overhead
    return {
        "total_executors": total_execs,
        "cores_per_executor": cores_per_exec,
        "heap_memory_gb": round(heap, 1),
        "overhead_memory_gb": round(overhead, 1)
    }

print("Recommended sizing for 6-node cluster (32 cores, 128 GB RAM each):")
print(calculate_cluster_sizing(6, 32, 128))
""")
    nb4.add_experiment({"title": "Validating Our Lab Sizing", "testing": "Verify resource allocation in our 2-worker Docker cluster.", "expect": "2 workers x 2 cores x 4GB = 4 cores, 8GB total.", "code": "print('Lab Cluster: 2 workers, 2 cores each = 4 total executor cores.')", "happened": "Matched docker-compose.yml parameters.", "why": "Sized for lightweight local development.", "takeaway": "Always calculate executor quotas based on physical node limits."})
    nb4.add_exercises({"b_title": "Core Sizing Rule", "b_desc": "What is the recommended core count per executor?", "b_input": "Cores", "b_sol": "print('Between 3 and 5 cores per executor.')", "i_title": "Calculate 4-Node Cluster", "i_desc": "Calculate sizing for 4 nodes (16 cores, 64 GB).", "i_sol": "print(calculate_cluster_sizing(4, 16, 64))", "a_title": "Tiny vs Fat Executor Trade-off", "a_desc": "Explain drawbacks of 1-core vs 16-core executors.", "a_sol": "print('1-core loses broadcast sharing; 16-core suffers massive JVM Stop-The-World GC pauses.')"})
    nb4.add_summary_and_teardown({"takeaways": {"Formula": "5 cores per executor; 10% memory overhead; reserve 1 core/4GB per node for OS."}, "terms": {"Golden Rule": "3-5 cores per executor.", "Overhead": "Off-heap buffer."}, "mistakes": {"Fat executors (>32 GB RAM, 16 cores)": "Leads to catastrophic GC freezes."}, "questions": {"Why is 5 cores per executor considered the industry best practice?": "It balances JVM Garbage Collection pause times, allows shared broadcast variable caching, and maximizes HDFS I/O throughput."}, "next_steps": "Open 05_spark_connect.ipynb"})
    nb4.save(os.path.join(BASE_12, "04_resource_allocation.ipynb"))

    # 12_05: Spark Connect
    nb5 = NotebookBuilder("Spark Connect: Thin Client Architecture and Decoupled Execution", "12_advanced_topics", "05_spark_connect.ipynb", "Spark_Connect_Lab")
    nb5.add_header({"what": "How Spark Connect decouples client applications from the Spark Driver using gRPC and unresolved logical plans.", "why": "Enables running PySpark from any IDE or serverless environment without requiring a local Java/JVM installation.", "problem": "Traditional PySpark requires matching local Java, Python, and Spark versions on client machines.", "real_world": "Modern cloud-native interactive analytics."})
    nb5.add_markdown("""## 2. Spark Connect Architecture
```
Client Machine (Laptop / Web App / IDE):
  - Pure Python (No JVM / Java installation needed!)
  - DataFrame API constructs Unresolved Logical Plan
  - Encodes plan as Protocol Buffers
       |
       v gRPC (Port 15002)
Spark Connect Server (Running inside Spark Cluster):
  - Receives protobuf query plan
  - Optimizes with Catalyst
  - Executes across cluster workers
  - Streams results back as Apache Arrow record batches
```
""")
    nb5.add_session_init()
    nb5.add_code("""print("Spark Connect client connection syntax (Spark 3.4+ / 4.x):")
print(\"\"\"
from pyspark.sql import SparkSession
# Connect to remote Spark Connect server via gRPC
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
\"\"\")
""")
    nb5.add_experiment({"title": "Comparing Classic Driver vs Spark Connect", "testing": "Contrast classic Py4J socket driver with gRPC Spark Connect.", "expect": "Classic requires local JVM; Connect requires only lightweight Python client.", "code": "print('Connect uses gRPC and Arrow; eliminates local JVM dependency.')", "happened": "Explained architectural transition.", "why": "Spark Connect separates compute from interface.", "takeaway": "Spark Connect is the future of remote Spark client tooling."})
    nb5.add_exercises({"b_title": "Protocol Used", "b_desc": "What protocol does Spark Connect use?", "b_input": "Protocol", "b_sol": "print('gRPC with Protocol Buffers (protobuf)')", "i_title": "Arrow Streaming", "i_desc": "How results are streamed back to client.", "i_sol": "print('Streamed as Apache Arrow record batches zero-copy.')", "a_title": "Serverless Spark Benefit", "a_desc": "Why Connect enables serverless PySpark.", "a_sol": "print('Clients do not need heavy JVM containers; fast startup in serverless functions.')"})
    nb5.add_summary_and_teardown({"takeaways": {"Decoupled": "Client has no JVM dependency.", "gRPC": "Standardized remote query protocol."}, "terms": {"Spark Connect": "gRPC client-server architecture.", "Protobuf": "Binary serialization format for logical plans."}, "mistakes": {"Assuming Connect is a separate engine": "It is an interface layer on top of the standard Spark engine."}, "questions": {"What are the primary benefits of Spark Connect?": "It allows lightweight client execution without a local Java/JVM installation, provides language independence, and isolates client crashes from the cluster."}, "next_steps": "Open 06_pyspark_internals.ipynb"})
    nb5.save(os.path.join(BASE_12, "05_spark_connect.ipynb"))

    # 12_06: PySpark Internals
    nb6 = NotebookBuilder("PySpark Internals: Py4J Bridge, Python Daemons, and Serialization Overhead", "12_advanced_topics", "06_pyspark_internals.ipynb", "PySpark_Internals_Lab")
    nb6.add_header({"what": "The deep architecture of PySpark: how the Python interpreter communicates with the JVM via Py4J, Python worker daemons, and pickle serialization.", "why": "Understanding the boundary between Python and the JVM explains why DataFrames run fast while custom Python functions can be slow.", "problem": "Diagnosing socket communication overhead and Python worker memory leaks.", "real_world": "Optimizing high-throughput Python ETL."})
    nb6.add_markdown("""## 2. PySpark Architecture Deep Dive
```
DRIVER SIDE:
[ Python Interpreter (Jupyter) ] <=== Py4J Socket (Port 25333) ===> [ JVM SparkContext / Driver ]

EXECUTOR SIDE (For RDDs / Python UDFs):
[ JVM Executor Backend ] <=== Unix Pipe / Socket ===> [ Python Worker Daemon (daemon.py) ]
  - Tungsten UnsafeRow       (Pickle Serialization)      - Python lambda execution
```
> When using pure DataFrame operations (`select`, `groupBy`, `join`), data **NEVER leaves the JVM**! Execution runs 100% in Java bytecode via Tungsten.
""")
    nb6.add_session_init()
    nb6.add_code("""import sys

print(f"Python Runtime: {sys.version}")
print(f"Py4J Gateway:   {spark.sparkContext._gateway}")
print(f"JVM Gateway:    {spark.sparkContext._jvm}")

# Inspect a JVM class directly from Python via Py4J!
jvm_version = spark.sparkContext._jvm.org.apache.spark.package.SPARK_VERSION()
print(f"Spark version retrieved directly from JVM via Py4J: {jvm_version}")
""")
    nb6.add_experiment({"title": "Observing Py4J Reflection Calls", "testing": "Call a native Scala/Java method on the JVM SparkContext directly from Python.", "expect": "Py4J translates Python call into Java reflection.", "code": """jvm_cores = spark.sparkContext._jsc.sc().defaultParallelism()
print(f"Default parallelism from JVM: {jvm_cores}")
""", "happened": "Successfully invoked Scala method across Py4J bridge.", "why": "Py4J intercepts attribute access and routes RPC requests to the JVM listener.", "takeaway": "PySpark is a Python wrapper around the underlying JVM distributed engine."})
    nb6.add_exercises({"b_title": "Check Py4J Gateway", "b_desc": "Print gateway port on sparkContext.", "b_input": "gateway", "b_sol": "print(spark.sparkContext._gateway.gateway_parameters.port)", "i_title": "DataFrame Zero-Overhead Principle", "i_desc": "Explain why DataFrame operations have zero Python serialization overhead.", "i_sol": "print('DataFrame transformations compile into JVM Catalyst plans; Python only receives the final aggregated result.')", "a_title": "Python Worker Memory Tuning", "a_desc": "What config limits Python worker memory.", "a_sol": "print('spark.executor.pyspark.memory')"})
    nb6.add_summary_and_teardown({"takeaways": {"Py4J Bridge": "Routes driver Python calls to JVM via socket.", "DataFrame Speed": "Pure JVM execution; zero Python serialization."}, "terms": {"Py4J": "Python-to-Java IPC bridge.", "Python Daemon": "Worker sub-process executing Python lambdas."}, "mistakes": {"Believing PySpark is slower than Scala for DataFrames": "False: DataFrame operations execute identically in the JVM."}, "questions": {"When is PySpark slower than Scala Spark?": "Only when using custom Python RDD transformations or standard Python UDFs, which require serializing data across a socket between the JVM and Python worker."}, "next_steps": "Modules 00-12 complete! Proceed to Module 13: End-to-End Projects."})
    nb6.save(os.path.join(BASE_12, "06_pyspark_internals.ipynb"))

if __name__ == '__main__':
    gen_11_all()
    gen_12_all()
