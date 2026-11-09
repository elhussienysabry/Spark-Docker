#!/usr/bin/env python3
"""
Generates docs/SPARK_ARCHITECTURE.md
"""
import os

content = """#  Apache Spark Internals & Cluster Architecture Guide

> **Target Version**: Apache Spark 4.2.1 · Scala 2.13 · Java 21 · Python 3.13  
> **Infrastructure**: Standalone Cluster (1 Master, 2 Workers, JupyterLab Driver, History Server)  

---

## 1. High-Level Distributed Architecture

Apache Spark follows a **master-slave distributed architecture**. The master coordinates the distribution of compute tasks and resource assignments, while the worker nodes host executor processes that perform in-memory computations and store cached data blocks.

```
+-----------------------------------------------------------------------------------------+
|                                      HOST MACHINE                                       |
|                                                                                         |
|   +--------------------------+                                                          |
|   | spark-jupyter (Driver)   |                                                          |
|   |  - User Python Code      |                                                          |
|   |  - SparkSession          |                                                          |
|   |  - DAGScheduler          |                                                          |
|   |  - TaskScheduler         |                                                          |
|   |  - Web UI (Port 4040)    |                                                          |
|   +------------+-------------+                                                          |
|                |                                                                        |
|      1. Submit Application & Request Resources                                          |
|                |                                                                        |
|                v                                                                        |
|   +--------------------------+               Heartbeats / Worker Registration           |
|   | spark-master (Master)    |<========================================+                |
|   |  - Standalone Master     |                                         |                |
|   |  - Web UI (Port 8080)    |                                         |                |
|   |  - RPC Port (7077)       |                                         |                |
|   +------------+-------------+                                         |                |
|                |                                                       |                |
|      2. Launch Executors                                               |                |
|                |                                                       |                |
|        +-------+-----------------------+                               |                |
|        |                               |                               |                |
|        v                               v                               |                |
|   +-----------------------+       +-----------------------+            |                |
|   | spark-worker-1        |       | spark-worker-2        |------------+                |
|   |  - Worker Daemon      |       |  - Worker Daemon      |                             |
|   |  - Web UI (Port 8081) |       |  - Web UI (Port 8082) |                             |
|   |  +------------------+ |       |  +------------------+ |                             |
|   |  | CoarseGrained    | |       |  | CoarseGrained    | |                             |
|   |  | ExecutorBackend  | |       |  | ExecutorBackend  | |                             |
|   |  | (JVM Process)    | |       |  | (JVM Process)    | |                             |
|   |  | - 2 Cores        | |       |  | - 2 Cores        | |                             |
|   |  | - 4 GB RAM       | |       |  | - 4 GB RAM       | |                             |
|   |  | - BlockManager   | |       |  | - BlockManager   | |                             |
|   |  +------------------+ |       |  +------------------+ |                             |
|   +-----------------------+       +-----------------------+                             |
|                ^                               ^                                        |
|                |                               |                                        |
|                +---------------+---------------+                                        |
|                                |                                                        |
|         3. Tasks Dispatched, Status Tracked & Blocks Exchanged                          |
|                     Directly between Driver & Executors                                 |
|                                                                                         |
|   +---------------------------------------------------------------------------------+   |
|   | Shared Storage Volumes (Mounted on Host)                                        |   |
|   |  - ./data   -> /opt/spark-data   (Data lake, retail tables, warehouse)          |   |
|   |  - ./events -> /opt/spark-events (Persistent Event Logs for History Server)     |   |
|   +---------------------------------------------------------------------------------+   |
+-----------------------------------------------------------------------------------------+
```

---

## 2. Core Architectural Components

### 2.1 The Driver Program
The **Driver** is the control center of any Spark application. In our environment, the Driver runs inside the `spark-jupyter` container when executing interactive notebooks, or inside the submitting container during `spark-submit`.

#### Key Responsibilities:
1. **Instantiates the Entry Point**: Creates the `SparkSession` and underlying `SparkContext`.
2. **Constructs the Execution Graph**: Translates transformations into a Directed Acyclic Graph (DAG) of RDDs and logical operators.
3. **Optimizes with Catalyst**: Transforms user DataFrame/SQL expressions through analysis, optimization, and physical planning.
4. **DAGScheduler**: Deconstructs the physical plan into **Stages** based on shuffle dependencies.
5. **TaskScheduler**: Packages stages into individual **Tasks** and schedules them onto worker executors based on data locality (`PROCESS_LOCAL`, `NODE_LOCAL`, `RACK_LOCAL`, `ANY`).
6. **Task Coordination**: Tracks running task attempts, handles retries, and collects terminal results (e.g. for `collect()`, `take()`, `count()`).
7. **Hosts the Spark UI**: Serves the active web UI on port `4040`.

> [!CAUTION]
> The Driver must never be treated as a data processing node. Calling `.collect()` on a multi-gigabyte DataFrame pulls all partitions across the network into the Driver JVM heap, causing an instant `java.lang.OutOfMemoryError: Java heap space`.

---

### 2.2 The Cluster Manager
The **Cluster Manager** allocates physical or virtual resources across the cluster. Spark supports four cluster managers:
1. **Standalone**: Spark's built-in manager (used in this project). Highly efficient, minimal external dependencies.
2. **Kubernetes (K8s)**: Cloud-native execution using pods as executors and drivers.
3. **Hadoop YARN**: The enterprise standard for shared multi-tenant Hadoop clusters.
4. **Apache Mesos**: Fine-grained legacy distributed systems kernel.

In our Standalone setup:
- The **Master** daemon runs at `spark://spark-master:7077`.
- Manages worker heartbeats, available cores, and memory capacity.
- Handles application queueing and executor allocation.
- Does **not** participate in task execution or data transfer.

---

### 2.3 Worker Daemons vs. Executors
A frequent point of confusion is the distinction between a **Worker** and an **Executor**:

| Dimension | Worker (`Worker`) | Executor (`CoarseGrainedExecutorBackend`) |
| :--- | :--- | :--- |
| **What it is** | A persistent daemon representing a physical or containerized node. | A dynamic JVM process spawned by the worker for a **single application**. |
| **Lifecycle** | Runs continuously as long as the cluster is up. | Created when an application starts; dies when the application terminates. |
| **Responsibility**| Reports node health, memory, and core capacity to the Master. | Executes tasks, manages memory pools, caches data, and serves shuffle blocks. |
| **Isolation** | Shared across applications. | Strictly isolated to one application. Cannot access memory of another executor. |
| **Count in Lab** | Exactly 2 (`spark-worker-1`, `spark-worker-2`). | 1 per worker per application (by default). |

---

## 3. Detailed Application Execution Lifecycle

```
[User Code]
     |
     v
 1. SparkSession.builder.getOrCreate()
     |
     +--> Driver starts in container (spark-jupyter)
     +--> Driver registers with Master (spark://spark-master:7077)
     |
 2. Master Resource Allocation
     |
     +--> Master checks available slots on spark-worker-1 and spark-worker-2
     +--> Sends 'LaunchExecutor' command to workers
     |
 3. Worker Spawns Executor JVMs
     |
     +--> spark-worker-1 launches CoarseGrainedExecutorBackend (2 cores, 4GB)
     +--> spark-worker-2 launches CoarseGrainedExecutorBackend (2 cores, 4GB)
     |
 4. Executor Registration
     |
     +--> Executors connect directly back to Driver via Netty RPC (spark.driver.host=jupyter)
     +--> Executors register their BlockManagers with Driver's BlockManagerMaster
     |
 5. Pipeline Definition (Lazy Evaluation)
     |
     +--> df.filter(...).groupBy(...).agg(...)
     +--> Driver builds Logical Plan; Catalyst optimizes; NO execution occurs
     |
 6. Action Invocation
     |
     +--> df.count() or df.write.parquet(...)
     +--> Action triggers DAGScheduler.createJob()
     |
 7. Stage & Task Decomposition
     |
     +--> DAGScheduler splits DAG at ShuffleDependencies into Stages
     +--> TaskScheduler creates a TaskSet (1 task per partition)
     |
 8. Task Execution
     |
     +--> Tasks serialized and dispatched over network to Executor thread pools
     +--> Executors deserialize bytecode and process partitions in memory
     +--> Intermediate shuffle data written to local worker disk if needed
     |
 9. Completion & Resource Cleanup
     |
     +--> Result returned to Driver
     +--> spark.stop() releases executors back to worker pool
```

---

## 4. Driver-to-Executor Networking & Communication

Spark relies on high-performance **Netty** asynchronous network transport for all cluster communications:

1. **RPC Messages**: Lightweight control frames (task launch, task completion, heartbeats, block status updates).
2. **Shuffle Data Transfers**: Point-to-point block transfers between executors. Reducers pull shuffle blocks directly from the map-side worker nodes without passing through the Driver or Master.
3. **Broadcast Variables**: BitTorrent-inspired P2P data transfer protocol. Large read-only lookup structures are transferred from the Driver to executors, and then shared executor-to-executor to avoid saturating the Driver's network interface.

```
       +--------------------+
       |   Driver (Jupyter) |
       +----+----------+----+
            |          |
      RPC   |          |  RPC
   Dispatch |          | Dispatch
            v          v
   +--------+---+  +---+--------+
   | Executor 1 |  | Executor 2 |
   +--------+---+  +---+--------+
            ^          ^
            |  Shuffle |
            +----------+
            Data Transfer
            (Point-to-Point)
```

> [!IMPORTANT]
> Because the Driver is running inside the `spark-jupyter` container while Executors run inside `spark-worker-1` and `spark-worker-2`, the Driver **must** advertise its Docker network hostname:
> `spark.driver.host = "jupyter"`
> `spark.driver.bindAddress = "0.0.0.0"`
> If set to `localhost`, executors will attempt to connect to their own loopback interface and fail to register.

---

## 5. Client Mode vs. Cluster Mode

Understanding deployment modes is essential for production operations:

| Feature | Client Mode (`--deploy-mode client`) | Cluster Mode (`--deploy-mode cluster`) |
| :--- | :--- | :--- |
| **Driver Location** | Runs on the client machine submitting the job (e.g., your laptop, edge node, Jupyter). | Runs **inside** one of the worker nodes in the cluster. |
| **Network Dependency**| Requires permanent, low-latency connection between client and cluster. | Client can disconnect immediately after submission (`fire-and-forget`). |
| **Failure Tolerance** | If client machine disconnects, the entire application dies immediately. | Application continues running until completion regardless of client state. |
| **Best Used For** | Interactive development, debugging, Jupyter notebooks, ad-hoc analysis. | Production scheduled jobs, automated Airflow DAGs, enterprise ETL. |
| **Lab Setup** | **Client Mode** (JupyterLab hosts the Driver; Workers host Executors). | Supported via `spark-submit --deploy-mode cluster` inside the cluster. |

---

## 6. Memory Allocation: Docker Container vs. Spark JVM

To prevent kernel Out-Of-Memory kills (`OOMKilled`), one must understand the relationship between Docker container memory and Spark's internal JVM allocations:

```
+---------------------------------------------------------------------------------+
| Worker Docker Container Limit (Host RAM)                                        |
|                                                                                 |
|   +-------------------------------------------------------------------------+   |
|   | Executor Process                                                        |   |
|   |                                                                         |   |
|   |   +-----------------------------------------------------------------+   |   |
|   |   | JVM Heap Memory (spark.executor.memory = 2g)                    |   |   |
|   |   |  - Spark Memory Pool (Execution + Storage)                      |   |   |
|   |   |  - User Memory (Data structures, UDF allocations)               |   |   |
|   |   |  - Reserved Memory (300 MB)                                     |   |   |
|   |   +-----------------------------------------------------------------+   |   |
|   |                                                                         |   |
|   |   +-----------------------------------------------------------------+   |   |
|   |   | Off-Heap Memory Overhead (spark.executor.memoryOverhead)        |   |   |
|   |   |  - Python Worker daemons (PySpark Py4J communication)           |   |   |
|   |   |  - Netty shuffle transfer buffers                               |   |   |
|   |   |  - JVM Metaspace, GC metadata, thread stacks                    |   |   |
|   |   +-----------------------------------------------------------------+   |   |
|   +-------------------------------------------------------------------------+   |
|                                                                                 |
|   Worker Daemon Memory Overhead (SPARK_DAEMON_MEMORY = 1g)                      |
+---------------------------------------------------------------------------------+
```

In our lab:
- `SPARK_WORKER_MEMORY=4g`: The worker will allocate up to 4 GB of total executor capacity.
- `spark.executor.memory=2g`: An individual application requests 2 GB of heap memory per executor.
- Off-heap overhead is automatically added on top (default $\\max(384\\text{MB}, 10\\% \\times 2\\text{GB}) = 384\\text{MB}$).
- The combined memory footprint must fit comfortably within the host machine's physical RAM.

---

## 7. Cluster Service Configuration Reference

| Service Name | Container Name | Web UI Port | Internal Port | Environment Variables & Defaults |
| :--- | :--- | :--- | :--- | :--- |
| **Spark Master** | `spark-master` | `8080` (Host) | `7077` (RPC) | `SPARK_DAEMON_MEMORY=1g` |
| **Spark Worker 1** | `spark-worker-1` | `8081` (Host) | Dynamic | `SPARK_WORKER_CORES=2`, `SPARK_WORKER_MEMORY=4g` |
| **Spark Worker 2** | `spark-worker-2` | `8082` (Host) | Dynamic | `SPARK_WORKER_CORES=2`, `SPARK_WORKER_MEMORY=4g` |
| **History Server** | `spark-history` | `18080` (Host)| N/A | `spark.history.fs.logDirectory=file:///opt/spark-events` |
| **JupyterLab** | `spark-jupyter` | `8888` (Host) | `4040` (Driver)| `JUPYTER_TOKEN=spark`, `SPARK_MASTER=spark://spark-master:7077` |
"""

with open('/home/elhussieny/spark-docker/docs/SPARK_ARCHITECTURE.md', 'w') as f:
    f.write(content.strip() + '\n')
print("Successfully wrote docs/SPARK_ARCHITECTURE.md")
