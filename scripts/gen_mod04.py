#!/usr/bin/env python3
"""
Generates Module 04 (Execution Engine) notebooks:
- 04_execution_engine/01_lazy_evaluation.ipynb
- 04_execution_engine/02_dag_and_execution_plans.ipynb
- 04_execution_engine/03_dag_scheduler.ipynb
- 04_execution_engine/04_task_scheduler.ipynb
- 04_execution_engine/05_jobs_stages_and_tasks.ipynb
- 04_execution_engine/06_narrow_and_wide_transformations.ipynb
- 04_execution_engine/07_dependencies_and_stage_boundaries.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/04_execution_engine"

def gen_04_01():
    nb = NotebookBuilder(
        title="Lazy Evaluation: Philosophy, Mechanics, and Optimization Benefits",
        module_name="04_execution_engine",
        notebook_name="01_lazy_evaluation.ipynb",
        app_name="Lazy_Evaluation_Lab"
    )
    nb.add_header({
        "what": "How lazy evaluation works in Apache Spark, why transformations do not compute immediately, and how actions trigger execution.",
        "why": "Lazy evaluation is the prerequisite for Catalyst query optimization, Whole-Stage Code Generation, and fault-tolerant DAG lineage.",
        "problem": "Beginners frequently misattribute slow runtimes to the wrong lines of code (e.g. blaming filter() when count() actually ran).",
        "real_world": "Preventing premature action execution in automated ETL pipelines."
    })
    nb.add_markdown("""## 2. Imperative vs Lazy Declarative Execution

```
Imperative Execution (Python / Pandas):
Step 1: df = read_csv()           -> Reads entire file from disk immediately!
Step 2: df = df[df.val > 100]     -> Allocates new in-memory DataFrame!
Step 3: df = df[['id', 'val']]    -> Allocates third DataFrame!
(Intermediate memory churn, no cross-step optimization)

Lazy Execution (Apache Spark):
Step 1: df = read_csv()           -> Appends 'LogicalRelation' to Plan. Zero I/O!
Step 2: df = df.filter(val > 100) -> Appends 'Filter' node. Zero I/O!
Step 3: df = df.select('id','val')-> Appends 'Project' node. Zero I/O!
Step 4: df.count()                -> ACTION! Catalyst fuses steps 1-3 into ONE SCAN!
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Timing Benchmark: Transformation vs Action Execution""")
    nb.add_code("""# Measure time to define transformations
t0 = time.time()
orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
transformed = (
    orders
    .filter("quantity > 2")
    .filter("unit_price > 500")
    .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
    .groupBy("channel")
    .sum("revenue")
)
t_declaration = time.time() - t0

# Measure time to trigger terminal action
t0 = time.time()
result = transformed.collect()
t_action = time.time() - t0

print(f"Time to declare transformations: {t_declaration:.5f}s (Building AST only)")
print(f"Time to execute terminal Action:  {t_action:.5f}s (Actual execution across cluster)")
""")
    nb.add_experiment({
        "title": "Proving Predicate Pushdown via Lazy Evaluation",
        "testing": "Demonstrate that filtering after reading CSV is pushed down and fused into the scan rather than reading all rows into memory.",
        "expect": "Physical plan shows Filter pushed directly above or inside FileScan.",
        "code": """plan_df = orders.filter("status = 'Cancelled'").select("order_id", "status")
print("Execution plan showing operator fusion:")
plan_df.explain("formatted")
""",
        "happened": "Filter and Project operators are executed as a single fused WholeStageCodegen step.",
        "why": "Catalyst inspects the full plan before generating execution bytecode.",
        "takeaway": "Because Spark is lazy, you do not need to manually optimize the order of select and filter operations."
    })
    nb.add_exercises({
        "b_title": "Identify Action vs Transformation",
        "b_desc": "Given 5 operations (filter, count, show, select, write), classify each as Lazy Transformation or Eager Action.",
        "b_input": "List of operations",
        "b_starter": "# Exercise 1: Classify operations\n",
        "b_sol": """ops = {
    "filter": "Lazy Transformation",
    "count": "Eager Action",
    "show": "Eager Action",
    "select": "Lazy Transformation",
    "write": "Eager Action"
}
for op, cat in ops.items():
    print(f"  {op:10}: {cat}")""",
        "i_title": "Measure Latency Across Multiple Actions",
        "i_desc": "Run count(), first(), and take(5) on an uncached transformed DataFrame and observe that each executes independently.",
        "i_input": "orders DataFrame",
        "i_starter": "# Exercise 2: Multiple actions latency\n",
        "i_sol": """tf = orders.filter("quantity > 3").groupBy("status").count()
for act in ["count()", "first()", "take(5)"]:
    t0 = time.time()
    if act == "count()": tf.count()
    elif act == "first()": tf.first()
    else: tf.take(5)
    print(f"{act:10} executed in {time.time()-t0:.4f}s")""",
        "a_title": "Eliminate Redundant Action Execution",
        "a_desc": "Refactor a code snippet that calls count() and show() into a single action using persist().",
        "a_input": "orders DataFrame",
        "a_starter": "# Exercise 3: Cache before multiple actions\n",
        "a_sol": """cached_df = orders.filter("status = 'Completed'").groupBy("channel").count().persist()
print("Count from first action:", cached_df.count())
cached_df.show()
cached_df.unpersist()"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Laziness enables Optimization": "Allows Catalyst to prune columns, reorder joins, and fuse operators.",
            "Actions trigger Jobs": "Nothing moves across the network until an Action is invoked.",
            "Debugging Misconceptions": "Slow lines in Python are almost always the terminal Action calls."
        },
        "terms": {
            "Lazy Evaluation": "Deferring computation until result is requested by an Action.",
            "AST": "Abstract Syntax Tree representing the logical query.",
            "WholeStageCodegen": "Fusing multiple physical operations into a single bytecode loop."
        },
        "mistakes": {
            "Sprinkling count() for progress logs": "Recomputes the entire upstream query from scratch on every call.",
            "Blaming filter() for slow runtime": "filter() takes 0.001s; the subsequent action executes the work."
        },
        "questions": {
            "Why did Spark design transformations to be lazy?": "To allow the Catalyst optimizer to inspect the entire query tree, eliminate unused columns, push predicates, and generate optimized physical execution code.",
            "What triggers execution in Spark?": "An Action (e.g. collect, count, take, write)."
        },
        "next_steps": "Open 04_execution_engine/02_dag_and_execution_plans.ipynb to read physical and logical plans."
    })
    nb.save(os.path.join(BASE_DIR, "01_lazy_evaluation.ipynb"))

def gen_04_02():
    nb = NotebookBuilder(
        title="The DAG and Query Execution Plans: Parsed, Analyzed, Optimized, and Physical",
        module_name="04_execution_engine",
        notebook_name="02_dag_and_execution_plans.ipynb",
        app_name="Execution_Plans_Lab"
    )
    nb.add_header({
        "what": "How Catalyst converts raw code into the 4 plan stages: Parsed Logical Plan, Analyzed Logical Plan, Optimized Logical Plan, and Physical Plan.",
        "why": "Reading `df.explain()` is the #1 forensic skill for Senior Data Engineers to diagnose query performance.",
        "problem": "Queries fail or run slowly because of unnoticed Cartesian joins, missing predicate pushdown, or unnecessary shuffles.",
        "real_world": "Optimizing multi-hour enterprise ETL jobs by analyzing explain plan trees."
    })
    nb.add_markdown("""## 2. The 4 Stages of Catalyst Query Planning

```
User Code / SQL
       |
       v
1. PARSED LOGICAL PLAN (Unresolved: verifies syntax; column names unresolved)
       |
       v (Catalog Resolution: checks table schemas, column types)
2. ANALYZED LOGICAL PLAN (Resolved: all columns and functions verified)
       |
       v (Rule-Based Optimization: Predicate Pushdown, Column Pruning, Constant Folding)
3. OPTIMIZED LOGICAL PLAN (Minimal algebraic operations)
       |
       v (Cost-Based Model: Join Strategy Selection, Algorithm Choice)
4. PHYSICAL PLAN (Concrete executable operators: FileScan, HashAggregate, SortMergeJoin)
       |
       v (Tungsten: WholeStageCodegen)
  Java Bytecode Execution
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Practical: Inspecting All 4 Plan Stages with `explain(True)`""")
    nb.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
products = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/products.csv")

query_df = (
    orders
    .filter("quantity > 2")
    .join(products, on="product_id")
    .filter("category = 'Electronics'")
    .select("order_id", "product_name", "quantity", "unit_price")
)

# explain(True) prints Parsed, Analyzed, Optimized, and Physical plans
query_df.explain(True)
""")
    nb.add_markdown("""## 4. Modern Formatted Execution Plan with `explain('formatted')`""")
    nb.add_code("""print("=== Formatted Plan Output ===")
query_df.explain("formatted")
""")
    nb.add_experiment({
        "title": "Predicate Pushdown Observation in Physical Plan",
        "testing": "Verify in the Physical Plan that 'category = Electronics' is pushed down directly into the FileScan node.",
        "expect": "PushedFilters in FileScan contains StringEqualTo(category, Electronics).",
        "code": """print("Check FileScan PushedFilters:")
query_df.explain("cost")
""",
        "happened": "FileScan showed PushedFilters directly reading matching rows.",
        "why": "Catalyst moved the filter condition past the join directly into the table reader.",
        "takeaway": "Always verify that your filters appear under PushedFilters in physical plans to prevent scanning unneeded data."
    })
    nb.add_exercises({
        "b_title": "Identify WholeStageCodegen in Plan",
        "b_desc": "Run explain() and identify which physical plan operators are prefixed with an asterisk (*), indicating WholeStageCodegen.",
        "b_input": "query_df",
        "b_starter": "# Exercise 1: Identify codegen operators\n",
        "b_sol": """print("Operators with * run inside WholeStageCodegen:")
for line in query_df._jdf.queryExecution().executedPlan().toString().split("\\n"):
    if "*" in line:
        print("  ->", line.strip())""",
        "i_title": "Detect Exchange (Shuffle) Operators",
        "i_desc": "Write a query that groups and sorts data, and count how many Exchange nodes appear in explain().",
        "i_input": "orders DataFrame",
        "i_starter": "# Exercise 2: Count Exchange nodes\n",
        "i_sol": """p = orders.groupBy("status").count().orderBy("count")
plan_str = p._jdf.queryExecution().executedPlan().toString()
print("Exchange count:", plan_str.count("Exchange"))""",
        "a_title": "Analyze Join Strategy in Physical Plan",
        "a_desc": "Inspect query_df and determine whether Spark selected BroadcastHashJoin or SortMergeJoin.",
        "a_input": "query_df",
        "a_starter": "# Exercise 3: Join strategy identification\n",
        "a_sol": """plan = query_df._jdf.queryExecution().executedPlan().toString()
if "BroadcastHashJoin" in plan:
    print("Selected Join: BroadcastHashJoinExec")
elif "SortMergeJoin" in plan:
    print("Selected Join: SortMergeJoinExec")
else:
    print("Other Join Strategy Selected")"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "4 Plan Levels": "Parsed -> Analyzed -> Optimized -> Physical.",
            "explain(True)": "The universal tool for inspecting Catalyst decision-making.",
            "PushedFilters": "Indicates storage-level filtering (predicate pushdown)."
        },
        "terms": {
            "Parsed Plan": "Unresolved syntax tree.",
            "Analyzed Plan": "Resolved against catalog metadata.",
            "Physical Plan": "Executable algorithm graph."
        },
        "mistakes": {
            "Ignoring Exchange nodes": "Missing that your query is doing 5 unnecessary network shuffles.",
            "Assuming queries execute imperatively": "Catalyst reorders filters and joins for optimal execution."
        },
        "questions": {
            "What is the difference between Logical Plan and Physical Plan?": "The Logical Plan describes WHAT data to compute; the Physical Plan describes HOW to compute it (which concrete algorithms and join strategies to execute).",
            "What does an asterisk (*) next to an operator in explain() mean?": "It signifies that the operator participates in Whole-Stage Code Generation (Tungsten)."
        },
        "next_steps": "Open 04_execution_engine/03_dag_scheduler.ipynb to dissect the DAGScheduler."
    })
    nb.save(os.path.join(BASE_DIR, "02_dag_and_execution_plans.ipynb"))

def gen_04_03_to_07():
    # 04_03: DAGScheduler
    nb3 = NotebookBuilder("The DAGScheduler: Converting DAGs into Execution Stages", "04_execution_engine", "03_dag_scheduler.ipynb", "DAGScheduler_Lab")
    nb3.add_header({"what": "How the DAGScheduler decomposes physical plans into stages, handles ShuffleMapStages vs ResultStages, and manages stage retries.", "why": "The DAGScheduler is the brain that manages distributed execution pipelines.", "problem": "Understanding fetch failures and stage re-computations.", "real_world": "Diagnosing pipeline failures in production."})
    nb3.add_markdown("## 2. DAGScheduler Architecture\n\nThe `DAGScheduler` works backward from the final action target, identifying `ShuffleDependencies` to draw stage boundaries. Stages with no dependencies run first.")
    nb3.add_session_init()
    nb3.add_markdown("## 3. Practical: ShuffleMapStage vs ResultStage")
    nb3.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
res = orders.groupBy("status").count()
print("Trigger action to observe stages in Spark UI:")
res.show()
print("Visit http://localhost:4040/stages/ to see ShuffleMapStage (Stage 0) and ResultStage (Stage 1)!")
""")
    nb3.add_experiment({"title": "Stage Retries on Fetch Failure", "testing": "Explain DAGScheduler retry policy.", "expect": "Upstream stage is retried if shuffle files are lost.", "code": "print('Max stage failures:', spark.conf.get('spark.stage.maxConsecutiveAttempts', '4'))", "happened": "Default 4 attempts before aborting application.", "why": "Transient network drops can cause fetch failures.", "takeaway": "DAGScheduler retries stages automatically upon transient fetch errors."})
    nb3.add_exercises({"b_title": "Identify Stages", "b_desc": "Count stages in a simple select query.", "b_input": "orders.select('order_id')", "b_sol": "orders.select('order_id').count() # 1 stage", "i_title": "Two Shuffles Stage Count", "i_desc": "Count stages in groupBy + repartition.", "b_starter": "# Sol 1\n", "i_starter": "# Sol 2\n", "a_starter": "# Sol 3\n", "i_sol": "orders.groupBy('status').count().repartition(4).count() # 3 stages", "a_title": "DAGScheduler Metrics", "a_desc": "Check stage duration in UI.", "a_sol": "print('Inspected UI Stages tab')"})
    nb3.add_summary_and_teardown({"takeaways": {"DAGScheduler": "Splits DAG at shuffles into stages."}, "terms": {"ShuffleMapStage": "Writes shuffle files.", "ResultStage": "Computes final action."}, "mistakes": {"Assuming 1 job = 1 stage": "Jobs have multiple stages if shuffles exist."}, "questions": {"What creates a stage boundary?": "A wide dependency (ShuffleDependency)."}, "next_steps": "Open 04_task_scheduler.ipynb"})
    nb3.save(os.path.join(BASE_DIR, "03_dag_scheduler.ipynb"))

    # 04_04: TaskScheduler
    nb4 = NotebookBuilder("The TaskScheduler: Task Distribution and Data Locality", "04_execution_engine", "04_task_scheduler.ipynb", "TaskScheduler_Lab")
    nb4.add_header({"what": "How the TaskScheduler dispatches TaskSets to executors, evaluates data locality, and handles speculative execution.", "why": "TaskScheduler handles low-level thread scheduling and core utilization.", "problem": "Mitigating stragglers and node failures.", "real_world": "Cluster resource tuning."})
    nb4.add_markdown("## 2. TaskScheduler Locality Levels: PROCESS_LOCAL > NODE_LOCAL > NO_PREF > RACK_LOCAL > ANY")
    nb4.add_session_init()
    nb4.add_code("""rdd = spark.sparkContext.parallelize(range(100), 4)
print("Tasks run across available worker cores:")
print(rdd.map(lambda x: x*2).collect()[:10])
""")
    nb4.add_experiment({"title": "Speculative Execution Flag", "testing": "Check speculation config.", "expect": "False by default.", "code": "print('Speculation enabled?:', spark.conf.get('spark.speculation', 'false'))", "happened": "Printed false.", "why": "Speculation runs duplicate tasks for stragglers.", "takeaway": "Enable speculation on large shared clusters with flaky nodes."})
    nb4.add_exercises({"b_title": "Default Parallelism", "b_desc": "Print sc.defaultParallelism.", "b_input": "sc", "b_starter": "# B\n", "i_starter": "# I\n", "a_starter": "# A\n", "b_sol": "print(spark.sparkContext.defaultParallelism)", "i_title": "Task Attempt ID", "i_desc": "Inspect task attempt context.", "i_sol": "from pyspark import TaskContext\nprint(spark.sparkContext.parallelize(range(2)).map(lambda _: TaskContext.get().attemptNumber()).collect())", "a_title": "Locality Inspection", "a_desc": "Check locality in UI.", "a_sol": "print('Inspected task locality in UI')"})
    nb4.add_summary_and_teardown({"takeaways": {"TaskScheduler": "Assigns tasks to executor threads based on locality."}, "terms": {"TaskSet": "Set of tasks for 1 stage.", "Speculation": "Duplicate copy for slow tasks."}, "mistakes": {"Ignoring locality": "Remote data transfer slows tasks."}, "questions": {"What is speculative execution?": "Launching a duplicate task on another worker if a task lags behind."}, "next_steps": "Open 05_jobs_stages_and_tasks.ipynb"})
    nb4.save(os.path.join(BASE_DIR, "04_task_scheduler.ipynb"))

    # 04_05: Jobs Stages Tasks
    nb5 = NotebookBuilder("Jobs, Stages, and Tasks: The Three Tiers of Execution", "04_execution_engine", "05_jobs_stages_and_tasks.ipynb", "Jobs_Stages_Tasks_Lab")
    nb5.add_header({"what": "The precise mathematical and operational relationships between Jobs, Stages, and Tasks.", "why": "Foundational mental model for all Spark performance debugging.", "problem": "Confusing job count with task count.", "real_world": "Capacity planning and Spark UI monitoring."})
    nb5.add_markdown("## 2. Hierarchy: 1 Action = 1 Job -> N Stages (at Shuffles) -> M Tasks (1 per partition)")
    nb5.add_session_init()
    nb5.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
print("Trigger Action:")
orders.groupBy("channel").count().show()
""")
    nb5.add_experiment({"title": "Tasks = Partitions Formula", "testing": "Verify tasks count equals input partitions count.", "expect": "Equal numbers.", "code": "df = orders.repartition(6); print('Partitions:', df.rdd.getNumPartitions())", "happened": "Stage ran exactly 6 tasks.", "why": "1 task processes 1 partition.", "takeaway": "To increase tasks in a stage, increase the partition count."})
    nb5.add_exercises({"b_title": "Count Tasks", "b_desc": "Determine tasks for 4 partitions.", "b_input": "4 partitions", "b_starter": "# B\n", "i_starter": "# I\n", "a_starter": "# A\n", "b_sol": "print('4 tasks')", "i_title": "Predict Stages", "i_desc": "Predict stages in join + groupBy.", "i_sol": "print('Typically 3 stages (2 map + 1 reduce/join)')", "a_title": "UI Verification", "a_desc": "Verify in Spark UI.", "a_sol": "print('Verified in localhost:4040/jobs')"})
    nb5.add_summary_and_teardown({"takeaways": {"Hierarchy": "Job -> Stage -> Task."}, "terms": {"Job": "Triggered by action.", "Stage": "Pipelined tasks.", "Task": "Thread on 1 partition."}, "mistakes": {"Assuming 1 task per core": "Tasks = Partitions; cores run them in waves."}, "questions": {"How many tasks run in a stage?": "Exactly as many as there are partitions in the stage's input."}, "next_steps": "Open 06_narrow_and_wide_transformations.ipynb"})
    nb5.save(os.path.join(BASE_DIR, "05_jobs_stages_and_tasks.ipynb"))

    # 04_06: Narrow vs Wide
    nb6 = NotebookBuilder("Narrow vs Wide Transformations: Shuffles and Stage Boundaries", "04_execution_engine", "06_narrow_and_wide_transformations.ipynb", "Narrow_Wide_Lab")
    nb6.add_header({"what": "Categorizing transformations as Narrow (map, filter) or Wide (groupBy, join, distinct, repartition) and observing network shuffles.", "why": "Wide transformations dictate execution time and network I/O in distributed systems.", "problem": "Unnecessary wide transformations destroy query scalability.", "real_world": "ETL pipeline performance optimization."})
    nb6.add_markdown("## 2. Narrow (1:1 partition) vs Wide (1:N or N:M shuffle)")
    nb6.add_session_init()
    nb6.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
print("=== Narrow Plan (No Exchange) ===")
orders.filter("quantity > 2").select("order_id").explain()

print("\\n=== Wide Plan (Contains Exchange) ===")
orders.groupBy("status").count().explain()
""")
    nb6.add_experiment({"title": "Exchange Node Detection", "testing": "Look for Exchange in physical plan.", "expect": "Present only in wide transformations.", "code": "plan = orders.distinct()._jdf.queryExecution().executedPlan().toString(); print('Contains Exchange?:', 'Exchange' in plan)", "happened": "Exchange node present.", "why": "distinct() requires a shuffle to deduplicate across partitions.", "takeaway": "Exchange in physical plan = network shuffle = stage boundary."})
    nb6.add_exercises({"b_title": "Classify Filter", "b_desc": "Is filter narrow or wide?", "b_input": "filter", "b_starter": "# B\n", "i_starter": "# I\n", "a_starter": "# A\n", "b_sol": "print('Narrow')", "i_title": "Classify Distinct", "i_desc": "Is distinct narrow or wide?", "i_sol": "print('Wide (Exchange node)')", "a_title": "Pipeline Optimization", "a_desc": "Filter before wide groupBy.", "a_sol": "orders.filter('quantity > 5').groupBy('channel').count().show()"})
    nb6.add_summary_and_teardown({"takeaways": {"Narrow": "In-memory pipelining, no network transfer.", "Wide": "Network shuffle, writes shuffle files to disk."}, "terms": {"Exchange": "Physical operator executing shuffle.", "Pipelining": "Chaining narrow steps."}, "mistakes": {"Assuming distinct() is free": "distinct() requires a full network shuffle."}, "questions": {"Why is narrow faster than wide?": "Narrow does not write to disk or transfer data over the network."}, "next_steps": "Open 07_dependencies_and_stage_boundaries.ipynb"})
    nb6.save(os.path.join(BASE_DIR, "06_narrow_and_wide_transformations.ipynb"))

    # 04_07: Dependencies and Stage Boundaries
    nb7 = NotebookBuilder("Dependencies and Stage Boundaries: The Scheduler Algorithm", "04_execution_engine", "07_dependencies_and_stage_boundaries.ipynb", "Dependencies_Lab")
    nb7.add_header({"what": "The exact algorithm DAGScheduler uses to traverse dependencies backward and split stages.", "why": "Deep mastery of stage boundary creation enables precise execution prediction.", "problem": "Unexpected stage creation slowing queries.", "real_world": "Senior Data Engineering technical interviews."})
    nb7.add_markdown("## 2. The Stage Boundary Algorithm: Traverse backward from terminal RDD, split at ShuffleDependency.")
    nb7.add_session_init()
    nb7.add_code("""rdd = spark.sparkContext.parallelize(range(100), 2).map(lambda x: (x%5, x)).reduceByKey(lambda a, b: a+b)
print("Lineage tree showing ShuffledRDD:")
print(rdd.toDebugString().decode('utf-8'))
""")
    nb7.add_experiment({"title": "Verifying Stage Split Algorithm", "testing": "Check stage count in Spark UI.", "expect": "2 stages.", "code": "print('Count result:', rdd.count())", "happened": "Result computed across 2 stages.", "why": "reduceByKey introduced ShuffleDependency.", "takeaway": "ShuffleDependency is the sole trigger for stage boundary creation."})
    nb7.add_exercises({"b_title": "Check Dependencies", "b_desc": "Inspect dependencies array.", "b_input": "rdd.dependencies", "b_starter": "# B\n", "i_starter": "# I\n", "a_starter": "# A\n", "b_sol": "print([type(d).__name__ for d in rdd.dependencies])", "i_title": "Count Shuffles", "i_desc": "Count shuffles in complex pipeline.", "i_sol": "print('Count ShuffleDependencies')", "a_title": "Verify in UI", "a_desc": "Check UI stages graph.", "a_sol": "print('Inspected stage DAG in localhost:4040')"})
    nb7.add_summary_and_teardown({"takeaways": {"Algorithm": "Backward traversal splitting at ShuffleDependency.", "Stages": "ShuffleMapStage followed by downstream stage."}, "terms": {"ShuffleDependency": "Triggers stage boundary.", "OneToOneDependency": "Continues stage."}, "mistakes": {"Thinking each transformation is a stage": "Only wide dependencies create stages."}, "questions": {"How does DAGScheduler decide stage boundaries?": "By traversing the RDD lineage backwards from the action and cutting a new stage boundary at every ShuffleDependency."}, "next_steps": "Module 04 complete! Proceed to Module 05: Partitions and Shuffle Internals."})
    nb7.save(os.path.join(BASE_DIR, "07_dependencies_and_stage_boundaries.ipynb"))

if __name__ == '__main__':
    gen_04_01()
    gen_04_02()
    gen_04_03_to_07()
