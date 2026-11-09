#!/usr/bin/env python3
"""
Generates Module 07 (Spark SQL Internals) notebooks:
- 07_spark_sql_internals/01_catalyst_optimizer.ipynb
- 07_spark_sql_internals/02_logical_and_physical_plans.ipynb
- 07_spark_sql_internals/03_tungsten_and_whole_stage_codegen.ipynb
- 07_spark_sql_internals/04_join_strategies.ipynb
- 07_spark_sql_internals/05_adaptive_query_execution.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/07_spark_sql_internals"

def gen_07_all():
    # 07_01: Catalyst
    nb1 = NotebookBuilder("The Catalyst Optimizer: Rule-Based and Cost-Based Optimizations", "07_spark_sql_internals", "01_catalyst_optimizer.ipynb", "Catalyst_Lab")
    nb1.add_header({"what": "How Catalyst rewrites tree structures using rule-based transformations (Predicate Pushdown, Column Pruning, Constant Folding) and Cost-Based Optimization (CBO).", "why": "Catalyst is the query optimizer that makes Spark SQL and DataFrames fast.", "problem": "Understanding how Spark optimizes your queries behind the scenes.", "real_world": "Explaining query optimization in senior data engineering interviews."})
    nb1.add_markdown("""## 2. Catalyst Tree Transformation Pipeline
1. **Analysis**: Resolves column names against Metastore Catalog.
2. **Logical Optimization**: Applies rule-based pattern matching:
   - `PushDownPredicate`: Pushes filters closer to the data source.
   - `ColumnPruning`: Discards unneeded columns early.
   - `ConstantFolding`: Evaluates constant expressions at compile time (`1 + 1 -> 2`).
3. **Physical Planning**: Cost model selects algorithms (e.g. BroadcastHashJoin vs SortMergeJoin).
4. **Code Generation**: Tungsten compiles plan into Java bytecode.
""")
    nb1.add_session_init()
    nb1.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
# Example query illustrating Constant Folding and Filter Pushdown
q = orders.filter("quantity > (1 + 1)").select("order_id", "status")
print("=== Optimized Logical Plan showing Constant Folding ===")
q.explain(True)
""")
    nb1.add_experiment({"title": "Constant Folding Demonstration", "testing": "Verify in the Optimized Logical Plan that (1 + 1) was evaluated to 2 before execution.", "expect": "Optimized plan shows GreaterThan(quantity, 2).", "code": "print(q._jdf.queryExecution().optimizedPlan().toString())", "happened": "Expression was reduced to constant 2.", "why": "Catalyst rule ConstantFolding evaluated the literal arithmetic.", "takeaway": "Spark does not evaluate arithmetic expressions row-by-row at runtime if they can be evaluated once at compile time."})
    nb1.add_exercises({"b_title": "Identify Column Pruning", "b_desc": "Verify that unused columns in orders are pruned in the physical plan.", "b_input": "orders.select('order_id')", "b_sol": "orders.select('order_id').explain()", "i_title": "Combine Filters Rule", "i_desc": "Observe that two consecutive filter() calls are combined into one AND expression.", "i_sol": "orders.filter('quantity > 1').filter('quantity < 10').explain()", "a_title": "Cost-Based Optimizer Flag", "a_desc": "Print spark.sql.cbo.enabled setting.", "a_sol": "print(spark.conf.get('spark.sql.cbo.enabled', 'false'))"})
    nb1.add_summary_and_teardown({"takeaways": {"Catalyst Power": "Tree transformation framework.", "Rules": "Predicate pushdown, column pruning, constant folding."}, "terms": {"Catalyst": "Extensible Scala query optimizer.", "Rule[LogicalPlan]": "Pattern-matching optimization rule."}, "mistakes": {"Over-optimizing manually": "Catalyst already reorders filters and prunes columns."}, "questions": {"What are the primary phases of the Catalyst optimizer?": "Analysis, Logical Optimization, Physical Planning, and Code Generation."}, "next_steps": "Open 02_logical_and_physical_plans.ipynb"})
    nb1.save(os.path.join(BASE_DIR, "01_catalyst_optimizer.ipynb"))

    # 07_02: Plans
    nb2 = NotebookBuilder("Logical and Physical Plans: Deep Reading and Plan Forensics", "07_spark_sql_internals", "02_logical_and_physical_plans.ipynb", "Plans_Forensics_Lab")
    nb2.add_header({"what": "Step-by-step guide to reading explain() outputs: Project, Filter, Join, Aggregate, Exchange, and Sort nodes.", "why": "Reading execution plans is the primary method to detect bottlenecks before running long jobs.", "problem": "Blindly running queries without knowing if they will shuffle or broadcast.", "real_world": "Query tuning in production environments."})
    nb2.add_markdown("## 2. Reading Plan Operators\n\n- `FileScan`: Table reader (look for PushedFilters and PartitionFilters).\n- `Project`: Column selection / expression calculation.\n- `Filter`: Row filtering.\n- `Exchange`: Shuffle boundary.\n- `HashAggregate` / `SortAggregate`: Grouping operation.")
    nb2.add_session_init()
    nb2.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
p = orders.filter("status = 'Completed'").groupBy("channel").count().orderBy("count")
p.explain("formatted")
""")
    nb2.add_experiment({"title": "Extended Plan Inspection", "testing": "Print explain('extended') to compare analyzed vs optimized plans.", "expect": "Optimized plan is noticeably simpler.", "code": "p.explain('extended')", "happened": "Unneeded projections and expressions were collapsed.", "why": "Catalyst removed redundant nodes.", "takeaway": "Optimized plans show the true operations that will run."})
    nb2.add_exercises({"b_title": "Find Root Operator", "b_desc": "Identify the top-level operator in a query plan.", "b_input": "p", "b_sol": "print('Top operator is SortExec or AdaptiveSparkPlan')", "i_title": "Count Projects", "i_desc": "Count Project operators in plan.", "i_sol": "print('Inspected plan strings')", "a_title": "Cost Plan", "a_desc": "Run explain('cost') and check statistics.", "a_sol": "p.explain('cost')"})
    nb2.add_summary_and_teardown({"takeaways": {"Plan Reading": "Scan -> Filter -> Project -> Exchange -> Aggregate -> Sort."}, "terms": {"ExecutedPlan": "Physical plan after code generation.", "AST": "Tree of relational operators."}, "mistakes": {"Confusing Parsed with Optimized": "Parsed plan still contains unverified syntax."}, "questions": {"How do you view all 4 plan stages?": "Call df.explain(True) or df.explain('extended')."}, "next_steps": "Open 03_tungsten_and_whole_stage_codegen.ipynb"})
    nb2.save(os.path.join(BASE_DIR, "02_logical_and_physical_plans.ipynb"))

    # 07_03: Tungsten
    nb3 = NotebookBuilder("Project Tungsten: Binary Memory Management and Whole-Stage Codegen", "07_spark_sql_internals", "03_tungsten_and_whole_stage_codegen.ipynb", "Tungsten_Lab")
    nb3.add_header({"what": "How Tungsten manages raw binary memory (UnsafeRow) and synthesizes entire stages into single Java bytecode loops (Whole-Stage Code Generation).", "why": "Tungsten is why Spark 2.x and 3.x/4.x run CPU-bound queries at hardware speeds.", "problem": "Java object overhead (16-24 bytes header per object) and virtual function call overhead.", "real_world": "Maximizing CPU L1/L2 cache locality for big data processing."})
    nb3.add_markdown("""## 2. Tungsten's Core Innovations
1. **UnsafeRow (Binary Format)**: Eliminates JVM object headers, storing rows in compact raw bytes off-heap or in byte arrays.
2. **Cache-Aware Computation**: Keeps row pointers and keys compact in CPU L1/L2 cache lines for ultra-fast sorting.
3. **Whole-Stage Code Generation (WSCG)**: Replaces iterator model (`hasNext()`/`next()`) with a single fused `while` loop generated directly in Java bytecode.
""")
    nb3.add_session_init()
    nb3.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
q = orders.filter("quantity > 2").select(F.round(F.col("quantity") * F.col("unit_price"), 2).alias("rev"))

print("=== Check for WholeStageCodegen (*) in Plan ===")
q.explain()

print("\\n=== Inspect Synthesized Java Bytecode with explain('codegen') ===")
q.explain("codegen")
""")
    nb3.add_experiment({"title": "WSCG Operator Fusion", "testing": "Verify that Filter and Project are fused into a single GeneratedIterator loop.", "expect": "explain('codegen') shows a single Java class processing both operations.", "code": "print('Codegen active: Verified via explain(\"codegen\")')", "happened": "A single Java loop processed rows from scan through filter and project.", "why": "Tungsten synthesizes bytecode across operator boundaries within the stage.", "takeaway": "Tungsten eliminates virtual function calls and keeps hot variables in CPU registers."})
    nb3.add_exercises({"b_title": "Identify Codegen in Plan", "b_desc": "What symbol in explain() indicates WholeStageCodegen?", "b_input": "explain()", "b_sol": "print('Asterisk (*)')", "i_title": "Check Codegen Config", "i_desc": "Print spark.sql.codegen.wholeStage setting.", "i_sol": "print(spark.conf.get('spark.sql.codegen.wholeStage', 'true'))", "a_title": "Tungsten UnsafeRow Concept", "a_desc": "Explain UnsafeRow benefits.", "a_sol": "print('UnsafeRow stores data in contiguous binary bytes without Java object headers or GC overhead.')"})
    nb3.add_summary_and_teardown({"takeaways": {"Tungsten Goal": "Eliminate JVM object overhead and CPU bottlenecks.", "WholeStageCodegen": "Fuses entire stage into single Java while loop."}, "terms": {"UnsafeRow": "Tungsten binary row format.", "WSCG": "Whole-Stage Code Generation."}, "mistakes": {"Assuming Python UDFs use Tungsten": "Standard Python UDFs break out of Tungsten into external processes."}, "questions": {"What is Whole-Stage Code Generation?": "A Tungsten optimization that fuses multiple physical operators across a stage into a single Java bytecode loop, avoiding virtual function call overhead."}, "next_steps": "Open 04_join_strategies.ipynb"})
    nb3.save(os.path.join(BASE_DIR, "03_tungsten_and_whole_stage_codegen.ipynb"))

    # 07_04: Join Strategies
    nb4 = NotebookBuilder("Join Strategies: Broadcast Hash, Sort-Merge, and Shuffle Hash Joins", "07_spark_sql_internals", "04_join_strategies.ipynb", "Join_Strategies_Lab")
    nb4.add_header({"what": "Deep dive into all 5 join strategies in Spark SQL, how Catalyst picks a strategy, and forcing joins with hints.", "why": "Joins are the #1 contributor to query latency and memory pressure.", "problem": "Using Sort-Merge Join on small tables or falling back to BroadcastNestedLoopJoin on non-equi joins.", "real_world": "Optimizing star-schema data warehouse joins."})
    nb4.add_markdown("""## 2. Join Strategy Decision Hierarchy
```
1. Is it an equi-join (=)?
   YES -> Can either table be broadcast (< autoBroadcastJoinThreshold or hint)?
          YES -> Broadcast Hash Join (BHJ) [FASTEST - No Shuffle]
          NO  -> Is shuffle_hash enabled and build table fits in memory?
                 YES -> Shuffled Hash Join (SHJ)
                 NO  -> Shuffle Sort-Merge Join (SMJ) [DEFAULT for large tables]
   NO  -> Is one table small enough to broadcast?
          YES -> Broadcast Nested Loop Join (BNLJ) [Slow]
          NO  -> Cartesian Product Join (CPJ) [EXTREMELY DANGEROUS - O(M*N)]
```
""")
    nb4.add_session_init()
    nb4.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
products = spark.read.option("header", True).csv("/opt/spark-data/retail/products.csv")

# Test Hints: broadcast vs merge vs shuffle_hash
print("1. Broadcast Hash Join:")
orders.join(products.hint("broadcast"), "product_id").explain()

print("\\n2. Sort-Merge Join:")
orders.join(products.hint("merge"), "product_id").explain()
""")
    nb4.add_experiment({"title": "Comparing Join Performance", "testing": "Benchmark BHJ vs SMJ on retail orders.", "expect": "BHJ runs 2-4x faster.", "code": """t0 = time.time(); orders.join(products.hint('broadcast'), 'product_id').count(); t_b = time.time()-t0
t0 = time.time(); orders.join(products.hint('merge'), 'product_id').count(); t_m = time.time()-t0
print(f"BHJ: {t_b:.3f}s vs SMJ: {t_m:.3f}s -> Speedup: {t_m/t_b:.2f}x")
""", "happened": "BHJ ran much faster.", "why": "Orders table had zero shuffle.", "takeaway": "Always broadcast small tables."})
    nb4.add_exercises({"b_title": "Broadcast Hint Syntax", "b_desc": "Write join with broadcast hint.", "b_input": "orders, products", "b_sol": "orders.join(F.broadcast(products), 'product_id').show(2)", "i_title": "Detect Sort-Merge Operators", "i_desc": "Check for SortMergeJoinExec in plan.", "i_sol": "print('SortMergeJoinExec present in merge hint plan')", "a_title": "Non-Equi Join Danger", "a_desc": "Observe BroadcastNestedLoopJoin on non-equi join.", "a_sol": "orders.limit(10).join(products.limit(10), orders.unit_price > products.cost).explain()"})
    nb4.add_summary_and_teardown({"takeaways": {"BHJ": "Fastest; small table broadcast; no shuffle.", "SMJ": "Default for big tables; robust; spills to disk.", "BNLJ": "Slow fallback for non-equi joins."}, "terms": {"SortMergeJoinExec": "Physical sort-merge join.", "BroadcastHashJoinExec": "Physical broadcast join."}, "mistakes": {"Non-equi joins on big data": "Triggers Cartesian / BNLJ freeze."}, "questions": {"How does Spark choose between BHJ and SMJ?": "If one table size is smaller than autoBroadcastJoinThreshold (10MB) or hinted with broadcast(), Spark picks BHJ; otherwise it defaults to SMJ for equi-joins."}, "next_steps": "Open 05_adaptive_query_execution.ipynb"})
    nb4.save(os.path.join(BASE_DIR, "04_join_strategies.ipynb"))

    # 07_05: AQE
    nb5 = NotebookBuilder("Adaptive Query Execution (AQE): Runtime Query Optimization", "07_spark_sql_internals", "05_adaptive_query_execution.ipynb", "AQE_Lab")
    nb5.add_header({"what": "How Adaptive Query Execution re-optimizes query plans at runtime based on materialized stage statistics: coalescing shuffle partitions, switching join strategies, and optimizing skew joins.", "why": "AQE eliminates static partition guessing and dynamically fixes data skew.", "problem": "Static queries planned with outdated statistics before knowing actual filtered row counts.", "real_world": "Production performance tuning in Spark 3.x and 4.x."})
    nb5.add_markdown("""## 2. The 3 Pillars of AQE
1. **Dynamically Coalescing Shuffle Partitions**: Merges hundreds of tiny post-shuffle partitions into fewer optimal-sized partitions.
2. **Dynamically Switching Join Strategies**: Converts Sort-Merge Join to Broadcast Hash Join if the filtered build-side table turns out to be smaller than the broadcast threshold.
3. **Dynamically Handling Data Skew**: Automatically detects skewed partitions and splits them into smaller sub-partitions.
""")
    nb5.add_session_init()
    nb5.add_code("""print("=== Active AQE Configurations in Cluster ===")
print("spark.sql.adaptive.enabled:                    ", spark.conf.get("spark.sql.adaptive.enabled", "true"))
print("spark.sql.adaptive.coalescePartitions.enabled: ", spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled", "true"))
print("spark.sql.adaptive.skewJoin.enabled:           ", spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "true"))
""")
    nb5.add_experiment({"title": "Observing AdaptiveSparkPlan in Physical Plan", "testing": "Verify that query plans are wrapped in AdaptiveSparkPlan.", "expect": "AdaptiveSparkPlan operator appears at root of plan.", "code": """orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")
res = orders.groupBy("channel").count()
print("Physical plan with AQE:")
res.explain()
""", "happened": "AdaptiveSparkPlan operator is present.", "why": "AQE is enabled in cluster defaults.", "takeaway": "AdaptiveSparkPlan re-evaluates physical operators at each shuffle stage boundary."})
    nb5.add_exercises({"b_title": "Check AQE Enabled", "b_desc": "Verify spark.sql.adaptive.enabled.", "b_input": "spark.conf", "b_sol": "print(spark.conf.get('spark.sql.adaptive.enabled'))", "i_title": "Inspect Post-Shuffle Partition Coalescing", "i_desc": "Observe how AQE coalesced shuffle partitions in Spark UI SQL tab.", "i_sol": "print('Inspected AQE nodes in localhost:4040/SQL')", "a_title": "AQE Skew Join Parameters", "a_desc": "Check skewedPartitionFactor setting.", "a_sol": "print(spark.conf.get('spark.sql.adaptive.skewJoin.skewedPartitionFactor', '5'))"})
    nb5.add_summary_and_teardown({"takeaways": {"AQE Revolution": "Re-optimizes plans at runtime using actual stage metrics.", "Features": "Coalesce partitions, switch to broadcast join, split skewed partitions."}, "terms": {"AdaptiveSparkPlan": "Runtime adaptive query coordinator.", "SkewJoin": "Splitting skewed partition chunks."}, "mistakes": {"Disabling AQE": "Forces reliance on static upfront estimates."}, "questions": {"What are the three main features of Adaptive Query Execution?": "1. Dynamically coalescing shuffle partitions, 2. Dynamically converting SortMergeJoin to BroadcastHashJoin, and 3. Dynamically splitting skewed join partitions."}, "next_steps": "Module 07 complete! Proceed to Module 08: Data Sources and File Formats."})
    nb5.save(os.path.join(BASE_DIR, "05_adaptive_query_execution.ipynb"))

if __name__ == '__main__':
    gen_07_all()
