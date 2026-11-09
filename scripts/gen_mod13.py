#!/usr/bin/env python3
"""
Generates Module 13 (End-to-End Projects) notebooks:
- 13_end_to_end_projects/01_batch_etl_pipeline.ipynb
- 13_end_to_end_projects/02_large_scale_sales_analytics.ipynb
- 13_end_to_end_projects/03_data_quality_pipeline.ipynb
- 13_end_to_end_projects/04_streaming_analytics_pipeline.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/13_end_to_end_projects"

def gen_13_all():
    # Project 1: Batch ETL Pipeline
    nb1 = NotebookBuilder("Project 1: Production Batch ETL Pipeline (CSV/JSON to Partitioned Parquet Lake)", "13_end_to_end_projects", "01_batch_etl_pipeline.ipynb", "Batch_ETL_Project")
    nb1.add_header({
        "what": "Building a full end-to-end production batch ETL pipeline: schema validation, extraction from CSV & JSON, deduplication, enrichment joins, metric aggregation, and writing to a partitioned Parquet data lake.",
        "why": "Integrates all core skills (schemas, cleaning, joins, partitioning, and writing) into an industry-standard Data Engineering project.",
        "problem": "Raw data arriving in disparate formats with missing values, dirty strings, and unindexed tables.",
        "real_world": "Core responsibility of every Data Engineer in modern enterprise data teams."
    })
    nb1.add_markdown("""## 2. Project Architecture Diagram
```
[ Raw CSV: orders.csv ] --------+
                                |
[ Raw CSV: customers.csv ] ------> [ Extract & Validate Schemas ]
                                |                |
[ Raw JSON: order_events.json ]-+                v
                                    [ Clean, Deduplicate & Impute Nulls ]
                                                 |
                                                 v
                                    [ Broadcast & Star-Schema Joins ]
                                                 |
                                                 v
                                    [ Derived Revenue Metrics & Aggs ]
                                                 |
                                                 v
                                    [ Load: Partitioned Parquet Lake ]
                                    (year=YYYY / month=MM / status=...)
```
""")
    nb1.add_session_init()
    nb1.add_markdown("""## 3. Step 1: Explicit Schema Extraction""")
    nb1.add_code("""orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("order_ts", TimestampType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("discount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True)
])

orders_raw = spark.read.option("header", True).schema(orders_schema).csv("/opt/spark-data/retail/orders.csv")
customers_raw = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/customers.csv")
products_raw = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/products.csv")

print(f"Extracted: {orders_raw.count():,} orders, {customers_raw.count():,} customers, {products_raw.count():,} products.")
""")
    nb1.add_markdown("""## 4. Step 2: Data Cleaning, Deduplication, and Normalization""")
    nb1.add_code("""# Deduplicate and handle nulls
orders_clean = (
    orders_raw
    .dropDuplicates(["order_id"])
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") > 0.0)
    .na.fill({"discount": 0.0, "status": "Pending", "channel": "Unknown"})
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("gross_rev", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("net_rev", F.round(F.col("gross_rev") * (1.0 - F.col("discount")), 2))
)

print(f"Cleaned orders count: {orders_clean.count():,}")
orders_clean.show(3)
""")
    nb1.add_markdown("""## 5. Step 3: Star-Schema Broadcast Enrichment Joins""")
    nb1.add_code("""# Broadcast small dimension tables (products and customers)
enriched = (
    orders_clean
    .join(F.broadcast(products_raw.select("product_id", "product_name", "category")), on="product_id", how="left")
    .join(F.broadcast(customers_raw.select("customer_id", "country", "region", "segment", "loyalty_tier")), on="customer_id", how="left")
)

enriched.show(3)
""")
    nb1.add_markdown("""## 6. Step 4: Loading into Partitioned Parquet Lakehouse""")
    nb1.add_code("""lake_output = "/opt/spark-data/warehouse/project_batch_lake"

(
    enriched
    .coalesce(4)
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(lake_output)
)

print("ETL Batch Pipeline successfully loaded into data lake!")
# Verify roundtrip read
read_back = spark.read.parquet(lake_output)
print(f"Verified rows written: {read_back.count():,}")
""")
    nb1.add_experiment({
        "title": "Verifying Partition Directory Structure on Disk",
        "testing": "Inspect the generated file tree on disk to confirm year=YYYY/month=MM partitioning.",
        "expect": "Nested year and month subdirectories containing Parquet files.",
        "code": """dirs = [d for d in os.listdir(lake_output) if d.startswith("year=")]
print("Root partition folders in data lake:", dirs[:4])
""",
        "happened": "Parquet files were successfully segregated into year and month directories.",
        "why": "partitionBy('year', 'month') configured the directory layout.",
        "takeaway": "Partitioned data lakes allow downstream consumers to query specific time slices with zero full scans."
    })
    nb1.add_exercises({
        "b_title": "Calculate Aggregated Regional Revenue",
        "b_desc": "Using read_back, compute total net revenue by region.",
        "b_input": "read_back",
        "b_sol": "read_back.groupBy('region').agg(F.round(F.sum('net_rev'), 2).alias('total')).show()",
        "i_title": "Verify No Null Keys",
        "i_desc": "Assert that no order_id or customer_id is null in the lake output.",
        "i_sol": "assert read_back.filter('order_id IS NULL OR customer_id IS NULL').count() == 0; print('Assertion passed!')",
        "a_title": "Incremental Ingestion Pattern",
        "a_desc": "Explain how to run this job incrementally with Dynamic Partition Overwrite.",
        "a_sol": "print('Set partitionOverwriteMode=dynamic to overwrite only the specific year/month partitions in the incoming batch.')"
    })
    nb1.add_summary_and_teardown({
        "takeaways": {
            "ETL Blueprint": "Extract with schema -> Clean/Deduplicate -> Broadcast Join -> Aggregate -> Partitioned Write.",
            "Production Parquet": "Columnar format with Snappy compression and partitioned directories."
        },
        "terms": {
            "ETL": "Extract, Transform, Load.",
            "Star-Schema": "Fact table surrounded by dimension tables."
        },
        "mistakes": {
            "Omitting deduplication": "Causes duplicate analytics and inaccurate financial reporting.",
            "Writing unpartitioned outputs": "Slows downstream queries."
        },
        "questions": {
            "What is the advantage of broadcast joins in an ETL pipeline?": "They eliminate network shuffles of the massive fact table by distributing the dimension table to every executor once."},
        "next_steps": "Open 02_large_scale_sales_analytics.ipynb for advanced analytical windowing."
    })
    nb1.save(os.path.join(BASE_DIR, "01_batch_etl_pipeline.ipynb"))

    # Project 2: Sales Analytics
    nb2 = NotebookBuilder("Project 2: Large-Scale Sales Analytics & Windowed Business Intelligence", "13_end_to_end_projects", "02_large_scale_sales_analytics.ipynb", "Sales_Analytics_Project")
    nb2.add_header({
        "what": "Comprehensive sales analytics: monthly revenue trajectories, customer lifetime value (LTV), year-over-year growth, product ranking, and channel performance.",
        "why": "Demonstrates the power of advanced DataFrame transformations, joins, and Window functions on realistic enterprise retail datasets.",
        "problem": "Answering multi-dimensional executive dashboard questions efficiently at scale.",
        "real_world": "Powering executive dashboards and BI reporting models."
    })
    nb2.add_session_init()
    nb2.add_markdown("## 3. Extracting and Enriching Sales Dataset")
    nb2.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
customers = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/customers.csv")
products = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/products.csv")

# Denormalize
sales = (
    orders
    .filter("status = 'Completed'")
    .join(products, "product_id")
    .join(customers, "customer_id")
    .withColumn("order_month", F.date_format("order_ts", "yyyy-MM"))
    .withColumn("order_val", F.round(F.col("quantity") * F.col("unit_price") * (1.0 - F.col("discount")), 2))
)

sales.show(3)
""")
    nb2.add_markdown("## 4. Analysis 1: Top 5 Best-Selling Products per Category via DenseRank")
    nb2.add_code("""from pyspark.sql.window import Window

cat_win = Window.partitionBy("category").orderBy(F.desc("total_units"))

top_products = (
    sales
    .groupBy("category", "product_name")
    .agg(F.sum("quantity").alias("total_units"), F.round(F.sum("order_val"), 2).alias("total_rev"))
    .withColumn("rank", F.dense_rank().over(cat_win))
    .filter("rank <= 3")
    .orderBy("category", "rank")
)

top_products.show(15, truncate=False)
""")
    nb2.add_markdown("## 5. Analysis 2: Customer Lifetime Value (LTV) and Loyalty Tier Breakdown")
    nb2.add_code("""ltv_summary = (
    sales
    .groupBy("loyalty_tier", "segment")
    .agg(
        F.countDistinct("customer_id").alias("customer_count"),
        F.round(F.sum("order_val"), 2).alias("total_tier_revenue"),
        F.round(F.avg("order_val"), 2).alias("avg_order_value")
    )
    .orderBy(F.desc("total_tier_revenue"))
)

ltv_summary.show()
""")
    nb2.add_experiment({
        "title": "Month-over-Month Revenue Growth with Lag()",
        "testing": "Compute monthly revenue and month-over-month growth percentage using F.lag().",
        "expect": "Window function computes difference from prior month without self-join.",
        "code": """monthly_rev = sales.groupBy("order_month").agg(F.round(F.sum("order_val"), 2).alias("revenue")).orderBy("order_month")
w_time = Window.orderBy("order_month")

mom_growth = (
    monthly_rev
    .withColumn("prev_month_rev", F.lag("revenue", 1).over(w_time))
    .withColumn("mom_growth_pct", F.round(((F.col("revenue") - F.col("prev_month_rev")) / F.col("prev_month_rev")) * 100, 2))
)

mom_growth.show(12)
""",
        "happened": "MoM growth calculated sequentially across order months.",
        "why": "Window lag offset retrieved previous row values in sorted stream.",
        "takeaway": "Window lag() eliminates expensive self-joins in time-series growth queries."
    })
    nb2.add_exercises({
        "b_title": "Channel Revenue Share",
        "b_desc": "Calculate total revenue and percentage share per channel.",
        "b_input": "sales",
        "b_sol": "tot = sales.select(F.sum('order_val')).collect()[0][0]\nsales.groupBy('channel').agg(F.round(F.sum('order_val'), 2).alias('rev')).withColumn('pct', F.round(F.col('rev')/tot*100, 2)).show()",
        "i_title": "Regional Sales Pivot",
        "i_desc": "Pivot completed sales by region across product category.",
        "i_sol": "sales.groupBy('region').pivot('category').agg(F.round(F.sum('order_val'), 2)).show()",
        "a_title": "Customer Repeat Purchase Rate",
        "a_desc": "Calculate percentage of customers with > 1 order.",
        "a_sol": "orders_per_cust = sales.groupBy('customer_id').count()\nrepeat_custs = orders_per_cust.filter('count > 1').count()\nprint(f'Repeat rate: {repeat_custs / orders_per_cust.count() * 100:.2f}%')"
    })
    nb2.add_summary_and_teardown({
        "takeaways": {
            "Analytical Mastery": "Window functions handle complex rankings and time offsets efficiently.",
            "LTV Modeling": "Aggregations across dimensions reveal customer purchase behavior."
        },
        "terms": {
            "LTV": "Customer Lifetime Value.",
            "MoM Growth": "Month-over-month percentage change."
        },
        "mistakes": {
            "Self-joins for time series": "Slow and error-prone; always prefer Window lag()."},
        "questions": {
            "How do Window functions outperform self-joins for ranking?": "Window functions evaluate partitions in a single streaming pass without duplicating datasets or creating Cartesian comparisons."},
        "next_steps": "Open 03_data_quality_pipeline.ipynb for automated data quality gates."
    })
    nb2.save(os.path.join(BASE_DIR, "02_large_scale_sales_analytics.ipynb"))

    # Project 3: Data Quality Pipeline
    nb3 = NotebookBuilder("Project 3: Production Data Quality Gate and Automated Audit Pipeline", "13_end_to_end_projects", "03_data_quality_pipeline.ipynb", "Data_Quality_Project")
    nb3.add_header({
        "what": "Building an automated data quality pipeline: schema conformance, null auditing, uniqueness verification, range checks, foreign key integrity, and generating an automated data health scorecard.",
        "why": "Garbage in, garbage out. Production data platforms must halt bad data at ingestion gates.",
        "problem": "Handling corrupt records in /opt/spark-data/quality/orders_dirty.csv without crashing the job.",
        "real_world": "Implementing Great Expectations / Deequ style data quality gates."
    })
    nb3.add_session_init()
    nb3.add_markdown("## 3. Reading Dirty Data and Defining Quality Constraints")
    nb3.add_code("""dirty_df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/quality/orders_dirty.csv")
print(f"Total input rows in audit pool: {dirty_df.count():,}")
dirty_df.show(3)
""")
    nb3.add_markdown("## 4. Rule-Based Record Categorization (Valid vs Quarantined)")
    nb3.add_code("""# Define quality checks
# Rule 1: order_id is not null and positive
# Rule 2: customer_id is not null
# Rule 3: quantity > 0 and unit_price > 0
# Rule 4: discount between 0.0 and 1.0

flagged_df = (
    dirty_df
    .withColumn("is_null_id", F.col("order_id").isNull() | F.col("customer_id").isNull())
    .withColumn("is_invalid_qty", F.col("quantity").isNull() | (F.col("quantity") <= 0))
    .withColumn("is_invalid_price", F.col("unit_price").isNull() | (F.col("unit_price") <= 0))
    .withColumn("is_invalid_discount", F.col("discount").isNull() | (F.col("discount") < 0.0) | (F.col("discount") > 1.0))
    .withColumn("is_valid", ~(F.col("is_null_id") | F.col("is_invalid_qty") | F.col("is_invalid_price") | F.col("is_invalid_discount")))
)

valid_records = flagged_df.filter("is_valid = True").drop("is_null_id", "is_invalid_qty", "is_invalid_price", "is_invalid_discount", "is_valid")
quarantine_records = flagged_df.filter("is_valid = False")

print(f"Valid Clean Records:     {valid_records.count():,}")
print(f"Quarantined Bad Records: {quarantine_records.count():,}")
""")
    nb3.add_experiment({
        "title": "Generating Automated Data Quality Scorecard",
        "testing": "Aggregate metrics across all quality rules into an executive summary report.",
        "expect": "A structured DataFrame reporting error counts and pass percentages per rule.",
        "code": """total_rows = dirty_df.count()

scorecard = flagged_df.select(
    F.lit(total_rows).alias("total_records"),
    F.sum(F.col("is_null_id").cast("int")).alias("failed_null_ids"),
    F.sum(F.col("is_invalid_qty").cast("int")).alias("failed_quantity_check"),
    F.sum(F.col("is_invalid_price").cast("int")).alias("failed_price_check"),
    F.sum(F.col("is_invalid_discount").cast("int")).alias("failed_discount_check"),
    F.sum(F.col("is_valid").cast("int")).alias("passed_records")
)

print("=== DATA QUALITY SCORECARD ===")
scorecard.show(vertical=True)
""",
        "happened": "Generated complete data quality audit report.",
        "why": "Aggregated rule flags into summary counters.",
        "takeaway": "Always publish data quality scorecards to monitoring systems (Datadog/Prometheus) to alert on ingestion anomalies."
    })
    nb3.add_exercises({
        "b_title": "Write Quarantine Table",
        "b_desc": "Write quarantined records to /opt/spark-data/warehouse/quarantine_orders.",
        "b_input": "quarantine_records",
        "b_sol": "quarantine_records.write.mode('overwrite').parquet('/opt/spark-data/warehouse/quarantine_orders')",
        "i_title": "Referential Integrity Check",
        "i_desc": "Check if customer_ids in dirty_df exist in customers.csv.",
        "i_sol": "custs = spark.read.option('header', True).csv('/opt/spark-data/retail/customers.csv')\norphan_orders = dirty_df.join(custs, 'customer_id', 'left_anti')\nprint('Orphan orders count:', orphan_orders.count())",
        "a_title": "Automated Quality Threshold Gate",
        "a_desc": "Raise an exception if pass rate is below 90%.",
        "a_sol": "pass_rate = valid_records.count() / total_rows\nassert pass_rate >= 0.90, f'Quality threshold failed! Pass rate: {pass_rate:.2%}'\nprint('Quality gate passed!')"
    })
    nb3.add_summary_and_teardown({
        "takeaways": {
            "Quality Gates": "Quarantine bad rows without crashing the pipeline.",
            "Scorecarding": "Quantify data quality metrics across batch runs."
        },
        "terms": {
            "Quarantine": "Isolating corrupt records into a separate dead-letter queue table.",
            "Data Quality Gate": "Automated threshold validation step."
        },
        "mistakes": {
            "Silently dropping bad data without audit": "Hides upstream data ingestion bugs."},
        "questions": {
            "How do you handle bad records without failing an entire batch job?": "Flag records using boolean rule expressions, split the DataFrame into valid and quarantine subsets, write valid rows to the lake, and write quarantine rows to a dead-letter queue with audit metadata."},
        "next_steps": "Open 04_streaming_analytics_pipeline.ipynb for real-time event analytics."
    })
    nb3.save(os.path.join(BASE_DIR, "03_data_quality_pipeline.ipynb"))

    # Project 4: Streaming Analytics Pipeline
    nb4 = NotebookBuilder("Project 4: Real-Time Event Streaming Analytics with Watermarking and Checkpoints", "13_end_to_end_projects", "04_streaming_analytics_pipeline.ipynb", "Streaming_Analytics_Project")
    nb4.add_header({
        "what": "Complete end-to-end real-time streaming pipeline: simulating incoming JSON event streams, tumbling window aggregations, watermarking, checkpointing, and failure recovery.",
        "why": "Real-time stream processing is a cornerstone of modern event-driven architectures.",
        "problem": "Handling streaming event ingestion without external Kafka dependencies in local Docker clusters.",
        "real_world": "Live operational telemetry and real-time revenue monitoring dashboards."
    })
    nb4.add_session_init()
    nb4.add_markdown("""## 3. Setting Up Directory Stream Paths and Synthetic Event Generator""")
    nb4.add_code("""import json

stream_input = "/opt/spark-data/streaming/events"
chk_path = "/opt/spark-data/warehouse/project_stream_chk"
out_path = "/opt/spark-data/warehouse/project_stream_out"

# Reset directories
os.system(f"rm -rf {stream_input}/* {chk_path} {out_path}")
os.makedirs(stream_input, exist_ok=True)

event_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_ts", StringType()),
    StructField("channel", StringType()),
    StructField("revenue", DoubleType())
])

# Read streaming files
events_stream = (
    spark.readStream
    .schema(event_schema)
    .json(stream_input)
    .withColumn("timestamp", F.to_timestamp("order_ts"))
    .withWatermark("timestamp", "10 seconds")
)

print("Streaming input table configured. isStreaming:", events_stream.isStreaming)
""")
    nb4.add_markdown("## 4. Real-Time Windowed Revenue Aggregations")
    nb4.add_code("""# Windowed revenue by channel over 5-second tumbling windows
windowed_rev = (
    events_stream
    .groupBy(
        F.window("timestamp", "5 seconds"),
        F.col("channel")
    )
    .agg(
        F.count("order_id").alias("order_count"),
        F.round(F.sum("revenue"), 2).alias("total_channel_revenue")
    )
)

# Start streaming query writing to memory sink
query = (
    windowed_rev
    .writeStream
    .format("memory")
    .queryName("live_sales_dashboard")
    .outputMode("complete")
    .start()
)

print(f"Streaming query started with Query ID: {query.id}")
""")
    nb4.add_experiment({
        "title": "Simulating Micro-Batch Event Ingestion and Live Querying",
        "testing": "Drop event files into stream_input and query the live_sales_dashboard table.",
        "expect": "Dashboard updates in real time as files are written.",
        "code": """# Simulate 2 event bursts
for batch_idx in range(1, 3):
    events = [
        {"order_id": 100 + batch_idx*10 + i, "order_ts": "2026-11-09 20:00:00", "channel": "Mobile" if i%2==0 else "Web", "revenue": 100.0 * i}
        for i in range(1, 4)
    ]
    with open(f"{stream_input}/batch_{batch_idx}.json", "w") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\\n")
    time.sleep(2)

query.processAllAvailable()

print("=== LIVE STREAMING SALES DASHBOARD ===")
spark.sql("SELECT window.start, window.end, channel, order_count, total_channel_revenue FROM live_sales_dashboard ORDER BY total_channel_revenue DESC").show(truncate=False)

query.stop()
print("Streaming pipeline completed and stopped.")
""",
        "happened": "Live streaming aggregation updated dynamically as new files landed.",
        "why": "Micro-batch query recomputed the windowed result table in Complete mode.",
        "takeaway": "Structured Streaming provides unified DataFrame syntax for both real-time event streams and batch files."
    })
    nb4.add_exercises({
        "b_title": "Check Query Last Progress",
        "b_desc": "Inspect query.lastProgress dictionary.",
        "b_input": "query",
        "b_sol": "print('query.lastProgress provides inputRowsPerSecond, processedRowsPerSecond, batchId.')",
        "i_title": "Parquet Streaming Sink",
        "i_desc": "Write a stream in Append mode to Parquet sink with checkpoint.",
        "i_sol": "print('events_stream.writeStream.format(\"parquet\").option(\"checkpointLocation\", chk).start(out)')",
        "a_title": "Stateful Fault Recovery",
        "a_desc": "Explain how restarting the query with the same checkpoint resumes state.",
        "a_sol": "print('Structured Streaming loads state store snapshots from state/ and offset WAL from offsets/.')"
    })
    nb4.add_summary_and_teardown({
        "takeaways": {
            "Unified Paradigm": "Batch and Streaming share the exact same DataFrame / SQL APIs.",
            "Watermarking & Checkpoints": "Mandatory for state bounding and exactly-once recovery."
        },
        "terms": {
            "Tumbling Window": "Fixed-duration non-overlapping time window.",
            "Complete Output Mode": "Emits full result table on every trigger."
        },
        "mistakes": {
            "Omitting Checkpoint in Production": "Stream cannot recover from crash without checkpoints."},
        "questions": {
            "What happens if a streaming query crashes and is restarted with the same checkpointLocation?": "It reads the offset log and state store from the checkpoint directory, resuming exactly where it left off with zero data loss or duplicate processing."},
        "next_steps": "Curriculum complete! Review documentation in docs/ and project README.md."
    })
    nb4.save(os.path.join(BASE_DIR, "04_streaming_analytics_pipeline.ipynb"))

if __name__ == '__main__':
    gen_13_all()
