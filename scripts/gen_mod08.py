#!/usr/bin/env python3
"""
Generates Module 08 (Data Sources & Formats) notebooks:
- 08_data_sources_and_formats/01_csv_and_json.ipynb
- 08_data_sources_and_formats/02_parquet_and_orc.ipynb
- 08_data_sources_and_formats/03_partitioned_data_lake.ipynb
- 08_data_sources_and_formats/04_schema_evolution.ipynb
- 08_data_sources_and_formats/05_reading_and_writing_data.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/08_data_sources_and_formats"

def gen_08_all():
    # 08_01: CSV & JSON
    nb1 = NotebookBuilder("Working with CSV and JSON: Options, Modes, and Nested Structures", "08_data_sources_and_formats", "01_csv_and_json.ipynb", "CSV_JSON_Lab")
    nb1.add_header({"what": "Reading and writing CSV and JSON files, handling read modes (PERMISSIVE, DROPMALFORMED, FAILFAST), and flattening nested JSON payloads.", "why": "CSV and JSON are ubiquitous raw ingestion formats in modern data platforms.", "problem": "Corrupted rows crashing pipelines or silent type mismatches.", "real_world": "Raw bronze-layer ingestion pipelines in data lakes."})
    nb1.add_markdown("## 2. Read Modes: PERMISSIVE (default) vs DROPMALFORMED vs FAILFAST")
    nb1.add_session_init()
    nb1.add_code("""# Read CSV with options
sales = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/sales.csv")
sales.show(3)

# Read nested JSON from retail/order_events.json
events = spark.read.json("/opt/spark-data/retail/order_events.json")
events.printSchema()
events.select("event_id", "order.order_id", "customer.country").show(3)
""")
    nb1.add_experiment({"title": "Testing FAILFAST on Corrupt CSV", "testing": "Attempt to read dirty CSV in FAILFAST mode.", "expect": "Spark throws an exception immediately on first bad row.", "code": """try:
    spark.read.option("header", True).option("mode", "FAILFAST").schema("order_id int, date date").csv("/opt/spark-data/quality/orders_dirty.csv").count()
except Exception as e:
    print(f"FAILFAST successfully threw error on malformed data: {type(e).__name__}")
""", "happened": "Threw exception upon parsing invalid dates/ints.", "why": "FAILFAST stops execution immediately.", "takeaway": "Use PERMISSIVE with columnNameOfCorruptRecord for resilient batch ingestion."})
    nb1.add_exercises({"b_title": "Read Multi-line JSON", "b_desc": "Set multiline option to true.", "b_input": "json", "b_sol": "print('spark.read.option(\"multiline\", True).json(path)')", "i_title": "Flatten Struct", "i_desc": "Flatten nested customer struct.", "i_sol": "events.select('customer.*').show(3)", "a_title": "Write JSON", "a_desc": "Write sales to JSON format.", "a_sol": "sales.write.mode('overwrite').json('/opt/spark-data/warehouse/sales_json')"})
    nb1.add_summary_and_teardown({"takeaways": {"Modes": "PERMISSIVE tracks bad rows; FAILFAST throws; DROPMALFORMED discards.", "JSON": "Natively parsed into StructType."}, "terms": {"PERMISSIVE": "Default parsing mode.", "columnNameOfCorruptRecord": "Captures raw bad string."}, "mistakes": {"Using text formats for petabyte analytics": "CSV/JSON lack columnar compression and predicate pushdown."}, "questions": {"What are the three read modes in Spark?": "PERMISSIVE, DROPMALFORMED, and FAILFAST."}, "next_steps": "Open 02_parquet_and_orc.ipynb"})
    nb1.save(os.path.join(BASE_DIR, "01_csv_and_json.ipynb"))

    # 08_02: Parquet & ORC
    nb2 = NotebookBuilder("Parquet and ORC: Columnar Storage, Compression, and Speedups", "08_data_sources_and_formats", "02_parquet_and_orc.ipynb", "Parquet_ORC_Lab")
    nb2.add_header({"what": "Why Parquet and ORC are the gold standard for big data analytics: columnar storage, row-group statistics, dictionary encoding, and predicate pushdown.", "why": "Switching from CSV to Parquet typically yields 5x-10x storage savings and 10x-100x query speedups.", "problem": "Inefficient full-table scans on uncompressed text formats.", "real_world": "Enterprise silver/gold lakehouse tables."})
    nb2.add_markdown("""## 2. Row vs Columnar Format
```
Row-Oriented (CSV):
[Row 0: 1001, 2026-01-03, North, Laptop] [Row 1: 1002, 2026-01-03, South, Phone]
(To read only 'revenue', MUST scan all bytes across all columns!)

Columnar Format (Parquet):
[Column 'order_id': 1001, 1002, ...]
[Column 'region':   North, South, ...]  <-- Dictionary Encoded & Compressed!
(Reads ONLY the bytes of requested columns; skips unneeded row groups via min/max stats!)
```
""")
    nb2.add_session_init()
    nb2.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
out_parquet = "/opt/spark-data/warehouse/orders_parquet"

# Write as Parquet
orders.write.mode("overwrite").parquet(out_parquet)
print("Written orders to Parquet successfully!")

# Read back and inspect
read_parquet = spark.read.parquet(out_parquet)
print(f"Parquet row count: {read_parquet.count()}")
""")
    nb2.add_experiment({"title": "Storage Size: CSV vs Parquet", "testing": "Compare file size on disk between raw CSV and Snappy-compressed Parquet.", "expect": "Parquet is 3x to 6x smaller than CSV.", "code": """csv_size = os.path.getsize("/opt/spark-data/retail/orders.csv")
parquet_size = sum(os.path.getsize(os.path.join(out_parquet, f)) for f in os.listdir(out_parquet) if f.endswith(".parquet"))
print(f"Raw CSV Size:     {csv_size:,} bytes")
print(f"Parquet Size:     {parquet_size:,} bytes")
print(f"Compression Ratio: {csv_size / parquet_size:.2f}x smaller!")
""", "happened": "Parquet achieved massive compression savings.", "why": "Columnar layout enables run-length encoding and dictionary compression.", "takeaway": "Never store historical data in CSV/JSON; always convert to Parquet."})
    nb2.add_exercises({"b_title": "Read Parquet Subset", "b_desc": "Read only order_id and unit_price from Parquet.", "b_input": "read_parquet", "b_sol": "spark.read.parquet(out_parquet).select('order_id', 'unit_price').show(3)", "i_title": "Verify Predicate Pushdown", "i_desc": "Filter Parquet on quantity > 10 and verify PushedFilters in plan.", "i_sol": "spark.read.parquet(out_parquet).filter('quantity > 10').explain()", "a_title": "Snappy vs ZSTD Compression", "a_desc": "Write with compression codec 'zstd'.", "a_sol": "orders.write.mode('overwrite').option('compression', 'zstd').parquet('/opt/spark-data/warehouse/orders_zstd')" })
    nb2.add_summary_and_teardown({"takeaways": {"Columnar Format": "Compresses identical types together; supports column pruning and predicate pushdown.", "Standard": "Parquet is Spark's native format."}, "terms": {"Row Group": "Chunk of columnar data with min/max statistics.", "Snappy": "Default fast compression codec."}, "mistakes": {"Using CSV in Production": "Wastes storage and kills query performance."}, "questions": {"How does Parquet skip reading data?": "Through row group metadata storing min/max statistics for each column, allowing Spark to skip entire blocks if the filter range does not intersect."}, "next_steps": "Open 03_partitioned_data_lake.ipynb"})
    nb2.save(os.path.join(BASE_DIR, "02_parquet_and_orc.ipynb"))

    # 08_03: Partitioned Data Lake
    nb3 = NotebookBuilder("Partitioned Data Lake Architecture: Designing Directory Layouts", "08_data_sources_and_formats", "03_partitioned_data_lake.ipynb", "Data_Lake_Lab")
    nb3.add_header({"what": "Designing partition directories (`year=YYYY/month=MM/`), directory partitioning strategies, and partition pruning.", "why": "Directory partitioning enables queries to skip 90%+ of files during scanning.", "problem": "Creating too many partitions (over-partitioning) or partitioning on high-cardinality keys.", "real_world": "Enterprise data lake storage layout design."})
    nb3.add_markdown("## 2. Data Lake Partition Directory Hierarchy: `warehouse/orders/year=2025/month=09/data.parquet`")
    nb3.add_session_init()
    nb3.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
out_lake = "/opt/spark-data/warehouse/lake_orders"

# Add year and month partition columns
lake_df = orders.withColumn("year", F.year("order_ts")).withColumn("month", F.month("order_ts"))
lake_df.write.mode("overwrite").partitionBy("year", "month").parquet(out_lake)
print("Data Lake partitioned layout created successfully!")
""")
    nb3.add_experiment({"title": "Partition Pruning Verification", "testing": "Filter on year=2025 and month=9 and verify that only 1 directory is scanned.", "expect": "PartitionFilters skips all other month directories.", "code": """read_lake = spark.read.parquet(out_lake)
read_lake.filter("year = 2025 AND month = 9").explain("formatted")
""", "happened": "PartitionFilters pushed to FileScan; irrelevant directories were ignored.", "why": "Spark pruned unneeded directories before listing files.", "takeaway": "Partition by query filter columns to accelerate analytical scans."})
    nb3.add_exercises({"b_title": "Inspect Directory Tree", "b_desc": "List generated directories under out_lake.", "b_input": "out_lake", "b_sol": "print([d for d in os.listdir(out_lake) if d.startswith('year')])", "i_title": "Filter Single Month", "i_desc": "Count orders in month 11.", "i_sol": "print('Count:', read_lake.filter('month = 11').count())", "a_title": "Max Records Per File", "a_desc": "Write with maxRecordsPerFile setting.", "a_sol": "lake_df.write.mode('overwrite').option('maxRecordsPerFile', 5000).parquet('/opt/spark-data/warehouse/lake_chunked')"})
    nb3.add_summary_and_teardown({"takeaways": {"Partition Pruning": "Skips entire directories at the storage layer.", "Cardinality Rule": "Use low/medium cardinality columns (year, month, region)."}, "terms": {"partitionBy": "Writes data into nested subdirectories.", "PartitionFilters": "Physical operator pruning directories."}, "mistakes": {"Partitioning by timestamp": "Creates millions of empty directories, destroying file system performance."}, "questions": {"What is partition pruning?": "Spark reading only matching partition directory folders, ignoring all other folders entirely."}, "next_steps": "Open 04_schema_evolution.ipynb"})
    nb3.save(os.path.join(BASE_DIR, "03_partitioned_data_lake.ipynb"))

    # 08_04: Schema Evolution
    nb4 = NotebookBuilder("Schema Evolution: Merging Schemas in Parquet and Handling Field Changes", "08_data_sources_and_formats", "04_schema_evolution.ipynb", "Schema_Evolution_Lab")
    nb4.add_header({"what": "How Spark handles schema evolution over time: adding nullable columns, renaming columns, and using mergeSchema=True in Parquet.", "why": "Production datasets change over time. Ingestion pipelines must accommodate evolving schemas without breaking.", "problem": "Schema mismatch errors when historical data lacks newly added columns.", "real_world": "Long-lived data lakes with evolving business entities."})
    nb4.add_markdown("## 2. Schema Evolution Rules: Parquet supports adding new nullable columns seamlessly via `mergeSchema`.")
    nb4.add_session_init()
    nb4.add_code("""evo_path = "/opt/spark-data/warehouse/schema_evo_test"

# Batch 1 (Version 1 Schema: id, name)
v1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
v1.write.mode("overwrite").parquet(f"{evo_path}/batch1")

# Batch 2 (Version 2 Schema: id, name, email) - Added new column!
v2 = spark.createDataFrame([(3, "Charlie", "charlie@example.com")], ["id", "name", "email"])
v2.write.mode("overwrite").parquet(f"{evo_path}/batch2")

# Read combined dataset with mergeSchema
combined = spark.read.option("mergeSchema", "true").parquet(f"{evo_path}/*")
combined.printSchema()
combined.show()
""")
    nb4.add_experiment({"title": "Reading Without mergeSchema vs With mergeSchema", "testing": "Observe schema behavior when mergeSchema is false vs true.", "expect": "Without mergeSchema, Spark infers schema from a single file and may miss new columns.", "code": """print("With mergeSchema=True:")
combined.show()
""", "happened": "Missing column in batch 1 was automatically populated with nulls.", "why": "mergeSchema merged StructTypes across Parquet file footers.", "takeaway": "Enable mergeSchema when querying datasets written under evolving schemas."})
    nb4.add_exercises({"b_title": "Check Nulls in New Column", "b_desc": "Filter rows where email is null in evolved schema.", "b_input": "combined", "b_sol": "combined.filter('email IS NULL').show()", "i_title": "Global mergeSchema Config", "i_desc": "Print spark.sql.parquet.mergeSchema config.", "i_sol": "print(spark.conf.get('spark.sql.parquet.mergeSchema', 'false'))", "a_title": "Add Nested Field", "a_desc": "Simulate adding a nested struct field.", "a_sol": "print('Nested structs can be merged with Delta Lake / Iceberg')"})
    nb4.add_summary_and_teardown({"takeaways": {"mergeSchema": "Merges disparate Parquet schemas across files.", "Backwards Compatibility": "New columns must be nullable."}, "terms": {"Schema Evolution": "Adapting to changing data definitions over time.", "mergeSchema": "Parquet read option combining file footers."}, "mistakes": {"Changing column data types": "Incompatible type changes (e.g. String to Int) break mergeSchema."}, "questions": {"How does Spark handle missing columns in older Parquet files when mergeSchema is enabled?": "It populates the missing column with null values for all historical rows."}, "next_steps": "Open 05_reading_and_writing_data.ipynb"})
    nb4.save(os.path.join(BASE_DIR, "04_schema_evolution.ipynb"))

    # 08_05: Reading & Writing Data
    nb5 = NotebookBuilder("DataFrameReader and DataFrameWriter Comprehensive Reference", "08_data_sources_and_formats", "05_reading_and_writing_data.ipynb", "Read_Write_Lab")
    nb5.add_header({"what": "The complete reference guide to DataFrameReader and DataFrameWriter APIs: formats, modes (append, overwrite, error, ignore), bucketing, sorting, and small file control.", "why": "Data ingestion and egress are the boundaries of every Spark pipeline.", "problem": "Accidentally wiping tables with wrong save modes or creating millions of output files.", "real_world": "Building resilient production ETL jobs."})
    nb5.add_markdown("## 2. Save Modes: Append, Overwrite, ErrorIfExists, Ignore")
    nb5.add_session_init()
    nb5.add_code("""orders = spark.read.option("header", True).csv("/opt/spark-data/retail/orders.csv")

# DataFrameWriter options: mode, format, partitionBy, coalesce
out = "/opt/spark-data/warehouse/orders_writer_test"
(
    orders
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(out)
)
print("Successfully written with DataFrameWriter!")
""")
    nb5.add_experiment({"title": "SaveMode Overwrite vs Append", "testing": "Verify row count when writing in append mode vs overwrite mode.", "expect": "Append doubles the row count; Overwrite replaces it.", "code": """orders.limit(5).write.mode("append").parquet(out)
print(f"Total count after append: {spark.read.parquet(out).count()}")
""", "happened": "New rows appended alongside existing files.", "why": "Append mode creates new part files without removing old ones.", "takeaway": "Use overwrite mode for idempotent batch reprocessing; use append for incremental loads."})
    nb5.add_exercises({"b_title": "Read with Format", "b_desc": "Read using spark.read.format('parquet').load(path).", "b_input": "out", "b_sol": "spark.read.format('parquet').load(out).show(2)", "i_title": "Save As Table", "i_desc": "Explain saveAsTable vs save.", "i_sol": "print('saveAsTable registers table metadata in the Metastore catalog.')", "a_title": "Dynamic Partition Overwrite", "a_desc": "Set dynamic partition overwrite mode.", "a_sol": "spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')"})
    nb5.add_summary_and_teardown({"takeaways": {"Reader/Writer": "Unified API across CSV, JSON, Parquet, ORC, JDBC.", "Idempotency": "Use overwrite mode for reproducible batch pipelines."}, "terms": {"SaveMode": "Append, Overwrite, Ignore, ErrorIfExists.", "Dynamic Partition Overwrite": "Overwrites only touched partition folders."}, "mistakes": {"Uncontrolled partition output": "Writing without coalesce/repartition produces 200 tiny files."}, "questions": {"What are the 4 save modes in DataFrameWriter?": "Append, Overwrite, ErrorIfExists (default), and Ignore."}, "next_steps": "Module 08 complete! Proceed to Module 09: Structured Streaming."})
    nb5.save(os.path.join(BASE_DIR, "05_reading_and_writing_data.ipynb"))

if __name__ == '__main__':
    gen_08_all()
