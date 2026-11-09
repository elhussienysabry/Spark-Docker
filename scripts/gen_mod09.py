#!/usr/bin/env python3
"""
Generates Module 09 (Streaming) notebooks:
- 09_streaming/01_streaming_fundamentals.ipynb
- 09_streaming/02_structured_streaming.ipynb
- 09_streaming/03_triggers_and_output_modes.ipynb
- 09_streaming/04_watermarks_and_late_data.ipynb
- 09_streaming/05_checkpoints_and_fault_tolerance.ipynb
- 09_streaming/06_kafka_integration.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/09_streaming"

def gen_09_all():
    # 09_01: Streaming Fundamentals
    nb1 = NotebookBuilder("Stream Processing Fundamentals: Micro-Batch vs Continuous Processing", "09_streaming", "01_streaming_fundamentals.ipynb", "Streaming_Fund_Lab")
    nb1.add_header({"what": "The evolution of Spark streaming: legacy DStreams to Structured Streaming, the unbounded table mental model, and latency vs throughput trade-offs.", "why": "Real-time streaming powers anomaly detection, live analytics dashboards, and event-driven architectures.", "problem": "Managing unbounded streams without state store explosion.", "real_world": "Real-time payment fraud scoring and clickstream analysis."})
    nb1.add_markdown("""## 2. Structured Streaming Mental Model: Stream as an Unbounded Table
```
Incoming Event Stream:
[Event 1] ---> [Event 2] ---> [Event 3] ---> ... (Never ends!)
      |             |              |
      v             v              v
+-----------------------------------------------------------+
| UNBOUNDED INPUT TABLE (New rows appended continuously)    |
+-----------------------------------------------------------+
                            |
                            v Query Expression (DataFrame / SQL)
+-----------------------------------------------------------+
| RESULT TABLE (Updated incrementally per micro-batch)     |
+-----------------------------------------------------------+
                            |
                            v Output Mode (Append / Update / Complete)
+-----------------------------------------------------------+
| EXTERNAL SINK (Console, Files, Kafka, Delta Lake)        |
+-----------------------------------------------------------+
```
""")
    nb1.add_session_init()
    nb1.add_markdown("## 3. Rate Stream Source Demo (Synthetic Stream)")
    nb1.add_code("""# Create built-in rate source (generates sequential numbers at specified rows/second)
rate_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
)

print("Is stream active in DataFrame?:", rate_stream.isStreaming)
rate_stream.printSchema()
""")
    nb1.add_experiment({"title": "Running a Short-Lived Streaming Query to In-Memory Sink", "testing": "Start a micro-batch query writing to memory sink and inspect results.", "expect": "Query populates an in-memory table that can be queried with spark.sql().", "code": """query = (
    rate_stream
    .writeStream
    .format("memory")
    .queryName("rate_sink")
    .outputMode("append")
    .start()
)

# Wait 4 seconds for a few micro-batches to process
time.sleep(4)

print("Querying in-memory streaming sink:")
spark.sql("SELECT * FROM rate_sink ORDER BY timestamp DESC LIMIT 5").show()

query.stop()
print("Streaming query stopped successfully.")
""", "happened": "Rate stream generated timestamped integers into in-memory table.", "why": "Structured Streaming micro-batches processed incoming rows as fast as possible.", "takeaway": "Memory sink is ideal for unit testing and interactive debugging in Jupyter."})
    nb1.add_exercises({"b_title": "Check Stream Status", "b_desc": "Query query.isActive and query.status.", "b_input": "query", "b_sol": "print('query.isActive, query.status')", "i_title": "Rate Source with Ramptime", "i_desc": "Configure rampUpTime option on rate source.", "i_sol": "spark.readStream.format('rate').option('rampUpTime', '5s').load()", "a_title": "Explain Legacy DStreams", "a_desc": "Contrast DStreams with Structured Streaming.", "a_sol": "print('DStreams used RDDs; Structured Streaming uses the Catalyst DataFrame engine with native event-time.')"})
    nb1.add_summary_and_teardown({"takeaways": {"Unbounded Table": "Stream is conceptually an infinite append-only table.", "Micro-batch": "Processes incoming records in small discrete batches."}, "terms": {"isStreaming": "Boolean flag indicating streaming DataFrame.", "rate source": "Synthetic benchmark source."}, "mistakes": {"Calling collect() on streaming DataFrame": "Throws AnalysisException; streaming data must use writeStream."}, "questions": {"What is Structured Streaming?": "A scalable stream processing engine built on the Spark SQL engine, treating streams as unbounded tables."}, "next_steps": "Open 02_structured_streaming.ipynb"})
    nb1.save(os.path.join(BASE_DIR, "01_streaming_fundamentals.ipynb"))

    # 09_02: Structured Streaming
    nb2 = NotebookBuilder("Structured Streaming: File Source and Ingestion Directory Monitoring", "09_streaming", "02_structured_streaming.ipynb", "File_Streaming_Lab")
    nb2.add_header({"what": "Setting up file-based streaming monitoring `/opt/spark-data/streaming/events/`, reading JSON streams, and writing to file sinks.", "why": "Directory monitoring is the standard pattern for landing zones and file-based ETL ingestion.", "problem": "Processing newly arriving files without re-reading processed files.", "real_world": "Automated Bronze layer ingestion in cloud data lakes."})
    nb2.add_markdown("## 2. Directory File Stream Architecture: Watches a directory and atomically processes new files as they arrive.")
    nb2.add_session_init()
    nb2.add_code("""import json

stream_dir = "/opt/spark-data/streaming/events"
checkpoint_dir = "/opt/spark-data/warehouse/stream_chk_02"
out_stream = "/opt/spark-data/warehouse/stream_out_02"

# Clean test dirs
os.system(f"rm -rf {stream_dir}/* {checkpoint_dir} {out_stream}")
os.makedirs(stream_dir, exist_ok=True)

# Define schema for incoming JSON events
stream_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("channel", StringType())
])

# Create file stream reader
file_stream = (
    spark.readStream
    .schema(stream_schema)
    .json(stream_dir)
)

print("Streaming DataFrame created from file directory:", file_stream.isStreaming)
""")
    nb2.add_experiment({"title": "Generating Files and Observing Stream Pickup", "testing": "Write sample JSON files into stream_dir and verify the stream picks them up.", "expect": "Streaming query processes new files incrementally.", "code": """# Start streaming query writing to parquet sink
q = (
    file_stream
    .writeStream
    .format("parquet")
    .option("checkpointLocation", checkpoint_dir)
    .outputMode("append")
    .start(out_stream)
)

# Generate file 1
with open(f"{stream_dir}/event_1.json", "w") as f:
    f.write(json.dumps({"order_id": 1, "customer_id": 101, "amount": 99.5, "channel": "Web"}) + "\\n")

time.sleep(3)
q.processAllAvailable()

print(f"Rows processed so far in out_stream: {spark.read.parquet(out_stream).count()}")
q.stop()
""", "happened": "Spark detected the new file and wrote output to Parquet.", "why": "Structured Streaming tracks processed file names in the checkpoint directory.", "takeaway": "File streaming provides automated, exactly-once directory ingestion without external orchestrators."})
    nb2.add_exercises({"b_title": "Inspect Checkpoint Directory", "b_desc": "List contents of checkpoint directory.", "b_input": "checkpoint_dir", "b_sol": "print(os.listdir(checkpoint_dir))", "i_title": "Add Second Event File", "i_desc": "Write a second file and verify incremental addition.", "i_sol": "print('File streaming is incremental.')", "a_title": "Atomic File Renaming", "a_desc": "Why files should be moved into directory atomically.", "a_sol": "print('Writing directly to directory risks Spark reading partial half-written files.')"})
    nb2.add_summary_and_teardown({"takeaways": {"File Stream": "Monitors directory for new files.", "Checkpoint": "Records processed file offsets."}, "terms": {"processAllAvailable()": "Processes pending data synchronously (great for testing).", "checkpointLocation": "Storage for progress metadata."}, "mistakes": {"Writing files directly into stream folder": "Causes partial read errors; always write to temp and atomic mv."}, "questions": {"How does Structured Streaming avoid reprocessing files?": "It tracks processed file paths and versions in the internal offset log inside the checkpoint directory."}, "next_steps": "Open 03_triggers_and_output_modes.ipynb"})
    nb2.save(os.path.join(BASE_DIR, "02_structured_streaming.ipynb"))

    # 09_03: Triggers and Output Modes
    nb3 = NotebookBuilder("Streaming Triggers and Output Modes: Append, Complete, and Update", "09_streaming", "03_triggers_and_output_modes.ipynb", "Triggers_Modes_Lab")
    nb3.add_header({"what": "Deep dive into Triggers (Default, ProcessingTime, Once, AvailableNow) and Output Modes (Append, Complete, Update).", "why": "Matching the correct output mode to your query type is mandatory; invalid combinations fail at query planning.", "problem": "Attempting to run Complete mode on a non-aggregated stream or Append mode on global aggregations.", "real_world": "Production scheduling of hourly micro-batches using Trigger.AvailableNow."})
    nb3.add_markdown("""## 2. Output Mode Matrix
- **Append**: Emits only newly inserted rows. (Required for file sinks; disallowed for global non-watermarked aggregations).
- **Complete**: Re-emits the entire updated result table. (Required for global aggregations; unsupported on file sinks).
- **Update**: Emits only the rows updated since the previous trigger.
""")
    nb3.add_session_init()
    nb3.add_code("""rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
print("Rate stream initialized.")
""")
    nb3.add_experiment({"title": "Trigger.AvailableNow Execution", "testing": "Execute a stream using Trigger.AvailableNow() and observe that it shuts down automatically.", "expect": "Processes all available rows and terminates cleanly.", "code": """q = (
    rate_stream
    .writeStream
    .format("memory")
    .queryName("avail_now_sink")
    .trigger(availableNow=True)
    .start()
)
q.awaitTermination()
print("Query terminated automatically after Trigger.AvailableNow!")
spark.sql("SELECT count(*) as total_rows FROM avail_now_sink").show()
""", "happened": "Stream ran to completion and shut down automatically.", "why": "Trigger.AvailableNow processes pending micro-batches and terminates.", "takeaway": "Use Trigger.AvailableNow for cost-effective incremental batch ETL scheduled via Airflow."})
    nb3.add_exercises({"b_title": "Fixed Interval Trigger", "b_desc": "Configure trigger with processingTime='5 seconds'.", "b_input": "trigger", "b_sol": "print('trigger(processingTime=\"5 seconds\")')", "i_title": "Complete Mode Aggregation", "i_desc": "Run a streaming group count in Complete mode to memory sink.", "i_sol": "print('rate_stream.groupBy().count().writeStream.outputMode(\"complete\")')", "a_title": "Trigger.Once vs AvailableNow", "a_desc": "Explain why AvailableNow replaced Once.", "a_sol": "print('Trigger.Once processed only 1 micro-batch; AvailableNow processes multiple batches until caught up.')"})
    nb3.add_summary_and_teardown({"takeaways": {"Modes": "Append (new rows), Complete (all rows), Update (modified rows).", "AvailableNow": "Cost-effective serverless batch processing."}, "terms": {"Trigger.AvailableNow": "Process all pending data and stop.", "OutputMode": "Defines what is emitted to the sink."}, "mistakes": {"Complete mode on File Sink": "File sink does not support rewriting full tables; requires Append mode."}, "questions": {"When should you use Trigger.AvailableNow()?": "When you want incremental streaming semantics but run as an hourly or daily scheduled batch to save compute costs."}, "next_steps": "Open 04_watermarks_and_late_data.ipynb"})
    nb3.save(os.path.join(BASE_DIR, "03_triggers_and_output_modes.ipynb"))

    # 09_04: Watermarks and Late Data
    nb4 = NotebookBuilder("Watermarks and Late-Arriving Data: Bounding Streaming State Memory", "09_streaming", "04_watermarks_and_late_data.ipynb", "Watermarks_Lab")
    nb4.add_header({"what": "How watermarks bound state store memory during streaming aggregations and drop late-arriving events.", "why": "Without watermarks, streaming aggregations maintain historical state indefinitely, eventually crashing with OOM.", "problem": "State store growing unbounded on high-velocity event streams.", "real_world": "Sessionizing user clickstreams and windowed IoT metrics."})
    nb4.add_markdown("""## 2. Watermark Mechanics
`Watermark = MaxEventTimeSeen - AllowedDelay`
- Events with `event_time >= Watermark` are accepted into the aggregation window.
- Events with `event_time < Watermark` are discarded as too late.
- State for windows older than the watermark is cleared from memory.
""")
    nb4.add_session_init()
    nb4.add_code("""# Define streaming query with 10-minute watermark and 5-minute tumbling window
rate = spark.readStream.format("rate").option("rowsPerSecond", 2).load()

windowed = (
    rate
    .withWatermark("timestamp", "10 seconds")
    .groupBy(F.window("timestamp", "5 seconds"))
    .count()
)

print("Streaming plan with Watermark:")
windowed.explain()
""")
    nb4.add_experiment({"title": "Running Watermarked Aggregation to Memory Sink", "testing": "Run windowed aggregation with watermark for 6 seconds.", "expect": "Computes window counts and prunes expired windows.", "code": """q = windowed.writeStream.format("memory").queryName("window_sink").outputMode("update").start()
time.sleep(6)
spark.sql("SELECT window.start, window.end, count FROM window_sink ORDER BY window.start DESC LIMIT 5").show(truncate=False)
q.stop()
""", "happened": "Output windows emitted incrementally.", "why": "Update mode emits changed window counts as rows arrive.", "takeaway": "Always define withWatermark() on all stateful streaming aggregations."})
    nb4.add_exercises({"b_title": "Watermark Syntax", "b_desc": "Write withWatermark on timestamp column for 1 hour.", "b_input": "code", "b_sol": "print('df.withWatermark(\"timestamp\", \"1 hour\")')", "i_title": "Tumbling vs Sliding Window", "i_desc": "Explain window('timestamp', '10 minutes', '5 minutes').", "i_sol": "print('10-minute window sliding every 5 minutes (sliding window).')", "a_title": "State Eviction Inspection", "a_desc": "Check state memory in streaming query progress.", "a_sol": "print('q.lastProgress[\"stateOperators\"]')"})
    nb4.add_summary_and_teardown({"takeaways": {"Watermark Purpose": "Purges old state from RAM to prevent OOM.", "Late Data": "Data older than watermark is dropped."}, "terms": {"Watermark": "MaxEventTime - AllowedDelay threshold.", "State Store": "In-memory or RocksDB state repository."}, "mistakes": {"Omitting Watermark on GroupBy": "State store grows indefinitely until executor crashes."}, "questions": {"What problem does watermarking solve in Structured Streaming?": "It bounds state memory by specifying how long to wait for late data before purging historical aggregate windows from the state store."}, "next_steps": "Open 05_checkpoints_and_fault_tolerance.ipynb"})
    nb4.save(os.path.join(BASE_DIR, "04_watermarks_and_late_data.ipynb"))

    # 09_05: Checkpoints and Fault Tolerance
    nb5 = NotebookBuilder("Streaming Checkpoints and Fault Tolerance: Exactly-Once Recovery", "09_streaming", "05_checkpoints_and_fault_tolerance.ipynb", "Streaming_Checkpoint_Lab")
    nb5.add_header({"what": "Checkpoint directory internals, offset write-ahead logs (WAL), state recovery, and achieving end-to-end exactly-once guarantees.", "why": "Checkpoints allow streaming queries to restart after crashes from the exact point of interruption without data loss or duplication.", "problem": "Stream restarting from scratch and duplicating side-effects on restart.", "real_world": "Zero-data-loss streaming pipelines."})
    nb5.add_markdown("## 2. Checkpoint Directory Structure: `offsets/`, `commits/`, `state/`, `metadata`")
    nb5.add_session_init()
    nb5.add_code("""chk_path = "/opt/spark-data/warehouse/chk_structure_demo"
os.system(f"rm -rf {chk_path}")

# Run short rate stream to generate checkpoint files
rate = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
q = rate.writeStream.format("noop").option("checkpointLocation", chk_path).start()
time.sleep(3)
q.stop()

print("Files created inside checkpoint directory:")
for root, dirs, files in os.walk(chk_path):
    for f in files:
        rel = os.path.relpath(os.path.join(root, f), chk_path)
        print(f"  - {rel}")
""")
    nb5.add_experiment({"title": "Resuming Stream from Checkpoint", "testing": "Restart stream pointing to same checkpoint and verify it resumes from saved offset.", "expect": "Stream continues from last committed micro-batch without reprocessing batch 0.", "code": """q2 = rate.writeStream.format("noop").option("checkpointLocation", chk_path).start()
time.sleep(2)
last_id = q2.lastProgress["batchId"] if q2.lastProgress else 0
print(f"Resumed stream active! Last processed batch ID: {last_id}")
q2.stop()
""", "happened": "Stream resumed without re-reading historical batches.", "why": "The commit log confirmed previous batches were already acknowledged.", "takeaway": "Never delete the checkpoint directory if you need uninterrupted recovery."})
    nb5.add_exercises({"b_title": "Check Commit Log", "b_desc": "Read the contents of the latest commit file in checkpoint.", "b_input": "commits", "b_sol": "print('Commit file records batch completion status.')", "i_title": "Exactly-Once Requirements", "i_desc": "List the 4 requirements for exactly-once streaming.", "i_sol": "print('1. Replayable source, 2. Deterministic execution, 3. Idempotent sink, 4. Checkpointing.')", "a_title": "Changing Query Logic", "a_desc": "Can you change DataFrame query logic with an existing checkpoint?", "a_sol": "print('Certain breaking schema changes or stateful operator mutations require starting a new checkpoint directory.')"})
    nb5.add_summary_and_teardown({"takeaways": {"Exactly-Once": "Achieved through offset log + idempotent sink.", "Checkpoint Integrity": "Must be stored on reliable storage."}, "terms": {"WAL": "Write-ahead log recording offsets.", "Commit Log": "Confirms batch completion."}, "mistakes": {"Storing checkpoints on local /tmp": "Container restart wipes checkpoints, causing stream to reprocess from beginning."}, "questions": {"What are the components of a streaming checkpoint directory?": "offsets/ (uncommitted source offsets), commits/ (completed batches), state/ (state store snapshots), and metadata (unique query run ID)."}, "next_steps": "Open 06_kafka_integration.ipynb"})
    nb5.save(os.path.join(BASE_DIR, "05_checkpoints_and_fault_tolerance.ipynb"))

    # 09_06: Kafka Integration
    nb6 = NotebookBuilder("Apache Kafka Integration: Producers, Consumers, and Schema Handling (Optional)", "09_streaming", "06_kafka_integration.ipynb", "Kafka_Integration_Lab")
    nb6.add_header({"what": "How Spark Structured Streaming integrates with Apache Kafka: reading topics, consumer offsets, parsing key-value payloads, and writing to Kafka sinks.", "why": "Kafka + Spark is the dominant real-time streaming architecture in enterprise tech.", "problem": "Deserializing binary Kafka payloads and managing consumer offsets.", "real_world": "Enterprise event streams, IoT ingestion, and transaction event buses."})
    nb6.add_markdown("""## 2. Kafka + Spark Integration Architecture

```
Apache Kafka Cluster:
[ Topic: 'orders-stream', Partitions: 0, 1, 2 ]
                   |
                   v (1 Task per Kafka Partition!)
Spark Structured Streaming (Driver & Executors):
  - readStream.format("kafka").option("subscribe", "orders-stream")
  - Each task connects directly to a Kafka Broker partition
                   |
                   v Deserialization & Business Logic (JSON / Avro)
  - df.select(from_json(col("value").cast("string"), schema))
                   |
                   v Output to Storage or Write Back to Kafka Sink
```

> **Note on Infrastructure**: This notebook provides complete production code examples. In environments without an active Kafka container broker, the code serves as an authoritative architectural reference.
""")
    nb6.add_session_init()
    nb6.add_markdown("""## 3. Production Kafka Consumer Template

```python
# Reading from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders_raw")
    .option("startingOffsets", "latest") # or "earliest"
    .option("failOnDataLoss", "false")
    .load()
)

# Kafka streams contain fixed schema: [key, value, topic, partition, offset, timestamp, timestampType]
parsed_stream = (
    kafka_stream
    .selectExpr("CAST(key AS STRING) as order_key", "CAST(value AS STRING) as payload", "timestamp")
    .select(F.col("order_key"), F.from_json(F.col("payload"), orders_schema).alias("order"), "timestamp")
    .select("order_key", "order.*", "timestamp")
)
```
""")
    nb6.add_experiment({"title": "Kafka Producer Specification", "testing": "Explain how Spark writes to Kafka using key and value columns.", "expect": "DataFrame with 'key' and 'value' columns written directly to Kafka topic.", "code": """print(\"\"\"
Writing to Kafka requires 'topic', 'key', and 'value' columns:
(
    df
    .selectExpr("CAST(order_id AS STRING) as key", "to_json(struct(*)) as value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "orders_processed")
    .option("checkpointLocation", "/opt/spark-data/warehouse/chk_kafka_out")
    .start()
)
\"\"\")
""", "happened": "Displayed Kafka writer structure.", "why": "Kafka expects byte arrays for key and value.", "takeaway": "Always serialize DataFrame records with to_json() or to_avro() before writing to Kafka."})
    nb6.add_exercises({"b_title": "Kafka Stream Schema", "b_desc": "What columns does readStream.format('kafka') return?", "b_input": "Schema", "b_sol": "print('key, value, topic, partition, offset, timestamp, timestampType')", "i_title": "failOnDataLoss Option", "i_desc": "Why set failOnDataLoss=false?", "i_sol": "print('Prevents stream failure if Kafka log retention purges older offsets before Spark finishes reading them.')", "a_title": "Docker Compose Kafka Service", "a_desc": "Show how to add Kafka to Docker Compose.", "a_sol": "print('Add wurstmeister/kafka or confluentinc/cp-kafka service mapped to spark-net.')"})
    nb6.add_summary_and_teardown({"takeaways": {"Kafka Standard": "1 Spark task per Kafka topic partition.", "Serialization": "Values arrive as binary; cast to String and parse with from_json."}, "terms": {"bootstrap.servers": "Kafka broker connection string.", "startingOffsets": "earliest or latest."}, "mistakes": {"Forgetting to cast binary value": "Attempting string operations directly on binary columns fails."}, "questions": {"How is parallelism determined when reading from Kafka in Structured Streaming?": "Each partition of the Kafka topic is read by exactly one Spark task, so parallelism matches the number of Kafka topic partitions."}, "next_steps": "Module 09 complete! Proceed to Module 10: Performance and Optimization."})
    nb6.save(os.path.join(BASE_DIR, "06_kafka_integration.ipynb"))

if __name__ == '__main__':
    gen_09_all()
