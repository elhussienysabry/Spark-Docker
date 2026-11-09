# Running jobs

All examples assume:

```bash
cd ~/spark-docker
```

and a running stack (`docker compose up -d`).

## Where code and data live

| Inside a Spark container | On the host | Writable |
|---|---|---|
| `/opt/spark-apps` | `./apps` | no |
| `/opt/spark-data` | `./data` | yes |
| `/opt/spark-events` | `./events` | yes |
| `/home/jovyan/work` (Jupyter) | `./notebooks` | yes |

Put scripts and jars in `./apps`, inputs in `./data`, and use `/opt/spark-data/output/...` for results.

## `spark-submit` basics

Run `spark-submit` inside `spark-master` so the driver shares the cluster network:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name"My job"\
  --executor-memory 2g \
  --executor-cores 1 \
  --total-executor-cores 4 \
  /opt/spark-apps/my_job.py \
  arg1 arg2
```

Useful options:

| Option | Meaning |
|---|---|
| `--master spark://spark-master:7077` | Standalone cluster master |
| `--deploy-mode client` | Driver in the submitting container (default, logs visible) |
| `--deploy-mode cluster` | Driver on a worker; results only in the History Server |
| `--name"..."` | Application name in the UIs |
| `--executor-memory 2g` | Memory per executor |
| `--executor-cores 1` | Cores per executor |
| `--total-executor-cores 4` | Cap total cores used |
| `--conf k=v` | Any Spark configuration |
| `--py-files deps.zip` | Extra Python files/zip for driver and executors |
| `--jars extra.jar` | Extra JVM jars |
| `--files file` | Files distributed to executors |

The defaults in `conf/spark-defaults.conf` already point `spark.master` at `spark://spark-master:7077`, set executor memory to `2g` and executor cores to `1`, so the `--master` flag is optional but explicit is clearer.

## Included example jobs

### Sales report

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/sales_report.py
```

Arguments:

```bash
# custom input and output
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/sales_report.py \
  /opt/spark-data/sales.csv \
  /opt/spark-data/output/sales_custom
```

What it does:

1. Reads the CSV with header and schema inference.
2. Adds a `revenue = units * unit_price` column.
3. Aggregates revenue, units and distinct orders by region.
4. Aggregates revenue by product.
5. Prints both tables and writes Parquet to `data/output/sales_report/{by_region,by_product}`.

### Monte Carlo Pi

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/pi.py 16
```

The argument is the number of partitions (default `8`); each partition generates 100,000 samples. It prints an estimate such as `Pi is roughly 3.141xxx`.

### Word count

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/wordcount.py \
  /opt/spark-data/words.txt \
  /opt/spark-data/output/wordcount
```

It prints the top 15 words and writes the full `(count, word)` list as text.

## Interactive shells

### PySpark

```bash
make pyspark
```

or:

```bash
docker compose exec spark-master /opt/spark/bin/pyspark \
  --master spark://spark-master:7077
```

Inside:

```python
df = spark.read.csv("/opt/spark-data/sales.csv", header=True, inferSchema=True)
df.groupBy("region").count().show()
```

### Spark SQL

```bash
make sql
```

Inside:

```sql
CREATE OR REPLACE TEMP VIEW sales
USING csv
OPTIONS (path'/opt/spark-data/sales.csv', header'true', inferSchema'true');

SELECT region, ROUND(SUM(units * unit_price), 2) AS revenue
FROM sales
GROUP BY region
ORDER BY revenue DESC;
```

### Scala

```bash
docker compose exec spark-master /opt/spark/bin/spark-shell \
  --master spark://spark-master:7077
```

Inside:

```scala
val sales = spark.read.option("header","true").option("inferSchema","true")
  .csv("/opt/spark-data/sales.csv")
sales.groupBy("region").count().show()
```

## Cluster mode

In client mode (the default), the driver runs in the container where you ran `spark-submit`. In cluster mode, the master picks a worker to run the driver:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --name"Cluster mode sales"\
  /opt/spark-apps/sales_report.py
```

Considerations:

- The command returns after the application finishes, but stdout from the driver is not streamed to your terminal.
- Results and logs are available in the History Server (event logging is on by default here).
- The application jar/script and data must be reachable from the worker. `/opt/spark-apps` and `/opt/spark-data` are mounted in every Spark container, so this works for the included apps.
- Cluster mode uses a driver on a worker; driver memory is taken from `spark.driver.memory` (`1g` by default in `conf/spark-defaults.conf`).

## Reading and writing data

Supported formats include CSV, JSON, Parquet, ORC, Avro (with the Spark Avro package) and JDBC.

```python
# CSV
df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("/opt/spark-data/sales.csv")

# Parquet
df.write.mode("overwrite").parquet("/opt/spark-data/output/sales_parquet")
back = spark.read.parquet("/opt/spark-data/output/sales_parquet")

# Partitioned output
df.write.mode("overwrite").partitionBy("region") \
    .parquet("/opt/spark-data/output/sales_by_region")

# JSON
df.write.mode("overwrite").json("/opt/spark-data/output/sales_json")
```

Output paths are directories. Use `mode("overwrite")` when rerunning a job.

When a job writes as the `spark` user (UID 185), the resulting files may be owned by UID 185 on the host. The `data` directory is world-writable in this development setup so you can delete or replace them. See [permissions](05-troubleshooting.md#permission-denied-under-data-or-events).

## Monitoring a running job

1. **Driver UI** — <http://localhost:4041> for jobs submitted from `spark-master`, or <http://localhost:4040> for notebook jobs, while the application runs.
2. **Master UI** — <http://localhost:8080> shows the running application and its executors.
3. **Worker UIs** — <http://localhost:8081> and <http://localhost:8082>.
4. **Logs** — `docker compose logs -f spark-master spark-worker-1 spark-worker-2`.
5. **History Server** — <http://localhost:18080> after completion.

If the driver UI is not on the default port because another app owns it, the kernel prints the actual URL:

```python
print(spark.sparkContext.uiWebUrl)
```

Host ports `4040` (Jupyter driver) and `4041` (driver in `spark-master`) are published. Additional driver ports are not; stop the app holding the port or use the History Server.

## Event logs and History Server

Jobs submitted from the cluster inherit:

```
spark.eventLog.enabled=true
spark.eventLog.dir=file:///opt/spark-events
```

so they appear in the History Server automatically. If you override `spark.eventLog.dir`, use a path that all containers can see (ideally under `/opt/spark-events`).

Notebook sessions must enable event logging in the `SparkSession` (the sample notebook does).

## Python dependencies

The Spark services use `spark-local:4.2.1`, built from `Dockerfile.spark`. It runs Python 3.13 and includes **pandas**, **numpy** and **pyarrow**, matching the Python version used by Jupyter.

To add another library for jobs, add it to `Dockerfile.spark`:

```dockerfile
# Dockerfile.spark — add before the USER spark line
RUN pip install --no-cache-dir scikit-learn
```

Rebuild the image and recreate the Spark services:

```bash
docker compose build
docker compose up -d --force-recreate spark-master spark-worker-1 spark-worker-2 spark-history
```

Or rebuild and recreate in one step:

```bash
docker compose up -d --build --force-recreate spark-master spark-worker-1 spark-worker-2 spark-history
```

The driver in `spark-master` and the executors in both workers then share the same image and Python minor version (3.13, same as Jupyter). Installing a package only in the Jupyter container is **not** enough for Python UDFs, because the UDF runs on the executors.

For pure-Python helper modules you can also ship a zip:

```bash
cd apps
zip -r deps.zip mypackage/
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark-apps/deps.zip \
  /opt/spark-apps/my_job.py
```

## Spark 4 notes

- Scala 2.13 is the default; Spark 4 no longer ships Scala 2.12 builds.
- Java 17+ is required; the Spark image runs Java 21.
- ANSI SQL mode is enabled by default in Spark 4. Casts such as `CAST('abc'AS INT)` now raise an error instead of returning `NULL`. To restore the old behavior for a job:

```bash
--conf spark.sql.ansi.enabled=false
```

- Spark Connect is not enabled in this stack; jobs use the classic `spark-submit` / `SparkSession` path. See [operations](04-operations.md#enabling-spark-connect-optional) if you need it.
