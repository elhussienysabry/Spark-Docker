# Quickstart and verification

This page walks through a clean start and verifies that every component works.

## 1. Check Docker

```bash
docker --version
docker compose version
docker info
```

Expected: Docker 24 or newer, Compose v2 or newer, and `docker info` exiting successfully.

If Docker is not running:

```bash
sudo systemctl start docker
```

## 2. Start the stack

```bash
cd ~/spark-docker
docker compose up -d
```

The first run pulls the base images and builds the local Spark image:

- `spark-local:4.2.1` — built from `Dockerfile.spark`; the official Spark 4.2.1 distribution on Python 3.13, plus pandas, pyarrow and numpy
- `quay.io/jupyter/pyspark-notebook:spark-4.2.1` (a full JupyterLab + PySpark stack)

The build takes a few minutes the first time. Watch the startup:

```bash
docker compose ps
docker compose logs -f --tail=50
```

Expected state:

```
NAME             IMAGE                                            STATUS
spark-master     spark-local:4.2.1                                Up (healthy)
spark-worker-1   spark-local:4.2.1                                Up
spark-worker-2   spark-local:4.2.1                                Up
spark-history    spark-local:4.2.1                                Up (healthy)
spark-jupyter    quay.io/jupyter/pyspark-notebook:spark-4.2.1     Up
```

`spark-worker-*` and `spark-jupyter` wait for the master health check; the workers do not publish a health check themselves.

## 3. Verify the versions

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --version
```

Look for:

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/'_/
   /___/ .__/\_,_/_/ /_/\_\   version 4.2.1
      /_/
Using Scala version 2.13.x, OpenJDK 64-Bit Server VM, 21.0.x
```

In Jupyter:

```bash
docker compose exec jupyter python -c"import pyspark; print(pyspark.__version__)"
```

Both must print `4.2.1`. A client/executor version mismatch produces confusing serialization errors.

Python must match at the minor-version level too:

```bash
docker compose exec spark-master python3 --version
docker compose exec jupyter python3 --version
```

Both print `Python 3.13.x`. `Dockerfile.spark` uses Python 3.13 specifically so executor-side RDDs and UDFs launched from Jupyter do not fail with `[PYTHON_VERSION_MISMATCH]`.

## 4. Verify the cluster

Open <http://localhost:8080>.

Under **Workers** you should see:

| Worker ID | Address | State | Cores | Memory |
|---|---|---|---|---|
| `spark-worker-1` | `spark-worker-1:xxxxx` | ALIVE | 2 (0 used) | 4096 MB |
| `spark-worker-2` | `spark-worker-2:xxxxx` | ALIVE | 2 (0 used) | 4096 MB |

If a worker is missing, see [troubleshooting](05-troubleshooting.md#workers-do-not-register).

## 5. Run the sample jobs

```bash
make test
```

Or individually:

```bash
make pi
make wordcount
make sales
```

Expected highlights:

```
Pi is roughly 3.14xxxx (1,600,000 samples)
...
Top 15 words
...
Rows read from /opt/spark-data/sales.csv: 48
...
Revenue by region
+------+----------+--------+------+
|region|units_sold| revenue|orders|
+------+----------+--------+------+
...
```

The sales job writes Parquet under `data/output/sales_report/` on the host. Check it:

```bash
find data/output/sales_report -maxdepth 2 -type d
```

## 6. Open JupyterLab

<http://localhost:8888/?token=spark>

Open `work/00_spark_quickstart.ipynb` and run the cells. The first cell that creates the `SparkSession` should print something like:

```
Spark 4.2.1
Master: spark://spark-master:7077
Default parallelism: 4
```

Default parallelism is the total worker cores (2 workers x 2 cores).

## 7. Check the History Server

After `make test` finishes, open <http://localhost:18080>.

Each application should appear as a row with its name (`Pi Estimation`, `Word Count`, `Sales Report`) and a link to the completed Spark UI.

Event logs are written to `./events` on the host; the History Server reads the same directory.

## 8. Stop

```bash
docker compose down        # keep notebooks, data, events
make clean-output          # optional: delete generated output and event logs
```

Next start:

```bash
docker compose up -d
```

Data, notebooks and apps persist because they live on the host, not in the containers.
