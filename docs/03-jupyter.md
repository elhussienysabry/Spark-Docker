# JupyterLab and PySpark

## Open JupyterLab

<http://localhost:8888/?token=spark>

The token comes from `JUPYTER_TOKEN` in `.env`. To print it:

```bash
grep JUPYTER_TOKEN .env
```

If you changed `.env`, recreate the container:

```bash
docker compose up -d --force-recreate jupyter
```

## What is installed

The `quay.io/jupyter/pyspark-notebook:spark-4.2.1` image contains:

- JupyterLab 4.x
- Python 3.13
- PySpark **4.2.1** (same version as the cluster)
- pandas, numpy, pyarrow, matplotlib, scikit-learn, and the usual scientific stack
- a `spark-submit` / `pyspark` installation on `PATH`

The Spark cluster image (`spark-local:4.2.1`) also runs Python 3.13 and includes pandas, numpy and pyarrow. Because the driver and executors use the same Python minor version, Python RDDs and UDFs launched from a notebook work without extra configuration.

Check versions from a notebook or terminal:

```python
import pyspark, sys
print(pyspark.__version__, sys.version)
```

```bash
docker compose exec jupyter spark-submit --version
```

## Notebooks folder

`./notebooks` on the host is mounted at `/home/jovyan/work` in the container. Create notebooks there; they persist across container recreation.

The included notebook is `notebooks/00_spark_quickstart.ipynb`.

## Connect to the cluster

The Jupyter container runs the **driver**; the executors run in the worker containers. The driver must advertise an address the workers can reach, so set `spark.driver.host=jupyter`:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("My analysis")
    .master("spark://spark-master:7077")
    .config("spark.driver.host","jupyter")
    .config("spark.driver.bindAddress","0.0.0.0")
    .config("spark.eventLog.enabled","true")
    .config("spark.eventLog.dir","file:///opt/spark-events")
    .config("spark.sql.warehouse.dir","file:///opt/spark-data/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

Why each setting matters:

| Setting | Reason |
|---|---|
| `master("spark://spark-master:7077")` | Use the standalone cluster instead of a local process |
| `spark.driver.host=jupyter` | Workers resolve and connect back to the Jupyter container |
| `spark.driver.bindAddress=0.0.0.0` | Listen on all container interfaces |
| `spark.eventLog.*` | Make the app visible in the History Server |
| `spark.sql.warehouse.dir` | Keep managed tables in the shared data folder |

Without `spark.driver.host`, the driver advertises an address that workers cannot resolve, and jobs hang or fail with `Connection refused` / `Lost executor`.

## Recommended workflow

1. Start the stack (`docker compose up -d`).
2. Open JupyterLab and the Spark Master UI side by side.
3. Run the notebook cells; watch stages in <http://localhost:4040>.
4. When finished, run `spark.stop()` and restart the kernel before the next session.

Only one Spark application can bind port `4040` in the Jupyter container. After `spark.stop()`, the port is released.

## Read the shared data

```python
sales = spark.read.csv("/opt/spark-data/sales.csv", header=True, inferSchema=True)
sales.show(5)
```

Write results to `/opt/spark-data/output/...` so they appear in `./data/output` on the host:

```python
sales.write.mode("overwrite").parquet("/opt/spark-data/output/from_notebook")
```

## Run scripts from a Jupyter terminal

JupyterLab's terminal starts in `/home/jovyan/work`. The shared apps are at `/opt/spark-apps`:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=jupyter \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///opt/spark-events \
  /opt/spark-apps/sales_report.py
```

For scripts that only use PySpark, this works without changes. For third-party libraries, see the next section.

### Verify Python RDD/UDF execution

Both the cluster image and Jupyter use Python 3.13, so Python RDDs and pandas UDFs run on the workers. Quick check with the Pi job:

```bash
docker compose exec jupyter spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=jupyter \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /opt/spark-apps/pi.py 8
```

Expected: `Pi is roughly 3.14...` and no `[PYTHON_VERSION_MISMATCH]`.

## Python packages

You can install a package in the running Jupyter container:

```bash
docker compose exec jupyter pip install seaborn
```

This persists only until the container is recreated. To keep packages, build a small image:

```dockerfile
FROM quay.io/jupyter/pyspark-notebook:spark-4.2.1
RUN pip install --no-cache-dir seaborn
```

```bash
docker build -t spark-jupyter-custom:4.2.1 .
```

Then change the `jupyter` service `image:` in `docker-compose.yml` and recreate it.

**Important:** installing a package in Jupyter makes it available to the driver, but Python UDFs run on the executors in the worker containers. The workers use `spark-local:4.2.1`, which includes pandas, numpy and pyarrow but not `seaborn` or the rest of the Jupyter stack. If your UDF imports a third-party package, add it to `Dockerfile.spark` and rebuild (see [Running jobs](02-running-jobs.md#python-dependencies)).

## Spark UI from Jupyter

While a session is active:

- Driver UI: <http://localhost:4040>
- Master UI: <http://localhost:8080>
- The exact URL is also available in Python:

```python
print(spark.sparkContext.uiWebUrl)
```

If port 4040 is already taken by another session, Spark picks 4041+ inside the container; that port is not published. Stop the other session or use the History Server.

## Notebook tips

- Create the session once in the first cell; reuse `spark` in later cells.
- After changing `SparkSession` configs, run `spark.stop()` and restart the kernel, then recreate the session. Configs are immutable once the session starts.
- `display(df)` renders a table in JupyterLab; `df.show()` prints a plain table.
- Use `df.explain(True)` to inspect the physical plan.
- Use `spark.sql(...)` for SQL after `df.createOrReplaceTempView("name")`.
- Large `collect()` calls pull data into the driver; prefer `show()`, `write`, or aggregations.

## Troubleshooting Jupyter

| Symptom | Fix |
|---|---|
| Token rejected | Read `JUPYTER_TOKEN` from `.env`; recreate the container |
| `Connection refused` to `spark-master:7077` | Check `docker compose ps`; the master may still be starting |
| Jobs hang at `RUNNING` with no executors | `spark.driver.host` is missing or wrong; use `jupyter` |
| `ModuleNotFoundError` in executor logs | Install the package in the worker image, not only Jupyter |
| Port 4040 not reachable | Another session owns it; stop it or check `uiWebUrl` |
| Kernel dies on large job | Reduce data size or executor memory; check host memory with `free -h` |

More: [Troubleshooting](05-troubleshooting.md).
