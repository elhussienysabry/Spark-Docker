# Troubleshooting

Run this first for a quick health check:

```bash
cd ~/spark-docker
docker compose ps
docker compose logs --tail=50 spark-master
docker compose exec spark-master spark-submit --version
curl -s -o /dev/null -w'master UI: %{http_code}\n'http://localhost:8080
curl -s -o /dev/null -w'history UI: %{http_code}\n'http://localhost:18080
curl -s -o /dev/null -w'jupyter: %{http_code}\n'http://localhost:8888
```

## Docker daemon is not running

Symptom:

```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock
```

Fix:

```bash
sudo systemctl start docker
sudo systemctl enable docker   # optional, start at boot
docker info
```

## Port already in use

Symptom:

```
Error response from daemon: driver failed programming external connectivity ...
Bind for 127.0.0.1:8080 failed: port is already allocated
```

Find the process:

```bash
ss -ltnp | grep -E'7077|8080|8081|8082|8888|18080|4040|4041'
```

Stop the other service, or change the host port in `docker-compose.yml`:

```yaml
ports:
  -"127.0.0.1:8088:8080"
```

Then recreate the service:

```bash
docker compose up -d --force-recreate spark-master
```

## Workers do not register

Symptoms:

- Master UI shows no workers.
- `docker compose ps` shows workers restarting or exited.
- Worker log contains `Failed to connect to master`.

Checks:

```bash
docker compose ps
docker compose logs spark-master
docker compose logs spark-worker-1
docker compose logs spark-worker-2
```

The workers wait for the master health check. If they failed once, start them again:

```bash
docker compose up -d spark-worker-1 spark-worker-2
```

Verify name resolution from a worker:

```bash
docker compose exec spark-worker-1 getent hosts spark-master
docker compose exec spark-worker-1 bash -c'exec 3<>/dev/tcp/spark-master/7077 && echo master-reachable'
```

If you changed service names or hostnames in `docker-compose.yml`, make sure the worker command still points at `spark://spark-master:7077` and that `hostname`/container names are unique.

## `ModuleNotFoundError` in an executor

Symptom: the driver starts, then tasks fail with `ModuleNotFoundError: No module named'pandas'` (or `numpy`, `pyarrow`, etc.).

Cause: Python UDFs execute in the worker containers. They use `spark-local:4.2.1`, which includes pandas, numpy and pyarrow, but not every library installed in Jupyter. Installing a package only in the Jupyter container does not install it on the executors.

Fix: add the library to `Dockerfile.spark` and rebuild:

```dockerfile
# Dockerfile.spark — add before the USER spark line
RUN pip install --no-cache-dir seaborn
```

```bash
docker compose build
docker compose up -d --force-recreate spark-master spark-worker-1 spark-worker-2 spark-history
```

Or rebuild and recreate in one step:

```bash
docker compose up -d --build --force-recreate spark-master spark-worker-1 spark-worker-2 spark-history
```

Alternatively ship pure-Python modules with `--py-files deps.zip`; third-party binary packages still need to be installed in the image.

## Workers cannot reach the Jupyter driver

Symptoms:

- Notebook job stays `RUNNING` with zero executors completing tasks.
- Worker log shows `Connection refused` to the driver.
- `Lost executor` or `TaskSchedulerImpl: initial job has not accepted any resources`.

Cause: the driver is in the `jupyter` container, but `spark.driver.host` advertises an address the workers cannot reach.

Fix: in the notebook:

```python
.config("spark.driver.host","jupyter")
.config("spark.driver.bindAddress","0.0.0.0")
```

Then stop the session, restart the kernel, and create it again. For `spark-submit` from a Jupyter terminal:

```bash
--conf spark.driver.host=jupyter
--conf spark.driver.bindAddress=0.0.0.0
```

Do **not** use `localhost` or `127.0.0.1` as the driver host: from a worker container those point at the worker itself.

## History Server is empty

Checks:

```bash
ls -lah events/ | tail
docker compose logs spark-history
```

Requirements:

- The application had `spark.eventLog.enabled=true`.
- `spark.eventLog.dir` is `file:///opt/spark-events` (shared by all Spark containers).
- The application finished or is still running; the History Server polls periodically.
- The event log files are readable by the `spark` user (UID 185).

Jobs submitted with the default `conf/spark-defaults.conf` satisfy these. Notebook sessions need the settings shown in [JupyterLab](03-jupyter.md#connect-to-the-cluster).

Refresh <http://localhost:18080> after a few seconds. If the UI is up but empty, the event log directory may differ; compare:

```bash
docker compose exec spark-master printenv SPARK_CONF_DIR
docker compose exec spark-master grep -E'eventLog|history'/opt/spark-conf/spark-defaults.conf
```

## Permission denied under data or events

Symptom:

```
PermissionError: [Errno 13] Permission denied:'/opt/spark-data/output/...'
```

or the History Server cannot read event logs.

The Spark containers run as UID **185** (`spark`). For this local stack, the `data/` and `events/` roots are world-writable, so Spark can create files there.

Files that Spark creates inside those directories (for example `data/output/...`) are owned by UID 185, and files created by Jupyter are owned by UID 1000. You can read them, but deleting them from the host may fail. Use the cleanup target, which runs the removal as root in a one-off container:

```bash
make clean-output
```

If you want to remove only one output directory:

```bash
docker compose run --rm --no-deps --user root --entrypoint /bin/bash spark-master -c \
'rm -rf /opt/spark-data/output/my_result'
```

Event logs are created with mode `770` on purpose; only the History Server needs to read them. They may not be readable directly from the host. The History Server is at <http://localhost:18080>.

For anything beyond local development, use a custom image whose UID/GID matches the host user, and do not use world-writable directories.

## SELinux denials

Fedora and other SELinux systems require container labels on bind mounts. The Compose file uses `:z` on every mount. If you add a new mount, add `:z` too:

```yaml
volumes:
  - ./my-data:/opt/spark-data:rw,z
```

Check recent denials:

```bash
sudo ausearch -m avc -ts recent
```

As a last resort for a local dev machine, you can set the container to unconfined SELinux:

```yaml
security_opt:
  - label=disable
```

Prefer fixing the label over disabling SELinux.

## Out of memory

Symptoms:

- Containers are `OOMKilled` (`docker inspect ... | grep OOMKilled`).
- Worker loses executors.
- Kernel dies in Jupyter.

Check:

```bash
free -h
docker stats --no-stream
docker inspect spark-worker-1 --format'{{.State.OOMKilled}}'
```

Fix:

```bash
# .env
SPARK_WORKER_MEMORY=2g
SPARK_WORKER_CORES=1
```

```bash
docker compose up -d --force-recreate spark-worker-1 spark-worker-2
```

Also lower `spark.executor.memory` and `spark.driver.memory` in `conf/spark-defaults.conf` if needed. Close idle Jupyter kernels before launching big jobs.

## Version mismatch

Symptoms:

- `py4j.protocol.Py4JJavaError`
- `java.lang.ClassNotFoundException`
- `[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.10 than that in driver: 3.13`
- serialization errors after upgrading one image
- jobs hang right after submission

Check both versions:

```bash
docker compose exec spark-master spark-submit --version
docker compose exec jupyter python -c"import pyspark; print(pyspark.__version__)"
```

They must match (`4.2.1` in this stack), and the Python **minor** versions must match too. Check the Python versions:

```bash
docker compose exec spark-master python3 --version
docker compose exec jupyter python3 --version
```

Both must report `Python 3.13.x`. If they differ, adjust the `FROM python:X.Y-slim-bookworm` line in `Dockerfile.spark` to match Jupyter, then rebuild and recreate:

```bash
docker compose build
docker compose up -d --force-recreate spark-master spark-worker-1 spark-worker-2 spark-history
```

## Jupyter token rejected

Find the token:

```bash
grep JUPYTER_TOKEN .env
```

If you changed `.env`, recreate Jupyter:

```bash
docker compose up -d --force-recreate jupyter
```

You can also bypass the token for a local session by setting `JUPYTER_TOKEN` to a new value and reopening the URL.

## Container name conflicts

Symptom:

```
Error response from daemon: Conflict. The container name"/spark-master"is already in use
```

Another stack or an old container uses the same name. Check:

```bash
docker ps -a --filter name=spark-
```

Remove the old containers (this deletes only containers, not host files):

```bash
docker compose down
# or, if they belong to another project:
docker rm -f spark-master spark-worker-1 spark-worker-2 spark-history spark-jupyter
```

## Spark 4 ANSI errors

Spark 4 enables ANSI SQL mode by default. Queries that silently produced `NULL` in Spark 3 can now fail, for example:

```
org.apache.spark.SparkNumberFormatException: [CAST_INVALID_INPUT] ...
```

Fix the data or disable ANSI for the job:

```bash
--conf spark.sql.ansi.enabled=false
```

In a notebook:

```python
.config("spark.sql.ansi.enabled","false")
```

## No space left on device

Check Docker and host disk usage:

```bash
df -h / /home
docker system df
```

Reclaim unused Docker data:

```bash
docker image prune
docker builder prune
```

Do not run `docker system prune -a` unless you understand that it removes stopped containers and unused images for **all** projects.

## Useful diagnostic commands

```bash
# Compose state and configuration
docker compose ps
docker compose config

# Logs
docker compose logs --tail=200 spark-master
docker compose logs --tail=200 spark-worker-1
docker compose logs --tail=200 jupyter

# Cluster network
docker compose exec spark-worker-1 getent hosts spark-master
docker compose exec spark-worker-1 bash -c'exec 3<>/dev/tcp/spark-master/7077 && echo ok'

# Event logs
ls -lah events/

# Spark versions
docker compose exec spark-master spark-submit --version
docker compose exec jupyter python -c"import pyspark; print(pyspark.__version__)"

# Resources
docker stats --no-stream
free -h
df -h /home
```
