# Operations, scaling and maintenance

## Daily commands

```bash
cd ~/spark-docker

make ps               # container status
make up               # start everything
make down             # stop and remove containers (host data kept)
make restart          # restart containers
make logs             # follow all logs
make shell            # bash in spark-master
```

Raw Compose:

```bash
docker compose ps
docker compose up -d
docker compose stop
docker compose start
docker compose restart
docker compose logs -f --tail=100 spark-master
docker compose exec spark-master bash
```

## Container health

```bash
docker compose ps
docker inspect --format'{{.Name}} {{.State.Health.Status}}'\
  spark-master spark-history
```

The master and history server have HTTP health checks. Jupyter uses the health check built into its image. Workers have no health check; check `docker compose ps` and the Master UI instead.

To see why a container restarted:

```bash
docker compose logs --tail=200 spark-worker-1
docker inspect spark-worker-1 --format'{{json .State}}'| python3 -m json.tool
```

## Resource usage

```bash
docker stats --no-stream
free -h
df -h /home
```

The default stack reserves:

| Component | Memory |
|---|---|
| Master JVM | 1 GiB |
| Worker 1 | 4 GiB executor capacity + ~1 GiB JVM |
| Worker 2 | 4 GiB executor capacity + ~1 GiB JVM |
| History server | 1 GiB |
| Jupyter | ~1–2 GiB, depending on the driver |
| **Total** | **~12 GiB peak** |

On an 8 GB machine, lower `SPARK_WORKER_MEMORY` to `2g` (or run one worker).

## Scale workers

### Add a third worker

Copy the `spark-worker-2` service in `docker-compose.yml` and change:

```yaml
  spark-worker-3:
    <<: *spark-common
    container_name: spark-worker-3
    hostname: spark-worker-3
    command:
      - /opt/spark/bin/spark-class
      - org.apache.spark.deploy.worker.Worker
      - spark://spark-master:7077
      - --host
      - spark-worker-3
      - --cores
      -"${SPARK_WORKER_CORES:-2}"
      - --memory
      -"${SPARK_WORKER_MEMORY:-4g}"
      - --webui-port
      -"8081"
    environment:
      <<: *spark-env
      SPARK_DAEMON_MEMORY: 1g
    ports:
      -"127.0.0.1:8083:8081"
    depends_on:
      spark-master:
        condition: service_healthy
```

Then:

```bash
docker compose up -d
```

The new worker appears in <http://localhost:8080> within a few seconds.

### Change worker capacity

Edit `.env`:

```dotenv
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=6g
```

Recreate the workers:

```bash
docker compose up -d --force-recreate spark-worker-1 spark-worker-2
```

Make sure the total does not exceed host RAM. Also adjust `spark.executor.memory` in `conf/spark-defaults.conf` if needed: executors are carved out of the worker's advertised memory.

## Change Spark configuration

Edit `conf/spark-defaults.conf`, then restart the Spark services:

```bash
docker compose restart spark-master spark-worker-1 spark-worker-2 spark-history
```

Jupyter keeps its own Spark defaults; pass configuration in the notebook or with `--conf` when submitting from Jupyter.

Validate the Compose file after edits:

```bash
docker compose config --quiet && echo OK
```

## Change ports

The stack binds to `127.0.0.1` to avoid exposing Spark and Jupyter to your LAN. To change a host port, edit the left side of a mapping in `docker-compose.yml`:

```yaml
ports:
  -"127.0.0.1:9090:8080"# master UI on host port 9090
```

Then recreate the affected service:

```bash
docker compose up -d --force-recreate spark-master
```

Check for conflicts first:

```bash
ss -ltnp | grep -E'7077|8080|8081|8082|8888|18080|4040|4041'
```

## Persistence and backups

Everything important is on the host:

| Path | Back up? | Notes |
|---|---|---|
| `notebooks/` | Yes | Your analysis |
| `apps/` | Yes | Job code |
| `data/` | Yes | Inputs and outputs (could be large) |
| `conf/` | Yes | Cluster configuration |
| `events/` | Optional | Event logs for the History Server |
| `docker-compose.yml`, `Dockerfile.spark`, `.dockerignore`, `.env` | Yes | Stack definition |

The containers themselves are disposable. `docker compose down` does not delete any of the paths above.

Backup example:

```bash
tar czf ~/spark-docker-backup-$(date +%F).tgz \
  -C ~/spark-docker \
  docker-compose.yml Dockerfile.spark .dockerignore .env conf apps notebooks data
```

## Upgrade Spark

1. Check that the Jupyter image has a matching Spark tag:
   <https://quay.io/repository/jupyter/pyspark-notebook?tab=tags>
2. Check the Apache Spark base image tags:
   <https://hub.docker.com/r/apache/spark/tags>
3. Update the Spark version copied into the local image in `Dockerfile.spark`:

```dockerfile
COPY --from=apache/spark:4.2.2 /opt/spark /opt/spark
```

   Also update the `FROM python:X.Y-slim-bookworm` line if the Jupyter image moves to a different Python minor version. The driver and executor Python minor versions **must match**; otherwise RDD/UDF jobs fail with `[PYTHON_VERSION_MISMATCH]`.
   The pandas, pyarrow and numpy pins in `Dockerfile.spark` match the current Jupyter image; update them if the new Jupyter tag ships different versions.
4. Update the tags in `docker-compose.yml`:
   - `image: spark-local:4.2.2` in the shared `x-spark-common` block
   - `image: quay.io/jupyter/pyspark-notebook:spark-4.2.2` for `jupyter`
5. Build the local image, pull Jupyter, and recreate:

```bash
docker compose build --pull
docker compose pull jupyter
docker compose up -d --force-recreate
```

6. Verify:

```bash
docker compose exec spark-master spark-submit --version
docker compose exec spark-master python3 --version
docker compose exec jupyter python -c"import pyspark, sys; print(pyspark.__version__, sys.version)"
```

At the time this stack was created, Docker Hub carried `apache/spark:4.2.1`; Apache Spark 4.3.0 was released but its Docker tag was not yet published. Pin explicitly and avoid `latest`.

Existing event logs from an older Spark version may not render correctly in the new History Server. Clear `events/` when upgrading major versions:

```bash
make clean-output
```

## Clean up

```bash
# Remove generated outputs, warehouse and event logs
make clean-output

# Stop containers
docker compose down

# Remove the service images (base build images stay cached)
docker compose down --rmi all

# Remove unused Docker data (careful: affects other projects)
docker system df
# docker system prune
```

`make clean-output` preserves `data/sales.csv`, `data/words.txt`, notebooks and apps.

## Security

This stack is for local development:

- Spark standalone has **no authentication** on port 7077.
- Jupyter uses a single shared token (`spark` by default).
- Ports are bound to `127.0.0.1`, which prevents LAN access from other machines.
- If you expose the stack, put it behind a reverse proxy with TLS and strong authentication, and do not publish the Spark master RPC port.

## Enabling Spark Connect (optional)

Spark Connect is not configured in this stack. If you need it, start a Connect server in the master container and publish port `15002`:

```bash
docker compose exec spark-master \
  /opt/spark/sbin/start-connect-server.sh \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-connect_2.13:4.2.1
```

Add a port mapping for `15002` to the master service, and connect a client with:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

This is optional and not covered by the sample apps.
