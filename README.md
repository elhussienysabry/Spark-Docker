# Apache Spark Learning Lab

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.2.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2%2B-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A self-contained Apache Spark 4.2.1 lab that runs a real standalone cluster in Docker and
teaches distributed data engineering through 80 curriculum notebooks, from fundamentals to
production-style case studies.

The stack includes a Spark master, two workers, a Spark History Server, and JupyterLab.
Everything runs locally and starts with one command.

## Features

- **Real distributed cluster** — one master and two workers, with live Spark UIs, event
  logs, and post-run history.
- **80 curriculum notebooks** — 76 core topics across 14 modules plus four end-to-end
  business case studies.
- **Hands-on throughout** — runnable examples, controlled experiments, production failure
  modes, and interview questions in every core topic.
- **Observable internals** — inspect DAGs, shuffle files, memory spills, task retries, and
  executor loss rather than only reading about them.
- **Deterministic datasets** — synthetic retail, log, streaming, telecom, fraud, and
  logistics data that regenerate identically.
- **Automated checks** — notebook JSON/AST validation and local link checking in CI.

## Technology

| Layer | Stack |
| :--- | :--- |
| Runtime | Apache Spark 4.2.1, PySpark 4.2.1 |
| Languages | Python 3.13, Java 21, Scala 2.13 |
| Interface | JupyterLab 4, Spark Web UIs, Spark History Server |
| Platform | Docker Engine 24+, Docker Compose v2+ |
| Libraries | pandas 2.3, PyArrow 25, NumPy 2.5 |

## Quick start

**Prerequisites:** Docker Engine 24+ with Compose v2, at least 8 GB RAM (16 GB
recommended), 15 GB free disk space, and ports `4040`, `4041`, `7077`, `8080`–`8082`,
`8888`, and `18080` available.

```bash
git clone https://github.com/elhussienysabry/Spark-Docker.git
cd Spark-Docker
cp .env.example .env   # optional; defaults work for local use
docker compose up -d
docker compose ps
```

Open the services:

| Service | URL |
| :--- | :--- |
| JupyterLab | <http://localhost:8888/?token=spark> |
| Spark Master UI | <http://localhost:8080> |
| Worker 1 / Worker 2 UI | <http://localhost:8081> / <http://localhost:8082> |
| Spark History Server | <http://localhost:18080> |
| Spark Driver UI (while a job runs) | <http://localhost:4040> (Jupyter) / <http://localhost:4041> (`spark-submit`) |

Start with
[`notebooks/00_environment_and_spark_setup/00_spark_environment.ipynb`](notebooks/00_environment_and_spark_setup/00_spark_environment.ipynb)
to verify driver-worker communication. `make help` lists the available shortcuts, and
`make test` runs the sample batch jobs.

## Project structure

```text
.
├── notebooks/      # 80 curriculum notebooks, two quickstarts, and a template
├── apps/           # sample spark-submit jobs
├── data/           # deterministic synthetic datasets
├── docs/           # guides, roadmap, architecture, and case-study notes
├── conf/           # shared Spark configuration
├── scripts/        # generators and validation tools
├── docker-compose.yml
├── Dockerfile.spark
└── Makefile
```

The curriculum is organized as `notebooks/00_environment_and_spark_setup/` through
`notebooks/13_end_to_end_projects/`, with business case studies in
`notebooks/14_business_case_studies/`. See the
[topic index](docs/TOPIC_INDEX.md) for every notebook.

## Documentation

| Guide | What it covers |
| :--- | :--- |
| [Learning roadmap](docs/LEARNING_ROADMAP.md) | 12-week path from setup to production, with milestones and role paths |
| [Topic index](docs/TOPIC_INDEX.md) | All 76 core topics with priority, effort, prerequisites, and experiments |
| [Case studies](docs/CASE_STUDIES.md) | Four runnable end-to-end projects: e-commerce, telecom, fraud, and logistics |
| [Datasets](docs/DATASETS.md) | Dataset inventory, container paths, and regeneration commands |
| [Spark architecture](docs/SPARK_ARCHITECTURE.md) | Execution lifecycle, memory layout, networking, and schedulers |
| [Use cases](docs/USE_CASES.md) | Real-world scenarios mapped to every core notebook |
| [Business context](docs/BUSINESS_CONTEXT.md) | How Spark fits into a data platform, and when another tool is better |
| [Interview preparation](docs/INTERVIEW_PREPARATION.md) | Questions across junior, mid, senior, and staff levels |
| [Common mistakes](docs/COMMON_MISTAKES.md) | Production antipatterns with before/after fixes |
| [Production checklist](docs/PRODUCTION_CHECKLIST.md) | Readiness checks for cluster sizing, storage, shuffle, memory, and streaming |
| [Full documentation index](docs/README.md) | Complete list of guides and references |

For setup, operations, and troubleshooting details, see the
[quickstart](docs/01-quickstart.md), [operations guide](docs/04-operations.md), and
[troubleshooting guide](docs/05-troubleshooting.md).

## Contributing

Contributions are welcome. Read the [contributing guide](CONTRIBUTING.md) before opening a
pull request, and follow the [code of conduct](CODE_OF_CONDUCT.md). Report security issues
according to the [security policy](SECURITY.md).

Run the validation suite before submitting changes:

```bash
make validate          # notebook JSON/AST checks plus local link checks
docker compose config  # validate the Compose file
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

Copyright (c) 2026 Elhussieny Sabry.
