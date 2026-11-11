# Repository Audit and Improvement Plan

> **Status**: working document for the curriculum upgrade. It records what was inspected,
> what was changed, and what remains. It is intentionally concrete so future contributors
> can continue the work without guessing.

**Audit date**: 2026-11-10  
**Spark version inspected**: Apache Spark 4.2.1 (PySpark 4.2.1, Python 3.13, Java 21)  
**Environment inspected**: Docker Standalone cluster (`spark-master`, two workers, History Server, JupyterLab)

---

## 1. What Was Inspected

| Area | Files / commands | Result |
|---|---|---|
| Repository layout | `find`, `git status`, `git log` | 78 notebooks, 14 curriculum modules, 15 docs, Docker stack, sample apps, deterministic datasets |
| Docker environment | `docker compose ps`, `docker compose config`, container logs | All five services were running and healthy |
| Runtime versions | PySpark probe inside `spark-jupyter` | Spark 4.2.1 confirmed; Python 3.13; Java 21 |
| Notebook structure | JSON parser + heading/section inventory | 76 core notebooks follow a 7-part pattern; 2 supplementary notebooks are outside it |
| Documentation | README, roadmap, architecture, interview, glossary, mistakes, experiments | Strong reference material; some count, priority, and technical-accuracy issues |
| Datasets | `data/generate_datasets.py`, CSV/JSON samples | Deterministic, synthetic, reproducible; suitable for the labs |
| Validation | `python3 scripts/validate_notebooks.py` | JSON and AST checks passed for the 76 core notebooks |
| Code quality | Import/static scan | 7 notebooks use `os` without importing it; a few experiment claims are stronger than the evidence |

---

## 2. Strengths to Preserve

- A real multi-container Spark cluster, not a single-process simulation.
- Event logging and a working History Server.
- Deterministic synthetic datasets with deliberate skew and dirty records.
- Rich hand-written notebooks for RDD fundamentals, DataFrame fundamentals, architecture,
  and several advanced topics.
- A large documentation suite (architecture, interview, glossary, mistakes, experiments).
- A working validation script and CI workflow.
- Clear separation between Spark control-plane RPC and data-plane shuffle/broadcast traffic.

**Rule for this upgrade**: preserve existing cells, experiments, and explanations unless a
change is needed for correctness, navigation, or completeness. Additive changes are preferred.

---

## 3. Gaps Found

### 3.1 Educational structure

Most generated notebooks (about 60 of the 76) contain:

1. A short title and four learning-objective bullets.
2. One architecture paragraph or ASCII diagram.
3. A SparkSession setup cell.
4. One short experiment.
5. Three exercises with reference solutions.
6. A summary.

They generally **lack**:

- A full problem/motivation narrative.
- A step-by-step core-concepts section with definitions.
- A detailed internal-architecture explanation.
- Real-world use cases with scenario, problem, implementation, result, alternative, and trade-offs.
- Business value.
- Production engineering guidance (monitoring, retries, idempotency, security, failure modes).
- Trade-offs and alternatives (inside and outside Spark).
- A structured investigation checklist for the Spark UI.
- A production/debugging exercise.

### 3.2 Repository hygiene

> **Status**: these were the findings at audit time, kept for traceability. Most are now
> resolved; see sections 4 and 6 for the current state.

- README claims 76 notebooks, but the repository contains 78 `.ipynb` files:
  76 curriculum notebooks, one root-level quickstart, and one root-level scratch notebook.
- The root-level scratch notebook (`notebooks/01_spark.ipynb`) duplicates the first RDD
  partition exercise and has no explanatory markdown.
- `scripts/validate_notebooks.py` hard-codes `/home/elhussieny/spark-docker/notebooks`,
  so the CI workflow would fail on a GitHub runner.
- The validation script skips the two root notebooks and prints a fixed "76" message.
- Priority symbols in `docs/LEARNING_ROADMAP.md` and `docs/PRIORITY_GUIDE.md` were stripped
  by an earlier emoji-removal pass, leaving blank cells such as `|  Must Know |`.
- Checkboxes are written as `[]` rather than `[ ]`.
- Some quote spacing was damaged by the same cleanup (`the"Hello World"Trap`).
- `.ipynb_checkpoints` directories exist locally (untracked).

### 3.3 Technical accuracy

| Issue | Where | Correction |
|---|---|---|
| "Every DataFrame operation eventually compiles down to RDD operations" | `02_rdd/01_rdd_fundamentals.ipynb`, `LEARNING_ROADMAP.md` | DataFrames execute physical operators; RDDs are a lower-level abstraction, not a literal compilation target for every operation |
| "Exactly-once" stated without conditions | Streaming notebooks and docs | Explain source replayability, checkpointing, deterministic processing, and idempotent/transactional sinks; Spark alone does not guarantee end-to-end exactly-once |
| Cache described as "data in RAM" | Several notebooks/docs | `cache()`/`persist()` may spill to disk or recompute; DataFrame `cache()` is `MEMORY_AND_DISK` by default in this stack |
| Worker memory described as executor heap | README / architecture diagram | Worker advertises 4 GiB capacity; the application requests 2 GiB executor heap plus overhead, with 1 core per executor by default |
| Jupyter defaults not using `conf/spark-defaults.conf` | Docker Compose / docs | Jupyter did not mount `./conf`; `spark.sql.shuffle.partitions` was 200 in a raw session. The notebook builders set 8, but the environment should be consistent |
| "Kryo is up to 10x faster" without qualification | `10_performance_and_optimization/05_memory_optimization.ipynb` | Label as workload-dependent, not guaranteed |
| "Parquet is 5x-10x smaller / 10x-100x faster" | `08_data_sources_and_formats/02_parquet_and_orc.ipynb` | Label as typical/hypothetical and measure in the lab |
| "DataFrames are 2-10x faster than RDDs" | `02_rdd/01_rdd_fundamentals.ipynb` | Workload-dependent; explain why, do not promise a multiplier |
| "Zero memory spill" as a production checklist item | `PRODUCTION_CHECKLIST.md` | Spill can be a healthy safety valve; the goal is to understand and bound it, not always eliminate it |
| "3-5 cores per executor" presented as a universal rule | `PRODUCTION_CHECKLIST.md` | Treat as a heuristic; small clusters and PySpark workloads often use fewer cores |
| `spark.sql.catalogImplementation=in-memory` | `00-spark-complete-guide.md` | Correct for the default non-Hive session catalog, but clarify that the lab does not configure an external Hive metastore |
| `spark.driver.host` and `spark.driver.bindAddress` | Docs are correct | Keep; explain why `localhost` fails in bridge networking |

### 3.4 Code issues

> **Status**: the missing `os` imports and unmeasured speedup claims were corrected during
> the upgrade. The notes below are retained for traceability.

- `os` is used without an import in:
  `05/05`, `08/02`, `09/02`, `09/05`, `11/04`, `13/01`, `13/04`.
- `08/02` reads Parquet file sizes with `os.listdir`, which fails if the output contains
  subdirectories; use a recursive size helper.
- Several generated experiments state a specific speedup in the "what happened" cell without
  measuring it. Replace with "what to observe" and explain that results depend on the cluster.
- Some experiments use `format("noop")`, which is not a built-in Spark sink in all
  distributions. Prefer `foreachBatch` or write to a temporary Parquet path.

### 3.5 Navigation and documentation

> **Status**: resolved. The README, docs index, topic index, use-case guide, business-context
> guide, and case-study index now cross-link.

- README does not link to a use-case index, business-context guide, topic index, or case studies.
- The roadmap has priorities but not the requested four-level vocabulary consistently
  (`Essential`, `Important`, `Advanced`, `Optional`) or a per-topic experiment/effort view.
- No dedicated end-to-end business case studies exist beyond the four project notebooks.
- No standard notebook template document exists.
- `docs/README.md` is a short table without learning paths or navigation.

---

## 4. Improvement Plan and Checklist

### Phase 1 — Audit and standards

- [x] Inspect repository, Docker stack, notebooks, docs, and datasets.
- [x] Record findings in this document.
- [x] Create `docs/NOTEBOOK_TEMPLATE.md` and a template notebook.
- [x] Add a consistent learning-contract block to every core notebook.

### Phase 2 — Enrich every topic

For each of the 76 core notebooks, add (without deleting existing material):

- [x] Priority, estimated effort, prerequisites, required depth, and role guidance.
- [x] Problem and motivation.
- [x] Core concepts and terminology.
- [x] Internal architecture / execution flow.
- [x] Two or more topic-specific real-world use cases.
- [x] Business value.
- [x] Production engineering guidance.
- [x] Trade-offs and alternatives.
- [x] Investigation checklist for Spark UI / plans / metrics.
- [x] One production/debugging exercise with hints.
- [x] Two interview questions with model answers.

### Phase 3 — Case studies

- [x] E-commerce analytics platform.
- [x] Telecom network analytics.
- [x] Financial transactions and fraud patterns (educational, not production fraud detection).
- [x] Logistics and delivery analytics.
- [x] Case-study index with business context, schema, architecture, and expected results.

### Phase 4 — Documentation and navigation

- [x] `docs/USE_CASES.md` mapping topics to scenarios and notebooks.
- [x] `docs/BUSINESS_CONTEXT.md` explaining Spark's role in a modern data stack.
- [x] `docs/TOPIC_INDEX.md` with priority, prerequisites, effort, and links.
- [x] Update `docs/LEARNING_ROADMAP.md` with four-level priorities and role paths.
- [x] Update `README.md` and `docs/README.md` with navigation and correct counts.
- [x] Add case-study and business-context links.

### Phase 5 — Cleanup and correctness

- [ ] Move the root scratch notebook into `notebooks/99_sandbox/`. **Decision**: keep
  `notebooks/01_spark.ipynb` as a supplementary scratch notebook for now; moving it would
  break links and add no curriculum value.
- [x] Decide whether the root quickstart stays supplementary or moves into Module 00.
  **Decision**: keep it supplementary and label it as a smoke test.
- [x] Fix missing `os` imports and fragile file-size code.
- [x] Correct RDD/DataFrame, cache, exactly-once, memory, and speedup claims.
- [x] Fix priority symbols and checkboxes in docs.
- [x] Make the validation script portable and comprehensive.
- [x] Add a link checker.
- [x] Re-run validation and selected notebooks.

### Phase 6 — Final report

- [x] Record what was inspected, changed, validated, and what remains.
- [x] Document any validation that could not be performed (for example, full execution of
  all 76 core notebooks, which is time-consuming on a four-core lab).

---

## 5. Design Rules for the Enrichment

1. **Do not delete useful content.** Insert new sections around existing cells.
2. **Be topic-specific.** A caching notebook gets caching use cases, not generic "Spark is
   used in e-commerce" paragraphs.
3. **Separate fact from estimate.** Mark hypothetical numbers. Never claim a benchmark
   result that was not measured in this lab.
4. **Respect the runtime.** Examples use Spark 4.2.1 APIs and the existing datasets under
   `/opt/spark-data/`.
5. **Keep the lab self-contained.** No Kafka, cloud storage, or external services are
   required for the core path. Kafka remains optional.
6. **Explain the business "why" without overselling Spark.** Include alternatives such as
   pandas, DuckDB, Flink, Kafka, dbt, and a data warehouse where they are genuinely better.
7. **Make production advice actionable.** Mention concrete metrics, configs, and failure
   modes, but avoid pretending the four-core lab is a production cluster.

---

## 6. Validation Status

This section is updated as work completes.

| Check | Status | Notes |
|---|---|---|
| Notebook JSON validity | Passed | 83 notebook files: 76 core, 4 case studies, 2 supplementary, 1 template |
| Python AST syntax | Passed | 723 code cells parsed; IPython magics skipped intentionally |
| Notebook structure | Passed | Required keys and cell fields present; no duplicate non-null cell IDs |
| Docker Compose config | Passed | `docker compose config` |
| Runtime version | Passed | Spark 4.2.1 in Jupyter and cluster containers |
| Local link checking | Passed | 349 local links across 28 Markdown files and 82 notebooks |
| Case-study execution | Passed, local mode | All four completed with zero cell errors on 2026-11-11 using `SPARK_MASTER=local[2]` |
| Full cluster-mode execution | Not run | The four-core cluster previously had a stale application holding all worker cores; do not claim cluster-mode validation for the current notebooks |
| Full execution of all 76 core notebooks | Not run | Time-consuming on a four-core lab; syntax validation is not execution |

---

## 7. Notes for Future Contributors

- The cluster is intentionally small. Some experiments will show only a few partitions or
  modest timing differences. That is expected; the goal is to observe mechanics, not to
  reproduce a production benchmark.
- When adding a notebook, start from `docs/NOTEBOOK_TEMPLATE.md` and keep the section order.
- When changing a configuration, update both the Docker Compose file and the documentation
  table that describes it.
- When adding a speedup claim, either measure it in the lab and record the hardware, or label
  it clearly as an illustrative/hypothetical range.
