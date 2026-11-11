# Business Context: Where Spark Fits in a Modern Data Platform

> **Purpose**: this guide explains the business and architectural context behind the Spark
> curriculum. It is written for data engineers, analytics engineers, platform engineers, and
> technical leaders who need to justify a design, estimate its value, or decide when Spark is
> the wrong tool.
>
> **Scope**: Apache Spark 4.2.1, primarily PySpark and Spark SQL, in a standalone Docker lab.
> Production platforms may use YARN, Kubernetes, or a managed Spark service; the engineering
> principles are similar, but the operational details differ.

---

## 1. The Problem Spark Exists to Solve

A modern organization produces data from transactional systems, event streams, SaaS APIs,
sensors, logs, and files. The business wants to turn that data into decisions:

- Finance needs a trusted revenue number.
- Operations needs to know which shipment or cell site is failing now.
- Marketing needs to understand retention and customer value.
- Risk needs to prioritize a review queue.
- Product needs to know how a feature is used.

A single machine can answer many of these questions. The difficulty appears when the data is
too large, too varied, too late, or too shared for one machine and one team. Spark's value is
not "big data" in the abstract. Its value is a **general-purpose distributed execution engine**
for batch and micro-batch workloads that need:

- distributed storage scans and joins,
- fault tolerance across many machines,
- a shared SQL/DataFrame API,
- integration with a data lake and many file formats,
- and enough operational maturity to run on a shared cluster.

That value comes with cost: cluster operations, shuffle tuning, memory management, and a
development experience that is heavier than a local SQL engine.

---

## 2. Where Spark Sits in the Data Stack

```text
 Sources                 Ingestion              Storage                  Processing                 Serving
 ───────                 ─────────              ───────                  ──────────                 ───────
 OLTP databases   ──►   CDC / batch       ──►  Object storage / lake  ──►  Spark batch / streaming ──►  Warehouse / BI
 Event streams    ──►   Kafka / Kinesis    ──►  Delta / Iceberg / Hudi ──►  Spark SQL / PySpark      ──►  Feature store
 SaaS APIs        ──►   Airbyte / Fivetran ──►  Parquet / ORC          ──►  Validation / marts       ──►  Applications
 Logs / files     ──►   File landing zone  ──►  Hive-style partitions  ──►  ML feature engineering   ──►  APIs / dashboards
```

Spark usually does **not** own the source of truth, the transactional serving layer, or the
semantic layer. It sits between storage and serving, where large-scale transformation,
validation, and feature computation happen.

### Typical responsibilities

| Layer | Typical tools | Spark's role |
|---|---|---|
| **Source systems** | PostgreSQL, Oracle, Kafka, SaaS APIs | Read via connectors; never replace the source |
| **Ingestion** | CDC, Kafka Connect, Fivetran, Airbyte, custom jobs | Sometimes Spark, but dedicated ingestion is often better |
| **Storage** | S3/GCS/ADLS, HDFS, local files | Read/write Parquet, ORC, Delta, Iceberg, Hudi |
| **Processing** | Spark, Flink, dbt, warehouse SQL | Batch joins, aggregations, windows, quality checks, ML prep |
| **Serving** | Warehouse, BI, feature store, application DB | Publish curated tables; do not serve point lookups from Spark |
| **Governance** | Catalog, lineage, policy engine | Integrate with catalog and access controls; Spark is not the policy owner |

A useful rule: **Spark transforms data; it is rarely the system of record and rarely the
user-facing serving layer.**

---

## 3. Batch, Streaming, and Interactive Work

Spark supports several execution styles. The business requirement should choose the style,
not the other way around.

| Style | Latency | Typical use | Spark component | Main trade-off |
|---|---|---|---|---|
| **Batch** | Minutes to hours | Daily finance marts, historical backfills, model training | Spark SQL / DataFrames | Simple and efficient; not fresh enough for real-time decisions |
| **Micro-batch streaming** | Seconds to minutes | Near-real-time KPIs, alerting, continuous ingestion | Structured Streaming | State, checkpoints, late data, and operational complexity |
| **Interactive SQL** | Seconds | Ad-hoc analysis by analysts | Spark SQL / notebooks | Cluster startup and concurrency; a warehouse may be better |
| **Machine learning** | Batch or iterative | Feature engineering, model training | Spark ML / Pandas UDFs | Distributed training is not always faster than a single GPU |
| **Graph** | Batch or iterative | Fraud rings, network analysis | GraphX / GraphFrames (external) | Niche; dedicated graph systems may be better |

### Freshness is a business decision

"Do we need real-time?" is usually the wrong question. Better questions:

- What decision changes if the data is 5 minutes old instead of 1 hour old?
- What is the cost of a false alert caused by late data?
- Who is on call when the streaming job fails at 02:00?
- Can the business tolerate a restatement after late events arrive?

If the answer is "the dashboard is reviewed each morning", a batch pipeline is often the
right choice. If the answer is "we must block a transaction before authorization", Spark
Structured Streaming alone is usually not the right latency profile; a feature store and a
low-latency rules/model service are more appropriate.

---

## 4. What Spark Is Good At

- **Large scans and joins** over partitioned files in a data lake.
- **Multi-source transformation** where data arrives in different formats and cadences.
- **Windowed analytics** such as cohort retention, ranking, sessionization, and running totals.
- **Batch feature engineering** for machine learning.
- **Historical backfills and reprocessing** with the same code path as incremental runs.
- **Data quality and reconciliation** at scale.
- **Unified batch and streaming APIs** for teams that want one programming model.
- **SQL and DataFrame interoperability** for different skill sets.

## 5. What Spark Is Not Good At

- **Point lookups** for an application (use a database or cache).
- **Sub-second event processing** with strict latency (use Flink, Kafka Streams, or a streaming
  database).
- **Small-data exploration** where cluster startup and shuffle overhead dominate (use DuckDB,
  pandas, or Polars).
- **Governed, high-concurrency BI** without a warehouse or semantic layer.
- **Transactional consistency** across arbitrary files; lakehouse table formats add transactions,
  but Spark alone is not a database.
- **Replacing domain logic** with a generic "data lake" layer. Business definitions still need
  ownership.

---

## 6. Common Architecture Patterns

### 6.1 Medallion / layered lake

```text
Raw / bronze  ──►  Clean / silver  ──►  Curated / gold  ──►  BI / applications
```

- **Bronze**: immutable landing data, close to the source, with ingestion metadata.
- **Silver**: cleaned, conformed, deduplicated, and joined at a documented grain.
- **Gold**: business marts and metrics with explicit definitions and access controls.

Spark is commonly used for silver and gold transformations. The case studies in this
repository follow a simplified version of this pattern: raw CSV → quality gate → enriched
fact → curated Parquet mart.

### 6.2 Lambda vs Kappa

- **Lambda**: a batch layer for correctness and a speed layer for freshness. More moving parts,
  but batch reprocessing is straightforward.
- **Kappa**: a single streaming path, with replay from the log for reprocessing. Simpler
  conceptually, but requires a replayable source and mature streaming operations.

Spark Structured Streaming can implement a Kappa-style path, but only if the source is
replayable and the sink is idempotent or transactional. "Exactly once" is an end-to-end property,
not a Spark configuration flag.

### 6.3 Event-driven enrichment

A stream of events is joined to reference data or another stream, then written to a serving
store. Spark is a good fit when the enrichment is large or stateful and a few seconds of latency
is acceptable. For per-request enrichment, a cache or feature store is usually better.

### 6.4 Feature engineering and ML preparation

Spark can compute historical features, point-in-time joins, and training datasets. The hard
parts are **label timing**, **feature availability**, and **training/serving skew**, not the
distributed join itself. A feature store may be the better system of record for online features.

---

## 7. The Economics of Spark

A Spark job consumes:

- **Cluster time**: driver, executors, shuffle service, storage I/O.
- **Human time**: pipeline development, tuning, on-call, and incident response.
- **Opportunity cost**: data freshness and the decisions that are delayed.
- **Risk**: incorrect metrics, data leaks, failed SLAs, and unreproducible backfills.

### A simple cost model

```text
job cost ≈ (driver hours × driver rate)
         + (executor core-hours × core rate)
         + (storage scanned / written × storage rate)
         + (human hours × loaded rate)
         + (cost of a bad decision caused by stale or wrong data)
```

The last term is often the largest and the hardest to measure. A "cheap" job that produces an
untrusted number can be more expensive than a well-governed warehouse query.

### Hypothetical example

> A nightly Spark job takes 4 hours on 20 executor cores. If the job is reduced to 90 minutes
> through better partitioning and join strategy, the compute saving is proportional to the
> core-hours removed. The business value may be larger: a replenishment decision can move from
> the next day to the same morning. These numbers are illustrative; measure your own job before
> claiming a saving.

### When to optimize

Optimize when one of these is true:

1. The job misses a business SLA.
2. The job is a large share of cluster cost.
3. The job is unstable or requires frequent manual intervention.
4. A downstream team is blocked by the runtime.
5. The data volume is growing and the current design will not survive the next year.

Do not optimize a job that is already fast enough and correct. Spend the effort on a risk that
matters.

---

## 8. Data Quality, Contracts, and Ownership

Spark can enforce checks, but it cannot decide what "correct" means. A data contract should
define:

- **Grain**: what one row means.
- **Schema**: types, nullability, allowed values.
- **Freshness**: when the data is expected and what "late" means.
- **Completeness**: required partitions, row-count bounds, and referential integrity.
- **Metric definitions**: revenue, active customer, on-time, fraud rate.
- **Ownership**: who is paged when a check fails.
- **Change policy**: how schema or metric changes are communicated.

### A practical quality gate

| Check | Example | Action |
|---|---|---|
| **Row count** | Orders within ±20% of the 7-day baseline | Alert; block publish if severe |
| **Key uniqueness** | One row per `order_id` | Deduplicate or fail |
| **Referential integrity** | Every `product_id` exists in the product dimension | Quarantine unmatched rows |
| **Null rate** | Customer ID null rate below 5% | Alert; classify guest orders |
| **Value range** | Quantity > 0, price ≥ 0 | Reject invalid rows |
| **Freshness** | Latest event within 15 minutes | Alert on lag |
| **Reconciliation** | Mart total equals source total for the period | Block publish on mismatch |

A successful Spark job is not a quality gate. A job can complete with zero rows, duplicate
rows, or silently dropped late data.

---

## 9. Reliability and Service Levels

Define the service level in business terms, then translate it into engineering metrics.

| Business SLA | Engineering metric | Example alert |
|---|---|---|
| "Finance has revenue by 07:00" | Job completion time, data freshness | Publish misses 07:00 two days in a row |
| "The NOC sees cell issues within 5 minutes" | End-to-end latency, watermark lag | p95 latency > 5 minutes for 10 minutes |
| "Fraud alerts are reviewed within 1 hour" | Alert volume, queue age | Queue age > 60 minutes |
| "No duplicate shipments in the mart" | Duplicate rate, reconciliation | Any duplicate shipment ID |
| "Backfills do not change published history silently" | Restatement log, versioning | A published partition changes without a version |

### Reliability practices

- **Idempotent writes**: rerunning a partition should not duplicate data.
- **Checkpointing** for streaming state and offsets.
- **Retries with backoff** for transient failures; do not retry data-quality failures blindly.
- **Dead-letter/quarantine** paths for malformed records.
- **Schema evolution** policy and tests.
- **Runbooks** for common failures.
- **Event logs and Spark UI** retention for post-mortems.
- **Cost and duration dashboards** so regressions are visible.

---

## 10. Security, Privacy, and Governance

Spark jobs often touch the most sensitive data in the company. Treat the platform as part of
the security boundary.

- **Least privilege**: the job identity should read only the tables it needs and write only its
  target location.
- **PII minimization**: do not copy emails, card numbers, or addresses into a broad mart unless
  required. Tokenize or mask at ingestion.
- **Access control**: table/column-level policies in the catalog or lakehouse format; filesystem
  permissions alone are not enough.
- **Audit**: log who ran what, over which date range, and which version of the code.
- **Retention**: delete raw personal data according to policy; do not keep it "just in case".
- **Secrets**: never put credentials in notebooks or code; use the platform secret manager.
- **Regulatory context**: fraud, credit, health, and telecom data may have additional legal
  constraints. A synthetic lab cannot model those obligations.

---

## 11. Roles and Responsibilities

| Role | Owns | Success looks like |
|---|---|---|
| **Data Engineer** | Ingestion, transformation, quality gates, orchestration | Pipelines are correct, repeatable, and observable |
| **Senior Data Engineer** | Architecture, performance, reliability, data contracts | The platform scales and failures are contained |
| **Analytics Engineer** | Curated marts, semantic definitions, BI usability | Business users trust and can self-serve |
| **Platform / Infrastructure Engineer** | Cluster, networking, storage, security, cost | Resources are available, isolated, and affordable |
| **Data Scientist / ML Engineer** | Features, models, evaluation, serving | Training and serving are consistent and monitored |
| **Product / Business Owner** | Decisions, metric definitions, priorities | The data changes a decision and the value is measured |

A common failure is to assign all of these to "the data team" without decision rights. Spark
performance is only one part of a reliable data product.

---

## 12. Building a Business Case

Use a one-page structure:

1. **Decision**: what will someone do differently?
2. **Current state**: how is the decision made today, and what does it cost?
3. **Proposed change**: what data product or pipeline enables the new decision?
4. **Alternatives**: warehouse, DuckDB, streaming, vendor, or do nothing.
5. **Value**: time, cost, quality, risk, or revenue; label assumptions.
6. **Cost**: compute, storage, people, and ongoing operations.
7. **Risks**: data quality, adoption, security, vendor lock-in.
8. **Measurement**: baseline, target, and review date.

### Example template

| Element | Example (hypothetical) |
|---|---|
| Decision | Replenish top categories twice daily instead of weekly |
| Current state | 3 analyst-days/month reconciling spreadsheets; stockouts in 4% of SKUs |
| Proposed change | Trusted daily sales mart + store-level replenishment view |
| Alternatives | Warehouse-only mart; vendor planning tool; do nothing |
| Value | Faster replenishment, less manual reconciliation, fewer stockouts |
| Cost | Spark jobs, storage, 0.5 FTE engineering, on-call |
| Risks | Source late data, poor store compliance, metric disputes |
| Measurement | Stockout rate, reconciliation hours, job SLA, adoption |

The value case should survive a skeptical review. "Spark is fast" is not a business case.
"Finance can close the books one day earlier because the revenue mart is trusted by 07:00"
is.

---

## 13. Common Failure Modes (and What They Look Like)

| Failure | Symptom | Root cause | Prevention |
|---|---|---|---|
| **Grain fan-out** | Revenue doubles after a join | Dimension key is not unique | Test key uniqueness; aggregate before join |
| **Silent data loss** | Row count drops, job still succeeds | Filter or bad join drops records | Reconciliation, quarantine, count checks |
| **Small-file explosion** | Planning and listing dominate runtime | Too many partitions/files | Compact, target file size, avoid over-partitioning |
| **Skew** | A few tasks run for hours | Hot key or partition | Salting, AQE skew join, repartition |
| **Memory pressure** | Spill, GC, executor loss | Oversized partitions, cache, shuffle | Right-size partitions, project columns, tune memory |
| **Late data** | Metrics change after publication | No lateness policy | Watermark or reconciliation window, restatement log |
| **Non-idempotent writes** | Duplicates after retry | Append without dedup | Partition overwrite, MERGE, staging/publish |
| **Metric drift** | Two teams report different revenue | No semantic layer or ownership | Metric dictionary, tests, review |
| **Orphaned jobs** | Cluster full, jobs waiting | No cleanup or resource limits | Dynamic allocation, quotas, TTL |
| **Security incident** | PII in a broad mart | No classification or masking | Data catalog, policy, least privilege |

---

## 14. Choosing the Right Tool

| Requirement | Spark | Warehouse | DuckDB / pandas | Flink / Kafka Streams | Notes |
|---|---|---|---|---|---|
| Multi-TB historical batch | Strong | Strong | Weak | Weak | Compare cost and governance |
| Sub-second streaming | Weak | Weak | Weak | Strong | Spark is micro-batch |
| Governed BI with concurrency | Moderate | Strong | Weak | Weak | Warehouse + semantic layer |
| Local ad-hoc analysis | Overkill | Overkill | Strong | Weak | Start small |
| Complex file lake processing | Strong | Moderate | Moderate | Weak | Lakehouse formats help |
| ML feature engineering | Strong | Moderate | Strong (small) | Weak | Feature store for online |
| Simple scheduled SQL | Moderate | Strong | Weak | Weak | dbt + warehouse often simpler |

The right answer is usually a **combination**: Spark for lake transformation, a warehouse for
governed BI, a streaming engine for low latency, and a feature store for online ML. Avoid
turning one tool into an architecture.

---

## 15. How the Case Studies Fit

The four runnable case studies are deliberately different:

| Case study | Grain | Core lesson | What it teaches about business context |
|---|---|---|---|
| **E-commerce** | Order line | Revenue, margin, retention, order outcomes | A shared metric definition is a business asset |
| **Telecom** | Network event | Availability, p95 latency, busy-hour capacity | Averages hide the customer experience |
| **Fraud patterns** | Authorization | Velocity, explainable rules, precision/recall | Labels are delayed and false positives have a cost |
| **Logistics** | Shipment | On-time SLA, carrier/lane scorecards | The definition of "on time" is a contract |

Each notebook includes business context, a data model, an architecture, runnable code,
production guidance, trade-offs, experiments, and exercises. The point is not to memorize the
queries; it is to practice connecting a technical mechanism to a decision.

---

## 16. A Practical Reading Path

1. Read [USE_CASES.md](USE_CASES.md) to find a scenario close to your domain.
2. Read the relevant topic notebook's **Problem and Motivation** and **Production Engineering**
   sections.
3. Run the notebook and inspect the Spark UI.
4. Read [CASE_STUDIES.md](CASE_STUDIES.md) and complete one case study end to end.
5. Write a one-page business case for a pipeline in your own domain using section 12.
6. Review [PRODUCTION_CHECKLIST.md](PRODUCTION_CHECKLIST.md) before calling it production-ready.

---

## 17. Final Principle

> **Spark is an execution engine, not a data strategy.**

A successful Spark platform is built on clear ownership, trustworthy definitions, quality
gates, security, and a realistic service-level agreement. The cluster is the easy part to
diagram and the hard part to operate; the business decision is the reason it exists.

---

*Related: [Use cases](USE_CASES.md) · [Case studies](CASE_STUDIES.md) · [Topic index](TOPIC_INDEX.md) · [Production checklist](PRODUCTION_CHECKLIST.md) · [Learning roadmap](LEARNING_ROADMAP.md)*
