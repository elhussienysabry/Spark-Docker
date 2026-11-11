# Standard Notebook Template

Every topic notebook in this lab follows the same teaching contract. The order is deliberate:
the learner first understands the problem, then the mechanism, then sees it run, then connects
it to a real business system, and finally learns how it behaves in production.

> **Rule**: Keep sections in this order. A notebook may add sections, but should not skip the
> mandatory ones. If a notebook grows beyond roughly 40–50 cells and covers unrelated topics,
> split it into two notebooks.

---

## 1. Required Section Order

| Section | Purpose | Minimum content |
|---|---|---|
| **Header** | Title, module, notebook, environment | Spark version, runtime, link to previous/next topic |
| **Learning Contract** | Priority, effort, prerequisites, depth, roles | One table row per attribute |
| **A. Learning Objectives** | What the learner can do afterwards | 4–6 measurable objectives; include one interview objective |
| **B. Problem and Motivation** | Why the concept exists | Before/after example, failure mode without it |
| **C. Core Concepts** | Definitions and mental model | Terms table, step-by-step explanation, one simple example |
| **D. Internal Architecture** | What happens under the hood | Driver/executor/scheduler/shuffle/memory details relevant to the topic; ASCII or Mermaid diagram |
| **E. Practical Implementation** | Runnable PySpark | Objective, input, code, expected output characteristics, line-by-line explanation, internal behavior |
| **F. Real-World Use Cases** | 2–4 industry scenarios | Scenario, problem, why this concept, implementation idea, expected result, alternative, trade-offs |
| **G. Business Value** | Why a business cares | Time, cost, quality, decisions, reliability, scale; label hypothetical numbers |
| **H. Production Engineering** | How it behaves in production | Scalability, performance, fault tolerance, monitoring, logging, data quality, retries, idempotency, security, config, deployment |
| **I. Trade-offs and Alternatives** | When not to use it | Advantages, limitations, alternatives inside and outside Spark |
| **J. Experiments and Investigation** | Observe real behavior | Spark UI tab, `explain()`, partitions, config, logs, metrics; what to observe and what it means |
| **K. Exercises** | Practice ladder | Basic, practical, real-world, performance/debugging; hints before solutions |
| **L. Topic Summary** | Consolidation | Key concepts, internals, real-world applications, production notes, common mistakes, interview questions |

---

## 2. Learning Contract

Use the four priority levels from the roadmap:

| Level | Meaning |
|---|---|
| **Essential** | Must understand deeply; needed for almost every Data Engineering role |
| **Important** | Needed for practical Data Engineering work |
| **Advanced** | Useful for optimization, platform work, and specialized workloads |
| **Optional** | Learn when a project or job requires it |

Example:

```markdown
## Learning Contract

| Attribute | Value |
|---|---|
| **Priority** | Essential |
| **Estimated effort** | 2–3 hours |
| **Prerequisites** | DataFrame fundamentals, lazy evaluation |
| **Required depth** | Working knowledge + ability to read the physical plan |
| **Data Engineer** | Must be able to use it and debug common failures |
| **Senior Data Engineer** | Must understand internals and trade-offs |
| **Spark specialist** | Must be able to reason about scheduler and memory behavior |
```

---

## 3. Writing Guidance

### Problem and Motivation

Start with a concrete failure or cost, not a definition. For example:

> A nightly job joins a 2 TB fact table to a 40 MB country lookup. Without a broadcast join,
> both sides are shuffled, the network becomes the bottleneck, and the job misses its SLA.

Then explain what existed before the concept and what happens if the mechanism is missing.

### Core Concepts

Define terms in plain language first, then give the technical definition. Use a table for
terminology. Keep the first example small enough to trace by hand.

### Internal Architecture

Only include components relevant to the topic. Do not paste the same generic Driver/Executor
diagram into every notebook. Explain:

- Which component makes the decision.
- What is sent over the network.
- What is written to disk.
- What is held in memory.
- Where a failure can occur.

### Practical Implementation

Every code example should include:

1. **Objective** — what question the code answers.
2. **Input** — dataset path and schema assumptions.
3. **Code** — runnable in the existing Docker environment.
4. **Expected output** — shape, columns, or characteristics; avoid inventing exact numbers
   unless the dataset is deterministic and you have run it.
5. **Explanation** — what each important operation does.
6. **Internal behavior** — jobs, stages, shuffles, partitions, or memory implications.

### Real-World Use Cases

Use at least two scenarios. Each must include:

- **Scenario**: the company and system.
- **Problem**: the technical or operational pain.
- **Why this concept**: why this Spark mechanism is relevant.
- **Implementation idea**: how to build it with Spark.
- **Expected result**: what the system produces.
- **Alternative**: another technology or approach.
- **Trade-offs**: why choose Spark or the alternative.

Do not force every industry into every topic. A window-function notebook naturally fits
finance, retail, and telecom; a broadcast-variable notebook fits reference data and lookup
tables.

### Business Value

Answer the business question, not the technical one. Examples:

- Faster reporting enables a daily decision instead of a weekly one.
- Better data quality reduces manual reconciliation.
- Fewer shuffles can reduce infrastructure cost, but only if measured.

Label hypothetical numbers:

> *Hypothetical example*: reducing a 4-hour batch to 90 minutes could allow a same-day
> replenishment decision. This is an illustration, not a benchmark from this lab.

### Production Engineering

Cover only the dimensions that matter for the topic, but consider:

- Scalability and resource consumption.
- Performance and skew.
- Fault tolerance and retry behavior.
- Idempotency and duplicate prevention.
- Monitoring, logging, and alerting.
- Data quality and schema drift.
- Configuration and deployment.
- Security and access control.
- Operational reliability and backfills.

### Trade-offs and Alternatives

Include alternatives inside Spark (for example, broadcast join vs sort-merge join) and outside
Spark (pandas, DuckDB, a warehouse, Flink, Kafka, dbt). Avoid presenting Spark as the answer to
every data problem.

### Experiments and Investigation

Make the observation explicit:

- Which Spark UI tab should the learner open?
- Which metric should they compare?
- What does a healthy result look like?
- What does a problematic result look like?
- What config can they change to see the difference?

### Exercises

Use four levels:

1. **Basic understanding** — explain or predict.
2. **Practical implementation** — write code.
3. **Real-world scenario** — apply it to a business problem.
4. **Performance or debugging challenge** — diagnose and fix.

Put hints before the solution. Keep solutions in a collapsed `<details>` block.

### Topic Summary

End with:

- Key concepts.
- Important internal mechanisms.
- Real-world applications.
- Production considerations.
- Common mistakes.
- Interview questions.
- What to study next.

---

## 4. Code and Formatting Standards

- Use `spark` as the active `SparkSession`; do not create multiple sessions in one notebook.
- Use explicit schemas for file reads in production-style examples.
- Use `/opt/spark-data/...` paths inside the containers.
- Use `F.col`, `F.when`, `F.sum`, etc.; avoid `SELECT *` in final examples.
- Close with `spark.stop()` only in the final teardown cell.
- Keep code cells focused; one concept per cell.
- Do not commit credentials, tokens, or personal data.
- Do not claim a notebook was executed unless it was actually run.

---

## 5. Suggested Cell Skeleton

```text
[markdown] Title + metadata
[markdown] Learning Contract
[markdown] A. Learning Objectives
[markdown] B. Problem and Motivation
[markdown] C. Core Concepts
[markdown] D. Internal Architecture (diagram + steps)
[markdown] E. Practical Implementation (objective + input)
[code]     SparkSession setup
[code]     Practical example 1
[markdown] Explain output and internals
[code]     Practical example 2 (optional)
[markdown] F. Real-World Use Cases
[markdown] G. Business Value
[markdown] H. Production Engineering
[markdown] I. Trade-offs and Alternatives
[markdown] J. Experiments and Investigation
[code]     Experiment
[markdown] Observation guide
[markdown] K. Exercises (4 levels + hints)
[code]     Exercise starter cells
[markdown] Solutions in <details>
[markdown] L. Topic Summary
[code]     spark.stop()
```

---

## 6. Quality Checklist

Before committing a notebook:

- [ ] JSON is valid and every code cell parses with `ast.parse`.
- [ ] The notebook runs top-to-bottom in the Docker environment, or the unrun cells are
      explicitly marked as optional.
- [ ] No hard-coded absolute host paths (use `/opt/spark-data/...`).
- [ ] No secrets or credentials.
- [ ] At least two topic-specific real-world use cases.
- [ ] At least one experiment with an observation guide.
- [ ] At least one production/debugging exercise.
- [ ] Interview questions have model answers.
- [ ] Links to previous/next notebooks are correct.
- [ ] Claims about speed, cost, or guarantees are measured or labeled as illustrative.
