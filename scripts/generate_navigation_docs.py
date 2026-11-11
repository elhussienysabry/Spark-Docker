#!/usr/bin/env python3
"""Generate the navigation indexes from curriculum_metadata.py.

Outputs:
    docs/TOPIC_INDEX.md
    docs/USE_CASES.md

The metadata is the source of truth for priority, effort, prerequisites,
experiments, and real-world use cases. Re-run after editing metadata.
"""

from __future__ import annotations

import json
import sys
from collections import OrderedDict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import curriculum_metadata as metadata  # noqa: E402

DOCS = ROOT / "docs"
NOTEBOOKS = ROOT / "notebooks"

MODULE_TITLES = {
    "00_environment_and_spark_setup": "Module 00 — Environment and Spark setup",
    "01_spark_fundamentals": "Module 01 — Spark fundamentals",
    "02_rdd": "Module 02 — Resilient Distributed Datasets",
    "03_dataframe_and_spark_sql": "Module 03 — DataFrames and Spark SQL",
    "04_execution_engine": "Module 04 — Execution engine",
    "05_partitions_and_shuffle": "Module 05 — Partitions and shuffle",
    "06_memory_and_storage": "Module 06 — Memory and storage",
    "07_spark_sql_internals": "Module 07 — Spark SQL internals",
    "08_data_sources_and_formats": "Module 08 — Data sources and formats",
    "09_streaming": "Module 09 — Structured Streaming",
    "10_performance_and_optimization": "Module 10 — Performance and optimization",
    "11_fault_tolerance_and_reliability": "Module 11 — Fault tolerance and reliability",
    "12_advanced_topics": "Module 12 — Advanced topics",
    "13_end_to_end_projects": "Module 13 — End-to-end projects",
}


def notebook_title(rel_path: str) -> str:
    path = NOTEBOOKS / rel_path
    try:
        notebook = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return Path(rel_path).stem.replace("_", " ").title()
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "markdown":
            continue
        for line in "".join(cell.get("source", [])).splitlines():
            if line.startswith("# "):
                return line[2:].strip()
    return Path(rel_path).stem.replace("_", " ").title()


def module_of(rel_path: str) -> str:
    return rel_path.split("/", 1)[0]


def by_module() -> "OrderedDict[str, list[tuple[str, dict]]]":
    grouped: "OrderedDict[str, list[tuple[str, dict]]]" = OrderedDict()
    for rel_path, topic in metadata.TOPICS.items():
        grouped.setdefault(module_of(rel_path), []).append((rel_path, topic))
    for items in grouped.values():
        items.sort(key=lambda pair: pair[0])
    return grouped


def write_topic_index() -> None:
    grouped = by_module()
    lines = [
        "# Topic Index",
        "",
        "> **Generated file**: this index is built from `scripts/curriculum_metadata.py` by",
        "> `scripts/generate_navigation_docs.py`. Edit the metadata, not this table, then re-run",
        "> the generator.",
        "",
        "This index covers the 76 core curriculum notebooks. The four business case studies are",
        "indexed separately in [CASE_STUDIES.md](CASE_STUDIES.md); their business context is in",
        "[BUSINESS_CONTEXT.md](BUSINESS_CONTEXT.md).",
        "",
        "## How to use this index",
        "",
        "1. Pick a role path below, or start with the **Essential** topics in module order.",
        "2. Read the prerequisite column before opening a notebook.",
        "3. Use the experiment column as the minimum hands-on check for the topic.",
        "4. Follow the `Next topic` link at the end of each notebook.",
        "",
        "## Priority levels",
        "",
        "| Level | Meaning |",
        "|---|---|",
        "| **Essential** | Needed for almost every data-engineering role. Learn deeply. |",
        "| **Important** | Needed for practical production work. Learn with hands-on practice. |",
        "| **Advanced** | Needed for optimization, platform work, and specialized workloads. |",
        "| **Optional** | Learn when a project or role requires it. |",
        "",
        "## Role paths",
        "",
        "| Role | Start here | Then | Depth target |",
        "|---|---|---|---|",
        "| **Data Engineer** | Modules 00–05 | Modules 08, 10, 13 | Build and operate reliable batch pipelines |",
        "| **Senior Data Engineer** | Modules 04–07 | Modules 09–12, case studies | Diagnose plans, memory, shuffle, and reliability |",
        "| **Analytics Engineer** | Modules 03, 08 | Modules 10, 13, case studies | Curated marts, SQL, data quality, and cost |",
        "| **Platform / Infrastructure Engineer** | Modules 01, 04, 12 | Modules 06, 10, 11 | Cluster operations, resources, and isolation |",
        "| **Spark Specialist** | Modules 04–07 | Modules 10–12, source reading | Scheduler, Catalyst, Tungsten, and tuning |",
        "",
    ]

    for module, items in grouped.items():
        lines.extend(
            [
                f"## {MODULE_TITLES.get(module, module)}",
                "",
                "| Notebook | Priority | Effort | Prerequisites | Experiment |",
                "|---|---|---|---|---|",
            ]
        )
        for rel_path, topic in items:
            title = notebook_title(rel_path)
            link = f"[{title}](../notebooks/{rel_path})"
            experiment = topic.get("experiment", {}).get("title", "See notebook")
            lines.append(
                f"| {link} | {topic.get('priority', '')} | {topic.get('effort', '')} | "
                f"{topic.get('prereqs', '')} | {experiment} |"
            )
        lines.append("")

    lines.extend(
        [
            "## Business case studies",
            "",
            "| Notebook | Priority | Effort | Prerequisites | Business question |",
            "|---|---|---|---|---|",
            "| [E-commerce analytics](../notebooks/14_business_case_studies/01_ecommerce_analytics.ipynb) | Important | 3–4 h | Joins, windows, broadcast joins, partitioned writes | Where is profitable growth, and which customers retain? |",
            "| [Telecom network analytics](../notebooks/14_business_case_studies/02_telecom_network_analytics.ipynb) | Important | 3–4 h | Aggregations, percentiles, windows, event time | Which cells need capacity, and where is the latency tail? |",
            "| [Financial fraud patterns](../notebooks/14_business_case_studies/03_financial_fraud_patterns.ipynb) | Advanced | 3–4 h | Window functions, event time, precision/recall | Which transactions should analysts review first? |",
            "| [Logistics delivery analytics](../notebooks/14_business_case_studies/04_logistics_delivery_analytics.ipynb) | Important | 3–4 h | Date/time handling, joins, windows, SLA design | Which carriers and lanes are failing their promises? |",
            "",
            "## Related navigation",
            "",
            "- [Learning roadmap](LEARNING_ROADMAP.md)",
            "- [Use-case index](USE_CASES.md)",
            "- [Business context](BUSINESS_CONTEXT.md)",
            "- [Case studies](CASE_STUDIES.md)",
            "- [Notebook template](NOTEBOOK_TEMPLATE.md)",
            "",
        ]
    )
    (DOCS / "TOPIC_INDEX.md").write_text("\n".join(lines), encoding="utf-8")


def write_use_cases() -> None:
    grouped = by_module()
    lines = [
        "# Real-World Use Cases",
        "",
        "> **Generated file**: the topic map is built from `scripts/curriculum_metadata.py`.",
        "> Edit the metadata and re-run `scripts/generate_navigation_docs.py`.",
        "",
        "Spark is not the answer to every data problem. This page connects each curriculum topic",
        "to realistic scenarios, the business decision behind it, and the trade-offs that matter.",
        "Use it to choose a practice project, prepare for an interview, or explain why a design",
        "exists. The four runnable case studies are in [CASE_STUDIES.md](CASE_STUDIES.md).",
        "",
        "> **Numbers**: any cost, speed, or revenue figure in the notebooks or docs is illustrative",
        "> unless the notebook prints it from the deterministic lab data. This repository does not",
        "> claim production benchmarks.",
        "",
        "## The four runnable case studies",
        "",
        "| Case study | Industry | Core Spark patterns | Primary decision |",
        "|---|---|---|---|",
        "| [E-commerce analytics](../notebooks/14_business_case_studies/01_ecommerce_analytics.ipynb) | Retail | Joins, broadcast dimensions, windows, cohort/RFM, partitioned Parquet | Where to invest in assortment and retention |",
        "| [Telecom network analytics](../notebooks/14_business_case_studies/02_telecom_network_analytics.ipynb) | Telecommunications | Event-grain aggregation, percentile metrics, busy-hour ranking | Which cells need capacity first |",
        "| [Financial fraud patterns](../notebooks/14_business_case_studies/03_financial_fraud_patterns.ipynb) | Payments / banking | Event-time velocity, explainable rules, precision/recall | Which alerts analysts review |",
        "| [Logistics delivery analytics](../notebooks/14_business_case_studies/04_logistics_delivery_analytics.ipynb) | Logistics | SLA semantics, carrier/lane scorecards, late-arrival handling | Which carriers and lanes to renegotiate |",
        "",
        "## Cross-industry patterns",
        "",
        "| Pattern | Typical industries | Spark mechanism | Watch out for |",
        "|---|---|---|---|",
        "| **Event-time windows** | Telecom, payments, IoT, media | `Window` with `rangeBetween`, watermarks | Late data, time zones, state size |",
        "| **Entity 360** | Retail, banking, telecom | Joins, deduplication, window ranking | Grain fan-out, PII, identity resolution |",
        "| **Incremental marts** | Any analytics team | Partitioned writes, MERGE/overwrite patterns | Idempotency, backfills, small files |",
        "| **Anomaly review** | Payments, security, operations | Velocity features, rules, percentiles | Label delay, false positives, explainability |",
        "| **SLA scorecards** | Logistics, telecom, SaaS | Aggregations, percentile metrics, volume filters | Denominator definitions, missing scans |",
        "| **Data quality gates** | All industries | Null/duplicate checks, quarantine, reconciliation | Silent drops, schema drift, ownership |",
        "",
        "## Topic-to-use-case map",
        "",
        "Each row lists the topic-specific scenarios written into the notebook. The scenarios are",
        "teaching examples, not claims about a specific company.",
        "",
    ]

    for module, items in grouped.items():
        lines.extend(
            [
                f"### {MODULE_TITLES.get(module, module)}",
                "",
                "| Notebook | Priority | Real-world use cases |",
                "|---|---|---|",
            ]
        )
        for rel_path, topic in items:
            title = notebook_title(rel_path)
            use_cases = topic.get("use_cases", [])
            rendered = []
            for use_case in use_cases:
                rendered.append(
                    f"**{use_case.get('industry', 'Industry')} — {use_case.get('title', 'Use case')}**: "
                    f"{use_case.get('scenario', '')}"
                )
            cell = "<br><br>".join(rendered) if rendered else "See notebook"
            link = f"[{title}](../notebooks/{rel_path})"
            lines.append(f"| {link} | {topic.get('priority', '')} | {cell} |")
        lines.append("")

    lines.extend(
        [
            "## How to turn a use case into a project",
            "",
            "1. **Write the decision first.** Who will act differently because of the output?",
            "2. **Define the grain and the metric.** A dashboard without a grain is a future dispute.",
            "3. **Find the smallest dataset that can prove the mechanics.** Use the lab data first.",
            "4. **Add one quality gate and one failure test.** A successful run is not correctness.",
            "5. **Measure the plan.** Inspect joins, shuffles, task skew, and output file sizes.",
            "6. **Write the trade-off.** Explain why Spark, why this join, and why this partition layout.",
            "7. **State what you did not validate.** Synthetic data cannot prove production behavior.",
            "",
            "## When another tool is a better fit",
            "",
            "| Situation | Better starting point | Why |",
            "|---|---|---|",
            "| Data fits comfortably on one machine | DuckDB, pandas, Polars | Lower operational overhead and faster iteration |",
            "| Governed, concurrent BI is the main requirement | Warehouse + semantic layer | Concurrency, access control, and metric governance |",
            "| Sub-second event processing | Flink, Kafka Streams, a streaming database | Lower latency and mature state handling |",
            "| Simple scheduled SQL transforms | dbt + warehouse | Easier testing, lineage, and collaboration |",
            "| Ad-hoc exploration of a small extract | Notebook + pandas | No cluster startup or shuffle overhead |",
            "",
            "Spark earns its place when data volume, history, multi-source joins, or shared batch/streaming",
            "pipelines exceed what a single machine or warehouse query comfortably handles. Measure before",
            "assuming that threshold has been crossed.",
            "",
        ]
    )
    (DOCS / "USE_CASES.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    write_topic_index()
    write_use_cases()
    print("Wrote docs/TOPIC_INDEX.md and docs/USE_CASES.md")


if __name__ == "__main__":
    main()
