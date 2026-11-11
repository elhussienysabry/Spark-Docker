#!/usr/bin/env python3
"""Insert topic-specific educational sections into existing notebooks.

The script is additive and idempotent: a notebook that already contains the
marker is skipped. It never deletes or rewrites existing learner content.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from curriculum_metadata import TOPICS

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS = ROOT / "notebooks"
MARKER = "<!-- curriculum-enrichment:v1 -->"


def source(cell) -> str:
    return "".join(cell.get("source", []))


def md_cell(text: str, cell_id: str) -> dict:
    lines = text.strip().splitlines(True)
    return {
        "cell_type": "markdown",
        "id": cell_id,
        "metadata": {},
        "source": lines,
    }


def find_cell(cells, pattern, start=0):
    regex = re.compile(pattern, re.MULTILINE)
    for idx in range(start, len(cells)):
        if cells[idx].get("cell_type") == "markdown" and regex.search(source(cells[idx])):
            return idx
    return None


def learning_contract(meta) -> str:
    roles = meta["roles"]
    return f"""{MARKER}

## Learning Contract

| Attribute | Value |
|---|---|
| **Priority** | {meta['priority']} |
| **Estimated effort** | {meta['effort']} |
| **Prerequisites** | {meta['prereqs']} |
| **Required depth** | {meta['depth']} |
| **Data Engineer** | {roles['de']} |
| **Senior Data Engineer** | {roles['senior']} |
| **Spark specialist** | {roles['specialist']} |
| **Next topic** | {meta['next_step']} |

> **How to use this notebook**: read the problem first, then run the code, then connect the
> mechanism to the real-world cases and production notes. The goal is not to memorize API
> syntax; it is to be able to explain and defend a design decision.
"""


def problem_and_concepts(meta) -> str:
    concepts = "\n".join(
        f"| **{term}** | {plain} | {technical} |"
        for term, plain, technical in meta["concepts"]
    )
    internals = "\n".join(f"{i}. {step}" for i, step in enumerate(meta["internals"], 1))
    return f"""---

## Problem and Motivation

### Why this concept exists

{meta['problem']}

### What happens if you ignore it

A team can still write code that runs on small data. The failure appears later: a job that
looks correct becomes slow, unstable, or expensive at scale, and the team lacks the vocabulary
to diagnose which Spark mechanism is responsible. This notebook builds that vocabulary and
ties it to observable behavior in the lab.

### A small mental model

Think of the topic as a contract between the logical work you describe and the physical work
Spark performs. The contract has a cost, a failure mode, and a set of trade-offs. The sections
below make each of those explicit.

---

## Core Concepts

### Definitions

| Term | Plain-language meaning | Technical meaning |
|---|---|---|
{concepts}

### How it works, step by step

{internals}

### What to keep in mind

- The mechanism is not free; it consumes CPU, memory, network, or disk.
- The right choice depends on data size, distribution, latency target, and failure tolerance.
- A plan or UI observation is stronger evidence than an intuition about what "should" happen.
"""


def real_world(meta) -> str:
    parts = ["---", "", "## Real-World Use Cases", ""]
    parts.append(
        "> The scenarios below are illustrative. Company names are omitted, and any numbers are "
        "hypothetical unless they are measured in this lab."
    )
    parts.append("")
    for i, use_case in enumerate(meta["use_cases"], 1):
        parts.extend(
            [
                f"### Use Case {i}: {use_case['title']}",
                "",
                f"- **Industry**: {use_case['industry']}",
                f"- **Scenario**: {use_case['scenario']}",
                f"- **Problem**: {use_case['problem']}",
                f"- **Why this concept is relevant**: {use_case['why']}",
                f"- **Implementation idea**: {use_case['implementation']}",
                f"- **Expected result**: {use_case['result']}",
                f"- **Alternative**: {use_case['alternative']}",
                f"- **Trade-offs**: {use_case['tradeoffs']}",
                "",
            ]
        )
    return "\n".join(parts)


def business_value(meta) -> str:
    bullets = "\n".join(f"- {item}" for item in meta["business"])
    return f"""---

## Business Value

{bullets}

> **Note on numbers**: this repository does not claim production benchmarks. If you want to
> quantify a business case, measure the job in your own environment (runtime, shuffle bytes,
> spill, infrastructure cost) before and after the change.
"""


def production_and_tradeoffs(meta) -> str:
    production = "\n".join(f"- {item}" for item in meta["production"])
    advantages = "\n".join(f"- {item}" for item in meta["tradeoffs"]["advantages"])
    limitations = "\n".join(f"- {item}" for item in meta["tradeoffs"]["limitations"])
    use = "\n".join(f"- {item}" for item in meta["tradeoffs"]["use"])
    avoid = "\n".join(f"- {item}" for item in meta["tradeoffs"]["avoid"])
    inside = "\n".join(f"- {item}" for item in meta["tradeoffs"]["inside"])
    outside = "\n".join(f"- {item}" for item in meta["tradeoffs"]["outside"])
    exp = meta["experiment"]
    steps = "\n".join(f"{i}. {step}" for i, step in enumerate(exp["steps"], 1))
    return f"""---

## Production Engineering

### What changes when this runs in production

{production}

### Investigation checklist

- [ ] Open the Spark UI and identify the relevant tab for this topic.
- [ ] Compare the metric before and after a configuration or code change.
- [ ] Check driver and executor logs for warnings or retries.
- [ ] Confirm that the fix is measured, not assumed.

### Common production mistakes

1. Copying a local-lab setting into a production cluster without sizing it.
2. Optimizing a single job without checking the downstream SLA and data-quality impact.
3. Treating a successful run as proof of correctness; verify row counts, keys, and invariants.
4. Forgetting that retries and backfills must be idempotent.

### Experiment: {exp['title']}

**Steps**

{steps}

**What to observe**: {exp['observe']}

**Healthy result**: {exp['healthy']}

**Problematic result**: {exp['problem']}

**Engineering takeaway**: {exp['takeaway']}

---

## Trade-offs and Alternatives

### Advantages

{advantages}

### Limitations

{limitations}

### When to use it

{use}

### When not to use it

{avoid}

### Alternatives inside Spark

{inside}

### Alternatives outside Spark

{outside}

### Performance implications

Every choice has a cost. Measure runtime, shuffle read/write, spill, GC time, and task
duration percentiles before and after the change. A local four-core cluster is useful for
observing mechanics, but it is not a production benchmark.
"""


def debug_exercise(meta) -> str:
    debug = meta["debug"]
    hints = "\n".join(f"{i}. {hint}" for i, hint in enumerate(debug["hints"], 1))
    return f"""---

## Production Debugging Exercise

### {debug['title']}

**Scenario**: {debug['scenario']}

**Your task**: identify the most likely root cause, propose one diagnostic step, and propose
one fix. Write your answer before opening the model approach.

**Hints**

{hints}

<details>
<summary>Model approach (open after trying)</summary>

1. Reproduce the issue with the smallest possible dataset.
2. Use the Spark UI and the physical plan to locate the stage or operator that dominates.
3. Change one variable at a time and re-measure.
4. Record the before/after metric so the fix is defensible.

The exact fix depends on the evidence. The important habit is to separate allocation,
scheduling, shuffle, memory, and data-quality causes instead of guessing.

</details>
"""


def append_interview_questions(cells, meta, summary_idx):
    """Add two interview questions before the 'What to Study Next' subsection."""
    if summary_idx is None:
        return
    cell = cells[summary_idx]
    text = source(cell)
    questions = "\n".join(
        f"- **Q: {q}**\n  - *A*: {a}" for q, a in meta["interview"]
    )
    block = f"\n### More Interview Questions\n\n{questions}\n"
    if "### More Interview Questions" in text:
        return
    for marker in ("### What to Study Next", "### What to study next", "## What to Study Next"):
        if marker in text:
            text = text.replace(marker, block + "\n" + marker, 1)
            cell["source"] = text.splitlines(True)
            return
    cell["source"] = (text.rstrip() + "\n" + block).splitlines(True)


ENRICHMENT_IDS = {
    "learning-contract",
    "problem-concepts",
    "real-world-use-cases",
    "business-value",
    "production-tradeoffs",
    "debug-exercise",
}


def repair_cells(cells):
    """Remove previously inserted enrichment cells and interview blocks."""
    kept = []
    for cell in cells:
        if cell.get("id") in ENRICHMENT_IDS:
            continue
        text = source(cell)
        if "### More Interview Questions" in text:
            # Remove the block from this heading up to the next heading.
            text = re.sub(
                r"\n### More Interview Questions\n.*?(?=\n#{2,3}\s)",
                "\n",
                text,
                flags=re.DOTALL,
            )
            cell["source"] = text.splitlines(True)
        kept.append(cell)
    return kept


def enrich(path: Path, meta, repair: bool = False) -> bool:
    nb = json.loads(path.read_text(encoding="utf-8"))
    cells = nb.get("cells", [])
    if repair:
        cells = repair_cells(cells)
        nb["cells"] = cells
    if any(MARKER in source(c) for c in cells):
        return False

    if not cells or cells[0].get("cell_type") != "markdown":
        print(f"SKIP (no title markdown): {path}")
        return False

    # Insert the learning contract after the title/objectives cell.
    cells.insert(1, md_cell(learning_contract(meta), "learning-contract"))

    # Insert problem/concepts before the first real content section. Project
    # notebooks often start with a level-3 "### 1. Initialize ..." heading, so
    # match level 2 or 3 headings, skip the learning objectives/contract, and
    # stop at the first code cell if a heading has not appeared yet.
    first_content = None
    for idx in range(2, len(cells)):
        if cells[idx].get("cell_type") == "code":
            first_content = idx
            break
        if cells[idx].get("cell_type") != "markdown":
            continue
        text = source(cells[idx])
        if re.search(r"(?m)^#{2,3}\s+Learning (Objectives|Contract)", text):
            continue
        if re.search(r"(?m)^#{2,3}\s+", text):
            first_content = idx
            break
    if first_content is None:
        first_content = len(cells)
    cells.insert(first_content, md_cell(problem_and_concepts(meta), "problem-concepts"))

    # Locate insertion points after the new cells have shifted indices.
    exercises = find_cell(cells, r"(?m)^#{2,3}\s+.*Exercis")
    solutions = find_cell(cells, r"(?m)^#{2,3}\s+.*Solution")
    summary = find_cell(cells, r"(?m)^#{2,3}\s+.*(Module Summary|Topic Summary|Summary|Interview Guide)")

    insertion = exercises if exercises is not None else (solutions if solutions is not None else summary)
    if insertion is None:
        insertion = len(cells)
    # Insert in reverse order so the earlier insert keeps its position.
    cells.insert(insertion, md_cell(production_and_tradeoffs(meta), "production-tradeoffs"))
    cells.insert(insertion, md_cell(business_value(meta), "business-value"))
    cells.insert(insertion, md_cell(real_world(meta), "real-world-use-cases"))

    # Recompute positions after the three insertions above. Using the old
    # indices here would place the debugging exercise in the middle of the
    # existing exercise/solution cells.
    solutions = find_cell(cells, r"(?m)^#{2,3}\s+.*Solution")
    exercises = find_cell(cells, r"(?m)^#{2,3}\s+.*Exercis")
    debug_at = solutions if solutions is not None else exercises
    if debug_at is not None:
        cells.insert(debug_at, md_cell(debug_exercise(meta), "debug-exercise"))

    # Add interview questions to the existing summary.
    summary = find_cell(cells, r"(?m)^#{2,3}\s+.*(Module Summary|Topic Summary|Summary|Interview Guide)")
    append_interview_questions(cells, meta, summary)

    nb["cells"] = cells
    path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return True


def main() -> int:
    repair = "--repair" in sys.argv[1:]
    changed = 0
    missing = []
    for rel, meta in TOPICS.items():
        path = NOTEBOOKS / rel
        if not path.exists():
            missing.append(rel)
            continue
        if enrich(path, meta, repair=repair):
            changed += 1
            print(f"ENRICHED {rel}")
        else:
            print(f"SKIPPED  {rel}")
    print(f"\nChanged: {changed} / {len(TOPICS)}")
    if missing:
        print("Missing notebooks:")
        for rel in missing:
            print(f"  - {rel}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
