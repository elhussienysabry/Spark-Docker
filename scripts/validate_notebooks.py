#!/usr/bin/env python3
"""Validate every Jupyter notebook in the repository.

Checks:
- JSON structure and required notebook/cell fields.
- Python syntax in code cells via ``ast.parse`` (IPython magics are skipped).
- Stored error outputs (reported as warnings, because some labs demonstrate failures).
- Notebook inventory by category, without hard-coded totals.

The script is repository-relative and safe to run from CI. It does not execute
notebooks; execution is a separate, explicit step.
"""
from __future__ import annotations

import ast
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = ROOT / "notebooks"
SKIP_PARTS = {".ipynb_checkpoints", "__pycache__"}


def classify(rel_path: Path) -> str:
    """Return a human-readable category for inventory reporting."""
    parts = rel_path.parts
    if "templates" in parts:
        return "template"
    if "14_business_case_studies" in parts:
        return "case study"
    if len(parts) == 1:
        return "supplementary"
    return "core"


def cell_source(cell: dict) -> str:
    source = cell.get("source", "")
    if isinstance(source, list):
        return "".join(source)
    return source or ""


def clean_code(source: str) -> str:
    """Remove IPython line magics and shell escapes before AST parsing.

    A cell that starts with ``%%`` is a cell magic and cannot be parsed as
    Python, so it is skipped entirely. Line magics are replaced with blank
    lines to keep line numbers meaningful in error messages.
    """
    lines = source.splitlines()
    if lines and lines[0].lstrip().startswith("%%"):
        return ""
    cleaned = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith(("%", "!", "?")):
            cleaned.append("")
        else:
            cleaned.append(line)
    return "\n".join(cleaned)


def find_notebooks() -> list[Path]:
    notebooks = []
    for path in sorted(NOTEBOOK_DIR.rglob("*.ipynb")):
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        notebooks.append(path)
    return notebooks


def validate() -> int:
    notebooks = find_notebooks()
    counts: Counter[str] = Counter()
    json_errors: list[tuple[str, str]] = []
    structure_errors: list[tuple[str, str]] = []
    syntax_errors: list[tuple[str, int, str]] = []
    error_outputs: list[tuple[str, int, str]] = []
    missing_ids: list[str] = []

    total_code = 0
    total_markdown = 0
    total_other = 0

    for path in notebooks:
        rel = path.relative_to(ROOT)
        rel_display = str(rel)
        counts[classify(path.relative_to(NOTEBOOK_DIR))] += 1

        try:
            with path.open("r", encoding="utf-8") as handle:
                notebook = json.load(handle)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            json_errors.append((rel_display, str(exc)))
            continue

        for key in ("cells", "metadata", "nbformat", "nbformat_minor"):
            if key not in notebook:
                structure_errors.append((rel_display, f"missing notebook key: {key}"))

        cells = notebook.get("cells")
        if not isinstance(cells, list):
            structure_errors.append((rel_display, "cells is not a list"))
            continue

        cell_ids = [cell.get("id") for cell in cells if isinstance(cell, dict)]
        if any(cell_id is None for cell_id in cell_ids):
            missing_ids.append(rel_display)
        present_ids = [cell_id for cell_id in cell_ids if cell_id is not None]
        if len(present_ids) != len(set(present_ids)):
            structure_errors.append((rel_display, "duplicate cell ids"))

        for index, cell in enumerate(cells):
            if not isinstance(cell, dict):
                structure_errors.append((rel_display, f"cell {index} is not an object"))
                continue

            cell_type = cell.get("cell_type")
            if "source" not in cell:
                structure_errors.append((rel_display, f"cell {index} missing source"))
            source = cell_source(cell)

            if cell_type == "markdown":
                total_markdown += 1
            elif cell_type == "code":
                total_code += 1
                code = clean_code(source)
                if code.strip():
                    try:
                        ast.parse(code, filename=f"{rel_display}:cell-{index}")
                    except SyntaxError as exc:
                        syntax_errors.append(
                            (rel_display, index, f"{exc.msg} (line {exc.lineno})")
                        )
                for output in cell.get("outputs", []) or []:
                    if isinstance(output, dict) and output.get("output_type") == "error":
                        error_outputs.append(
                            (rel_display, index, str(output.get("ename", "Error")))
                        )
            else:
                total_other += 1

    print("=" * 72)
    print("APACHE SPARK CURRICULUM VALIDATION REPORT")
    print("=" * 72)
    print(f"Repository root:                 {ROOT}")
    print(f"Notebook files checked:          {len(notebooks)}")
    print(
        "  core / case study / supplementary / template: "
        f"{counts['core']} / {counts['case study']} / "
        f"{counts['supplementary']} / {counts['template']}"
    )
    print(f"Code cells inspected:            {total_code}")
    print(f"Markdown cells inspected:        {total_markdown}")
    print(f"Other cells inspected:           {total_other}")
    print(f"JSON errors:                     {len(json_errors)}")
    print(f"Structure errors:                {len(structure_errors)}")
    print(f"Python syntax errors:            {len(syntax_errors)}")
    print(f"Stored error outputs (warning):  {len(error_outputs)}")
    print(f"Notebooks with missing cell ids: {len(missing_ids)}")

    if json_errors:
        print("\nJSON errors:")
        for rel, error in json_errors:
            print(f"  {rel}: {error}")

    if structure_errors:
        print("\nStructure errors:")
        for rel, error in structure_errors:
            print(f"  {rel}: {error}")

    if syntax_errors:
        print("\nPython syntax errors:")
        for rel, index, error in syntax_errors:
            print(f"  {rel} (cell {index}): {error}")

    if error_outputs:
        print("\nStored error outputs (not a failure; verify they are intentional):")
        for rel, index, name in error_outputs[:20]:
            print(f"  {rel} (cell {index}): {name}")
        if len(error_outputs) > 20:
            print(f"  ... and {len(error_outputs) - 20} more")

    if missing_ids:
        print(
            "\nNotebooks without explicit cell ids (optional; Jupyter can regenerate): "
            f"{len(missing_ids)}"
        )
        for rel in missing_ids[:5]:
            print(f"  {rel}")
        if len(missing_ids) > 5:
            print(f"  ... and {len(missing_ids) - 5} more")

    failed = bool(json_errors or structure_errors or syntax_errors)
    print("=" * 72)
    if failed:
        print("VALIDATION FAILED: fix the errors above.")
        return 1
    print("VALIDATION PASSED: all notebooks are valid JSON and parse as Python.")
    print("Remember: syntax validation is not execution. Run selected notebooks explicitly.")
    return 0


if __name__ == "__main__":
    sys.exit(validate())
