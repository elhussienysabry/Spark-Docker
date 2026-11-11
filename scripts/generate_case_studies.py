#!/usr/bin/env python3
"""Generate the four runnable business case-study notebooks.

Usage:
    python3 scripts/generate_case_studies.py
    python3 scripts/generate_case_studies.py --check

The generator overwrites only the notebooks in
``notebooks/14_business_case_studies``. The synthetic datasets are produced by
``data/generate_case_studies.py``; the e-commerce case study uses the existing
retail datasets.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "notebooks" / "14_business_case_studies"

sys.path.insert(0, str(ROOT / "scripts"))

from case_study_common import NOTEBOOK_METADATA, md, code  # noqa: E402
import case_study_ecommerce as ecommerce  # noqa: E402
import case_study_fraud as fraud  # noqa: E402
import case_study_logistics as logistics  # noqa: E402
import case_study_telecom as telecom  # noqa: E402

CASE_STUDIES = {
    "01_ecommerce_analytics.ipynb": ecommerce.cells,
    "02_telecom_network_analytics.ipynb": telecom.cells,
    "03_financial_fraud_patterns.ipynb": fraud.cells,
    "04_logistics_delivery_analytics.ipynb": logistics.cells,
}


def write_notebook(path: Path, cells: list[dict]) -> None:
    for index, cell in enumerate(cells):
        cell.setdefault("id", f"cs-{index:02d}")
    notebook = {
        "cells": cells,
        "metadata": NOTEBOOK_METADATA,
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check that generated files are current")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    changed = []
    for name, cells in CASE_STUDIES.items():
        path = OUT_DIR / name
        before = path.read_text(encoding="utf-8") if path.exists() else None
        write_notebook(path, cells())
        after = path.read_text(encoding="utf-8")
        if before != after:
            changed.append(name)
        print(f"{'updated' if before != after else 'unchanged'}: {path.relative_to(ROOT)}")

    if args.check and changed:
        print("Generated notebooks are stale: " + ", ".join(changed))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
