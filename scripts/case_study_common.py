#!/usr/bin/env python3
"""Shared helpers for the business case-study notebook generator."""

from __future__ import annotations

import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "notebooks" / "14_business_case_studies"

NOTEBOOK_METADATA = {
    "kernelspec": {
        "display_name": "Python 3 (ipykernel)",
        "language": "python",
        "name": "python3",
    },
    "language_info": {
        "codemirror_mode": {"name": "ipython", "version": 3},
        "file_extension": ".py",
        "mimetype": "text/x-python",
        "name": "python",
        "nbconvert_exporter": "python",
        "pygments_lexer": "ipython3",
        "version": "3.13.18",
    },
}


def _lines(text: str) -> list[str]:
    text = textwrap.dedent(text).strip("\n")
    return [line + "\n" for line in text.split("\n")]


def md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": _lines(text)}


def code(text: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _lines(text),
    }


def spark_setup(app_name: str) -> str:
    """Return the common SparkSession cell used by every case study."""
    return f'''
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, StringType,
    DoubleType, DateType, BooleanType, TimestampType,
)
from pyspark import StorageLevel
import os

master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
builder = (
    SparkSession.builder
    .appName("{app_name}")
    .master(master)
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.warehouse.dir", "file:///opt/spark-data/warehouse")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
)
if master.startswith("local"):
    builder = builder.config("spark.ui.enabled", "false")
else:
    builder = (
        builder
        .config("spark.driver.host", "jupyter")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark-events")
    )
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"Spark {{spark.version}} on {{spark.sparkContext.master}}")
print(f"App: {{spark.sparkContext.appName}} | shuffle partitions: {{spark.conf.get('spark.sql.shuffle.partitions')}}")
'''
