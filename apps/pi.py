#!/usr/bin/env python3
"""Estimate Pi with a Monte Carlo workload spread over the cluster."""

import sys

from pyspark.sql import SparkSession


def inside_unit_circle(_):
    import random

    x, y = random.random(), random.random()
    return 1 if x * x + y * y <= 1 else 0


def main() -> None:
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    samples = 100_000 * partitions

    spark = SparkSession.builder.appName("Pi Estimation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    count = (
        spark.sparkContext.parallelize(range(1, samples + 1), partitions)
        .map(inside_unit_circle)
        .reduce(lambda a, b: a + b)
    )

    print(f"Pi is roughly {4.0 * count / samples:.6f} ({samples:,} samples)")
    spark.stop()


if __name__ == "__main__":
    main()
