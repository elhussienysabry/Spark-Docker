#!/usr/bin/env python3
"""Aggregate the sample sales CSV and optionally write the result as Parquet.

Usage (inside the cluster):
    spark-submit /opt/spark-apps/sales_report.py \
        [input.csv] [output_parquet_dir]
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark-data/sales.csv"
    output_path = (
        sys.argv[2] if len(sys.argv) > 2 else "/opt/spark-data/output/sales_report"
    )

    spark = SparkSession.builder.appName("Sales Report").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    sales = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
        .withColumn("revenue", F.round(F.col("units") * F.col("unit_price"), 2))
    )

    print(f"Rows read from {input_path}: {sales.count()}")
    sales.printSchema()

    by_region = (
        sales.groupBy("region")
        .agg(
            F.sum("units").alias("units_sold"),
            F.round(F.sum("revenue"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
        )
        .orderBy(F.desc("revenue"))
    )

    print("Revenue by region")
    by_region.show(truncate=False)

    top_products = (
        sales.groupBy("product")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
    )

    print("Revenue by product")
    top_products.show(truncate=False)

    (by_region.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/by_region"))
    (top_products.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/by_product"))
    print(f"Parquet output written to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
