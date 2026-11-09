#!/usr/bin/env python3
"""Classic word count over a text file.

Usage (inside the cluster):
    spark-submit /opt/spark-apps/wordcount.py [input.txt] [output_dir]
"""

import shutil
import sys

from pyspark.sql import SparkSession


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark-data/words.txt"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/spark-data/output/wordcount"

    spark = SparkSession.builder.appName("Word Count").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    counts = (
        spark.sparkContext.textFile(input_path)
        .flatMap(lambda line: line.lower().replace(",", " ").replace(".", " ").split())
        .filter(lambda word: word)
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .map(lambda pair: (pair[1], pair[0]))
        .sortByKey(ascending=False)
    )

    print("Top 15 words")
    for count, word in counts.take(15):
        print(f"{word:>15} {count:>4}")

    shutil.rmtree(output_path, ignore_errors=True)
    counts.map(lambda pair: f"{pair[0]}\t{pair[1]}").saveAsTextFile(output_path)
    print(f"Full word count written to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
