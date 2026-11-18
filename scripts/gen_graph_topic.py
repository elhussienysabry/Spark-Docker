#!/usr/bin/env python3
"""Generate the Graph Processing notebook for Module 12.

The notebook teaches graph modelling and three iterative algorithms using
only the DataFrame API that ships with PySpark:

* degree statistics
* connected components by label propagation
* breadth-first search for unweighted shortest paths
* PageRank with dangling-node redistribution

It deliberately avoids GraphFrames and GraphX so the lab has no extra
runtime dependency. Both alternatives are discussed in the notebook.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "notebooks" / "12_advanced_topics" / "07_graph_processing.ipynb"


def build() -> NotebookBuilder:
    nb = NotebookBuilder(
        "Graph Processing: Relationships, Traversal, and Iterative Algorithms",
        "12_advanced_topics",
        "07_graph_processing.ipynb",
        "Graph_Processing_Lab",
    )

    nb.add_header(
        {
            "what": "How to represent a graph as vertex and edge DataFrames and run degree, connected-component, breadth-first search, and PageRank algorithms with iterative joins.",
            "why": "Relationships are often the most valuable part of a dataset, and Spark can express many graph algorithms without a separate graph database.",
            "problem": "Choosing a graph representation and bounding the shuffle, skew, and lineage cost of iterative processing.",
            "real_world": "Fraud-ring detection, social recommendations, identity resolution, dependency analysis, and logistics networks.",
        }
    )

    nb.add_session_init()

    nb.add_markdown(
        """## 2. Graph Representation: Vertices, Edges, and Degrees

A graph is two tables:

- A **vertex table** with one row per entity and a unique `id`.
- An **edge table** with `src` and `dst` keys that reference the vertex ids.

The small directed graph below has two components. Vertices 1-6 form one component, and
vertices 7-9 form another. The 4-5-6 cycle is intentional: it makes PageRank interesting
and gives the traversal algorithms a loop to handle.
"""
    )

    nb.add_code(
        """vertices = spark.createDataFrame(
    [
        (1, "Alice", "Analytics"),
        (2, "Bob", "Engineering"),
        (3, "Carol", "Engineering"),
        (4, "Dave", "Sales"),
        (5, "Eve", "Sales"),
        (6, "Frank", "Support"),
        (7, "Grace", "Analytics"),
        (8, "Heidi", "Support"),
        (9, "Ivan", "Engineering"),
    ],
    ["id", "name", "team"],
)

edges = spark.createDataFrame(
    [
        (1, 2, "follows"),
        (1, 3, "follows"),
        (2, 3, "follows"),
        (3, 1, "follows"),
        (3, 4, "follows"),
        (4, 5, "follows"),
        (5, 6, "follows"),
        (6, 4, "follows"),
        (7, 8, "follows"),
        (8, 9, "follows"),
    ],
    ["src", "dst", "relationship"],
)

print(f"Vertices: {vertices.count()} | Edges: {edges.count()}")
vertices.show()
edges.show()
"""
    )

    nb.add_markdown(
        """## 3. Degrees: The Simplest Graph Metric

In-degree counts incoming edges; out-degree counts outgoing edges. Degree is cheap to
compute and often reveals hubs, sinks, and likely supernodes before a more expensive
algorithm runs.
"""
    )

    nb.add_code(
        """out_degrees = edges.groupBy(F.col("src").alias("id")).agg(F.count("*").alias("out_degree"))
in_degrees = edges.groupBy(F.col("dst").alias("id")).agg(F.count("*").alias("in_degree"))

degrees = (
    vertices.join(out_degrees, "id", "left")
    .join(in_degrees, "id", "left")
    .fillna(0)
    .select("id", "name", "team", "in_degree", "out_degree")
)

degrees.orderBy(F.desc("in_degree"), "id").show()
"""
    )

    nb.add_markdown(
        """## 4. Connected Components by Label Propagation

Connected components group vertices that are reachable from one another. The algorithm
starts with every vertex in its own component and repeatedly propagates the smallest
label across **undirected** edges until no label changes.

Each round is a join, a shuffle by vertex id, and an aggregation. `localCheckpoint` cuts
the growing logical plan between rounds. Without it, a long loop can produce a very deep
plan and exhaust the driver or the JVM. `localCheckpoint` is fast but not durable: if an
executor is lost, its checkpoint blocks can be lost too. Use `checkpoint()` to reliable
storage when the iteration must survive executor failure.
"""
    )

    nb.add_code(
        """undirected = (
    edges.select("src", "dst")
    .union(edges.select(F.col("dst").alias("src"), F.col("src").alias("dst")))
    .distinct()
)

labels = vertices.select("id", F.col("id").alias("component"))

for iteration in range(1, 11):
    proposals = (
        labels.alias("l")
        .join(undirected.alias("e"), F.col("l.id") == F.col("e.src"))
        .select(F.col("e.dst").alias("id"), F.col("l.component").alias("proposed"))
    )
    updated = labels.union(proposals).groupBy("id").agg(F.min("component").alias("component"))
    changed = (
        labels.alias("old")
        .join(updated.alias("new"), F.col("old.id") == F.col("new.id"))
        .filter(F.col("old.component") != F.col("new.component"))
        .count()
    )
    labels = updated.localCheckpoint(eager=True)
    print(f"Iteration {iteration}: {changed} label changes")
    if changed == 0:
        break

labels.orderBy("component", "id").show()
"""
    )

    nb.add_markdown(
        """## 5. Breadth-First Search for Unweighted Shortest Paths

BFS expands a frontier one hop at a time. The first time a vertex is reached is its
shortest distance in an unweighted graph. The `left_anti` join prevents already-visited
vertices from re-entering the frontier, which also prevents infinite loops on cycles.
"""
    )

    nb.add_code(
        """source_id = 1
frontier = spark.createDataFrame([(source_id, 0)], ["id", "distance"])
visited = frontier

for iteration in range(1, 10):
    next_frontier = (
        frontier.alias("f")
        .join(edges.alias("e"), F.col("f.id") == F.col("e.src"))
        .select(F.col("e.dst").alias("id"), (F.col("f.distance") + 1).alias("distance"))
        .distinct()
        .join(visited, "id", "left_anti")
    )
    new_vertices = next_frontier.count()
    print(f"Iteration {iteration}: {new_vertices} newly reached vertices")
    if new_vertices == 0:
        break
    visited = visited.union(next_frontier)
    frontier = next_frontier

visited.orderBy("distance", "id").show()
"""
    )

    nb.add_markdown(
        """## 6. PageRank with Dangling Nodes

PageRank models a random walk: with probability `d` the walk follows an outgoing edge,
and with probability `1 - d` it jumps to a random vertex. A vertex with no outgoing edges
is a **dangling node**. Its rank has nowhere to flow, so it is redistributed uniformly
across all vertices. Omitting that step makes total rank drift below 1.0 and biases every
score.

The implementation below uses `localCheckpoint(eager=True)` on every iteration. This is
not cosmetic: without checkpointing, 20 iterations build a deep plan and can exhaust the
driver heap even on a nine-vertex graph. Remember that a local checkpoint is not durable;
use a reliable `checkpoint()` when the job must recover after losing an executor.
"""
    )

    nb.add_code(
        """def pagerank(edge_df, vertex_df, iterations=20, damping=0.85):
    n = vertex_df.count()
    ranks = vertex_df.select("id", F.lit(1.0 / n).alias("rank"))
    out_degrees = edge_df.groupBy(F.col("src").alias("id")).agg(F.count("*").alias("out_degree"))

    for _ in range(iterations):
        contributions = (
            edge_df.alias("e")
            .join(ranks.alias("r"), F.col("e.src") == F.col("r.id"))
            .join(out_degrees.alias("o"), F.col("e.src") == F.col("o.id"))
            .select(
                F.col("e.dst").alias("id"),
                (F.col("r.rank") / F.col("o.out_degree")).alias("contribution"),
            )
        )
        incoming = contributions.groupBy("id").agg(F.sum("contribution").alias("incoming"))
        dangling = (
            ranks.alias("r")
            .join(out_degrees.alias("o"), F.col("r.id") == F.col("o.id"), "left")
            .filter(F.col("o.out_degree").isNull())
            .agg(F.coalesce(F.sum("r.rank"), F.lit(0.0)))
            .first()[0]
        )
        ranks = (
            ranks.select("id")
            .join(incoming, "id", "left")
            .fillna(0.0, subset=["incoming"])
            .select(
                "id",
                ((1 - damping) / n + damping * (F.col("incoming") + dangling / n)).alias("rank"),
            )
            .localCheckpoint(eager=True)
        )
    return ranks


ranks = pagerank(edges, vertices, iterations=20)
ranked = (
    ranks.join(vertices, "id")
    .select("id", "name", "team", F.round("rank", 4).alias("rank"))
    .orderBy(F.desc("rank"))
)
ranked.show()
"""
    )

    nb.add_experiment(
        {
            "title": "Convergence of Iterative PageRank",
            "testing": "Compare PageRank after one iteration and after 20 iterations on the same graph.",
            "expect": "One iteration ranks nodes mostly by direct incoming links; later iterations move rank through longer paths and the 4-5-6 cycle.",
            "code": """rank_1 = pagerank(edges, vertices, iterations=1)
rank_20 = pagerank(edges, vertices, iterations=20)

comparison = (
    rank_1.alias("one")
    .join(rank_20.alias("twenty"), "id")
    .select(
        "id",
        F.round(F.col("one.rank"), 4).alias("rank_1"),
        F.round(F.col("twenty.rank"), 4).alias("rank_20"),
        F.round(F.abs(F.col("one.rank") - F.col("twenty.rank")), 4).alias("abs_delta"),
    )
    .orderBy(F.desc("rank_20"))
)
comparison.show()
print("Sum of ranks after 20 iterations:", rank_20.agg(F.sum("rank")).first()[0])
""",
            "happened": "The largest per-node change between one and 20 iterations is about 0.107. Vertices 4, 5, and 6 rise as rank circulates through the 4-5-6 cycle, and the 20-iteration ranks sum to approximately 1.0.",
            "why": "PageRank is a fixed-point iteration. Each round propagates rank one hop, and the damping factor reduces the mass that has not yet reached a stable distribution.",
            "takeaway": "Choose an iteration count or convergence tolerance deliberately, checkpoint each round, and do not present a one-iteration ranking as converged.",
        }
    )

    nb.add_exercises(
        {
            "b_title": "Find the Most Connected Vertices",
            "b_desc": "Use the degree table to list the three vertices with the highest in-degree.",
            "b_input": "The `degrees` DataFrame",
            "b_sol": "degrees.orderBy(F.desc(\"in_degree\")).show(3)",
            "i_title": "Traverse the Second Component",
            "i_desc": "Change the BFS source to vertex 7 and explain why vertices 1-6 are absent from the result.",
            "i_input": "The BFS cell and the `edges` DataFrame",
            "i_sol": "source_id = 7\n# Re-run the BFS loop. Only vertices 7, 8, and 9 are reachable.\n# The graph has two disconnected components, so the frontier never reaches 1-6.",
            "a_title": "Add a Hub and Inspect Skew",
            "a_desc": "Add vertex 10 and connect it to every other vertex. Explain why the first join becomes skewed and propose one mitigation.",
            "a_input": "The `vertices` and `edges` DataFrames",
            "a_sol": "hub_edges = spark.createDataFrame(\n    [(10, i, \"hub\") for i in range(1, 10)],\n    [\"src\", \"dst\", \"relationship\"],\n)\nedges_with_hub = edges.union(hub_edges)\n# All edges with src = 10 hash to one partition, so one task receives every hub edge.\n# Salt the hub key, split it into virtual nodes, or use a graph engine that handles high-degree vertices.",
        }
    )

    nb.add_summary_and_teardown(
        {
            "takeaways": {
                "Graphs as tables": "Vertices and edges are DataFrames; joins express traversal and message passing.",
                "Iteration is explicit": "Connected components, BFS, and PageRank loop until a convergence condition is met.",
                "Checkpoint long lineages": "Each round adds to the logical plan; localCheckpoint or checkpoint truncates it.",
                "Know the ecosystem": "GraphX is a Scala/RDD API; GraphFrames is a separate DataFrame package; verify compatibility before adding either.",
            },
            "terms": {
                "Vertex": "An entity with a unique id.",
                "Edge": "A directed or undirected relationship between two vertices.",
                "Degree": "The number of incoming or outgoing edges.",
                "Connected component": "A maximal set of vertices reachable from one another.",
                "BFS": "Frontier expansion that finds unweighted shortest paths.",
                "PageRank": "An iterative importance score that flows along edges.",
                "Pregel": "A vertex-centric model of supersteps with message passing.",
                "Supernode": "A high-degree vertex that can skew one shuffle partition.",
                "Dangling node": "A vertex with no outgoing edges in a directed graph.",
            },
            "mistakes": {
                "Treating a directed graph as undirected": "Mirror the edges only when the algorithm needs undirected connectivity.",
                "Ignoring dangling nodes": "PageRank mass leaks unless it is redistributed.",
                "Iterating without checkpointing": "The logical plan grows every round and can exhaust the driver.",
                "Ignoring supernodes": "One key can dominate a shuffle partition and leave the rest of the cluster idle.",
            },
            "questions": {
                "How would you represent a graph in Spark?": "As a vertex DataFrame keyed by id and an edge DataFrame with src and dst. Algorithms join the current state to the edges and aggregate messages, iterating until convergence.",
                "What is the Pregel model, and how does it map to Spark?": "Pregel is vertex-centric iteration in supersteps: each vertex receives messages, updates its state, and sends messages to neighbors. In Spark, each superstep is a join and aggregation; GraphX implements it directly, while DataFrame code emulates it.",
                "Why can PageRank fail on a graph with dangling nodes?": "Dangling vertices have no outgoing edges, so their rank is never propagated. Without redistributing that mass, total rank drifts below 1.0 and the scores are biased.",
            },
            "next_steps": "Module 12 is complete. Continue to `13_end_to_end_projects/01_batch_etl_pipeline.ipynb`.",
        }
    )

    return nb


def main() -> None:
    notebook = build()
    notebook.save(str(OUTPUT))


if __name__ == "__main__":
    main()
