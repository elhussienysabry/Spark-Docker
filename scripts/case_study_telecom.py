#!/usr/bin/env python3
"""Cell content for the telecom network analytics case study."""

from __future__ import annotations

from case_study_common import code, md, spark_setup


def cells() -> list[dict]:
    return [
        md(
            """
            # Case Study 2: Telecom Network Quality and Capacity Analytics

            **Module**: 14_business_case_studies  
            **Notebook**: 02_telecom_network_analytics.ipynb  
            **Environment**: Apache Spark 4.2.1 · PySpark · Python 3.13 · Java 21  
            **Data**: `/opt/spark-data/case_studies/telecom/` (synthetic, deterministic)  
            **Estimated time**: 3–4 hours

            [Case-study index](../../docs/CASE_STUDIES.md) · [Use-case index](../../docs/USE_CASES.md) · [Business context](../../docs/BUSINESS_CONTEXT.md) · [Previous](01_ecommerce_analytics.ipynb) · [Next](03_financial_fraud_patterns.ipynb)

            > **Scenario note**: NileConnect is a hypothetical mobile operator. The cell sites,
            > customers, and events are synthetic. Network thresholds are teaching examples, not
            > vendor recommendations.
            """
        ),
        md(
            """
            ## Learning Contract

            | Attribute | Value |
            |---|---|
            | **Priority** | Important |
            | **Estimated effort** | 3–4 hours |
            | **Prerequisites** | DataFrame joins, aggregations, percentile approximations, window functions, partitioned writes |
            | **Required depth** | Can build cell-level KPIs and explain the operational meaning of each metric |
            | **Data Engineer** | Build reliable event and dimension pipelines for network operations |
            | **Senior Data Engineer** | Own SLA definitions, late-data handling, capacity models, and cost |
            | **Spark specialist** | Reason about skew, shuffle, broadcast joins, and incremental aggregation |

            > The goal is not to declare a cell "good" or "bad" from one week of synthetic data.
            > The goal is to design a repeatable measurement pipeline and defend its limitations.
            """
        ),
        md(
            """
            ## A. Learning Objectives

            By the end of this case study you will be able to:

            1. Model network events at a defined grain and join them to a small cell-site dimension.
            2. Compute availability, drop rate, p95 latency, throughput, and PRB utilization.
            3. Rank congested cells and identify busy hours without misleading global averages.
            4. Compare 4G and 5G quality while controlling for traffic mix.
            5. Explain why percentile metrics are more useful than averages for network SLAs.
            6. Design a production pipeline that handles late, duplicate, and malformed events.
            """
        ),
        md(
            """
            ## B. Business Context

            ### The company

            NileConnect operates eight cell sites across Cairo, Alexandria, the Delta, Upper Egypt,
            and the Canal region. The lab contains one week of synthetic voice, SMS, and data events.
            The operator is preparing a capacity investment case and a 5G rollout review.

            ### The problem before this pipeline

            - The NOC dashboard shows average latency, which hides the tail that customers feel.
            - Capacity requests are argued from raw event counts instead of utilization and drop rate.
            - 4G and 5G are compared without separating traffic mix or busy hours.
            - A cell with high PRB utilization but low drop rate is treated the same as a cell that is
              actually failing.
            - The data team cannot reproduce last month's report because the SQL lives in a notebook
              and the thresholds are undocumented.

            ### Decisions this pipeline must support

            | Decision | Question | Metric |
            |---|---|---|
            | Capacity investment | Which cells need more capacity first? | PRB utilization, busy-hour traffic, drop rate |
            | Customer experience | Where is the latency tail worst? | p95 data latency |
            | Technology strategy | Is 5G delivering better quality? | p95 latency, throughput, drop rate by technology |
            | Operations | Which cells and hours are riskiest? | Cell × hour ranking |
            | Commercial | Which plans or devices are most affected? | Drop rate and throughput by plan/device |

            ### Why Spark here?

            A week of 36,000 events is tiny. The production version may ingest billions of events per
            day from many cells, so the event-grain pipeline, partitioning, and aggregation patterns
            matter. For a single week, a time-series database or DuckDB would be a reasonable choice;
            Spark is justified by the shared multi-source platform and historical scale.
            """
        ),
        md(
            """
            ## C. Data Model and Core Concepts

            ### Grain

            One row in `network_events` is one network event for one customer on one cell. It is not
            one session and not one customer-day. A customer can generate many events.

            ### Schema

            | Table | Key | Important fields | Notes |
            |---|---|---|---|
            | `network_events` | `event_id` | timestamp, customer, tower, type, bytes, latency, throughput, PRB, dropped, plan, device | Event fact |
            | `towers` | `tower_id` | site name, city, region, technology, capacity | Small dimension |

            ### Metric definitions

            | Metric | Definition | Why it matters |
            |---|---|---|
            | **Drop rate** | Dropped events / all events | A simple availability proxy; it is not a full call-drop KPI |
            | **p95 latency** | 95th percentile of data-event latency | Captures the tail customers experience |
            | **Average throughput** | Mean data-event throughput | Useful for trend, but can hide a slow tail |
            | **PRB utilization** | Physical resource block utilization | Capacity pressure indicator |
            | **Busy hour** | Hour with the most events per cell | Capacity planning must use the peak, not the daily mean |
            | **Congestion flag** | PRB ≥ 80% or drop rate ≥ 2% | A teaching rule, not a production SLA |

            ### Important caveats

            - `latency_ms` is zero for voice and SMS events. All latency metrics filter to data events.
            - The synthetic generator deliberately makes `TWR-001` hot. Rankings are therefore
              illustrative and should not be read as a real network diagnosis.
            - `dropped` is an event-level flag, not a session outcome. A production model needs
              sessionization and a call-quality dataset.
            - Percentiles are approximate (`percentile_approx`). That is normal for distributed data;
              document the approximation and the error bound if it matters.
            """
        ),
        md(
            """
            ## D. Architecture and Data Flow

            ```text
            event stream / files            tower dimension
                    |                           |
                    v                           v
             explicit schema             broadcast join
                    |                           |
                    +-----------+---------------+
                                |
                                v
                     cleaned event fact (cache)
                                |
              +-----------------+------------------+
              |                 |                  |
              v                 v                  v
            cell health       busy hour        region/technology
              |                 |                  |
              +-----------------+------------------+
                                |
                                v
                     curated Parquet outputs
            ```

            ### What happens inside Spark

            - The event scan is narrow; the broadcast of the eight-row tower dimension avoids a
              shuffle of the event fact.
            - Grouping by cell and technology shuffles events by those keys. With eight shuffle
              partitions, each key may land in one task; skew is possible but small at this volume.
            - `percentile_approx` computes an approximate quantile in a distributed pass. It is much
              cheaper than collecting all latencies to the driver.
            - The busy-hour query groups by cell and hour, then ranks within each cell. That is a
              window shuffle plus a sort.
            - Writing partitioned Parquet by region makes region-filtered queries cheaper, but a
              region with little data can create small files.
            """
        ),
        md(
            """
            ## E. Practical Implementation

            **Objective**: build cell, region, technology, and busy-hour quality views from one week
            of network events.  
            **Input**: `/opt/spark-data/case_studies/telecom/{towers,network_events}.csv`.  
            **Expected output characteristics**: 36,000 event rows; eight towers; a small number of
            malformed tower IDs; data events with non-zero latency.
            """
        ),
        code(spark_setup("CaseStudy_Telecom")),
        md(
            """
            ### E1. Load the event fact and tower dimension
            """
        ),
        code(
            '''
            events_schema = StructType([
                StructField("event_id", StringType(), False),
                StructField("event_ts", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("tower_id", StringType(), True),
                StructField("region", StringType(), True),
                StructField("technology", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("duration_sec", IntegerType(), True),
                StructField("bytes_total", LongType(), True),
                StructField("latency_ms", IntegerType(), True),
                StructField("throughput_mbps", DoubleType(), True),
                StructField("prb_utilization", DoubleType(), True),
                StructField("dropped", IntegerType(), True),
                StructField("plan_type", StringType(), True),
                StructField("device_type", StringType(), True),
            ])
            towers_schema = StructType([
                StructField("tower_id", StringType(), False),
                StructField("site_name", StringType(), True),
                StructField("city", StringType(), True),
                StructField("region", StringType(), True),
                StructField("technology", StringType(), True),
                StructField("capacity_mbps", IntegerType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
            ])

            events_raw = (
                spark.read.option("header", True).schema(events_schema)
                .csv("/opt/spark-data/case_studies/telecom/network_events.csv")
            )
            towers = (
                spark.read.option("header", True).schema(towers_schema)
                .csv("/opt/spark-data/case_studies/telecom/towers.csv")
            )
            print(f"events: {events_raw.count():,} | towers: {towers.count()}")
            '''
        ),
        md(
            """
            ### E2. Quality gate

            The generator injects a few blank tower IDs. A production gate should quarantine them,
            not silently drop them, so the operations team can see feed problems.
            """
        ),
        code(
            '''
            events_raw.agg(
                F.count("*").alias("rows"),
                F.countDistinct("event_id").alias("distinct_event_ids"),
                F.sum(F.when(F.col("tower_id").isNull() | (F.trim(F.col("tower_id")) == ""), 1).otherwise(0)).alias("missing_tower"),
                F.sum(F.when(F.col("event_ts").isNull(), 1).otherwise(0)).alias("missing_timestamp"),
                F.sum(F.when(~F.col("dropped").isin(0, 1), 1).otherwise(0)).alias("invalid_dropped_flag"),
                F.sum(F.when(F.col("event_type") == "data", 1).otherwise(0)).alias("data_events"),
            ).show(truncate=False)
            '''
        ),
        code(
            '''
            tower_dim = towers.select(
                "tower_id",
                "site_name",
                F.col("region").alias("tower_region"),
                F.col("technology").alias("tower_technology"),
                "capacity_mbps",
            )

            events = (
                events_raw
                .filter(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull())
                .withColumn("event_ts", F.to_timestamp("event_ts"))
                .withColumn(
                    "tower_id",
                    F.when(F.trim(F.col("tower_id")) == "", F.lit(None).cast("string"))
                    .otherwise(F.col("tower_id")),
                )
                .drop("region", "technology")
                .join(F.broadcast(tower_dim), "tower_id", "left")
            )
            events.persist(StorageLevel.MEMORY_AND_DISK)
            print(f"clean events: {events.count():,}")
            events.select(
                F.sum(F.when(F.col("tower_id").isNull(), 1).otherwise(0)).alias("unmapped_tower"),
                F.sum(F.when(F.col("tower_region").isNull(), 1).otherwise(0)).alias("missing_region"),
                F.count("*").alias("rows"),
            ).show(truncate=False)
            '''
        ),
        md(
            """
            ### E3. Overall KPI snapshot

            A single global average is not an operations metric, but it is a useful sanity check.
            Note that latency and throughput are conditional on data events.
            """
        ),
        code(
            '''
            events.agg(
                F.count("*").alias("events"),
                F.sum("dropped").alias("dropped_events"),
                F.round(F.sum("dropped") / F.count("*") * 100, 2).alias("drop_rate_pct"),
                F.round(F.avg(F.when(F.col("event_type") == "data", F.col("latency_ms"))), 1).alias("avg_data_latency_ms"),
                F.round(F.percentile_approx(F.when(F.col("event_type") == "data", F.col("latency_ms")), 0.95), 1).alias("p95_data_latency_ms"),
                F.round(F.avg(F.when(F.col("event_type") == "data", F.col("throughput_mbps"))), 2).alias("avg_data_throughput_mbps"),
                F.round(F.avg("prb_utilization"), 1).alias("avg_prb_utilization"),
            ).show(truncate=False)
            '''
        ),
        md(
            """
            ### E4. Cell health scorecard

            The scorecard combines volume, availability, tail latency, throughput, and utilization.
            The flag is intentionally simple and explainable. A production threshold should be
            agreed with the network team and versioned.
            """
        ),
        code(
            '''
            cell_base = (
                events
                .filter(F.col("tower_id").isNotNull())
                .groupBy("tower_id", "site_name", "tower_region", "tower_technology", "capacity_mbps")
                .agg(
                    F.count("*").alias("events"),
                    F.sum("dropped").alias("dropped_events"),
                    F.round(F.sum("dropped") / F.count("*") * 100, 2).alias("drop_rate_pct"),
                    F.round(F.avg("prb_utilization"), 1).alias("avg_prb_utilization"),
                )
            )
            data_latency = (
                events
                .filter((F.col("event_type") == "data") & F.col("tower_id").isNotNull())
                .groupBy("tower_id")
                .agg(
                    F.round(F.avg("latency_ms"), 1).alias("avg_latency_ms"),
                    F.round(F.percentile_approx("latency_ms", 0.95), 1).alias("p95_latency_ms"),
                    F.round(F.avg("throughput_mbps"), 2).alias("avg_throughput_mbps"),
                )
            )
            cell_health = (
                cell_base
                .join(data_latency, "tower_id", "left")
                .withColumn(
                    "congestion_flag",
                    F.when((F.col("avg_prb_utilization") >= 80) | (F.col("drop_rate_pct") >= 2), "review")
                    .otherwise("ok"),
                )
                .withColumn("risk_rank", F.row_number().over(Window.orderBy(F.desc("drop_rate_pct"), F.desc("avg_prb_utilization"))))
                .orderBy("risk_rank")
            )
            cell_health.show(truncate=False)
            '''
        ),
        md(
            """
            ### E5. Busy hour by cell

            Capacity planning must use the busy hour, not the daily mean. This query ranks the top
            three hours per cell by event volume. It is a window aggregation over cell × hour.
            """
        ),
        code(
            '''
            busy_hour = (
                events
                .filter(F.col("tower_id").isNotNull())
                .withColumn("event_hour", F.hour("event_ts"))
                .groupBy("tower_id", "event_hour")
                .agg(
                    F.count("*").alias("events"),
                    F.round(F.avg("prb_utilization"), 1).alias("avg_prb_utilization"),
                    F.round(F.sum("dropped") / F.count("*") * 100, 2).alias("drop_rate_pct"),
                )
                .withColumn("hour_rank", F.row_number().over(Window.partitionBy("tower_id").orderBy(F.desc("events"))))
                .filter("hour_rank <= 3")
                .orderBy("tower_id", "hour_rank")
            )
            busy_hour.show(truncate=False)
            '''
        ),
        md(
            """
            ### E6. Region and technology comparison

            This comparison is still observational. A fair 4G/5G comparison would control for
            traffic mix, device capability, and cell load. The table is a starting point for that
            analysis, not a causal claim.
            """
        ),
        code(
            '''
            region_base = (
                events
                .filter(F.col("tower_region").isNotNull())
                .groupBy("tower_region", "tower_technology")
                .agg(
                    F.count("*").alias("events"),
                    F.sum("dropped").alias("dropped_events"),
                    F.round(F.sum("dropped") / F.count("*") * 100, 2).alias("drop_rate_pct"),
                    F.round(F.avg("prb_utilization"), 1).alias("avg_prb_utilization"),
                )
            )
            region_data = (
                events
                .filter((F.col("event_type") == "data") & F.col("tower_region").isNotNull())
                .groupBy("tower_region", "tower_technology")
                .agg(
                    F.round(F.percentile_approx("latency_ms", 0.95), 1).alias("p95_latency_ms"),
                    F.round(F.avg("throughput_mbps"), 2).alias("avg_throughput_mbps"),
                )
            )
            region_tech = (
                region_base
                .join(region_data, ["tower_region", "tower_technology"], "left")
                .orderBy(F.desc("drop_rate_pct"), F.desc("p95_latency_ms"))
            )
            region_tech.show(truncate=False)
            '''
        ),
        md(
            """
            ### E7. Customer impact by plan and device

            The same cell-level event can affect different customers differently. This view is a
            proxy: without sessionization, it cannot tell us how many customers were actually
            impacted or whether they retried successfully.
            """
        ),
        code(
            '''
            impact = (
                events
                .groupBy("plan_type", "device_type")
                .agg(
                    F.count("*").alias("events"),
                    F.sum("dropped").alias("dropped_events"),
                    F.round(F.sum("dropped") / F.count("*") * 100, 2).alias("drop_rate_pct"),
                    F.round(F.avg(F.when(F.col("event_type") == "data", F.col("throughput_mbps"))), 2).alias("avg_data_throughput_mbps"),
                )
                .orderBy(F.desc("drop_rate_pct"))
            )
            impact.show(truncate=False)
            '''
        ),
        md(
            """
            ### E8. Write curated outputs

            The cell scorecard is small and can be a single Parquet file. The event fact is
            partitioned by region so a regional investigation can prune the scan.
            """
        ),
        code(
            '''
            cell_path = "/opt/spark-data/case_studies/curated/telecom_cell_health"
            event_path = "/opt/spark-data/case_studies/curated/telecom_events"
            cell_health.write.mode("overwrite").parquet(cell_path)
            (
                events.select(
                    "event_id", "event_ts", "tower_id", "tower_region", "tower_technology",
                    "event_type", "latency_ms", "throughput_mbps", "prb_utilization", "dropped",
                )
                .write.mode("overwrite")
                .partitionBy("tower_region")
                .parquet(event_path)
            )
            print(f"cell rows: {spark.read.parquet(cell_path).count()}")
            print(f"event rows: {spark.read.parquet(event_path).count():,}")
            '''
        ),
        md(
            """
            ## F. Adjacent Real-World Use Cases

            ### Use case 1: 5G rollout prioritization

            - **Industry**: Telecommunications.
            - **Scenario**: A operator must decide which neighborhoods get 5G capacity first.
            - **Problem**: Marketing wants coverage; engineering wants utilization and revenue per
              site. A single "busy cell" list does not capture both.
            - **Why this pattern**: Cell health, busy-hour load, and customer impact can be joined
              into a prioritization score with explicit weights.
            - **Implementation idea**: Add revenue per cell, population density, and deployment cost;
              rank candidate sites with a transparent scorecard.
            - **Expected result**: A ranked investment list with assumptions and sensitivity.
            - **Alternative**: A network-planning tool with RF propagation models.
            - **Trade-offs**: Spark is good at the data join and scorecard, not at RF simulation.

            ### Use case 2: Roaming and partner quality

            - **Industry**: Mobile roaming.
            - **Scenario**: Customers roam on partner networks; the operator pays for quality failures.
            - **Problem**: Partner SLA reports arrive late and use different definitions.
            - **Why this pattern**: Normalize partner events to one grain, compute p95 and drop rate,
              and compare against contract thresholds.
            - **Implementation idea**: Ingest partner CDRs, map to home subscriptions, and publish a
              partner scorecard with a dispute workflow.
            - **Expected result**: Evidence-based SLA credits and partner discussions.
            - **Alternative**: A managed roaming-quality platform.
            - **Trade-offs**: Data-sharing and privacy constraints can be harder than the Spark work.

            ### Use case 3: IoT fleet telemetry

            - **Industry**: Industrial IoT.
            - **Scenario**: A fleet of devices reports connectivity, latency, and failures.
            - **Problem**: Averages hide a subset of devices that are effectively offline.
            - **Why this pattern**: Event-grain percentile and availability metrics generalize to
              device fleets; partition by device cohort and time.
            - **Implementation idea**: Sessionize device events, compute time-to-first-failure, and
              alert on a sustained drop in availability.
            - **Expected result**: A fleet health dashboard and a prioritized maintenance list.
            - **Alternative**: A time-series database with downsampling.
            - **Trade-offs**: A TSDB is often better for high-cardinality real-time dashboards;
              Spark is better for cross-source historical analysis.
            """
        ),
        md(
            """
            ## G. Business Value

            - **Time**: A repeatable cell scorecard replaces manual spreadsheet pulls before capacity
              reviews.
            - **Cost**: Investment can be directed by utilization and customer impact rather than by
              raw event volume.
            - **Quality**: p95 latency and drop rate expose the tail that averages hide.
            - **Decision-making**: The 4G/5G comparison becomes an evidence-based discussion with
              documented caveats.
            - **Reliability**: Quality gates catch missing tower mappings before they distort KPIs.
            - **Scale**: The same event-grain model can ingest more cells, more days, and streaming
              data.

            > **Hypothetical illustration**: If a capacity review currently takes three analyst-days
            > per month, a trusted scorecard could reduce the preparation to one day. That is an
            > illustration, not a measurement from this lab.
            """
        ),
        md(
            """
            ## H. Production Engineering

            ### Operational contract

            | Dimension | Recommendation |
            |---|---|
            | **Freshness** | Micro-batch or 5-minute streaming for NOC; daily batch for planning |
            | **Event time** | Use event time for windows; track ingestion time separately |
            | **Late data** | Define a lateness bound and a reconciliation path; do not silently drop |
            | **Duplicates** | Deduplicate by `event_id` with a deterministic tie-breaker |
            | **Dimension changes** | Version the tower dimension; preserve technology/region as of event time |
            | **Monitoring** | Event volume, drop rate, p95 latency, null tower rate, watermark lag |
            | **Security** | Customer IDs and location data are sensitive; restrict and mask |
            | **Backfills** | Recompute a bounded time range into a staging table, validate, then publish |

            ### Failure modes

            1. **Clock skew** between network elements makes event time unreliable. Monitor the
               difference between event time and ingestion time.
            2. **Tower renames or re-parenting** change region/technology history. Use an SCD-2
               dimension or event-time attributes.
            3. **Partial feeds** make a cell look healthy because it stopped reporting. Alert on
               volume drop, not only on quality thresholds.
            4. **Approximate percentiles** can mislead at low volume. Suppress cells below a minimum
               event count.
            5. **Skewed hot cells** create stragglers in cell-level aggregations. Salting or AQE skew
               handling may be needed at production scale.

            ### Investigation checklist

            - [ ] Confirm the broadcast join for the tower dimension in the SQL plan.
            - [ ] Check whether the busiest cell also has the worst p95 latency; do not assume.
            - [ ] Compare the number of tasks and shuffle bytes for cell vs region aggregations.
            - [ ] Validate the unmapped tower count and route it to a quarantine table.
            - [ ] Check output file counts per region.
            """
        ),
        md(
            """
            ## I. Trade-offs and Alternatives

            | Choice | Advantage | Limitation | When to prefer it |
            |---|---|---|---|
            | **Spark batch** | Historical joins, percentiles, multi-source | Latency of a batch job | Daily planning and backfills |
            | **Spark Structured Streaming** | Near-real-time cell KPIs | State, checkpoint, and late-data complexity | NOC alerting |
            | **Time-series database** | Fast high-cardinality dashboards | Less flexible cross-source joins | Real-time operational dashboards |
            | **Flink** | Event-time streaming and low latency | Operational complexity | Sub-second or continuous processing |
            | **DuckDB** | Simple local analytics | Single machine | One week of lab data |
            | **Average latency** | Easy to explain | Hides the tail | Never as the only SLA metric |
            | **p95 latency** | Captures customer experience | Approximate; needs volume | SLA and capacity reviews |

            **Averages are not wrong; they are incomplete.** Use them with percentiles and volume.
            """
        ),
        md(
            """
            ## J. Experiments and Investigation

            ### Experiment: compare 4G and 5G on data events

            Run the cell and inspect latency, throughput, and drop rate. Then ask whether the
            comparison is fair: are the technologies serving the same traffic mix and load?
            """
        ),
        code(
            '''
            (
                events
                .filter((F.col("event_type") == "data") & F.col("tower_technology").isNotNull())
                .groupBy("tower_technology")
                .agg(
                    F.count("*").alias("data_events"),
                    F.round(F.avg("latency_ms"), 1).alias("avg_latency_ms"),
                    F.round(F.percentile_approx("latency_ms", 0.95), 1).alias("p95_latency_ms"),
                    F.round(F.avg("throughput_mbps"), 2).alias("avg_throughput_mbps"),
                    F.round(F.sum("dropped") / F.count("*") * 100, 2).alias("drop_rate_pct"),
                )
                .orderBy("tower_technology")
                .show(truncate=False)
            )

            print("Broadcast join plan for the tower dimension:")
            events.join(F.broadcast(towers), "tower_id").explain("simple")
            '''
        ),
        md(
            """
            ### Observation guide

            - Look for `BroadcastHashJoin` and a small `BroadcastExchange`.
            - Compare p95 latency, not only the mean. A technology can have a good mean and a bad tail.
            - Check the event count per technology before drawing conclusions. Small groups produce
              unstable percentiles.
            - Inspect the Spark UI for shuffle bytes in the cell aggregation. With eight shuffle
              partitions and eight towers, some partitions may be empty.
            - Change `spark.sql.shuffle.partitions` to 4 and then 16 and observe task counts and
              runtime. Do not assume more partitions are always faster.

            ### Questions to answer with evidence

            1. Which cell has the highest drop rate, and is it also the busiest?
            2. Does 5G have a lower p95 latency after filtering to data events?
            3. Which region has the worst tail latency?
            4. How many events are unmapped to a tower, and what should happen to them?
            """
        ),
        md(
            """
            ## K. Exercises

            ### Exercise 1 (basic): cell availability table

            Produce a table with cell, site, region, events, dropped events, and drop rate. Sort by
            drop rate descending.
            """
        ),
        code("# Your solution for Exercise 1 here\n"),
        md(
            """
            ### Exercise 2 (practical): busy-hour utilization

            For each cell, find the busiest hour and report its average PRB utilization and drop
            rate. Use a window function; do not hard-code the hour.
            """
        ),
        code("# Your solution for Exercise 2 here\n"),
        md(
            """
            ### Exercise 3 (real-world): capacity candidate list

            Create a capacity candidate list with a transparent rule: at least 1,000 events, PRB
            utilization ≥ 80%, or drop rate ≥ 2%. Include the reason and rank by drop rate.
            """
        ),
        code("# Your solution for Exercise 3 here\n"),
        md(
            """
            ### Exercise 4 (debugging): the healthy cell that disappeared

            A cell stops sending events for six hours. The daily average looks excellent because the
            missing hours are absent. Diagnose the monitoring gap and propose a fix.

            **Hints**

            1. Compare expected event volume per hour with observed volume.
            2. Add a completeness check, not only a quality check.
            3. Alert on a sustained drop in volume per cell.
            """
        ),
        md(
            """
            ## Reference Solutions

            <details>
            <summary><b>Open after attempting the exercises</b></summary>

            #### Solution 1

            ```python
            (events.filter(F.col("tower_id").isNotNull())
             .groupBy("tower_id", "site_name", "tower_region")
             .agg(
                 F.count("*").alias("events"),
                 F.sum("dropped").alias("dropped_events"),
                 F.round(F.sum("dropped") / F.count("*") * 100, 2).alias("drop_rate_pct"),
             )
             .orderBy(F.desc("drop_rate_pct"))
             .show(truncate=False))
            ```

            #### Solution 2

            ```python
            busy = (events.filter(F.col("tower_id").isNotNull())
                    .withColumn("event_hour", F.hour("event_ts"))
                    .groupBy("tower_id", "event_hour")
                    .agg(
                        F.count("*").alias("events"),
                        F.round(F.avg("prb_utilization"), 1).alias("avg_prb"),
                        F.round(F.sum("dropped") / F.count("*") * 100, 2).alias("drop_rate_pct"),
                    )
                    .withColumn("rank", F.row_number().over(
                        Window.partitionBy("tower_id").orderBy(F.desc("events"))
                    ))
                    .filter("rank = 1"))
            busy.show(truncate=False)
            ```

            #### Solution 3

            ```python
            candidates = (cell_health
                          .filter((F.col("events") >= 1000) &
                                  ((F.col("avg_prb_utilization") >= 80) | (F.col("drop_rate_pct") >= 2)))
                          .withColumn("reason",
                              F.when(F.col("drop_rate_pct") >= 2, "drop rate")
                               .otherwise("PRB utilization"))
                          .orderBy(F.desc("drop_rate_pct")))
            candidates.show(truncate=False)
            ```

            #### Solution 4

            ```python
            # Build an expected hourly grid and compare observed counts.
            observed = (events.filter(F.col("tower_id") == "TWR-001")
                        .withColumn("event_hour", F.hour("event_ts"))
                        .groupBy("event_hour").count())
            observed.orderBy("event_hour").show(24, truncate=False)
            # A production check should compare against a baseline and alert on a sustained drop.
            ```

            </details>
            """
        ),
        md(
            """
            ## L. Case Study Summary

            ### What you built

            A clean event fact, cell health scorecard, busy-hour ranking, region/technology
            comparison, customer-impact proxy, and curated Parquet outputs.

            ### Key engineering lessons

            - Define the event grain and the metric denominator.
            - Use percentiles for tail-sensitive SLAs; report volume alongside them.
            - Broadcast small dimensions and verify the plan.
            - Busy-hour analysis is a capacity-planning requirement, not a nice-to-have.
            - Missing data can look like good performance; add completeness checks.
            - A synthetic ranking is a teaching device, not a network diagnosis.

            ### Interview questions

            **Q: Why use `percentile_approx` instead of collecting all latencies?**  
            A: Collecting all values to the driver is unscalable and can exhaust memory.
            `percentile_approx` computes an approximate quantile in a distributed pass with bounded
            state; the approximation is acceptable for most operational dashboards.

            **Q: How would you handle late network events?**  
            A: Define an event-time watermark or batch lateness window, keep a reconciliation path,
            and publish revised aggregates for the affected period. Do not silently drop late events
            without measuring the loss.

            **Q: Why can a cell look healthy while customers are unhappy?**  
            A: The cell may be dropping events, have a bad latency tail, or stop reporting entirely.
            Averages and partial feeds hide all three; use percentiles, completeness checks, and
            customer-impact proxies.

            ### Next steps

            Continue with [the financial fraud patterns case study](03_financial_fraud_patterns.ipynb)
            to see how event-time windows support velocity rules and how to evaluate them honestly.
            """
        ),
        code(
            '''
            events.unpersist()
            spark.stop()
            print("SparkSession stopped.")
            '''
        ),
    ]
