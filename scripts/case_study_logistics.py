#!/usr/bin/env python3
"""Cell content for the logistics delivery analytics case study."""

from __future__ import annotations

from case_study_common import code, md, spark_setup


def cells() -> list[dict]:
    return [
        md(
            """
            # Case Study 4: Logistics On-Time Delivery and Carrier Performance

            **Module**: 14_business_case_studies  
            **Notebook**: 04_logistics_delivery_analytics.ipynb  
            **Environment**: Apache Spark 4.2.1 · PySpark · Python 3.13 · Java 21  
            **Data**: `/opt/spark-data/case_studies/logistics/` (synthetic, deterministic)  
            **Estimated time**: 3–4 hours

            [Case-study index](../../docs/CASE_STUDIES.md) · [Use-case index](../../docs/USE_CASES.md) · [Business context](../../docs/BUSINESS_CONTEXT.md) · [Previous](03_financial_fraud_patterns.ipynb)

            > **Scenario note**: TransExpress is a hypothetical parcel network. The hubs, carriers,
            > shipments, and dates are synthetic. The SLA rules are teaching examples, not legal or
            > contractual terms.
            """
        ),
        md(
            """
            ## Learning Contract

            | Attribute | Value |
            |---|---|
            | **Priority** | Important |
            | **Estimated effort** | 3–4 hours |
            | **Prerequisites** | Joins, aggregations, window functions, date/time handling, partitioned writes |
            | **Required depth** | Can define an SLA, build a carrier scorecard, and explain late-delivery drivers |
            | **Data Engineer** | Build trustworthy shipment and hub facts |
            | **Senior Data Engineer** | Own SLA semantics, late-arriving scans, cost allocation, and backfills |
            | **Spark specialist** | Reason about time zones, event-time joins, shuffle, and incremental aggregation |

            > The hardest part of delivery analytics is usually semantic: what exactly counts as
            > on time, and which timestamp is authoritative?
            """
        ),
        md(
            """
            ## A. Learning Objectives

            By the end of this case study you will be able to:

            1. Model a shipment fact and a hub dimension with an explicit grain.
            2. Define promised, delivered, transit, and on-time semantics.
            3. Build carrier and lane scorecards with volume-aware rankings.
            4. Analyze late-delivery drivers by service level, distance, and weight.
            5. Handle missing delivery scans and impossible timestamps in a quality gate.
            6. Explain why time zones and late-arriving events change the answer.
            """
        ),
        md(
            """
            ## B. Business Context

            ### The company

            TransExpress moves parcels through ten hubs in Egypt, the Gulf, and Europe. The lab
            contains 20,000 shipments over two months and four carriers. Operations is renegotiating
            carrier contracts and needs an evidence-based view of on-time performance.

            ### The problem before this pipeline

            - Each carrier reports on-time performance using its own definition.
            - Operations averages all shipments, so a small expedited segment can hide a weak
              standard-service lane.
            - Late shipments are counted even when the delivery scan is missing.
            - A carrier with a low late rate but a high cost per kilogram looks better than it is.
            - Lane-level problems are invisible in a global dashboard.
            - Nobody can reproduce last quarter's contract review.

            ### Decisions this pipeline must support

            | Decision | Question | Output |
            |---|---|---|
            | Carrier contract | Which carrier meets the SLA at a reasonable cost? | Carrier scorecard |
            | Network design | Which lanes are consistently late? | Lane ranking |
            | Service design | Is the promise realistic for each service level? | On-time by service level |
            | Cost control | What is the cost per kilogram and per shipment? | Cost metrics |
            | Customer service | Where should exceptions be prioritized? | Exception and late-shipment views |

            ### Why Spark here?

            The lab data is small. A production parcel network generates millions of scan events per
            day from many carriers, often with late and duplicate feeds. Spark is useful for
            normalizing those feeds, joining hub and lane dimensions, and producing historical
            scorecards. A real-time control tower would need a streaming layer and an operational
            database as well.
            """
        ),
        md(
            """
            ## C. Data Model and Core Concepts

            ### Grain

            One row in `shipments` is one shipment. A shipment can have many scan events in a
            production system; this lab simplifies to one delivery timestamp. State the grain before
            joining or aggregating.

            ### Schema

            | Table | Key | Important fields | Notes |
            |---|---|---|---|
            | `shipments` | `shipment_id` | origin, destination, carrier, service, ship/promise/delivery timestamps, status, weight, cost, distance | Fact |
            | `hubs` | `hub_id` | city, region, country, coordinates | Small dimension |

            ### Definitions

            | Metric | Definition | Caveat |
            |---|---|---|
            | **Transit days** | Delivered timestamp minus ship timestamp, in days | Uses elapsed time, not business days |
            | **On time** | Delivered timestamp ≤ promised timestamp | Only defined when a delivery exists |
            | **On-time rate** | On-time deliveries / deliveries with a valid timestamp | Missing scans must be reported separately |
            | **Exception rate** | Exception shipments / all shipments | Status definitions vary by carrier |
            | **Cost per kg** | Total cost / total weight | Zero or missing weights must be excluded |
            | **Lane** | Origin hub → destination hub | A lane can have multiple service levels |

            ### Known data characteristics

            - Some shipments are still in transit; they have no delivery timestamp.
            - A few shipments have a zero weight to exercise the quality gate.
            - The generator gives carriers different on-time tendencies. The numbers are synthetic.
            - `promised_ts` includes a time-of-day component. A production SLA may promise a date,
              not a precise hour; that semantic choice changes the on-time rate.
            """
        ),
        md(
            """
            ## D. Architecture and Data Flow

            ```text
            carrier feeds / shipment files       hub dimension
                       |                             |
                       v                             v
                 quality gate                  broadcast join (origin/destination)
                       |                             |
                       +--------------+--------------+
                                      |
                                      v
                            cleaned shipment fact
                                      |
                   +------------------+------------------+
                   |                  |                  |
                   v                  v                  v
             carrier scorecard     lane ranking      late-driver analysis
                   |                  |                  |
                   +------------------+------------------+
                                      |
                                      v
                            curated Parquet outputs
            ```

            ### What happens inside Spark

            - The hub dimension is tiny, so both origin and destination joins can be broadcast.
            - Carrier and lane aggregations shuffle by those keys. At production scale, a hot lane
              or a dominant carrier can create skew.
            - On-time is computed per row before aggregation; the denominator must exclude missing
              delivery scans.
            - The weekly trend groups by a truncated timestamp, which is a narrow projection followed
              by a shuffle.
            - Writing partitioned Parquet by customer region helps regional operations, but the
              partition count must stay reasonable.
            """
        ),
        md(
            """
            ## E. Practical Implementation

            **Objective**: build a carrier scorecard, lane ranking, service-level view, and late-driver
            analysis from two months of shipments.  
            **Input**: `/opt/spark-data/case_studies/logistics/{hubs,shipments}.csv`.  
            **Expected output characteristics**: 20,000 shipments; ten hubs; four carriers; a mix of
            delivered, in-transit, exception, and returned shipments.
            """
        ),
        code(spark_setup("CaseStudy_Logistics")),
        md(
            """
            ### E1. Load shipments and hubs
            """
        ),
        code(
            '''
            shipments_schema = StructType([
                StructField("shipment_id", StringType(), False),
                StructField("origin_hub", StringType(), True),
                StructField("destination_hub", StringType(), True),
                StructField("carrier", StringType(), True),
                StructField("service_level", StringType(), True),
                StructField("ship_ts", StringType(), True),
                StructField("promised_ts", StringType(), True),
                StructField("delivered_ts", StringType(), True),
                StructField("status", StringType(), True),
                StructField("weight_kg", DoubleType(), True),
                StructField("cost_usd", DoubleType(), True),
                StructField("distance_km", DoubleType(), True),
                StructField("customer_region", StringType(), True),
            ])
            hubs_schema = StructType([
                StructField("hub_id", StringType(), False),
                StructField("city", StringType(), True),
                StructField("region", StringType(), True),
                StructField("country", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
            ])

            shipments_raw = (
                spark.read.option("header", True).schema(shipments_schema)
                .csv("/opt/spark-data/case_studies/logistics/shipments.csv")
            )
            hubs = (
                spark.read.option("header", True).schema(hubs_schema)
                .csv("/opt/spark-data/case_studies/logistics/hubs.csv")
            )
            print(f"shipments: {shipments_raw.count():,} | hubs: {hubs.count()}")
            '''
        ),
        md(
            """
            ### E2. Quality gate

            Delivery timestamps are the most fragile part of this dataset. A missing scan is not the
            same as a late delivery, and a delivery before shipment is an impossible event.
            """
        ),
        code(
            '''
            shipments_raw.agg(
                F.count("*").alias("rows"),
                F.countDistinct("shipment_id").alias("distinct_shipment_ids"),
                F.sum(F.when(F.col("weight_kg").isNull() | (F.col("weight_kg") <= 0), 1).otherwise(0)).alias("invalid_weight"),
                F.sum(F.when(F.col("ship_ts").isNull() | F.col("promised_ts").isNull(), 1).otherwise(0)).alias("missing_plan_timestamp"),
                F.sum(F.when(F.to_timestamp("delivered_ts") < F.to_timestamp("ship_ts"), 1).otherwise(0)).alias("delivered_before_ship"),
                F.sum(F.when((F.col("status") == "Delivered") & F.col("delivered_ts").isNull(), 1).otherwise(0)).alias("delivered_without_scan"),
            ).show(truncate=False)
            '''
        ),
        code(
            '''
            origin_dim = hubs.select(
                F.col("hub_id").alias("origin_hub"),
                F.col("city").alias("origin_city"),
                F.col("region").alias("origin_region"),
                F.col("country").alias("origin_country"),
            )
            destination_dim = hubs.select(
                F.col("hub_id").alias("destination_hub"),
                F.col("city").alias("destination_city"),
                F.col("region").alias("destination_region"),
                F.col("country").alias("destination_country"),
            )

            shipments = (
                shipments_raw
                .filter(F.col("shipment_id").isNotNull())
                .filter(F.col("ship_ts").isNotNull() & F.col("promised_ts").isNotNull())
                .withColumn("ship_ts", F.to_timestamp("ship_ts"))
                .withColumn("promised_ts", F.to_timestamp("promised_ts"))
                .withColumn("delivered_ts", F.to_timestamp("delivered_ts"))
                .withColumn("weight_kg", F.when(F.col("weight_kg") > 0, F.col("weight_kg")))
                .withColumn(
                    "transit_days",
                    F.round((F.col("delivered_ts").cast("long") - F.col("ship_ts").cast("long")) / 86400, 2),
                )
                .withColumn(
                    "on_time",
                    F.when(
                        (F.col("status") == "Delivered") & F.col("delivered_ts").isNotNull(),
                        (F.col("delivered_ts") <= F.col("promised_ts")).cast("int"),
                    ),
                )
                .withColumn("is_delivered", (F.col("status") == "Delivered").cast("int"))
                .join(F.broadcast(origin_dim), "origin_hub", "left")
                .join(F.broadcast(destination_dim), "destination_hub", "left")
            )
            shipments.persist(StorageLevel.MEMORY_AND_DISK)
            print(f"clean shipments: {shipments.count():,}")
            shipments.select(
                F.sum(F.when(F.col("origin_city").isNull(), 1).otherwise(0)).alias("unmapped_origin"),
                F.sum(F.when(F.col("destination_city").isNull(), 1).otherwise(0)).alias("unmapped_destination"),
                F.sum(F.when(F.col("status") == "Delivered", 1).otherwise(0)).alias("delivered_status"),
                F.sum(F.when(F.col("on_time").isNotNull(), 1).otherwise(0)).alias("with_on_time"),
            ).show(truncate=False)
            '''
        ),
        md(
            """
            ### E3. Overall delivery KPIs

            The on-time denominator is deliveries with a timestamp, not all shipments. Report the
            missing-scan rate separately; otherwise a carrier can improve its score by losing scans.
            """
        ),
        code(
            '''
            shipments.agg(
                F.count("*").alias("shipments"),
                F.sum("is_delivered").alias("delivered_status"),
                F.sum(F.when((F.col("status") == "Delivered") & F.col("delivered_ts").isNotNull(), 1).otherwise(0)).alias("with_delivery_scan"),
                F.round(F.avg("on_time") * 100, 2).alias("on_time_pct"),
                F.round(F.avg("transit_days"), 2).alias("avg_transit_days"),
                F.round(F.percentile_approx("transit_days", 0.95), 2).alias("p95_transit_days"),
                F.round(F.avg("cost_usd"), 2).alias("avg_cost_usd"),
                F.round(F.sum("cost_usd") / F.sum("weight_kg"), 2).alias("cost_per_kg"),
                F.sum(F.when(F.col("status") == "Exception", 1).otherwise(0)).alias("exceptions"),
            ).show(truncate=False)
            '''
        ),
        md(
            """
            ### E4. Carrier scorecard

            Volume matters. A carrier with 200 shipments and a 95% on-time rate is not necessarily
            better than one with 5,000 shipments and 92%. Keep both the rate and the volume visible.
            """
        ),
        code(
            '''
            carrier_score = (
                shipments
                .groupBy("carrier")
                .agg(
                    F.count("*").alias("shipments"),
                    F.sum(F.when((F.col("status") == "Delivered") & F.col("delivered_ts").isNotNull(), 1).otherwise(0)).alias("delivered_with_scan"),
                    F.sum("on_time").alias("on_time_shipments"),
                    F.round(F.avg("on_time") * 100, 2).alias("on_time_pct"),
                    F.round(F.avg("transit_days"), 2).alias("avg_transit_days"),
                    F.round(F.percentile_approx("transit_days", 0.95), 2).alias("p95_transit_days"),
                    F.sum(F.when(F.col("status") == "Exception", 1).otherwise(0)).alias("exceptions"),
                    F.round(F.sum("cost_usd"), 2).alias("total_cost_usd"),
                    F.round(F.sum("cost_usd") / F.sum("weight_kg"), 2).alias("cost_per_kg"),
                )
                .withColumn("exception_rate_pct", F.round(F.col("exceptions") / F.col("shipments") * 100, 2))
                .withColumn("on_time_rank", F.row_number().over(Window.orderBy(F.desc("on_time_pct"))))
                .orderBy("on_time_rank")
            )
            carrier_score.show(truncate=False)
            '''
        ),
        md(
            """
            ### E5. Lane performance

            A lane is origin hub → destination hub. Filter to lanes with enough volume before ranking;
            a single late shipment on a one-shipment lane is not evidence of a systemic problem.
            """
        ),
        code(
            '''
            lane_perf = (
                shipments
                .filter((F.col("status") == "Delivered") & F.col("delivered_ts").isNotNull())
                .groupBy(
                    "origin_hub", "origin_city", "destination_hub", "destination_city",
                    "origin_region", "destination_region",
                )
                .agg(
                    F.count("*").alias("shipments"),
                    F.round(F.avg("on_time") * 100, 2).alias("on_time_pct"),
                    F.round(F.avg("transit_days"), 2).alias("avg_transit_days"),
                    F.round(F.avg("cost_usd"), 2).alias("avg_cost_usd"),
                )
                .filter(F.col("shipments") >= 50)
                .orderBy("on_time_pct", F.desc("shipments"))
            )
            lane_perf.show(20, truncate=False)
            '''
        ),
        md(
            """
            ### E6. Service-level and distance drivers

            A single global late rate can hide a service-level problem. Compare promise length,
            distance band, and weight band. This is descriptive, not causal.
            """
        ),
        code(
            '''
            driver_view = (
                shipments
                .withColumn(
                    "distance_band",
                    F.when(F.col("distance_km") < 500, "< 500 km")
                    .when(F.col("distance_km") < 1500, "500–1499 km")
                    .when(F.col("distance_km") < 3000, "1500–2999 km")
                    .otherwise("3000+ km"),
                )
                .withColumn(
                    "weight_band",
                    F.when(F.col("weight_kg") < 1, "< 1 kg")
                    .when(F.col("weight_kg") < 5, "1–4.9 kg")
                    .when(F.col("weight_kg") < 15, "5–14.9 kg")
                    .otherwise("15+ kg"),
                )
                .groupBy("service_level", "distance_band")
                .agg(
                    F.count("*").alias("shipments"),
                    F.sum(F.when((F.col("status") == "Delivered") & F.col("delivered_ts").isNotNull(), 1).otherwise(0)).alias("delivered"),
                    F.round(F.avg("on_time") * 100, 2).alias("on_time_pct"),
                    F.round(F.avg("transit_days"), 2).alias("avg_transit_days"),
                )
                .orderBy("service_level", "distance_band")
            )
            driver_view.show(truncate=False)

            shipments.groupBy("service_level").agg(
                F.count("*").alias("shipments"),
                F.round(F.avg("on_time") * 100, 2).alias("on_time_pct"),
                F.round(F.avg("transit_days"), 2).alias("avg_transit_days"),
                F.round(F.avg("cost_usd"), 2).alias("avg_cost_usd"),
            ).orderBy("service_level").show(truncate=False)
            '''
        ),
        md(
            """
            ### E7. Weekly trend

            Weekly aggregation is a useful operational cadence. Be careful with the first and last
            partial weeks; label them or exclude them from trend comparisons.
            """
        ),
        code(
            '''
            weekly = (
                shipments
                .withColumn("ship_week", F.date_trunc("week", "ship_ts"))
                .groupBy("ship_week")
                .agg(
                    F.count("*").alias("shipments"),
                    F.round(F.avg("on_time") * 100, 2).alias("on_time_pct"),
                    F.round(F.avg("transit_days"), 2).alias("avg_transit_days"),
                    F.round(F.avg("cost_usd"), 2).alias("avg_cost_usd"),
                )
                .orderBy("ship_week")
            )
            weekly.show(20, truncate=False)
            '''
        ),
        md(
            """
            ### E8. Write curated outputs

            The shipment fact is partitioned by customer region for regional operations. The carrier
            scorecard is small and can be a single Parquet dataset.
            """
        ),
        code(
            '''
            carrier_path = "/opt/spark-data/case_studies/curated/logistics_carrier_scorecard"
            shipment_path = "/opt/spark-data/case_studies/curated/logistics_shipments"
            carrier_score.write.mode("overwrite").parquet(carrier_path)
            (
                shipments.select(
                    "shipment_id", "origin_hub", "destination_hub", "origin_region",
                    "destination_region", "carrier", "service_level", "ship_ts",
                    "promised_ts", "delivered_ts", "status", "transit_days", "on_time",
                    "weight_kg", "cost_usd", "distance_km", "customer_region",
                )
                .write.mode("overwrite")
                .partitionBy("customer_region")
                .parquet(shipment_path)
            )
            print(f"carrier rows: {spark.read.parquet(carrier_path).count()}")
            print(f"shipment rows: {spark.read.parquet(shipment_path).count():,}")
            '''
        ),
        md(
            """
            ## F. Adjacent Real-World Use Cases

            ### Use case 1: Returns logistics

            - **Industry**: E-commerce reverse logistics.
            - **Scenario**: Returned parcels must be picked up, consolidated, and refunded.
            - **Problem**: Return SLAs and refund SLAs are measured separately; customers see the
              worst of both.
            - **Why this pattern**: The shipment fact can be extended with return legs and refund
              events, then measured end to end.
            - **Implementation idea**: Model return shipments as a new fact with the same hub
              dimension; join to refund events and measure the customer wait.
            - **Expected result**: A return-cycle dashboard with carrier and warehouse bottlenecks.
            - **Alternative**: A warehouse management system report.
            - **Trade-offs**: End-to-end measurement requires cross-system keys and ownership.

            ### Use case 2: Cold-chain compliance

            - **Industry**: Pharma and food logistics.
            - **Scenario**: Temperature-sensitive shipments need excursion alerts and audit evidence.
            - **Problem**: A late delivery is not the only failure; a temperature breach can ruin the
              product even if it arrives on time.
            - **Why this pattern**: Shipment facts can join sensor telemetry and SLA events at the
              shipment grain.
            - **Implementation idea**: Ingest sensor windows, compute min/max temperature per leg, and
              join to shipment milestones.
            - **Expected result**: A compliance report with excursion duration and ownership.
            - **Alternative**: A validated cold-chain monitoring platform.
            - **Trade-offs**: Regulatory validation and sensor calibration dominate; Spark is the
              analytics layer, not the compliance system.

            ### Use case 3: Fleet telematics and driver safety

            - **Industry**: Road freight.
            - **Scenario**: A fleet wants to correlate driving behavior with late deliveries and
              incidents.
            - **Problem**: Telematics events are high-volume and noisy; delivery data is sparse.
            - **Why this pattern**: Event-time windows and joins to trips/shipments can compute harsh
              braking, speeding, and idle time per trip.
            - **Implementation idea**: Sessionize telematics into trips, join to shipment milestones,
              and build a driver/route safety scorecard.
            - **Expected result**: Targeted coaching and route-risk insights.
            - **Alternative**: A telematics vendor dashboard.
            - **Trade-offs**: Privacy, labor agreements, and data quality are significant constraints.
            """
        ),
        md(
            """
            ## G. Business Value

            - **Time**: A reproducible carrier scorecard shortens contract-review preparation.
            - **Cost**: Cost per kilogram and per lane exposes expensive routes that a global average
              hides.
            - **Quality**: Explicit on-time and missing-scan definitions prevent carriers from
              improving scores by losing data.
            - **Decision-making**: Lane-level evidence supports targeted network changes.
            - **Reliability**: Quality checks catch impossible delivery timestamps before they reach
              finance.
            - **Scale**: The same shipment grain can ingest more carriers, countries, and scan events.

            > **Hypothetical illustration**: If late deliveries on two lanes account for a large share
            > of service credits, fixing those lanes could reduce credits without changing the whole
            > carrier contract. The actual share must be measured with real data.
            """
        ),
        md(
            """
            ## H. Production Engineering

            ### Operational contract

            | Dimension | Recommendation |
            |---|---|
            | **SLA semantics** | Define whether promise is a date, a time, or a business-day promise; version it |
            | **Time zones** | Normalize to UTC and retain the local time zone for reporting |
            | **Late scans** | Allow a correction window; publish restatements instead of silently changing history |
            | **Duplicates** | Deduplicate scan events by shipment + milestone + event time |
            | **Carrier feeds** | Validate schema and status codes per carrier; quarantine unknown codes |
            | **Monitoring** | On-time rate, missing-scan rate, exception rate, lane volume, cost per kg |
            | **Security** | Customer addresses and phone numbers are sensitive; restrict and mask |
            | **Backfills** | Recompute bounded date ranges and compare to the published scorecard |

            ### Failure modes

            1. **Missing delivery scans** make a carrier look better or worse depending on the
               denominator.
            2. **Time-zone bugs** shift a delivery across midnight and change the on-time flag.
            3. **Carrier status mapping** differs; one carrier's "delivered" may be another's
               "handed to agent".
            4. **Late-arriving scans** change historical on-time rates after contract review.
            5. **Zero weights** create infinite or misleading cost-per-kg metrics.
            6. **Partial weeks** distort trend comparisons.

            ### Investigation checklist

            - [ ] Confirm both hub joins are broadcast.
            - [ ] Compare on-time rates with and without missing scans.
            - [ ] Inspect the worst lanes with a minimum-volume filter.
            - [ ] Check whether a carrier's cost advantage survives cost per kg.
            - [ ] Review weekly trends for partial-week artifacts.
            - [ ] Validate a sample of impossible timestamps with the carrier.
            """
        ),
        md(
            """
            ## I. Trade-offs and Alternatives

            | Choice | Advantage | Limitation | When to prefer it |
            |---|---|---|---|
            | **Spark batch** | Cross-carrier history, joins, scalable aggregation | Batch latency | Daily/weekly scorecards and backfills |
            | **Streaming** | Near-real-time exception alerts | State and late-data complexity | Control tower and customer notifications |
            | **Operational database** | Fast single-shipment lookup | Weak historical analytics | Tracking pages and customer service |
            | **Warehouse** | Governed BI and concurrency | Ingestion and cost model | Executive reporting |
            | **Average transit time** | Simple | Hides the tail | Never as the only SLA metric |
            | **p95 transit time** | Captures the tail | Needs volume and clear semantics | Contract review |

            **A carrier scorecard is a decision aid, not a contract.** Legal and procurement teams must
            agree on the definition before it is used commercially.
            """
        ),
        md(
            """
            ## J. Experiments and Investigation

            ### Experiment: compare carriers on volume and tail performance

            Run the cell and inspect on-time rate, p95 transit days, volume, and cost per kg together.
            Then decide which carrier deserves a deeper lane-level review.
            """
        ),
        code(
            '''
            (
                shipments
                .groupBy("carrier", "service_level")
                .agg(
                    F.count("*").alias("shipments"),
                    F.round(F.avg("on_time") * 100, 2).alias("on_time_pct"),
                    F.round(F.percentile_approx("transit_days", 0.95), 2).alias("p95_transit_days"),
                    F.round(F.sum("cost_usd") / F.sum("weight_kg"), 2).alias("cost_per_kg"),
                )
                .orderBy("carrier", "service_level")
                .show(truncate=False)
            )

            print("Broadcast join plan for the hub dimension:")
            shipments.join(F.broadcast(hubs), shipments.origin_hub == hubs.hub_id).explain("simple")
            '''
        ),
        md(
            """
            ### Observation guide

            - A carrier with a good average but a bad p95 may be fine on typical days and terrible
              during peak.
            - A carrier with a low on-time rate on a small volume may not be statistically
              meaningful. Filter by volume.
            - Cost per kg can favor a carrier that handles light parcels; compare within service
              level and weight band.
            - Check whether the worst lanes share a hub, a carrier, or a service level.
            - Look for partial weeks at the start and end of the trend.

            ### Questions to answer with evidence

            1. Which carrier has the best on-time rate at a meaningful volume?
            2. Which lane has the worst on-time rate with at least 50 shipments?
            3. Does Express have a better on-time rate than Standard? Is the promise stricter?
            4. How many delivered-status shipments lack a delivery scan?
            """
        ),
        md(
            """
            ## K. Exercises

            ### Exercise 1 (basic): on-time rate by service level

            Compute shipments, delivered scans, and on-time rate by service level. Sort by on-time
            rate.
            """
        ),
        code("# Your solution for Exercise 1 here\n"),
        md(
            """
            ### Exercise 2 (practical): worst lanes with volume filter

            Find the ten lanes with the lowest on-time rate among lanes with at least 100 delivered
            shipments. Include average transit days and cost.
            """
        ),
        code("# Your solution for Exercise 2 here\n"),
        md(
            """
            ### Exercise 3 (real-world): carrier renegotiation brief

            Build a one-page carrier brief with volume, on-time rate, p95 transit, cost per kg, and
            the three worst lanes. State one recommendation and one caveat.
            """
        ),
        code("# Your solution for Exercise 3 here\n"),
        md(
            """
            ### Exercise 4 (debugging): the carrier that improved overnight

            A carrier's on-time rate jumps from 78% to 94% after a feed change. Investigate whether
            the improvement is real or caused by missing scans, a status-mapping change, or a
            time-zone shift.

            **Hints**

            1. Compare delivered volume and missing-scan rate before and after.
            2. Check the distribution of delivery timestamps around midnight.
            3. Compare the carrier's status codes before and after the change.
            """
        ),
        md(
            """
            ## Reference Solutions

            <details>
            <summary><b>Open after attempting the exercises</b></summary>

            #### Solution 1

            ```python
            shipments.groupBy("service_level").agg(
                F.count("*").alias("shipments"),
                F.sum(F.when((F.col("status") == "Delivered") & F.col("delivered_ts").isNotNull(), 1).otherwise(0)).alias("delivered"),
                F.round(F.avg("on_time") * 100, 2).alias("on_time_pct"),
            ).orderBy("on_time_pct").show()
            ```

            #### Solution 2

            ```python
            (shipments.filter((F.col("status") == "Delivered") & F.col("delivered_ts").isNotNull())
             .groupBy("origin_hub", "destination_hub")
             .agg(
                 F.count("*").alias("shipments"),
                 F.round(F.avg("on_time") * 100, 2).alias("on_time_pct"),
                 F.round(F.avg("transit_days"), 2).alias("avg_transit_days"),
                 F.round(F.avg("cost_usd"), 2).alias("avg_cost_usd"),
             )
             .filter("shipments >= 100")
             .orderBy("on_time_pct")
             .show(10, truncate=False))
            ```

            #### Solution 3

            ```python
            # Reuse carrier_score and lane_perf. A brief is a communication artifact,
            # not a new metric definition; keep the definitions consistent with section E.
            carrier_score.show(truncate=False)
            lane_perf.filter(F.col("origin_hub").isNotNull()).show(3, truncate=False)
            ```

            #### Solution 4

            ```python
            # Compare missing scans and status codes before/after the feed change.
            shipments.groupBy("carrier", "status").count().orderBy("carrier", "status").show()
            # Check deliveries near midnight and the time-zone conversion used by the feed.
            shipments.filter(F.col("carrier") == "NileExpress").select("ship_ts", "promised_ts", "delivered_ts").show(5)
            ```

            </details>
            """
        ),
        md(
            """
            ## L. Case Study Summary

            ### What you built

            A clean shipment fact, overall KPIs, carrier scorecard, lane ranking, service/distance
            driver view, weekly trend, and curated Parquet outputs.

            ### Key engineering lessons

            - Define on-time semantics before computing a rate.
            - Missing scans are not late deliveries; report them separately.
            - Volume and tail percentiles belong next to every carrier rate.
            - Lane-level analysis finds problems that a global average hides.
            - Time zones and late-arriving scans can change historical results.
            - A scorecard is a decision aid; procurement and legal must agree on the definition.

            ### Interview questions

            **Q: Why is on-time rate not simply on-time shipments divided by all shipments?**  
            A: Shipments without a delivery scan have an undefined on-time outcome. Including them as
            late or on time biases the rate. Report the missing-scan rate separately and define the
            denominator explicitly.

            **Q: How would you handle late-arriving delivery scans?**  
            A: Use a correction window, recompute the affected period, publish a restatement with a
            version, and keep the original published result for audit.

            **Q: Why compare p95 transit time as well as the average?**  
            A: The average hides the tail that drives customer complaints and service credits. A
            carrier can have a good average and a bad p95.

            ### Next steps

            Return to the [case-study index](../../docs/CASE_STUDIES.md) and compare the four case
            studies: order grain, event grain, transaction grain, and shipment grain. The differences
            in metric definitions and failure modes are the real lesson.
            """
        ),
        code(
            '''
            shipments.unpersist()
            spark.stop()
            print("SparkSession stopped.")
            '''
        ),
    ]
