#!/usr/bin/env python3
"""Cell content for the e-commerce business case study."""

from __future__ import annotations

from case_study_common import code, md, spark_setup


def cells() -> list[dict]:
    return [
        md(
            """
            # Case Study 1: E-Commerce Revenue, Retention and Order Outcomes

            **Module**: 14_business_case_studies  
            **Notebook**: 01_ecommerce_analytics.ipynb  
            **Environment**: Apache Spark 4.2.1 · PySpark · Python 3.13 · Java 21  
            **Data**: `/opt/spark-data/retail/` (synthetic, deterministic)  
            **Estimated time**: 3–4 hours

            [Case-study index](../../docs/CASE_STUDIES.md) · [Use-case index](../../docs/USE_CASES.md) · [Business context](../../docs/BUSINESS_CONTEXT.md) · [Next case study](02_telecom_network_analytics.ipynb)

            > **Scenario note**: Northstar Outfitters is a hypothetical retailer. The data is
            > synthetic. Any financial or operational figure is illustrative unless it is printed
            > by this notebook.
            """
        ),
        md(
            """
            ## Learning Contract

            | Attribute | Value |
            |---|---|
            | **Priority** | Important |
            | **Estimated effort** | 3–4 hours |
            | **Prerequisites** | DataFrame joins, aggregations, window functions, broadcast joins, partitioned writes |
            | **Required depth** | Can design a curated retail mart, defend its grain, and explain its physical plan |
            | **Data Engineer** | Build clean, reproducible revenue and customer marts |
            | **Senior Data Engineer** | Own metric definitions, data contracts, backfills, and cost/performance |
            | **Spark specialist** | Reason about broadcast joins, shuffles, caching, and partition layout |

            > **How to use this notebook**: read the business context first, run each cell, then
            > inspect the Spark UI or `explain()` output. The goal is not to memorize the query;
            > it is to defend the design and its trade-offs.
            """
        ),
        md(
            """
            ## A. Learning Objectives

            By the end of this case study you will be able to:

            1. Translate retail business questions into a star-schema-style analytical model.
            2. Build a clean order fact table with explicit schemas and a documented quality gate.
            3. Compute revenue, margin, cohort retention, RFM segments, and order outcomes.
            4. Use broadcast joins for small dimensions and explain why the plan changes.
            5. Write a partitioned Parquet mart and state its freshness, grain, and known limits.
            6. Discuss when Spark is the right tool and when DuckDB, pandas, or a warehouse is simpler.
            """
        ),
        md(
            """
            ## B. Business Context

            ### The company

            Northstar Outfitters sells electronics, accessories, home, office, and software products
            through web, mobile, store, and partner channels. The lab contains 60,000 orders from
            2025, 8,000 customers, and 300 products.

            ### The problem before this pipeline

            Finance, marketing, and operations each export the same order CSV and rebuild their own
            spreadsheet logic. The result is predictable:

            - Finance reports net revenue one way; marketing reports gross bookings another way.
            - A customer who bought twice is counted twice in some reports and once in others.
            - Cohort retention is re-computed with a different definition every month.
            - Nobody can explain why the category with the highest revenue has a weak margin.
            - The return and cancellation rate is invisible in the executive dashboard.

            ### Decisions this pipeline must support

            | Decision | Question | Mart |
            |---|---|---|
            | Merchandising | Which categories and products create profitable growth? | Category and product performance |
            | Marketing | Which acquisition cohorts retain, and which customers are most valuable? | Cohort retention and RFM |
            | Operations | Where do cancellations and returns concentrate? | Order-outcome rates |
            | Finance | What is the trusted monthly net revenue and margin? | Monthly revenue mart |

            ### Why Spark here?

            The data is intentionally small enough for a laptop. Spark is justified because the
            same pipeline pattern must handle years of history, multiple sources, and shared
            cluster execution. For a one-off 60,000-row analysis, DuckDB or pandas would be simpler;
            that trade-off is discussed in section I.
            """
        ),
        md(
            """
            ## C. Data Model and Core Concepts

            ### Grain

            One row in `orders` is one order line. The lab has one product per order, so the order
            grain and line grain coincide here. In a real system, an order can contain many lines;
            state the grain before joining or aggregating.

            ### Schema

            | Table | Key | Important fields | Notes |
            |---|---|---|---|
            | `orders` | `order_id` | customer, product, timestamp, quantity, price, discount, status, channel | Fact-like event table |
            | `customers` | `customer_id` | country, region, segment, loyalty tier, signup date | Small dimension |
            | `products` | `product_id` | name, category, list price, cost, launch date, active | Small dimension |

            ### Metric definitions used in this notebook

            | Metric | Definition |
            |---|---|
            | **Gross revenue** | `quantity * unit_price` before discount |
            | **Net revenue** | `quantity * unit_price * (1 - discount)` |
            | **Cost** | `quantity * product.cost` |
            | **Margin** | Net revenue minus cost |
            | **Completed order** | `status = 'Completed'` |
            | **AOV** | Net revenue divided by distinct completed orders |
            | **Guest order** | Order with no `customer_id`; included in revenue, excluded from customer-level analysis |

            ### Known data characteristics

            - About 1% of orders are guest orders with a missing `customer_id`.
            - Order status includes `Completed`, `Cancelled`, `Returned`, and `Pending`.
            - Product 1 is deliberately skewed in the generator, so product-level rankings are not
              representative of a real catalog.

            ### Star-schema view

            ```text
            customers (dim)            products (dim)
                  \\                       /
                   \\                     /
                    orders (fact-like event table)
                             |
                    derived marts:
                    - monthly revenue
                    - category/product performance
                    - cohort retention
                    - RFM segments
                    - order outcomes
            ```
            """
        ),
        md(
            """
            ## D. Architecture and Data Flow

            ```text
            CSV files
               |
               v
            explicit schema + quality gate
               |
               v
            clean orders ---> broadcast customers
               |                    |
               +---- broadcast products
               |
               v
            enriched sales (cached, MEMORY_AND_DISK)
               |
               +--> monthly revenue + MoM
               +--> category/product ranking
               +--> cohort retention
               +--> RFM segments
               +--> order outcomes
               |
               v
            partitioned Parquet mart
            ```

            ### What happens inside Spark

            - Reading CSV files is a narrow scan. Explicit schemas avoid an extra inference job.
            - Joining the small `products` and `customers` dimensions can use a broadcast hash join,
              so the fact table is not shuffled for those joins.
            - `groupBy` and window operations introduce shuffles. With eight shuffle partitions,
              this lab produces a small number of tasks; production sizing must be based on data
              volume and cores, not copied from this value.
            - Caching the enriched table avoids recomputing the same joins for five analyses. The
              cache can spill to disk; it is not a guarantee that everything stays in RAM.
            - Writing with `partitionBy("order_month")` creates one directory per month. That helps
              a monthly filter, but too many small partitions would hurt file planning.
            """
        ),
        md(
            """
            ## E. Practical Implementation

            **Objective**: produce a trusted sales fact and four business marts from the retail CSVs.  
            **Input**: `/opt/spark-data/retail/{orders,customers,products}.csv`.  
            **Expected output characteristics**: 60,000 order rows before the quality filter; about
            1% guest orders; a small number of months; a partitioned Parquet mart.
            """
        ),
        code(spark_setup("CaseStudy_Ecommerce")),
        md(
            """
            ### E1. Load with explicit schemas

            The schema is part of the data contract. It prevents Spark from spending a job on
            inference and makes type changes visible in code review.
            """
        ),
        code(
            '''
            orders_schema = StructType([
                StructField("order_id", IntegerType(), False),
                StructField("customer_id", IntegerType(), True),
                StructField("product_id", IntegerType(), True),
                StructField("order_ts", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("discount", DoubleType(), True),
                StructField("status", StringType(), True),
                StructField("channel", StringType(), True),
            ])
            customers_schema = StructType([
                StructField("customer_id", IntegerType(), False),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("country", StringType(), True),
                StructField("region", StringType(), True),
                StructField("segment", StringType(), True),
                StructField("signup_date", DateType(), True),
                StructField("loyalty_tier", StringType(), True),
            ])
            products_schema = StructType([
                StructField("product_id", IntegerType(), False),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("list_price", DoubleType(), True),
                StructField("cost", DoubleType(), True),
                StructField("launch_date", DateType(), True),
                StructField("active", BooleanType(), True),
            ])

            orders_raw = spark.read.option("header", True).schema(orders_schema).csv("/opt/spark-data/retail/orders.csv")
            customers = spark.read.option("header", True).schema(customers_schema).csv("/opt/spark-data/retail/customers.csv")
            products = spark.read.option("header", True).schema(products_schema).csv("/opt/spark-data/retail/products.csv")

            print(f"orders rows:    {orders_raw.count():,}")
            print(f"customers rows: {customers.count():,}")
            print(f"products rows:  {products.count():,}")
            '''
        ),
        md(
            """
            ### E2. Quality gate

            A quality gate should be explicit, cheap, and visible. These checks are not a complete
            data-quality framework; they are the invariants this case study depends on.
            """
        ),
        code(
            '''
            orders_raw.agg(
                F.count("*").alias("rows"),
                F.countDistinct("order_id").alias("distinct_order_ids"),
                F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)).alias("guest_orders"),
                F.sum(F.when(F.col("quantity") <= 0, 1).otherwise(0)).alias("non_positive_quantity"),
                F.sum(F.when(F.col("unit_price").isNull(), 1).otherwise(0)).alias("missing_price"),
                F.sum(F.when(~F.col("status").isin("Completed", "Cancelled", "Returned", "Pending"), 1).otherwise(0)).alias("unknown_status"),
            ).show(truncate=False)
            '''
        ),
        code(
            '''
            orders = (
                orders_raw
                .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
                .filter(F.col("quantity") > 0)
                .filter(F.col("unit_price").isNotNull() & F.col("order_ts").isNotNull())
                .withColumn("order_ts", F.to_timestamp("order_ts"))
                .withColumn("order_date", F.to_date("order_ts"))
                .withColumn("discount", F.coalesce("discount", F.lit(0.0)))
                .filter(F.col("status").isin("Completed", "Cancelled", "Returned", "Pending"))
            )

            product_dim = products.select(
                "product_id", "product_name", "category", "cost", "launch_date", "active"
            )
            customer_dim = customers.select(
                "customer_id", "country", "region", "segment", "signup_date", "loyalty_tier"
            )

            sales = (
                orders
                .join(F.broadcast(product_dim), "product_id", "left")
                .join(F.broadcast(customer_dim), "customer_id", "left")
                .withColumn("gross_revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
                .withColumn("net_revenue", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2))
                .withColumn("cost_amount", F.round(F.col("quantity") * F.col("cost"), 2))
                .withColumn("margin", F.round(F.col("net_revenue") - F.col("cost_amount"), 2))
                .withColumn("order_month", F.date_format("order_ts", "yyyy-MM"))
                .withColumn("is_completed", F.col("status") == "Completed")
            )

            sales.persist(StorageLevel.MEMORY_AND_DISK)
            print(f"enriched rows: {sales.count():,}")
            sales.select(
                F.count("*").alias("rows"),
                F.countDistinct("order_id").alias("orders"),
                F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)).alias("guest_rows"),
                F.sum(F.when(F.col("net_revenue") < 0, 1).otherwise(0)).alias("negative_revenue_rows"),
                F.sum(F.when(F.col("category").isNull(), 1).otherwise(0)).alias("missing_product_dim"),
            ).show(truncate=False)
            '''
        ),
        md(
            """
            ### E3. Monthly revenue and month-over-month growth

            This is the finance-facing mart. It uses completed orders only. The window is ordered by
            month, so the first month has no prior value and `mom_pct` is null.
            """
        ),
        code(
            '''
            completed = sales.filter(F.col("is_completed"))
            monthly = (
                completed
                .groupBy("order_month")
                .agg(
                    F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
                    F.round(F.sum("margin"), 2).alias("gross_margin"),
                    F.countDistinct("order_id").alias("orders"),
                )
                .withColumn("aov", F.round(F.col("net_revenue") / F.col("orders"), 2))
                .withColumn("margin_pct", F.round(F.col("gross_margin") / F.col("net_revenue") * 100, 2))
            )
            month_window = Window.orderBy("order_month")
            monthly = (
                monthly
                .withColumn("previous_revenue", F.lag("net_revenue", 1).over(month_window))
                .withColumn(
                    "mom_pct",
                    F.round((F.col("net_revenue") - F.col("previous_revenue")) / F.col("previous_revenue") * 100, 2),
                )
                .orderBy("order_month")
            )
            monthly.show(12, truncate=False)
            '''
        ),
        md(
            """
            ### E4. Category and product performance

            The category table answers "where is profitable growth?"; the product ranking answers
            "what should merchandising review?" `dense_rank` keeps ties visible and avoids gaps.
            """
        ),
        code(
            '''
            category_perf = (
                completed
                .groupBy("category")
                .agg(
                    F.countDistinct("order_id").alias("orders"),
                    F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
                    F.round(F.sum("margin"), 2).alias("gross_margin"),
                    F.round(F.avg("net_revenue"), 2).alias("avg_order_value"),
                )
                .withColumn("margin_pct", F.round(F.col("gross_margin") / F.col("net_revenue") * 100, 2))
                .orderBy(F.desc("net_revenue"))
            )
            category_perf.show(truncate=False)

            category_window = Window.partitionBy("category").orderBy(F.desc("net_revenue"))
            top_products = (
                completed
                .groupBy("category", "product_name")
                .agg(
                    F.countDistinct("order_id").alias("orders"),
                    F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
                )
                .withColumn("rank", F.dense_rank().over(category_window))
                .filter("rank <= 3")
                .orderBy("category", "rank")
            )
            top_products.show(15, truncate=False)
            '''
        ),
        md(
            """
            ### E5. Cohort retention

            A cohort is the customer's first completed-order month. Retention in month `n` is the
            share of that cohort with at least one completed order in that month. This is a
            customer-count view, not a revenue view. Both are useful; mixing them produces
            misleading percentages.

            The output is limited to the first six months so the pivot remains readable.
            """
        ),
        code(
            '''
            customer_first = (
                completed
                .filter(F.col("customer_id").isNotNull())
                .groupBy("customer_id")
                .agg(F.min("order_month").alias("cohort_month"))
            )

            cohort_activity = (
                completed
                .filter(F.col("customer_id").isNotNull())
                .join(customer_first, "customer_id")
                .withColumn(
                    "month_index",
                    F.months_between(
                        F.to_date(F.concat(F.col("order_month"), F.lit("-01"))),
                        F.to_date(F.concat(F.col("cohort_month"), F.lit("-01"))),
                    ).cast("int"),
                )
                .filter(F.col("month_index").between(0, 5))
            )

            cohort_size = customer_first.groupBy("cohort_month").agg(
                F.countDistinct("customer_id").alias("cohort_size")
            )
            retention = (
                cohort_activity
                .groupBy("cohort_month", "month_index")
                .agg(F.countDistinct("customer_id").alias("active_customers"))
                .join(cohort_size, "cohort_month")
                .withColumn("retention_pct", F.round(F.col("active_customers") / F.col("cohort_size") * 100, 2))
                .orderBy("cohort_month", "month_index")
            )
            retention.show(60, truncate=False)
            retention.groupBy("cohort_month").pivot("month_index").agg(F.first("retention_pct")).orderBy("cohort_month").show(truncate=False)
            '''
        ),
        md(
            """
            ### E6. RFM segmentation

            RFM uses recency, frequency, and monetary value. The scores are relative to this
            dataset, not universal thresholds. In production, thresholds should come from business
            policy and be reviewed over time.

            `ntile(4)` creates four roughly equal groups. Recency is inverted so a more recent
            customer receives a higher score.
            """
        ),
        code(
            '''
            snapshot_date = completed.agg(F.max("order_date")).first()[0]
            rfm = (
                completed
                .filter(F.col("customer_id").isNotNull())
                .groupBy("customer_id")
                .agg(
                    F.datediff(F.lit(snapshot_date), F.max("order_date")).alias("recency_days"),
                    F.countDistinct("order_id").alias("frequency"),
                    F.round(F.sum("net_revenue"), 2).alias("monetary"),
                )
                .withColumn("r_score", 5 - F.ntile(4).over(Window.orderBy("recency_days")))
                .withColumn("f_score", 5 - F.ntile(4).over(Window.orderBy(F.desc("frequency"))))
                .withColumn("m_score", 5 - F.ntile(4).over(Window.orderBy(F.desc("monetary"))))
            )

            rfm = rfm.withColumn(
                "segment",
                F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "Champions")
                .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "Loyal")
                .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), "At risk")
                .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), "New / promising")
                .otherwise("Needs attention"),
            )

            rfm.groupBy("segment").agg(
                F.count("*").alias("customers"),
                F.round(F.avg("recency_days"), 1).alias("avg_recency_days"),
                F.round(F.avg("frequency"), 2).alias("avg_frequency"),
                F.round(F.sum("monetary"), 2).alias("total_monetary"),
            ).orderBy(F.desc("total_monetary")).show(truncate=False)
            rfm.orderBy(F.desc("monetary")).show(10, truncate=False)
            '''
        ),
        md(
            """
            ### E7. Order outcomes by category and channel

            Revenue alone hides operational friction. A category with strong revenue but a high
            return rate may destroy margin after reverse logistics and markdowns. This mart uses
            all order statuses, not only completed orders.
            """
        ),
        code(
            '''
            outcome_by_category = (
                sales
                .groupBy("category")
                .agg(
                    F.count("*").alias("orders"),
                    F.sum(F.when(F.col("status") == "Completed", 1).otherwise(0)).alias("completed"),
                    F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled"),
                    F.sum(F.when(F.col("status") == "Returned", 1).otherwise(0)).alias("returned"),
                )
                .withColumn("cancel_rate_pct", F.round(F.col("cancelled") / F.col("orders") * 100, 2))
                .withColumn("return_rate_pct", F.round(F.col("returned") / F.col("orders") * 100, 2))
                .orderBy(F.desc("return_rate_pct"))
            )
            outcome_by_category.show(truncate=False)

            outcome_by_channel = (
                sales
                .groupBy("channel")
                .agg(
                    F.count("*").alias("orders"),
                    F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
                    F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled"),
                    F.sum(F.when(F.col("status") == "Returned", 1).otherwise(0)).alias("returned"),
                )
                .withColumn("cancel_rate_pct", F.round(F.col("cancelled") / F.col("orders") * 100, 2))
                .withColumn("return_rate_pct", F.round(F.col("returned") / F.col("orders") * 100, 2))
                .orderBy(F.desc("net_revenue"))
            )
            outcome_by_channel.show(truncate=False)
            '''
        ),
        md(
            """
            ### E8. Write the curated mart

            The mart is partitioned by month because most downstream queries filter by time. It is
            written in Parquet, which supports column pruning and predicate pushdown. `overwrite` is
            acceptable for a lab; a production pipeline needs a deliberate idempotency and
            backfill strategy (see section H).
            """
        ),
        code(
            '''
            mart_path = "/opt/spark-data/case_studies/curated/ecommerce_sales"
            (
                sales.select(
                    "order_id", "customer_id", "product_id", "category", "segment",
                    "loyalty_tier", "region", "order_month", "channel", "status",
                    "quantity", "net_revenue", "margin",
                )
                .write.mode("overwrite")
                .partitionBy("order_month")
                .parquet(mart_path)
            )
            print(f"wrote {mart_path}")
            print(f"rows read back: {spark.read.parquet(mart_path).count():,}")
            '''
        ),
        md(
            """
            ## F. Adjacent Real-World Use Cases

            ### Use case 1: Subscription commerce retention

            - **Industry**: Subscription e-commerce / SaaS.
            - **Scenario**: A monthly box or software plan needs cohort retention by signup month.
            - **Problem**: Revenue is recognized over time, but churn is measured on a different
              calendar; finance and growth disagree.
            - **Why this pattern**: The same first-month cohort and month-index logic works; replace
              order revenue with recurring revenue and add a cancellation event.
            - **Implementation idea**: Build a subscription fact, derive first active month, and
              compute logo and revenue retention separately.
            - **Expected result**: A retention matrix that finance and growth can both audit.
            - **Alternative**: A warehouse with dbt models and a BI semantic layer.
            - **Trade-offs**: Spark handles large event history; the warehouse is often better for
              low-latency, governed BI.

            ### Use case 2: Marketplace seller scorecards

            - **Industry**: Online marketplace.
            - **Scenario**: Each seller needs revenue, return rate, and product ranking.
            - **Problem**: Running a full-history query per seller is expensive and inconsistent.
            - **Why this pattern**: Pre-aggregate at seller/product/month grain and use window
              ranking within seller.
            - **Implementation idea**: Partition the mart by month and seller region; publish a
              seller-level scorecard with explicit minimum-volume rules.
            - **Expected result**: Seller dashboards that share one definition of net revenue.
            - **Alternative**: A per-tenant application database or search index.
            - **Trade-offs**: Pre-aggregation is fast but less flexible for ad-hoc questions.

            ### Use case 3: Retail finance reconciliation

            - **Industry**: Retail finance.
            - **Scenario**: Finance needs to reconcile the data mart to the ERP and payment
              processor.
            - **Problem**: Discounts, cancellations, and returns are applied at different stages.
            - **Why this pattern**: A single order-grain fact with explicit revenue definitions
              makes every adjustment traceable.
            - **Implementation idea**: Add payment and refund facts, reconcile by order and day, and
              publish an exception table.
            - **Expected result**: A daily reconciliation report with an owned break list.
            - **Alternative**: A finance-owned warehouse mart with strict controls.
            - **Trade-offs**: Finance may require stronger lineage and access controls than a data
              lake mart provides.
            """
        ),
        md(
            """
            ## G. Business Value

            - **Time**: A shared mart removes repeated CSV cleaning and spreadsheet reconciliation.
            - **Cost**: Broadcast joins and a single cached enrichment reduce repeated work; the
              actual saving must be measured, not assumed.
            - **Quality**: One definition of net revenue, margin, and completed order reduces
              conflicting reports.
            - **Decision-making**: Merchandising can compare profit, not only revenue; marketing can
              target cohorts rather than the whole base.
            - **Reliability**: Explicit quality checks and a documented grain make failures easier
              to detect.
            - **Scale**: The same pattern can process multiple years and additional channels without
              changing the business definitions.

            > **Hypothetical illustration**: If a team spends two analyst-days per month
            > reconciling spreadsheets, a trusted mart could recover roughly one day per analyst per
            > month. That is an illustration, not a measurement from this lab.
            """
        ),
        md(
            """
            ## H. Production Engineering

            ### Operational contract

            | Dimension | Recommendation for this case study |
            |---|---|
            | **Freshness** | Daily batch after the order system's cut-off; publish the cut-off time with the data |
            | **Grain** | One row per order line; state it in the table comment and tests |
            | **Idempotency** | Write to a staging path, validate, then atomically replace the target partition |
            | **Quality** | Block on duplicate order IDs, null product IDs, negative quantity, and unknown status |
            | **Schema** | Explicit schemas; fail on unexpected columns instead of silently inferring |
            | **Partitioning** | Month for this volume; avoid over-partitioning by day and customer |
            | **Monitoring** | Row counts, revenue totals, null rates, job duration, shuffle bytes, spill |
            | **Security** | Treat email and customer ID as personal data; restrict raw access and mask exports |
            | **Backfills** | Parameterize the date range; make reruns deterministic and partition-scoped |

            ### Failure modes to plan for

            1. **Duplicate orders** after a source retry. Deduplicate by order ID with a deterministic
               tie-breaker before aggregating.
            2. **Late-arriving orders** after the finance close. Define a restatement window and
               publish revised totals rather than silently changing history.
            3. **Dimension drift** when a product changes category. Use a slowly changing dimension
               or preserve the category as of the order date.
            4. **Small files** from daily overwrites. Compact partitions and target a sensible file
               size instead of one file per task.
            5. **Metric drift** when a new team defines revenue differently. Publish a semantic
               layer or a metric dictionary and test the definitions.

            ### Investigation checklist

            - [ ] Open the Spark UI SQL tab and confirm the broadcast joins.
            - [ ] Compare shuffle read/write bytes before and after removing the broadcast hints.
            - [ ] Check whether the cache actually helped; compare runtime with and without it.
            - [ ] Verify that the Parquet mart has the expected month partitions.
            - [ ] Reconcile the mart total to the source total for completed orders.
            - [ ] Inspect task duration skew; product 1 is intentionally hot.
            """
        ),
        md(
            """
            ## I. Trade-offs and Alternatives

            | Choice | Advantage | Limitation | When to prefer it |
            |---|---|---|---|
            | **Spark + Parquet mart** | Scales beyond one machine; handles joins and windows; shared cluster | Operational overhead; small-data latency | Multi-year history, multiple sources, shared pipelines |
            | **DuckDB / pandas** | Simple, fast for local data, easy to debug | Single-machine memory and concurrency limits | One-off analysis or data under a few GB |
            | **Warehouse (BigQuery/Snowflake/Redshift)** | Governed SQL, concurrency, BI integration | Cost model and ingestion complexity | BI-heavy, governed, concurrent workloads |
            | **Broadcast join** | Avoids fact-table shuffle for small dimensions | Must fit in executor memory; can fail on large dimensions | Dimensions are small and stable |
            | **Sort-merge join** | Robust for large dimensions | Shuffles both sides | Both sides are large |
            | **Cache enriched sales** | Reuses joins across five analyses | Memory/disk cost; stale if source changes | Multiple actions reuse the same lineage |
            | **Pre-aggregated mart** | Fast dashboards | Less flexible for new questions | Stable, repeated metrics |

            **Do not cache by reflex.** A single action does not benefit from a cache; the cost of
            materializing the data can exceed the recomputation. Measure the plan and the runtime.
            """
        ),
        md(
            """
            ## J. Experiments and Investigation

            ### Experiment: does the broadcast hint change the plan?

            Run the cell below. The first plan has no hint; the second forces a broadcast of the
            300-row product dimension. Look for `BroadcastHashJoin` versus `SortMergeJoin` in the
            physical plan.
            """
        ),
        code(
            '''
            print("Plan without a hint:")
            orders.join(products, "product_id").explain("simple")

            print("Plan with an explicit broadcast hint:")
            orders.join(F.broadcast(products), "product_id").explain("simple")
            '''
        ),
        md(
            """
            ### Observation guide

            - A broadcast join should show a `BroadcastExchange` and `BroadcastHashJoin`.
            - If the dimension is too large, Spark may ignore the hint or the executor may fail with
              an out-of-memory error. The threshold is a safety limit, not a performance guarantee.
            - Check the SQL tab for shuffle bytes. Removing a fact-side shuffle is often more
              valuable than a small reduction in CPU.
            - Compare the runtime of the full pipeline with and without `sales.persist()`. A cache
              is justified only when the same expensive lineage is reused.

            ### Questions to answer with evidence

            1. Which stage has the largest shuffle write?
            2. Which join is broadcast, and how large is the broadcast relation?
            3. Does the cohort query shuffle more than the monthly aggregation? Why?
            4. How many output files does the mart have per month?
            """
        ),
        md(
            """
            ## K. Exercises

            ### Exercise 1 (basic): channel revenue share

            Compute net revenue and percentage share by channel for completed orders. Use a window
            or a cross join to the total; do not hard-code the total.
            """
        ),
        code("# Your solution for Exercise 1 here\n"),
        md(
            """
            ### Exercise 2 (practical): regional category mix

            Pivot completed net revenue by region and category. Decide whether missing combinations
            should be zero or null, and state why.
            """
        ),
        code("# Your solution for Exercise 2 here\n"),
        md(
            """
            ### Exercise 3 (real-world): repeat purchase rate

            Calculate the share of customers with more than one completed order. Then calculate the
            same metric for each acquisition cohort. Explain why the cohort view is more actionable.
            """
        ),
        code("# Your solution for Exercise 3 here\n"),
        md(
            """
            ### Exercise 4 (debugging): the revenue that doubled

            A new analyst joins `orders` to a product dimension that contains two rows for one
            product because of a failed merge. Revenue doubles. Diagnose the grain, propose a test,
            and fix the dimension.

            **Hints**

            1. Count rows per product key before and after the join.
            2. Check whether the dimension key is unique.
            3. Aggregate or deduplicate the dimension before joining.
            """
        ),
        md(
            """
            ## Reference Solutions

            <details>
            <summary><b>Open after attempting the exercises</b></summary>

            #### Solution 1

            ```python
            channel = completed.groupBy("channel").agg(F.round(F.sum("net_revenue"), 2).alias("revenue"))
            total = channel.agg(F.sum("revenue")).first()[0]
            channel.withColumn("share_pct", F.round(F.col("revenue") / total * 100, 2)).orderBy(F.desc("revenue")).show()
            ```

            #### Solution 2

            ```python
            completed.groupBy("region").pivot("category").agg(F.round(F.sum("net_revenue"), 2)).show()
            ```

            #### Solution 3

            ```python
            per_customer = completed.filter(F.col("customer_id").isNotNull()).groupBy("customer_id").agg(
                F.countDistinct("order_id").alias("orders")
            )
            repeat = per_customer.filter(F.col("orders") > 1).count() / per_customer.count() * 100
            print(f"repeat purchase rate: {repeat:.2f}%")

            cohort_repeat = (
                completed.filter(F.col("customer_id").isNotNull())
                .join(customer_first, "customer_id")
                .groupBy("cohort_month", "customer_id")
                .agg(F.countDistinct("order_id").alias("orders"))
                .groupBy("cohort_month")
                .agg(
                    F.count("*").alias("customers"),
                    F.sum(F.when(F.col("orders") > 1, 1).otherwise(0)).alias("repeat_customers"),
                )
                .withColumn("repeat_rate_pct", F.round(F.col("repeat_customers") / F.col("customers") * 100, 2))
            )
            cohort_repeat.orderBy("cohort_month").show()
            ```

            #### Solution 4

            ```python
            # Check the dimension key first.
            products.groupBy("product_id").count().filter("count > 1").show()

            # Deduplicate with a deterministic rule, then join.
            product_dim_unique = products.dropDuplicates(["product_id"])
            orders.join(product_dim_unique, "product_id").count()
            ```

            </details>
            """
        ),
        md(
            """
            ## L. Case Study Summary

            ### What you built

            A clean order fact, a monthly revenue mart, category and product performance, cohort
            retention, RFM segments, order-outcome rates, and a partitioned Parquet output.

            ### Key engineering lessons

            - Define the grain and the metric before writing the query.
            - Explicit schemas are a contract, not ceremony.
            - Broadcast joins are appropriate for small dimensions; verify the plan.
            - Cache only when the same expensive lineage is reused.
            - A successful job is not proof of correctness; reconcile totals and test invariants.
            - Spark is not automatically the right tool for a small dataset. Justify the platform.

            ### Interview questions

            **Q: Why can a broadcast join be faster than a sort-merge join?**  
            A: It ships a small dimension to each executor and avoids shuffling the large fact table.
            The trade-off is memory pressure on the executors and a size limit on the broadcast side.

            **Q: How would you make this pipeline idempotent?**  
            A: Write to a staging location, validate row counts and totals, then replace only the
            target partitions in a transaction-like publish step. Rerunning the same input range
            should produce the same output without duplicates.

            **Q: Why is cohort retention more actionable than a single repeat-purchase rate?**  
            A: It separates acquisition quality from mature-customer behavior and shows whether
            retention is improving or deteriorating over time.

            ### Next steps

            Continue with [the telecom case study](02_telecom_network_analytics.ipynb), then compare
            how its event-grain model and percentile metrics differ from this order-grain model.
            """
        ),
        code(
            '''
            sales.unpersist()
            spark.stop()
            print("SparkSession stopped.")
            '''
        ),
    ]
