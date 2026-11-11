#!/usr/bin/env python3
"""Cell content for the financial fraud-pattern case study."""

from __future__ import annotations

from case_study_common import code, md, spark_setup


def cells() -> list[dict]:
    return [
        md(
            """
            # Case Study 3: Financial Transaction Fraud Patterns

            **Module**: 14_business_case_studies  
            **Notebook**: 03_financial_fraud_patterns.ipynb  
            **Environment**: Apache Spark 4.2.1 · PySpark · Python 3.13 · Java 21  
            **Data**: `/opt/spark-data/case_studies/fraud/` (synthetic, deterministic)  
            **Estimated time**: 3–4 hours

            [Case-study index](../../docs/CASE_STUDIES.md) · [Use-case index](../../docs/USE_CASES.md) · [Business context](../../docs/BUSINESS_CONTEXT.md) · [Previous](02_telecom_network_analytics.ipynb) · [Next](04_logistics_delivery_analytics.ipynb)

            > **Educational disclaimer**: this is a rule-based analytics exercise on synthetic data.
            > It is **not** a production fraud-detection system, a model-validation study, or financial
            > advice. The synthetic labels exist only so the lab can demonstrate precision, recall,
            > and the cost of false positives. Real fraud systems require governance, model risk
            > management, privacy controls, investigator feedback, and regulatory review.
            """
        ),
        md(
            """
            ## Learning Contract

            | Attribute | Value |
            |---|---|
            | **Priority** | Advanced (analytics pattern); Optional (production fraud systems) |
            | **Estimated effort** | 3–4 hours |
            | **Prerequisites** | Window functions, event time, joins, aggregations, basic classification metrics |
            | **Required depth** | Can build explainable rules, evaluate them honestly, and explain why labels are delayed |
            | **Data Engineer** | Build transaction features and an auditable alert dataset |
            | **Senior Data Engineer** | Own feature freshness, backtesting, drift monitoring, and label governance |
            | **Spark specialist** | Reason about event-time windows, state, skew, and streaming trade-offs |

            > **Do not use this notebook to make real fraud decisions.** A high recall on synthetic
            > data says nothing about production performance.
            """
        ),
        md(
            """
            ## A. Learning Objectives

            By the end of this case study you will be able to:

            1. Model a transaction fact and a merchant dimension at a clear grain.
            2. Compute event-time velocity features with a range window.
            3. Express fraud rules as explainable Spark columns.
            4. Evaluate precision, recall, false positives, and missed fraud against synthetic labels.
            5. Explain the trade-off between analyst capacity and loss prevention.
            6. Identify why production fraud analytics is harder than a batch rule score.
            """
        ),
        md(
            """
            ## B. Business Context

            ### The company

            SecurePay is a hypothetical payment processor. The lab contains 30,000 synthetic card
            transactions over 30 days. Exactly 600 transactions carry a synthetic fraud label and a
            pattern name: card testing, account takeover, or unusual geography.

            ### The problem before this pipeline

            - The risk team receives a flat transaction feed and writes one-off SQL for each
              investigation.
            - Rules are inconsistent across analysts and cannot be backtested.
            - A simple "large amount" rule creates too many false positives for the review queue.
            - Nobody knows whether a rule catches a pattern or just a merchant category.
            - Labels arrive weeks later after chargebacks, so a rule that looks good today may be
              wrong tomorrow.

            ### Decisions this pipeline must support

            | Decision | Question | Output |
            |---|---|---|
            | Review queue | Which transactions should an analyst inspect first? | Ranked alerts with reasons |
            | Rule tuning | Which threshold balances loss prevention and analyst load? | Precision/recall and alert volume |
            | Pattern coverage | Which fraud patterns are missed? | Recall by pattern |
            | Merchant risk | Are alerts concentrated in a few merchants? | Merchant/category breakdown |
            | Governance | Can we explain why a transaction was flagged? | Rule flags and score |

            ### Why Spark here?

            Velocity features need a window over each card's transaction history. At production scale,
            that is a large event-time computation across many cards and days. Spark is a reasonable
            batch/backtesting engine. A real-time blocking system would likely use a feature store and
            a low-latency rules engine, with Spark computing historical features and labels.
            """
        ),
        md(
            """
            ## C. Data Model and Core Concepts

            ### Grain

            One row in `transactions` is one authorization attempt. It is not one card, one customer,
            or one fraud case. A fraud case may contain many transactions; a card may have many
            legitimate transactions.

            ### Schema

            | Table | Key | Important fields | Notes |
            |---|---|---|---|
            | `transactions` | `transaction_id` | card, customer, merchant, amount, channel, country, IP country, device, event time, synthetic label | Event fact |
            | `merchants` | `merchant_id` | name, category, risk tier, country | Small dimension |

            ### Rule features

            | Feature | Definition | Why it can help | Failure mode |
            |---|---|---|---|
            | **Card testing** | Amount ≤ 5 and online | Fraudsters test stolen cards with tiny charges | Many legitimate micro-payments |
            | **Geo mismatch** | Transaction country ≠ IP country | Account takeover or stolen card | Travel, VPNs, corporate proxies |
            | **High amount + foreign** | Amount ≥ 500 and country mismatch | Higher loss per event | Legitimate international purchases |
            | **High amount online** | Amount ≥ 700 and online | Account takeover with a large basket | Legitimate large online purchases |
            | **Odd-hour high amount** | 00:00–05:59 and amount ≥ 300 | Unusual behavior | Shift workers, time zones |
            | **Velocity** | ≥ 5 transactions on one card in 10 minutes | Card testing or burst abuse | Legitimate repeat purchases |

            ### Metric definitions

            - **Precision** = true positives / all alerts. High precision means the review queue is
              efficient.
            - **Recall** = true positives / all labelled fraud. High recall means fewer missed cases.
            - **False positive** = a legitimate transaction that an analyst must review.
            - **Alert volume** = number of transactions above the score threshold.

            The label is synthetic and complete. In production, labels are delayed, biased, and
            incomplete. That is the central limitation of this exercise.
            """
        ),
        md(
            """
            ## D. Architecture and Data Flow

            ```text
            transactions                merchants
                 |                         |
                 v                         v
             quality gate             broadcast join
                 |                         |
                 +------------+------------+
                              |
                              v
                    cleaned transaction fact
                              |
                              v
                 event-time velocity window (10 min)
                              |
                              v
                 explainable rule flags + score
                              |
                 +------------+-------------+
                 |                          |
                 v                          v
            threshold sweep            alert dataset
            (precision/recall)         (analyst review)
            ```

            ### What happens inside Spark

            - The 10-minute velocity window partitions by card and orders by event timestamp. Spark
              shuffles by card and sorts within each card, so a high-volume card can become a
              straggler.
            - The merchant dimension is small, so a broadcast join avoids shuffling transactions.
            - The rule flags are narrow projections. The score is a sum of integer flags.
            - The threshold sweep can be computed from one grouped count by score, not one full scan
              per threshold.
            - Writing alerts by date supports incremental review and backfills. Never overwrite the
              entire alert history without an audit trail.
            """
        ),
        md(
            """
            ## E. Practical Implementation

            **Objective**: build explainable fraud-pattern rules, evaluate them against synthetic
            labels, and produce a ranked alert dataset.  
            **Input**: `/opt/spark-data/case_studies/fraud/{transactions,merchants}.csv`.  
            **Expected output characteristics**: 30,000 transactions; 600 synthetic frauds; three
            labelled patterns; an alert dataset with rule flags and a score.
            """
        ),
        code(spark_setup("CaseStudy_FraudPatterns")),
        md(
            """
            ### E1. Load the transaction fact and merchant dimension
            """
        ),
        code(
            '''
            transactions_schema = StructType([
                StructField("transaction_id", StringType(), False),
                StructField("customer_id", StringType(), True),
                StructField("card_id", StringType(), True),
                StructField("merchant_id", StringType(), True),
                StructField("merchant_category", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("country", StringType(), True),
                StructField("ip_country", StringType(), True),
                StructField("device_id", StringType(), True),
                StructField("event_ts", StringType(), True),
                StructField("is_fraud", IntegerType(), True),
                StructField("fraud_pattern", StringType(), True),
            ])
            merchants_schema = StructType([
                StructField("merchant_id", StringType(), False),
                StructField("merchant_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("risk_tier", StringType(), True),
                StructField("country", StringType(), True),
            ])

            transactions_raw = (
                spark.read.option("header", True).schema(transactions_schema)
                .csv("/opt/spark-data/case_studies/fraud/transactions.csv")
            )
            merchants_raw = (
                spark.read.option("header", True).schema(merchants_schema)
                .csv("/opt/spark-data/case_studies/fraud/merchants.csv")
            )
            print(f"transactions: {transactions_raw.count():,} | merchants: {merchants_raw.count()}")
            '''
        ),
        md(
            """
            ### E2. Quality gate

            A production fraud pipeline needs stronger checks than this. At minimum, it must detect
            duplicate authorizations, impossible amounts, missing event time, and label leakage.
            """
        ),
        code(
            '''
            transactions_raw.agg(
                F.count("*").alias("rows"),
                F.countDistinct("transaction_id").alias("distinct_transaction_ids"),
                F.sum(F.when(F.col("amount").isNull() | (F.col("amount") <= 0), 1).otherwise(0)).alias("invalid_amount"),
                F.sum(F.when(F.col("event_ts").isNull(), 1).otherwise(0)).alias("missing_event_ts"),
                F.sum(F.when(~F.col("is_fraud").isin(0, 1), 1).otherwise(0)).alias("invalid_label"),
                F.sum("is_fraud").alias("labelled_frauds"),
            ).show(truncate=False)
            '''
        ),
        code(
            '''
            merchant_dim = merchants_raw.select(
                "merchant_id",
                "merchant_name",
                F.col("category").alias("merchant_category_dim"),
                "risk_tier",
                F.col("country").alias("merchant_country"),
            )

            tx = (
                transactions_raw
                .filter(F.col("transaction_id").isNotNull() & F.col("event_ts").isNotNull())
                .filter(F.col("amount") > 0)
                .withColumn("event_ts", F.to_timestamp("event_ts"))
                .withColumn("event_hour", F.hour("event_ts"))
                .withColumn("event_date", F.to_date("event_ts"))
                .withColumn("is_weekend", F.dayofweek("event_ts").isin(1, 7).cast("int"))
                .join(F.broadcast(merchant_dim), "merchant_id", "left")
            )
            tx.persist(StorageLevel.MEMORY_AND_DISK)
            print(f"clean transactions: {tx.count():,}")
            tx.select(
                F.sum(F.when(F.col("merchant_name").isNull(), 1).otherwise(0)).alias("unmapped_merchant"),
                F.sum(F.when(F.col("country") != F.col("ip_country"), 1).otherwise(0)).alias("geo_mismatch_rows"),
                F.sum("is_fraud").alias("synthetic_frauds"),
            ).show(truncate=False)
            '''
        ),
        md(
            """
            ### E3. Exploratory view of the labelled patterns

            This is a luxury that production teams rarely have: a complete, immediate label. Use it
            to understand the patterns, not to claim real-world performance.
            """
        ),
        code(
            '''
            tx.filter(F.col("is_fraud") == 1).groupBy("fraud_pattern").agg(
                F.count("*").alias("transactions"),
                F.round(F.avg("amount"), 2).alias("avg_amount"),
                F.round(F.min("amount"), 2).alias("min_amount"),
                F.round(F.max("amount"), 2).alias("max_amount"),
                F.countDistinct("card_id").alias("cards"),
            ).orderBy("fraud_pattern").show(truncate=False)

            tx.withColumn(
                "amount_band",
                F.when(F.col("amount") < 10, "< 10")
                .when(F.col("amount") < 100, "10–99")
                .when(F.col("amount") < 500, "100–499")
                .when(F.col("amount") < 1500, "500–1499")
                .otherwise("1500+"),
            ).groupBy("amount_band").agg(
                F.count("*").alias("transactions"),
                F.sum("is_fraud").alias("frauds"),
                F.round(F.sum("is_fraud") / F.count("*") * 100, 2).alias("fraud_rate_pct"),
            ).orderBy("amount_band").show(truncate=False)
            '''
        ),
        md(
            """
            ### E4. Event-time velocity

            The range window looks back 600 seconds on each card's timeline. It counts the current
            transaction and any prior transaction in that interval. This is a simple feature; a
            production feature store would also handle late events, card reissues, and merchant
            bursts.
            """
        ),
        code(
            '''
            velocity_window = (
                Window.partitionBy("card_id")
                .orderBy(F.col("event_ts").cast("long"))
                .rangeBetween(-600, 0)
            )
            scored = (
                tx
                .withColumn("transactions_10m", F.count("*").over(velocity_window))
                .withColumn("rule_card_testing", (F.col("amount") <= 5) & (F.col("channel") == "online"))
                .withColumn("rule_geo_mismatch", F.col("country") != F.col("ip_country"))
                .withColumn(
                    "rule_high_amount_foreign",
                    (F.col("amount") >= 500) & (F.col("country") != F.col("ip_country")),
                )
                .withColumn(
                    "rule_high_amount_online",
                    (F.col("amount") >= 700) & (F.col("channel") == "online"),
                )
                .withColumn(
                    "rule_odd_hour_high",
                    F.col("event_hour").between(0, 5) & (F.col("amount") >= 300),
                )
                .withColumn("rule_velocity", F.col("transactions_10m") >= 5)
                .withColumn(
                    "risk_score",
                    F.col("rule_card_testing").cast("int")
                    + F.col("rule_geo_mismatch").cast("int")
                    + F.col("rule_high_amount_foreign").cast("int")
                    + F.col("rule_high_amount_online").cast("int")
                    + F.col("rule_odd_hour_high").cast("int")
                    + F.col("rule_velocity").cast("int"),
                )
            )
            scored.persist(StorageLevel.MEMORY_AND_DISK)
            print(f"scored transactions: {scored.count():,}")
            scored.groupBy("risk_score").agg(
                F.count("*").alias("transactions"),
                F.sum("is_fraud").alias("synthetic_frauds"),
            ).orderBy("risk_score").show(truncate=False)
            '''
        ),
        md(
            """
            ### E5. Evaluate the rules at a review threshold

            A score of 2 or more is the teaching threshold. The confusion matrix is computed against
            the synthetic labels. Do not present these numbers as production performance.
            """
        ),
        code(
            '''
            threshold = 2
            alerts = scored.filter(F.col("risk_score") >= threshold)

            tp = alerts.filter(F.col("is_fraud") == 1).count()
            fp = alerts.filter(F.col("is_fraud") == 0).count()
            fn = scored.filter((F.col("risk_score") < threshold) & (F.col("is_fraud") == 1)).count()
            tn = scored.filter((F.col("risk_score") < threshold) & (F.col("is_fraud") == 0)).count()

            precision = tp / (tp + fp) if (tp + fp) else 0.0
            recall = tp / (tp + fn) if (tp + fn) else 0.0
            print(f"threshold={threshold} | alerts={tp + fp:,} | TP={tp:,} | FP={fp:,} | FN={fn:,} | TN={tn:,}")
            print(f"precision={precision:.3f} | recall={recall:.3f}")
            print("Confusion matrix (rows=actual, cols=predicted):")
            print(f"  actual fraud:     predicted alert={tp:,}  predicted clear={fn:,}")
            print(f"  actual legitimate: predicted alert={fp:,}  predicted clear={tn:,}")
            '''
        ),
        md(
            """
            ### E6. Which rules contribute?

            One pass over the scored table computes flag counts and true positives per rule. This is
            more efficient than one full scan per rule and keeps the evaluation reproducible.
            """
        ),
        code(
            '''
            rule_names = [
                "rule_card_testing", "rule_geo_mismatch", "rule_high_amount_foreign",
                "rule_high_amount_online", "rule_odd_hour_high", "rule_velocity",
            ]
            aggregates = []
            for rule in rule_names:
                aggregates.append(F.sum(F.when(F.col(rule), 1).otherwise(0)).alias(f"{rule}_flagged"))
                aggregates.append(F.sum(F.when(F.col(rule) & (F.col("is_fraud") == 1), 1).otherwise(0)).alias(f"{rule}_tp"))
            metrics = scored.agg(*aggregates).first()

            for rule in rule_names:
                flagged = metrics[f"{rule}_flagged"]
                true_positives = metrics[f"{rule}_tp"]
                rule_precision = true_positives / flagged if flagged else 0.0
                print(f"{rule:26s} flagged={flagged:6,d}  tp={true_positives:4,d}  precision={rule_precision:.3f}")
            '''
        ),
        md(
            """
            ### E7. Pattern coverage and merchant concentration

            Aggregate metrics can hide a rule that catches one pattern and misses another. Always
            inspect recall by pattern and the merchants that dominate the queue.
            """
        ),
        code(
            '''
            pattern_coverage = (
                scored.filter(F.col("is_fraud") == 1)
                .groupBy("fraud_pattern")
                .agg(
                    F.count("*").alias("frauds"),
                    F.sum(F.when(F.col("risk_score") >= threshold, 1).otherwise(0)).alias("caught"),
                )
                .withColumn("recall_pct", F.round(F.col("caught") / F.col("frauds") * 100, 2))
                .orderBy("fraud_pattern")
            )
            pattern_coverage.show(truncate=False)

            merchant_concentration = (
                alerts
                .groupBy("merchant_name", "merchant_category_dim", "risk_tier")
                .agg(
                    F.count("*").alias("alerts"),
                    F.sum("is_fraud").alias("true_positives"),
                    F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
                )
                .orderBy(F.desc("alerts"))
            )
            merchant_concentration.show(15, truncate=False)
            '''
        ),
        md(
            """
            ### E8. Write the alert and scored datasets

            The alert dataset is the input to a human review workflow. The scored dataset is useful
            for backtesting and threshold analysis. Both should be versioned with the rule version
            and the data cut-off.
            """
        ),
        code(
            '''
            alert_path = "/opt/spark-data/case_studies/curated/fraud_alerts"
            scored_path = "/opt/spark-data/case_studies/curated/fraud_scored"
            (
                alerts.select(
                    "transaction_id", "event_ts", "event_date", "card_id", "customer_id",
                    "merchant_name", "merchant_category_dim", "risk_tier", "amount", "channel",
                    "country", "ip_country", "transactions_10m", "risk_score",
                    "rule_card_testing", "rule_geo_mismatch", "rule_high_amount_foreign",
                    "rule_high_amount_online", "rule_odd_hour_high", "rule_velocity",
                    "is_fraud", "fraud_pattern",
                )
                .write.mode("overwrite")
                .partitionBy("event_date")
                .parquet(alert_path)
            )
            (
                scored.select(
                    "transaction_id", "event_ts", "event_date", "card_id", "amount",
                    "risk_score", "transactions_10m", "is_fraud", "fraud_pattern",
                )
                .write.mode("overwrite")
                .partitionBy("event_date")
                .parquet(scored_path)
            )
            print(f"alerts: {spark.read.parquet(alert_path).count():,}")
            print(f"scored: {spark.read.parquet(scored_path).count():,}")
            '''
        ),
        md(
            """
            ## F. Adjacent Real-World Use Cases

            ### Use case 1: Chargeback triage

            - **Industry**: Card payments.
            - **Scenario**: Disputes arrive weeks after the transaction; analysts need to prioritize.
            - **Problem**: The label arrives late, and the review queue is flooded with low-value
              cases.
            - **Why this pattern**: Velocity and geo features can be computed historically and joined
              to chargeback labels for backtesting.
            - **Implementation idea**: Build a point-in-time feature table, join labels with the
              correct as-of date, and evaluate precision at a fixed analyst capacity.
            - **Expected result**: A ranked queue with expected loss and reason codes.
            - **Alternative**: A vendor fraud-scoring service.
            - **Trade-offs**: Vendor models are faster to deploy but less transparent and may not fit
              niche portfolios.

            ### Use case 2: Anti-money-laundering structuring

            - **Industry**: Banking / AML.
            - **Scenario**: Many small deposits below a reporting threshold may indicate structuring.
            - **Problem**: A single transaction looks normal; the pattern is in the sequence and
              relationships.
            - **Why this pattern**: Event-time windows and graph features over accounts and
              counterparties expose bursts and cycles.
            - **Implementation idea**: Combine transaction windows with entity resolution and a
              graph/network analysis; keep human investigators in the loop.
            - **Expected result**: A case file with linked accounts and explainable evidence.
            - **Alternative**: A dedicated AML platform with graph analytics.
            - **Trade-offs**: Regulatory requirements, false-positive cost, and privacy are dominant;
              Spark is only the data-processing layer.

            ### Use case 3: Merchant onboarding risk

            - **Industry**: Acquiring / marketplace.
            - **Scenario**: A new merchant may be a front for fraud or prohibited goods.
            - **Problem**: Risk must be assessed before activation, with limited history.
            - **Why this pattern**: Join application data, ownership, device, and early transaction
              behavior; use rules and anomaly features.
            - **Implementation idea**: Build a merchant risk feature set, score at onboarding, and
              monitor early chargebacks.
            - **Expected result**: A tiered onboarding decision with manual review for high risk.
            - **Alternative**: A specialist onboarding-risk vendor.
            - **Trade-offs**: Speed-to-onboard versus risk appetite; a false rejection has a direct
              revenue cost.
            """
        ),
        md(
            """
            ## G. Business Value

            - **Time**: Analysts spend time on higher-risk cases instead of manually scanning every
              transaction.
            - **Cost**: A better precision/recall trade-off can reduce losses and review hours, but
              the actual economics require real label and cost data.
            - **Quality**: Explainable rules make decisions auditable and easier to challenge.
            - **Decision-making**: Thresholds become an explicit business choice, not an analyst's
              habit.
            - **Reliability**: Backtesting and pattern coverage expose silent misses.
            - **Scale**: Event-time features can be computed over large histories with Spark.

            > **Hypothetical illustration**: If a review team can inspect 500 alerts per day, the
            > threshold should be chosen to fit that capacity, not to maximize recall in isolation.
            > The numbers are illustrative; the principle is the important part.
            """
        ),
        md(
            """
            ## H. Production Engineering

            ### Operational contract

            | Dimension | Recommendation |
            |---|---|
            | **Latency** | Batch for backtesting; streaming/feature store for blocking decisions |
            | **Labels** | Join labels as-of the decision time; never leak future chargebacks into features |
            | **Point-in-time correctness** | Store feature values with event time and ingestion time |
            | **Explainability** | Persist rule flags, score, rule version, and threshold |
            | **Monitoring** | Alert volume, precision proxy, false-positive rate, drift, missed patterns |
            | **Governance** | Human review, appeals, model/rule risk approval, audit logs |
            | **Privacy** | Tokenize card data; restrict PII; encrypt data at rest and in transit |
            | **Backfills** | Recompute features for a bounded window; keep the rule version immutable |

            ### Failure modes

            1. **Label leakage** from future chargebacks inflates offline performance.
            2. **Population drift** changes the false-positive rate after a merchant or channel mix
               shift.
            3. **Feedback loops** train only on cases the current rules flag, so missed fraud stays
               invisible.
            4. **Velocity window bugs** count across card reissues or ignore time zones.
            5. **Rule sprawl** creates overlapping, undocumented alerts that analysts cannot explain.
            6. **Automated blocking** without human review can cause unacceptable customer harm.

            ### Investigation checklist

            - [ ] Confirm the velocity window is partitioned by card and ordered by event time.
            - [ ] Check that labels are joined as-of the decision time, not the current date.
            - [ ] Inspect alert volume by day and merchant; sudden spikes may be data issues.
            - [ ] Review false positives by rule and channel.
            - [ ] Verify that the alert dataset includes a rule version.
            - [ ] Test the threshold sweep after any rule change.
            """
        ),
        md(
            """
            ## I. Trade-offs and Alternatives

            | Choice | Advantage | Limitation | When to prefer it |
            |---|---|---|---|
            | **Explainable rules** | Auditable, fast to deploy, easy to reason about | Brittle, easy to evade, manual tuning | Early-stage or regulated review queues |
            | **Supervised ML** | Can combine many weak signals | Needs labels, monitoring, and model risk management | Enough labelled history and mature governance |
            | **Graph analytics** | Finds rings and shared entities | Complex entity resolution and privacy | Organized fraud and AML |
            | **Batch Spark** | Historical backtesting at scale | Not suitable for real-time blocking | Feature engineering and investigations |
            | **Streaming rules** | Low latency | State and operational complexity | Card-present blocking |
            | **Vendor score** | Fast integration and benchmark data | Cost, opacity, limited customization | Teams without a fraud data science function |

            **A rule score is not a probability.** It is a prioritization device. Do not describe it
            as a likelihood of fraud without calibration.
            """
        ),
        md(
            """
            ## J. Experiments and Investigation

            ### Experiment: threshold sweep

            The grouped score distribution lets us compute precision, recall, and alert volume for
            every threshold in one pass. Choose a threshold based on review capacity and the relative
            cost of misses versus false positives.
            """
        ),
        code(
            '''
            distribution = (
                scored.groupBy("risk_score")
                .agg(F.count("*").alias("transactions"), F.sum("is_fraud").alias("frauds"))
                .orderBy("risk_score")
                .collect()
            )
            total_frauds = sum(row["frauds"] for row in distribution)
            print(f"total labelled frauds: {total_frauds}")
            print(f"{'threshold':>9} {'alerts':>8} {'tp':>5} {'fp':>6} {'fn':>5} {'precision':>9} {'recall':>7}")
            for threshold_value in range(1, 6):
                selected = [row for row in distribution if row["risk_score"] >= threshold_value]
                alerts_count = sum(row["transactions"] for row in selected)
                true_positives = sum(row["frauds"] for row in selected)
                false_positives = alerts_count - true_positives
                false_negatives = total_frauds - true_positives
                precision = true_positives / alerts_count if alerts_count else 0.0
                recall = true_positives / total_frauds if total_frauds else 0.0
                print(f"{threshold_value:9d} {alerts_count:8,d} {true_positives:5,d} {false_positives:6,d} {false_negatives:5,d} {precision:9.3f} {recall:7.3f}")
            '''
        ),
        md(
            """
            ### Observation guide

            - Raising the threshold usually improves precision and reduces recall. The exact shape
              depends on the rules and the population.
            - A rule with high precision but low recall may be safe but incomplete; a rule with high
              recall but low precision may overwhelm analysts.
            - Inspect the score distribution. If most frauds have score 1, the threshold choice is
              especially consequential.
            - Check whether the card-testing pattern dominates the queue. If so, the aggregate
              precision may hide poor coverage of account takeover.
            - Run the same analysis by pattern and channel before recommending a threshold.

            ### Questions to answer with evidence

            1. Which threshold fits a review capacity of 500 alerts?
            2. Which rule contributes the most true positives?
            3. Which pattern has the lowest recall?
            4. Are alerts concentrated in a few merchants? Is that expected or a data artifact?
            """
        ),
        md(
            """
            ## K. Exercises

            ### Exercise 1 (basic): fraud rate by channel

            Compute transaction count, fraud count, and fraud rate by channel. Sort by fraud rate.
            """
        ),
        code("# Your solution for Exercise 1 here\n"),
        md(
            """
            ### Exercise 2 (practical): add a new-account rule

            Add a rule that flags transactions on cards whose first observed transaction is within
            the last 24 hours. Use a window or a groupBy; explain the false-positive risk.
            """
        ),
        code("# Your solution for Exercise 2 here\n"),
        md(
            """
            ### Exercise 3 (real-world): capacity-constrained threshold

            Suppose the review team can handle 400 alerts per day. Choose a global score threshold
            and report the resulting precision, recall, and daily alert volume. Then propose a
            pattern-specific threshold.
            """
        ),
        code("# Your solution for Exercise 3 here\n"),
        md(
            """
            ### Exercise 4 (debugging): the rule that looked perfect

            An analyst reports 99% precision on a new rule. You discover the rule uses a field that
            is populated after chargeback review. Explain the leak, how to detect it, and how to fix
            the backtest.

            **Hints**

            1. List every input and its availability time.
            2. Join labels as-of the decision timestamp.
            3. Re-run the evaluation with only point-in-time features.
            """
        ),
        md(
            """
            ## Reference Solutions

            <details>
            <summary><b>Open after attempting the exercises</b></summary>

            #### Solution 1

            ```python
            tx.groupBy("channel").agg(
                F.count("*").alias("transactions"),
                F.sum("is_fraud").alias("frauds"),
                F.round(F.sum("is_fraud") / F.count("*") * 100, 2).alias("fraud_rate_pct"),
            ).orderBy(F.desc("fraud_rate_pct")).show()
            ```

            #### Solution 2

            ```python
            first_seen = tx.groupBy("card_id").agg(F.min("event_ts").alias("first_seen_ts"))
            with_age = tx.join(first_seen, "card_id").withColumn(
                "account_age_hours",
                (F.col("event_ts").cast("long") - F.col("first_seen_ts").cast("long")) / 3600,
            )
            with_age.withColumn("rule_new_card", F.col("account_age_hours") <= 24).groupBy("rule_new_card").agg(
                F.count("*").alias("transactions"),
                F.sum("is_fraud").alias("frauds"),
            ).show()
            # False positives: legitimate new customers often transact immediately.
            ```

            #### Solution 3

            ```python
            # Reuse the threshold table from section J, then filter to a daily capacity.
            # A production version should compute alerts per day, not only per threshold.
            # This sketch shows the shape of the decision.
            print("Use the threshold sweep and divide alert volume by the number of days.")
            ```

            #### Solution 4

            ```python
            # The leak is a feature that is only known after the label window.
            # Check feature availability timestamps and join labels with an as-of condition.
            # Rebuild the feature table using only data available at authorization time.
            print("Audit feature availability before evaluating any rule or model.")
            ```

            </details>
            """
        ),
        md(
            """
            ## L. Case Study Summary

            ### What you built

            A clean transaction fact, event-time velocity features, six explainable rules, a risk
            score, precision/recall evaluation, pattern coverage, merchant concentration, and
            curated alert/scored datasets.

            ### Key engineering lessons

            - Fraud labels are delayed and biased; point-in-time correctness is mandatory.
            - Velocity is an event-time window, not a simple daily count.
            - Precision and recall trade off against analyst capacity.
            - Aggregate metrics can hide a pattern that is completely missed.
            - Rules must be explainable and versioned; a score is not a probability.
            - Synthetic data can teach mechanics but cannot validate a production fraud system.

            ### Interview questions

            **Q: Why is a daily transaction count insufficient for card testing?**  
            A: Card testing happens in minutes. A daily count smooths the burst and may not exceed a
            threshold; a range window over event time captures the velocity.

            **Q: How would you prevent label leakage?**  
            A: Define the decision timestamp, join only features available at that time, and join
            labels with an as-of condition that excludes future outcomes. Audit every feature's
            availability.

            **Q: Why can a high-precision rule still be a bad business decision?**  
            A: It may miss most fraud, block legitimate customers, or create an unmanageable review
            queue. Precision alone is not enough; consider recall, alert volume, loss, and customer
            harm.

            ### Next steps

            Continue with [the logistics case study](04_logistics_delivery_analytics.ipynb) to apply
            event-time reasoning to promised-versus-actual delivery performance.
            """
        ),
        code(
            '''
            scored.unpersist()
            tx.unpersist()
            spark.stop()
            print("SparkSession stopped.")
            '''
        ),
    ]
