# Datasets

The `./data` directory is mounted into every cluster container at `/opt/spark-data/`.
All datasets are synthetic and deterministic. They are intended for teaching and local
experimentation, not for benchmarking production systems.

## Inventory

| Dataset path | Format | Records / size | Description |
| :--- | :--- | :--- | :--- |
| `/opt/spark-data/retail/orders.csv` | CSV | 60,000 rows (3.7 MB) | E-commerce orders: `order_id`, `customer_id`, `product_id`, `order_ts`, `quantity`, `unit_price`, `discount`, `status`, `channel` |
| `/opt/spark-data/retail/customers.csv` | CSV | 8,000 rows (652 KB) | Customer demographics: `customer_id`, `first_name`, `last_name`, `email`, `country`, `region`, `segment`, `loyalty_tier` |
| `/opt/spark-data/retail/products.csv` | CSV | 300 rows (20 KB) | Catalog items: `product_id`, `product_name`, `category`, `list_price`, `cost`, `launch_date`, `active` |
| `/opt/spark-data/retail/order_events.json` | JSON | 3,000 documents (1.2 MB) | Nested order events with `StructType` and `ArrayType` fields for complex JSON labs |
| `/opt/spark-data/quality/orders_dirty.csv` | CSV | 5,150 rows (320 KB) | Intentionally malformed records: null IDs, negative prices, future dates, and invalid categoricals |
| `/opt/spark-data/logs/access.log` | Text | 20,000 lines (2.7 MB) | Apache web server access logs for regex parsing and streaming forensics |
| `/opt/spark-data/sales.csv` | CSV | 48 rows (4 KB) | Compact order dataset for quick tests and demos |
| `/opt/spark-data/words.txt` | Text | 13 lines (4 KB) | Text file for distributed word-count examples |
| `/opt/spark-data/streaming/events/` | Text / JSON | Dynamic | File-stream landing directory for Structured Streaming ingestion |
| `/opt/spark-data/case_studies/telecom/` | CSV | 36,000 events, 8 towers (3.9 MB) | One week of synthetic voice, SMS, and data events across eight towers |
| `/opt/spark-data/case_studies/fraud/` | CSV | 30,000 transactions, 12 merchants (3.3 MB) | Synthetic card transactions, including 600 labelled frauds for teaching precision and recall |
| `/opt/spark-data/case_studies/logistics/` | CSV | 20,000 shipments, 10 hubs (2.8 MB) | Two months of synthetic shipments across four carriers |

Row counts exclude CSV headers.

## Regenerating data

Both generators are deterministic and write the same files on every run.

```bash
python3 data/generate_datasets.py
python3 data/generate_case_studies.py
```

Generated case-study outputs under `data/case_studies/curated/` are ignored by Git. Raw
inputs and the generators are tracked.

## Related documentation

- [Case studies](CASE_STUDIES.md): schemas, business context, and expected results.
- [Running jobs](02-running-jobs.md): reading and writing data with `spark-submit` and
  Spark SQL.
- [Quickstart](01-quickstart.md): verify the stack and run the sample jobs.
