#!/usr/bin/env python3
"""
Generates remaining Module 03 (DataFrame & Spark SQL) notebooks:
- 03_dataframe_and_spark_sql/02_schemas_and_data_types.ipynb
- 03_dataframe_and_spark_sql/03_dataframe_transformations.ipynb
- 03_dataframe_and_spark_sql/04_spark_sql.ipynb
- 03_dataframe_and_spark_sql/05_joins_and_aggregations.ipynb
- 03_dataframe_and_spark_sql/06_window_functions.ipynb
- 03_dataframe_and_spark_sql/07_nulls_and_data_quality.ipynb
- 03_dataframe_and_spark_sql/08_udfs_and_pandas_udfs.ipynb
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))
from nb_utils import NotebookBuilder

BASE_DIR = "/home/elhussieny/spark-docker/notebooks/03_dataframe_and_spark_sql"

def gen_03_02():
    nb = NotebookBuilder(
        title="Schemas and Data Types: Explicit StructTypes, Nested Types & Schema Enforcement",
        module_name="03_dataframe_and_spark_sql",
        notebook_name="02_schemas_and_data_types.ipynb",
        app_name="Schemas_DataTypes_Lab"
    )
    nb.add_header({
        "what": "How to define explicit StructType schemas, parse complex nested types (ArrayType, MapType, StructType), and enforce data contracts.",
        "why": "Relying on inferSchema causes runtime performance hits and pipeline failures when dirty records arrive.",
        "problem": "InferSchema triggers an extra full dataset scan and silently breaks when data types vary across batch files.",
        "real_world": "Enterprise data ingestion frameworks enforce strict schemas to reject corrupted records into dead-letter queues."
    })
    nb.add_markdown("""## 2. Schema Architecture & Data Types

```
StructType (Schema Definition)
  |-- StructField("order_id", IntegerType, nullable=False)
  |-- StructField("customer_id", IntegerType, nullable=True)
  |-- StructField("items", ArrayType(StructType(...)), nullable=True)  <-- Complex Nested Type
  |-- StructField("metadata", MapType(StringType, StringType))        <-- Key-Value Map Type
```

### Why Explicit Schemas Matter in Production:
1. **Zero Scan Overhead**: Spark skips the pre-scan required by `inferSchema=True`.
2. **Type Safety**: Enforces strict contracts (e.g. `order_id` is never accidentally parsed as String).
3. **Deterministic Evolution**: Allows controlled handling of new or missing columns.
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Practical: Defining Explicit Schemas for Retail Orders""")
    nb.add_code("""from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
)

orders_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("order_ts", TimestampType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("discount", DoubleType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True)
])

# Read with explicit schema enforcement
orders_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv("/opt/spark-data/retail/orders.csv")
)

orders_df.printSchema()
print(f"Total verified orders count: {orders_df.count()}")
orders_df.show(3)
""")
    nb.add_markdown("""## 4. Complex Nested Types: Parsing JSON (`/opt/spark-data/retail/order_events.json`)

Now let us parse deeply nested JSON containing nested StructTypes and ArrayTypes.""")
    nb.add_code("""events_raw = spark.read.text("/opt/spark-data/retail/order_events.json")

# Define nested schema matching JSON structure
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("order", StructType([
        StructField("order_id", IntegerType()),
        StructField("status", StringType()),
        StructField("channel", StringType()),
        StructField("items", ArrayType(StructType([
            StructField("product_id", IntegerType()),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DoubleType())
        ])))
    ])),
    StructField("customer", StructType([
        StructField("customer_id", IntegerType()),
        StructField("segment", StringType()),
        StructField("region", StringType()),
        StructField("country", StringType())
    ]))
])

# Parse JSON strings with explicit schema
events_df = (
    events_raw
    .select(F.from_json(F.col("value"), event_schema).alias("data"))
    .select("data.*")
)

events_df.printSchema()
events_df.select("event_id", "order.order_id", "customer.country").show(3)
""")
    nb.add_experiment({
        "title": "Exploding Nested Arrays into Relational Rows",
        "testing": "Use F.explode() on order.items to flatten nested array elements into individual row records.",
        "expect": "Each item in the items array will become a distinct row retaining parent order attributes.",
        "code": """flattened = (
    events_df
    .select(
        F.col("event_id"),
        F.col("order.order_id").alias("order_id"),
        F.col("customer.customer_id").alias("customer_id"),
        F.explode(F.col("order.items")).alias("item")
    )
    .select(
        "event_id", "order_id", "customer_id",
        "item.product_name", "item.quantity", "item.unit_price"
    )
)

flattened.show(5)
print(f"Original events: {events_df.count()} -> Flattened rows: {flattened.count()}")
""",
        "happened": "Array items were converted to separate rows; row count expanded proportionally.",
        "why": "explode() unrolls an ArrayType column, replicating parent fields for every nested element.",
        "takeaway": "F.explode() is the primary tool in Spark SQL for normalizing semi-structured document payloads."
    })
    nb.add_exercises({
        "b_title": "Extract Nested Struct Field",
        "b_desc": "Extract customer segment and country directly using dot-notation into new top-level columns.",
        "b_input": "events_df",
        "b_starter": "# Exercise 1: Extract nested fields\n",
        "b_sol": """res = events_df.select(
    F.col("order.order_id").alias("order_id"),
    F.col("customer.segment").alias("cust_segment"),
    F.col("customer.country").alias("cust_country")
)
res.show(3)""",
        "i_title": "Handle Corrupted Record Mode with PERMISSIVE",
        "i_desc": "Load /opt/spark-data/quality/orders_dirty.csv using PERMISSIVE mode and columnNameOfCorruptRecord.",
        "i_input": "/opt/spark-data/quality/orders_dirty.csv",
        "i_starter": "# Exercise 2: Corrupted record tracking\n",
        "i_sol": """dirty_schema = orders_schema.add(StructField("_corrupt_record", StringType(), True))
dirty_df = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(dirty_schema)
    .csv("/opt/spark-data/quality/orders_dirty.csv")
)
print("Dirty records count with nulls or format errors:")
dirty_df.filter(F.col("_corrupt_record").isNotNull()).select("_corrupt_record").show(3, truncate=False)""",
        "a_title": "Construct Schema with MapType",
        "a_desc": "Create a DataFrame with a MapType(StringType, DoubleType) and query a specific map key.",
        "a_input": "F.create_map()",
        "a_starter": "# Exercise 3: MapType usage\n",
        "a_sol": """map_df = spark.createDataFrame([
    (1, {"tax": 12.5, "discount": 5.0}),
    (2, {"tax": 8.0, "discount": 0.0})
], ["id", "financials"])
map_df.printSchema()
map_df.select("id", F.col("financials")["tax"].alias("tax_amount")).show()"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Explicit Schemas": "Mandatory in production; eliminates scan overhead and enforces contract safety.",
            "Nested Data": "Spark natively supports StructType, ArrayType, and MapType.",
            "explode()": "Converts ArrayType elements into individual relational rows."
        },
        "terms": {
            "StructType": "Collection of StructFields representing a schema.",
            "StructField": "Single column definition (name, type, nullable).",
            "explode": "Function that flattens an array into multiple rows."
        },
        "mistakes": {
            "inferSchema=True in Production": "Slows down ingestion and risks schema changes when dirty data arrives.",
            "Ignoring Nullability": "Setting nullable=False does not actively reject nulls in CSV; it is a metadata hint."
        },
        "questions": {
            "Why use explicit schemas over inferSchema?": "Explicit schemas avoid an extra full data scan and guarantee type safety across production batches.",
            "How do you query nested StructType fields?": "Using dot notation (e.g. col('customer.address.city'))."
        },
        "next_steps": "Open 03_dataframe_and_spark_sql/03_dataframe_transformations.ipynb for the complete transformation catalog."
    })
    nb.save(os.path.join(BASE_DIR, "02_schemas_and_data_types.ipynb"))

def gen_03_03():
    nb = NotebookBuilder(
        title="DataFrame Transformations: Select, Filter, WithColumn & Expressions",
        module_name="03_dataframe_and_spark_sql",
        notebook_name="03_dataframe_transformations.ipynb",
        app_name="Transformations_Lab"
    )
    nb.add_header({
        "what": "Complete practical guide to DataFrame transformations: select, selectExpr, filter/where, withColumn, withColumnRenamed, drop, distinct, and column functions.",
        "why": "Data engineers spend 80% of their coding time transforming columns, cleaning text, and manipulating dates and timestamps.",
        "problem": "Suboptimal expressions (like chained withColumn in tight loops) degrade Catalyst optimizer performance.",
        "real_world": "Data wrangling, feature engineering, and data cleaning pipelines."
    })
    nb.add_markdown("""## 2. Core Transformation Categories

```
+---------------------------------------------------------------+
| DATAFRAME TRANSFORMATIONS                                     |
+---------------------------------------------------------------+
| Structural:                                                   |
|  - select(), selectExpr(), withColumn(), withColumnRenamed()  |
|  - drop(), dropDuplicates()                                   |
| Row-Level Filtering:                                          |
|  - filter(), where()                                          |
| Column-Level Expressions:                                     |
|  - col(), lit(), when().otherwise()                           |
|  - String: lower, upper, trim, regexp_replace, split          |
|  - Date: to_date, to_timestamp, datediff, date_add, year      |
|  - Numeric: round, ceil, floor, abs                           |
+---------------------------------------------------------------+
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Practical: End-to-End Data Transformation Pipeline

We transform `/opt/spark-data/retail/orders.csv` with realistic business logic.""")
    nb.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")

# Chained transformation pipeline
transformed = (
    orders
    .filter(F.col("status").isin("Completed", "Shipped"))
    .withColumn("order_date", F.to_date(F.col("order_ts")))
    .withColumn("order_year", F.year(F.col("order_date")))
    .withColumn("order_month", F.month(F.col("order_date")))
    .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("discount_amount", F.round(F.col("gross_amount") * F.col("discount"), 2))
    .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
    .withColumn("channel_category", F.when(F.col("channel") == "Store", "Physical").otherwise("Digital"))
    .select(
        "order_id", "customer_id", "order_date", "order_year", "order_month",
        "gross_amount", "discount_amount", "net_amount", "channel_category", "status"
    )
)

transformed.show(5)
""")
    nb.add_markdown("""## 4. High-Performance Column Replacement with `select` vs Multiple `withColumn`

A common anti-pattern is calling `.withColumn()` 50 times in a row, which bloats the Catalyst tree. Using `select` or `selectExpr` generates a cleaner plan.""")
    nb.add_code("""# High performance multi-column projection
clean_df = orders.select(
    F.col("order_id"),
    F.col("customer_id"),
    F.col("quantity"),
    F.col("unit_price"),
    (F.col("quantity") * F.col("unit_price")).alias("raw_total"),
    F.upper(F.col("channel")).alias("channel_upper")
)
clean_df.show(3)
""")
    nb.add_experiment({
        "title": "Chained withColumn vs Projection Plan Complexity",
        "testing": "Compare the analyzed logical plan tree depth between 10 chained withColumn calls vs a single select projection.",
        "expect": "10 withColumns creates 10 nested Project operators; a single select creates exactly 1 Project operator.",
        "code": """# 10 chained withColumns
df_chained = orders
for i in range(10):
    df_chained = df_chained.withColumn(f"col_{i}", F.lit(i))

# Single select
df_single = orders.select("*", *[F.lit(i).alias(f"col_{i}") for i in range(10)])

print(f"Chained withColumn Plan Tree Depth: {len(df_chained._jdf.queryExecution().analyzed().treeString().splitlines())}")
print(f"Single Select Plan Tree Depth:      {len(df_single._jdf.queryExecution().analyzed().treeString().splitlines())}")
""",
        "happened": "Chained withColumns generated a massive nested logical plan tree.",
        "why": "Each withColumn creates a new DataFrame wrapper node in the unresolved logical tree.",
        "takeaway": "When adding or transforming multiple columns, prefer a single select() or selectExpr() call."
    })
    nb.add_exercises({
        "b_title": "Rename and Cast Columns",
        "b_desc": "Rename 'quantity' to 'units_ordered' and cast 'unit_price' to DecimalType(10, 2).",
        "b_input": "orders DataFrame",
        "b_starter": "# Exercise 1: Rename and cast\n",
        "b_sol": """ex1 = orders.withColumnRenamed("quantity", "units_ordered").withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
ex1.printSchema()""",
        "i_title": "Extract Domain from Customer Email",
        "i_desc": "Load customers.csv and use regexp_extract or split to extract email domains into a 'domain' column.",
        "i_input": "/opt/spark-data/retail/customers.csv",
        "i_starter": "# Exercise 2: Email domain extraction\n",
        "i_sol": """custs = spark.read.option("header", True).csv("/opt/spark-data/retail/customers.csv")
custs_domain = custs.withColumn("domain", F.split(F.col("email"), "@").getItem(1))
custs_domain.select("email", "domain").show(5)""",
        "a_title": "Conditional Multi-Tier Discount Calculation",
        "a_desc": "Calculate a loyalty discount percentage: Gold = 20%, Silver = 10%, Bronze/Consumer = 0% using nested when().otherwise().",
        "a_input": "custs DataFrame",
        "a_starter": "# Exercise 3: Loyalty discount tier\n",
        "a_sol": """discount_df = custs.withColumn("tier_discount",
    F.when(F.col("loyalty_tier") == "Gold", 0.20)
     .when(F.col("loyalty_tier") == "Silver", 0.10)
     .otherwise(0.0)
)
discount_df.select("first_name", "loyalty_tier", "tier_discount").show(5)"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Vectorized Transformations": "Always use built-in pyspark.sql.functions for string, date, and math operations.",
            "Single Projection": "Use select() with expressions rather than dozens of chained withColumns.",
            "Column Expressions": "Columns can be dynamically created with F.col() and F.lit()."
        },
        "terms": {
            "selectExpr": "Select projection accepting raw SQL string expressions.",
            "when().otherwise()": "Conditional case-when statement.",
            "cast()": "Converts a column to another DataType."
        },
        "mistakes": {
            "50 withColumn calls in a loop": "Causes excessive plan complexity and slow analysis.",
            "Missing lit() for constants": "Passing raw Python values into col() throws type errors."
        },
        "questions": {
            "What is the difference between filter() and where()?": "They are exact aliases of each other.",
            "Why is select() preferred over multiple withColumns?": "Creates a single Project operator in the logical plan rather than nesting dozens of intermediate projections."
        },
        "next_steps": "Open 03_dataframe_and_spark_sql/04_spark_sql.ipynb to master SQL queries and temporary views."
    })
    nb.save(os.path.join(BASE_DIR, "03_dataframe_transformations.ipynb"))

def gen_03_04():
    nb = NotebookBuilder(
        title="Spark SQL: Temporary Views, Global Views, and SQL Execution",
        module_name="03_dataframe_and_spark_sql",
        notebook_name="04_spark_sql.ipynb",
        app_name="Spark_SQL_Lab"
    )
    nb.add_header({
        "what": "Writing declarative ANSI SQL queries against Spark DataFrames, managing temporary and global views, and using Common Table Expressions (CTEs).",
        "why": "SQL is the universal language of data analytics. Spark SQL compiles ANSI SQL into the exact same optimized Catalyst physical plans as the DataFrame API.",
        "problem": "Knowing when to use raw SQL versus programmatic DataFrame transformations.",
        "real_world": "Migrating legacy SQL warehouse queries (Snowflake, BigQuery, Oracle) to Apache Spark."
    })
    nb.add_markdown("""## 2. Spark SQL vs DataFrame API Equivalence

```
User ANSI SQL Query:
"SELECT region, sum(revenue) FROM sales GROUP BY region"
                      |
                      v (Parsed by Catalyst SQL Parser)
          Parsed Logical Plan
                      ^
                      | (Built by DataFrame API)
User DataFrame API Call:
df.groupBy("region").agg(sum("revenue"))
```
> Both APIs compile into the **exact same Physical Plan and execute identically**!
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Registering Views and Running SQL Queries""")
    nb.add_code("""# Register retail datasets as SQL views
orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
customers = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/customers.csv")

orders.createOrReplaceTempView("orders")
customers.createOrReplaceTempView("customers")

# Query with CTE and aggregations
sql_query = \"\"\"
WITH customer_revenue AS (
    SELECT 
        o.customer_id,
        c.region,
        c.segment,
        SUM(o.quantity * o.unit_price) AS total_spent,
        COUNT(o.order_id) AS total_orders
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.status = 'Completed'
    GROUP BY o.customer_id, c.region, c.segment
)
SELECT 
    region,
    segment,
    COUNT(customer_id) AS active_customers,
    ROUND(SUM(total_spent), 2) AS regional_revenue,
    ROUND(AVG(total_spent), 2) AS avg_customer_spend
FROM customer_revenue
GROUP BY region, segment
ORDER BY regional_revenue DESC
\"\"\"

result_df = spark.sql(sql_query)
result_df.show(10)
""")
    nb.add_experiment({
        "title": "Verifying Physical Plan Parity: SQL vs DataFrame API",
        "testing": "Prove that an identical SQL query and DataFrame call generate the exact same Catalyst physical plan.",
        "expect": "Both explain() outputs will show identical physical operators.",
        "code": """# SQL approach
plan_sql = spark.sql("SELECT status, count(*) as cnt FROM orders GROUP BY status")

# DataFrame approach
plan_df = orders.groupBy("status").count()

print("=== SQL Physical Plan ===")
plan_sql.explain()

print("\\n=== DataFrame Physical Plan ===")
plan_df.explain()
""",
        "happened": "Both approaches generated identical HashAggregate and Exchange physical operators.",
        "why": "Spark SQL parses SQL into the same Catalyst AST (Abstract Syntax Tree) that DataFrame methods build.",
        "takeaway": "Choose SQL or DataFrame API based on readability and team preference; performance is identical."
    })
    nb.add_exercises({
        "b_title": "Monthly Revenue Trend Query",
        "b_desc": "Write an ANSI SQL query to compute monthly completed revenue and order count for 2025.",
        "b_input": "orders view",
        "b_starter": "# Exercise 1: Monthly revenue in SQL\n",
        "b_sol": """spark.sql(\"\"\"
    SELECT 
        date_format(order_ts, 'yyyy-MM') AS order_month,
        COUNT(order_id) AS total_orders,
        ROUND(SUM(quantity * unit_price), 2) AS monthly_revenue
    FROM orders
    WHERE status = 'Completed'
    GROUP BY date_format(order_ts, 'yyyy-MM')
    ORDER BY order_month
\"\"\").show()""",
        "i_title": "Top 5 Products per Channel Query",
        "i_desc": "Write a SQL query using window function DENSE_RANK() to find the top 3 selling products per channel.",
        "i_input": "orders view",
        "i_starter": "# Exercise 2: Top products per channel\n",
        "i_sol": """spark.sql(\"\"\"
    WITH ranked_products AS (
        SELECT 
            channel,
            product_id,
            SUM(quantity) AS total_units,
            DENSE_RANK() OVER (PARTITION BY channel ORDER BY SUM(quantity) DESC) as rnk
        FROM orders
        GROUP BY channel, product_id
    )
    SELECT * FROM ranked_products WHERE rnk <= 3 ORDER BY channel, rnk
\"\"\").show()""",
        "a_title": "Multi-Table SQL Join with Customer Loyalty",
        "a_desc": "Query revenue by loyalty_tier and channel, showing percentage share of total revenue.",
        "a_input": "orders and customers views",
        "a_starter": "# Exercise 3: Revenue share by loyalty\n",
        "a_sol": """spark.sql(\"\"\"
    SELECT 
        c.loyalty_tier,
        o.channel,
        ROUND(SUM(o.quantity * o.unit_price), 2) AS tier_revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.loyalty_tier, o.channel
    ORDER BY tier_revenue DESC
\"\"\").show(10)"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "SQL vs DataFrame": "Identical execution plans and performance under Catalyst.",
            "Temp Views": "Session-scoped virtual tables registered in internal Catalog.",
            "Advanced SQL": "Supports full ANSI SQL including CTEs, Window functions, and nested queries."
        },
        "terms": {
            "createOrReplaceTempView": "Registers a DataFrame as a temporary SQL view.",
            "spark.sql()": "Executes an SQL query string and returns a DataFrame.",
            "CTE": "Common Table Expression (WITH clause)."
        },
        "mistakes": {
            "Assuming SQL is slower than DataFrames": "False: both compile to the exact same bytecode.",
            "SQL Injection in Unsanitized Strings": "Avoid dynamic f-string queries with untrusted user input."
        },
        "questions": {
            "Is Spark SQL slower than the DataFrame API?": "No, both produce identical Catalyst logical plans and compile into the same Tungsten physical bytecode.",
            "How long do temporary views last?": "Only for the lifetime of that specific SparkSession."
        },
        "next_steps": "Open 03_dataframe_and_spark_sql/05_joins_and_aggregations.ipynb to master distributed joins."
    })
    nb.save(os.path.join(BASE_DIR, "04_spark_sql.ipynb"))

def gen_03_05():
    nb = NotebookBuilder(
        title="Distributed Joins and Complex Aggregations",
        module_name="03_dataframe_and_spark_sql",
        notebook_name="05_joins_and_aggregations.ipynb",
        app_name="Joins_Aggregations_Lab"
    )
    nb.add_header({
        "what": "Complete reference on distributed join types (inner, left, right, full, cross, semi, anti) and advanced aggregations (cube, rollup, pivot).",
        "why": "Joins and aggregations are the most compute- and network-intensive operations in distributed data engineering.",
        "problem": "Incorrect join types lead to duplicate rows, dropped data, or massive Cartesian products.",
        "real_world": "Denormalizing star-schema fact and dimension tables in data warehousing."
    })
    nb.add_markdown("""## 2. Distributed Join Types

```
+-----------------------------------------------------------------------+
| JOIN TYPES IN SPARK                                                   |
+-------------------+---------------------------------------------------+
| Inner Join        | Keeps matching rows in both tables.               |
| Left / Right Join | Keeps all rows from left/right + matching right/left|
| Full Outer Join   | Keeps all rows from both tables, filling nulls.   |
| Left Semi Join    | Filters left rows that have a match in right.     |
|                   | (Columns from right are NOT added).               |
| Left Anti Join    | Filters left rows that have NO match in right.    |
| Cross Join        | Cartesian product (M x N rows) - USE CAUTION!     |
+-------------------+---------------------------------------------------+
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Hands-On: Three-Table Join (Orders + Customers + Products)""")
    nb.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")
customers = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/customers.csv")
products = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/products.csv")

# Join orders with customers (left join) and products (left join)
enriched_orders = (
    orders
    .join(customers, on="customer_id", how="left")
    .join(products, on="product_id", how="left")
    .select(
        "order_id", "order_ts", "customer_id", "first_name", "country", "region",
        "product_id", "product_name", "category", "quantity", "unit_price", "status"
    )
)

enriched_orders.show(3)
""")
    nb.add_markdown("""## 4. Left Anti Join: Finding Orphan Customers

Find customers who have never placed an order using Left Anti Join.""")
    nb.add_code("""# Customers with ZERO orders
dormant_customers = (
    customers
    .join(orders, on="customer_id", how="left_anti")
    .select("customer_id", "first_name", "email", "signup_date")
)

print(f"Total customers: {customers.count()}")
print(f"Dormant customers with 0 orders: {dormant_customers.count()}")
dormant_customers.show(3)
""")
    nb.add_markdown("""## 5. Advanced Aggregations: `rollup`, `cube`, and `pivot`""")
    nb.add_code("""# Pivot: Total sales by Region across Product Categories
pivot_df = (
    enriched_orders
    .filter("status = 'Completed'")
    .groupBy("region")
    .pivot("category")
    .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2))
    .na.fill(0.0)
)

print("Sales Pivot Table (Region vs Category):")
pivot_df.show()
""")
    nb.add_experiment({
        "title": "Left Semi Join vs Inner Join Row Count and Performance",
        "testing": "Prove that Left Semi Join does not duplicate left table rows even if the right table has 1-to-many matches.",
        "expect": "Left Semi Join returns at most 1 copy of left rows and does not project right table columns.",
        "code": """# Left semi join: find customers who placed at least 1 order
semi_df = customers.join(orders, on="customer_id", how="left_semi")

print(f"Total customers:                 {customers.count()}")
print(f"Customers with >= 1 order (semi): {semi_df.count()}")
print("Columns in semi join result:", semi_df.columns)
""",
        "happened": "semi_df contains only customers columns; no duplicates occurred despite customers having multiple orders.",
        "why": "Left semi join tests existence of a match; it stops after finding the first match and outputs the left row.",
        "takeaway": "Use left_semi instead of inner join + distinct when filtering a table based on existence in another table."
    })
    nb.add_exercises({
        "b_title": "Multi-Column Aggregations with agg()",
        "b_desc": "Compute total revenue, avg unit price, and unique order count per product category.",
        "b_input": "enriched_orders",
        "b_starter": "# Exercise 1: Category metrics\n",
        "b_sol": """enriched_orders.groupBy("category").agg(
    F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("total_rev"),
    F.round(F.avg("unit_price"), 2).alias("avg_price"),
    F.countDistinct("order_id").alias("unique_orders")
).show()""",
        "i_title": "Rollup for Hierarchical Subtotals",
        "i_desc": "Use rollup('region', 'channel') to compute subtotals at the region level and overall grand total.",
        "i_input": "enriched_orders",
        "i_starter": "# Exercise 2: Rollup subtotals\n",
        "i_sol": """enriched_orders.rollup("region", "channel").agg(
    F.count("order_id").alias("orders_count"),
    F.round(F.sum(F.col("quantity")*F.col("unit_price")), 2).alias("revenue")
).orderBy(F.col("region").asc_nulls_last(), F.col("channel").asc_nulls_last()).show(10)""",
        "a_title": "Detect Products with Zero Sales",
        "a_desc": "Use a Left Anti Join between products and orders to identify all products that have never been purchased.",
        "a_input": "products and orders",
        "a_starter": "# Exercise 3: Products with zero orders\n",
        "a_sol": """unsold_products = products.join(orders, on="product_id", how="left_anti")
print("Unsold products count:", unsold_products.count())
unsold_products.select("product_id", "product_name", "category").show(5)"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Join Types": "Know when to use inner, left, left_semi, and left_anti.",
            "Left Semi Efficiency": "Filters for existence without duplicating rows or pulling right columns.",
            "Pivot & Rollup": "Powerful multidimensional aggregation primitives built into Spark SQL."
        },
        "terms": {
            "Left Semi Join": "Returns left rows matching right table without right columns.",
            "Left Anti Join": "Returns left rows with NO match in right table.",
            "Pivot": "Rotates distinct column values into horizontal column headers."
        },
        "mistakes": {
            "Unintentional Cross Joins": "Missing join conditions generate massive Cartesian products.",
            "Duplicate Column Names": "Joining two tables with matching column names without disambiguation causes ambiguous reference errors."
        },
        "questions": {
            "What is the difference between Left Semi and Inner Join?": "Left semi only checks for existence, does not duplicate left rows on 1:N matches, and does not include right table columns.",
            "How do you resolve ambiguous column names after a join?": "Drop the column prior to join, or join on a string name ('id') rather than expression (df1.id == df2.id)."
        },
        "next_steps": "Open 03_dataframe_and_spark_sql/06_window_functions.ipynb to master analytical window functions."
    })
    nb.save(os.path.join(BASE_DIR, "05_joins_and_aggregations.ipynb"))

def gen_03_06():
    nb = NotebookBuilder(
        title="Analytical Window Functions: Rankings, Lead/Lag, and Moving Averages",
        module_name="03_dataframe_and_spark_sql",
        notebook_name="06_window_functions.ipynb",
        app_name="Window_Functions_Lab"
    )
    nb.add_header({
        "what": "How to construct Window specifications, compute ranking functions (row_number, rank, dense_rank), analytical offsets (lead, lag), and cumulative running totals.",
        "why": "Window functions solve advanced analytical questions (e.g. top N items per group, year-over-year changes, 7-day rolling averages) without self-joins.",
        "problem": "Performing group-based ranking with self-joins causes severe performance degradation.",
        "real_world": "Financial reporting, fraud detection (time between transactions), and sales leaderboards."
    })
    nb.add_markdown("""## 2. Window Function Architecture

```
Window Specification:
Window.partitionBy("category").orderBy(desc("revenue")).rowsBetween(start, end)
  |                                 |                       |
  v Segregates data                 v Sorts inside frame    v Defines frame slice
[ Electronics ]                   [ Highest -> Lowest ]   [ e.g. -3 to Current Row ]
  |-- Row 1: Laptop ($1500)       -> row_number=1, rank=1, dense_rank=1
  |-- Row 2: Phone ($800)         -> row_number=2, rank=2, dense_rank=2
[ Clothing ]
  |-- Row 1: Jacket ($200)        -> row_number=1, rank=1, dense_rank=1
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Practical: Top 2 Highest-Grossing Orders per Channel""")
    nb.add_code("""from pyspark.sql.window import Window

orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")

# Define window specification
channel_window = Window.partitionBy("channel").orderBy(F.desc("quantity"))

ranked_orders = (
    orders
    .withColumn("row_num", F.row_number().over(channel_window))
    .withColumn("rank_num", F.rank().over(channel_window))
    .withColumn("dense_rank_num", F.dense_rank().over(channel_window))
    .filter(F.col("row_num") <= 2)
    .select("channel", "order_id", "customer_id", "quantity", "row_num", "rank_num", "dense_rank_num")
)

ranked_orders.show()
""")
    nb.add_markdown("""## 4. Analytical Offsets: `lag()` to Calculate Days Between Customer Orders""")
    nb.add_code("""customer_window = Window.partitionBy("customer_id").orderBy("order_ts")

order_intervals = (
    orders
    .withColumn("prev_order_ts", F.lag("order_ts", 1).over(customer_window))
    .withColumn("days_since_prev", F.datediff(F.col("order_ts"), F.col("prev_order_ts")))
    .filter(F.col("prev_order_ts").isNotNull())
    .select("customer_id", "order_id", "order_ts", "prev_order_ts", "days_since_prev")
)

order_intervals.show(5)
""")
    nb.add_markdown("""## 5. Running Totals and Moving Averages with `rowsBetween`""")
    nb.add_code("""# Cumulative revenue running sum per customer
running_window = Window.partitionBy("customer_id").orderBy("order_ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)

running_totals = (
    orders
    .withColumn("order_value", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("cumulative_spend", F.round(F.sum("order_value").over(running_window), 2))
    .select("customer_id", "order_ts", "order_value", "cumulative_spend")
)

running_totals.show(6)
""")
    nb.add_experiment({
        "title": "rowsBetween vs rangeBetween Semantics",
        "testing": "Demonstrate the difference between physical row offset (rowsBetween) and logical value range (rangeBetween).",
        "expect": "rowsBetween counts physical row positions; rangeBetween groups tied order values.",
        "code": """sample_data = spark.createDataFrame([
    ("A", 1, 100),
    ("A", 2, 100),
    ("A", 3, 200)
], ["user", "time_step", "value"])

w_rows = Window.partitionBy("user").orderBy("value").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_range = Window.partitionBy("user").orderBy("value").rangeBetween(Window.unboundedPreceding, Window.currentRow)

sample_data.withColumn("sum_rows", F.sum("value").over(w_rows)) \\
           .withColumn("sum_range", F.sum("value").over(w_range)) \\
           .show()
""",
        "happened": "For row 1 and 2 with tied value 100, sum_range immediately jumped to 200 for both rows, while sum_rows accumulated row by row.",
        "why": "rangeBetween treats duplicate values in the orderBy column as peers within the same frame slice.",
        "takeaway": "Use rowsBetween when you need strict row-based moving averages (e.g. 3-row moving average)."
    })
    nb.add_exercises({
        "b_title": "Top 1 Spending Customer per Region",
        "b_desc": "Join customers and orders, and find the single highest-spending customer in each region using row_number().",
        "b_input": "customers and orders",
        "b_starter": "# Exercise 1: Top customer per region\n",
        "b_sol": """cust_spend = orders.join(customers, "customer_id").groupBy("region", "customer_id").agg(F.sum(F.col("quantity")*F.col("unit_price")).alias("spend"))
w = Window.partitionBy("region").orderBy(F.desc("spend"))
top_custs = cust_spend.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
top_custs.show()""",
        "i_title": "Percentage of Total Spend per Customer",
        "i_desc": "Calculate each individual order's percentage contribution to that customer's lifetime total spend.",
        "i_input": "orders DataFrame",
        "i_starter": "# Exercise 2: Spend contribution\n",
        "i_sol": """w_cust_all = Window.partitionBy("customer_id")
orders.withColumn("val", F.col("quantity")*F.col("unit_price")) \\
      .withColumn("total_cust_spend", F.sum("val").over(w_cust_all)) \\
      .withColumn("pct_of_total", F.round((F.col("val") / F.col("total_cust_spend")) * 100, 2)) \\
      .select("customer_id", "order_id", "val", "pct_of_total").show(5)""",
        "a_title": "3-Order Rolling Moving Average",
        "a_desc": "Compute a 3-order moving average of unit_price for each product over time (1 preceding, current, 1 following).",
        "a_input": "orders DataFrame",
        "a_starter": "# Exercise 3: Rolling moving average\n",
        "a_sol": """w_roll = Window.partitionBy("product_id").orderBy("order_ts").rowsBetween(-1, 1)
orders.withColumn("moving_avg_price", F.round(F.avg("unit_price").over(w_roll), 2)) \\
      .select("product_id", "order_ts", "unit_price", "moving_avg_price").show(6)"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Window Power": "Performs aggregations without collapsing rows into a single record.",
            "Ranking Functions": "row_number() is unique; rank() skips numbers on ties; dense_rank() does not skip.",
            "Lead and Lag": "Enables time-series difference calculations without self-joins."
        },
        "terms": {
            "Window.partitionBy": "Defines the boundary of rows processed together.",
            "rowsBetween": "Defines physical row offset frame.",
            "rangeBetween": "Defines logical value range frame."
        },
        "mistakes": {
            "Window without partitionBy on Large Data": "Forces all data in the entire dataset into a single partition on 1 executor, causing OOM.",
            "Confusing rank() with dense_rank()": "Rank leaves gaps after ties; dense_rank is sequential."
        },
        "questions": {
            "What happens if you omit partitionBy() in a Window?": "Spark moves the entire dataset into a single partition on one executor, completely disabling parallelism and likely crashing with OOM.",
            "What is the difference between lag() and lead()?": "lag() looks at preceding rows; lead() looks at subsequent rows."
        },
        "next_steps": "Open 03_dataframe_and_spark_sql/07_nulls_and_data_quality.ipynb to master dirty record handling."
    })
    nb.save(os.path.join(BASE_DIR, "06_window_functions.ipynb"))

def gen_03_07():
    nb = NotebookBuilder(
        title="Null Handling, Missing Data Strategies, and Quality Validation",
        module_name="03_dataframe_and_spark_sql",
        notebook_name="07_nulls_and_data_quality.ipynb",
        app_name="Data_Quality_Lab"
    )
    nb.add_header({
        "what": "How Spark handles null and NaN values, using na.fill(), na.drop(), and na.replace(), null-safe equality (<=>), and building validation pipelines.",
        "why": "Real-world data is inherently dirty. Missing keys break joins, skew partitions, and corrupt aggregations.",
        "problem": "Silent null propagation causes misleading report totals or runtime NullPointerExceptions.",
        "real_world": "Auditing data quality gates in production Lakehouse architectures."
    })
    nb.add_markdown("""## 2. Null Semantics in Spark SQL

```
+-------------------------------------------------------------------------------+
| Spark SQL Null Handling Rules                                                 |
+-------------------------------------------------------------------------------+
| Standard Equality (col == null)     | Always returns NULL (unknown)           |
| Null-Safe Equality (col <=> null)   | Returns TRUE if both null, FALSE if one |
| Standard Aggregations (SUM, AVG)    | Silently ignores NULL values            |
| COUNT(*)                            | Counts ALL rows including nulls         |
| COUNT(col)                          | Counts only NON-NULL rows               |
+-------------------------------------------------------------------------------+
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Practical: Auditing Missing Data in `/opt/spark-data/quality/orders_dirty.csv`""")
    nb.add_code("""dirty_df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/quality/orders_dirty.csv")

print(f"Total rows in dirty dataset: {dirty_df.count()}")
dirty_df.printSchema()

# Profile null counts across all columns
null_counts = dirty_df.select([
    F.count(F.when(F.col(c).isNull() | F.isnan(F.col(c)), c)).alias(c)
    for c in dirty_df.columns
])

print("Null counts per column:")
null_counts.show()
""")
    nb.add_markdown("""## 4. Cleaning Strategies: `na.fill()`, `na.drop()`, and `coalesce()`""")
    nb.add_code("""# Strategy 1: Drop rows where critical primary keys are null
cleaned_step1 = dirty_df.na.drop(subset=["order_id", "customer_id"])

# Strategy 2: Fill null discounts with 0.0 and null status with 'Pending'
cleaned_step2 = cleaned_step1.na.fill({
    "discount": 0.0,
    "status": "Pending",
    "channel": "Unknown"
})

print(f"Rows before: {dirty_df.count()} -> Rows after key drop: {cleaned_step1.count()}")
cleaned_step2.filter("discount IS NULL OR status IS NULL").show()
print("Cleaned sample:")
cleaned_step2.show(5)
""")
    nb.add_experiment({
        "title": "Null-Safe Equality (<=>) in Filter and Joins",
        "testing": "Demonstrate that standard '==' evaluates to null when comparing null values, whereas '<=>' evaluates deterministically.",
        "expect": "df.filter(col('a') == col('b')) drops rows where both are null; col('a').eqNullSafe(col('b')) retains them.",
        "code": """test_nulls = spark.createDataFrame([
    (1, "A", "A"),
    (2, None, None),
    (3, "B", None)
], ["id", "val1", "val2"])

print("Standard Equality (val1 == val2):")
test_nulls.filter(F.col("val1") == F.col("val2")).show()

print("Null-Safe Equality (val1.eqNullSafe(val2)):")
test_nulls.filter(F.col("val1").eqNullSafe(F.col("val2"))).show()
""",
        "happened": "Standard equality dropped row 2 (both None); null-safe equality correctly preserved row 2.",
        "why": "In ANSI SQL three-valued logic, NULL == NULL produces NULL (falsy in filters). eqNullSafe explicitly tests for equivalence.",
        "takeaway": "Always use eqNullSafe() when joining or filtering nullable keys where null-to-null matching is required."
    })
    nb.add_exercises({
        "b_title": "Filter Non-Null Valid Records",
        "b_desc": "Filter dirty_df to retain only rows where both quantity and unit_price are NOT null and greater than zero.",
        "b_input": "dirty_df",
        "b_starter": "# Exercise 1: Valid numeric records\n",
        "b_sol": """valid = dirty_df.filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0) & F.col("unit_price").isNotNull() & (F.col("unit_price") > 0))
print("Valid count:", valid.count())""",
        "i_title": "Replace Invalid String Values with na.replace()",
        "i_desc": "Use na.replace() to standardize channel values: map 'App' to 'Mobile' and 'N/A' to 'Unknown'.",
        "i_input": "dirty_df channel column",
        "i_starter": "# Exercise 2: na.replace mapping\n",
        "i_sol": """standardized = dirty_df.na.replace({"App": "Mobile", "N/A": "Unknown"}, subset=["channel"])
standardized.groupBy("channel").count().show()""",
        "a_title": "Data Quality Metric Report Card",
        "a_desc": "Calculate the percentage completeness (non-null %) for every column in dirty_df and display as a summary DataFrame.",
        "a_input": "dirty_df",
        "a_starter": "# Exercise 3: Completeness report\n",
        "a_sol": """total_rows = dirty_df.count()
completeness_exprs = [
    F.round((F.count(c) / total_rows) * 100, 2).alias(f"{c}_pct_complete")
    for c in dirty_df.columns
]
dirty_df.select(completeness_exprs).show()"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Null Semantics": "NULL == NULL produces NULL; use eqNullSafe() for null-aware matching.",
            "na Package": "na.fill(), na.drop(), na.replace() provide high-level missing data management.",
            "Aggregation Impact": "Standard aggregations ignore nulls, but COUNT(*) includes them."
        },
        "terms": {
            "eqNullSafe": "Null-safe equality operator (<=>).",
            "na.fill": "Replaces null values with specified constants.",
            "na.drop": "Drops rows containing null values in designated subsets."
        },
        "mistakes": {
            "Using == to check for null": "df.filter(col('x') == None) fails silently; must use col('x').isNull().",
            "Forgetting that Joins Drop Null Keys": "Standard equi-joins drop rows with null keys on both sides."
        },
        "questions": {
            "What does COUNT(col) return if all values in that column are null?": "It returns 0.",
            "How does eqNullSafe differ from standard equality?": "It returns True when comparing NULL to NULL."
        },
        "next_steps": "Open 03_dataframe_and_spark_sql/08_udfs_and_pandas_udfs.ipynb to compare standard UDFs with Arrow Pandas UDFs."
    })
    nb.save(os.path.join(BASE_DIR, "07_nulls_and_data_quality.ipynb"))

def gen_03_08():
    nb = NotebookBuilder(
        title="UDFs vs Vectorized Pandas UDFs: Architecture & Performance Benchmarks",
        module_name="03_dataframe_and_spark_sql",
        notebook_name="08_udfs_and_pandas_udfs.ipynb",
        app_name="UDF_Benchmarking_Lab"
    )
    nb.add_header({
        "what": "How standard Python UDFs work under Py4J, how Apache Arrow enables Vectorized Pandas UDFs, and measuring performance differences.",
        "why": "Standard Python UDFs are one of the biggest performance killers in Spark. Vectorized UDFs provide 10x-50x speedups.",
        "problem": "Row-by-row socket serialization between JVM and Python worker processes creates massive CPU and GC overhead.",
        "real_world": "Running machine learning model inference and complex domain logic in PySpark."
    })
    nb.add_markdown("""## 2. Python UDF vs Pandas UDF Execution Models

```
Standard Python UDF (Row-by-Row Serialization):
[JVM Executor Heap] -> [Socket / Pickle] -> [Python Daemon] -> [Execute Function]
         ^                                                               |
         +------------------- [Socket / Unpickle] <----------------------+
(Severe overhead: 1 serialization event PER ROW; No Catalyst optimization)

Vectorized Pandas UDF (Apache Arrow Shared Memory):
[JVM Executor Heap] ======= Apache Arrow RecordBatch =======> [Python Worker]
                              (Zero-Copy / Bulk SIMD)         [Pandas Series vectorized]
```
""")
    nb.add_session_init()
    nb.add_markdown("""## 3. Implementing Standard Python UDF vs Pandas UDF""")
    nb.add_code("""import pandas as pd
from pyspark.sql.functions import udf, pandas_udf

# 1. Standard Python UDF
@udf(returnType=DoubleType())
def standard_tax_udf(price, quantity):
    if price is None or quantity is None:
        return 0.0
    return float(price * quantity * 0.14)

# 2. Vectorized Pandas UDF (Series to Series)
@pandas_udf(DoubleType())
def vectorized_tax_udf(price: pd.Series, quantity: pd.Series) -> pd.Series:
    return price.fillna(0.0) * quantity.fillna(0) * 0.14

# 3. Native Built-in Function (Baseline)
native_expr = F.round(F.coalesce(F.col("unit_price"), F.lit(0.0)) * F.coalesce(F.col("quantity"), F.lit(0)) * 0.14, 2)
""")
    nb.add_markdown("""## 4. Benchmark: Native vs Pandas UDF vs Standard Python UDF""")
    nb.add_code("""orders = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark-data/retail/orders.csv")

# Duplicate dataset to 240,000 rows to create meaningful timing differences
bench_df = orders.union(orders).union(orders).union(orders).cache()
bench_df.count() # Force cache population

# Benchmark 1: Native Spark Function
t0 = time.time()
bench_df.withColumn("tax", native_expr).write.mode("overwrite").format("noop").save()
t_native = time.time() - t0

# Benchmark 2: Vectorized Pandas UDF (Arrow)
t0 = time.time()
bench_df.withColumn("tax", vectorized_tax_udf(F.col("unit_price"), F.col("quantity"))).write.mode("overwrite").format("noop").save()
t_pandas = time.time() - t0

# Benchmark 3: Standard Python UDF
t0 = time.time()
bench_df.withColumn("tax", standard_tax_udf(F.col("unit_price"), F.col("quantity"))).write.mode("overwrite").format("noop").save()
t_standard = time.time() - t0

print(f"=== Execution Time Benchmark (240k rows) ===")
print(f"1. Native Spark Function: {t_native:.3f}s (Fastest - runs pure JVM)")
print(f"2. Vectorized Pandas UDF: {t_pandas:.3f}s (Arrow Batch Vectorization)")
print(f"3. Standard Python UDF:   {t_standard:.3f}s (Slowest - row-by-row socket pickle)")
print(f"Speedup of Vectorized over Standard: {t_standard / t_pandas:.2f}x")
bench_df.unpersist()
""")
    nb.add_experiment({
        "title": "Inspecting Whole-Stage Codegen Suppression",
        "testing": "Show that standard Python UDF suppresses Whole-Stage Code Generation while Native operations do not.",
        "expect": "Native expression shows WholeStageCodegen (*) in explain(); standard UDF drops out of codegen.",
        "code": """print("=== Native Expression Plan (WholeStageCodegen active) ===")
orders.select(native_expr).explain()

print("\\n=== Standard Python UDF Plan (BatchEvalPython operator) ===")
orders.select(standard_tax_udf(F.col("unit_price"), F.col("quantity"))).explain()
""",
        "happened": "The standard UDF introduced BatchEvalPython and disabled WholeStageCodegen.",
        "why": "Spark must serialize rows out of the JVM into external Python worker processes.",
        "takeaway": "Always exhaust native pyspark.sql.functions before resorting to custom Python functions."
    })
    nb.add_exercises({
        "b_title": "String Cleansing Pandas UDF",
        "b_desc": "Write a Pandas UDF that takes a Series of strings and strips whitespace and lowercases all characters.",
        "b_input": "Series of channel strings",
        "b_starter": "# Exercise 1: Clean string Pandas UDF\n",
        "b_sol": """@pandas_udf(StringType())
def clean_str_udf(s: pd.Series) -> pd.Series:
    return s.str.strip().str.lower()

orders.select(clean_str_udf(F.col("channel")).alias("clean_channel")).show(3)""",
        "i_title": "Grouped Map Pandas UDF with applyInPandas",
        "i_desc": "Use applyInPandas() to subtract the channel mean unit_price from each individual order within each channel.",
        "i_input": "orders DataFrame",
        "i_starter": "# Exercise 2: applyInPandas\n",
        "i_sol": """def subtract_mean(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf['price_diff'] = pdf['unit_price'] - pdf['unit_price'].mean()
    return pdf[['order_id', 'channel', 'unit_price', 'price_diff']]

res = orders.groupBy("channel").applyInPandas(subtract_mean, schema="order_id int, channel string, unit_price double, price_diff double")
res.show(5)""",
        "a_title": "Scalar Pandas UDF with Multiple Inputs",
        "a_desc": "Implement a Haversine distance or compound interest formula using vectorized NumPy inside a Pandas UDF.",
        "a_input": "Two numeric columns",
        "a_starter": "# Exercise 3: Vectorized math\n",
        "a_sol": """import numpy as np

@pandas_udf(DoubleType())
def compound_growth(principal: pd.Series, rate: pd.Series) -> pd.Series:
    return principal * np.power(1.0 + rate, 3)

spark.range(3).withColumn("growth", compound_growth(F.lit(100.0), F.lit(0.05))).show()"""
    })
    nb.add_summary_and_teardown({
        "takeaways": {
            "Hierarchy of Performance": "1. Native Functions (Best) > 2. Pandas UDF (Good) > 3. Standard UDF (Avoid).",
            "Arrow Zero-Copy": "Pandas UDFs transfer entire batches using Apache Arrow memory format.",
            "Whole-Stage Codegen": "Python UDFs break operator fusion across the stage."
        },
        "terms": {
            "Pandas UDF": "Vectorized UDF using Apache Arrow and Pandas Series.",
            "BatchEvalPython": "Physical operator indicating Python socket execution.",
            "Apache Arrow": "In-memory columnar data format enabling zero-copy cross-language sharing."
        },
        "mistakes": {
            "Writing UDFs for existing functions": "e.g. writing a UDF for lower() or round() kills performance.",
            "Returning scalar instead of Series": "A Series-to-Series Pandas UDF must accept and return pd.Series of identical length."
        },
        "questions": {
            "Why are standard Python UDFs slow in PySpark?": "They require row-by-row serialization across a socket between the JVM and Python worker, disabling Catalyst optimizations and Whole-Stage Codegen.",
            "How do Pandas UDFs achieve 10x-50x speedups?": "They use Apache Arrow to transfer entire columnar record batches directly into memory without row-level serialization overhead."
        },
        "next_steps": "Module 03 complete! Proceed to Module 04: Spark Execution Engine Deep Dive."
    })
    nb.save(os.path.join(BASE_DIR, "08_udfs_and_pandas_udfs.ipynb"))

if __name__ == '__main__':
    gen_03_02()
    gen_03_03()
    gen_03_04()
    gen_03_05()
    gen_03_06()
    gen_03_07()
    gen_03_08()
