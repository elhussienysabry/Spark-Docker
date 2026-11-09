#!/usr/bin/env python3
"""Generate the deterministic datasets used by the notebook curriculum.

The files are intentionally small enough for a four-core Docker lab but large
enough to produce multiple partitions, measurable shuffles, and observable
skew (one product accounts for ~35% of all orders).

Usage:
    python3 data/generate_datasets.py            # writes next to this script
    python3 data/generate_datasets.py --output /tmp/spark-data

Everything is seeded, so re-running the script reproduces byte-identical CSV
files (JSON lines and logs included).
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42
N_CUSTOMERS = 8_000
N_PRODUCTS = 300
N_ORDERS = 60_000
N_JSON_ORDERS = 3_000
N_DIRTY_ORDERS = 5_000
N_ACCESS_LOGS = 20_000

FIRST_NAMES = [
    "Ahmed", "Mona", "Omar", "Sara", "Youssef", "Nour", "Karim", "Laila",
    "Hassan", "Mariam", "Ali", "Salma", "Tarek", "Dina", "Khaled", "Aya",
    "James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Lucas", "Mia",
    "Ravi", "Priya", "Chen", "Yuki", "Diego", "Sofia", "Hans", "Clara",
]
LAST_NAMES = [
    "Hassan", "Ibrahim", "Mahmoud", "El-Sayed", "Fahmy", "Mostafa", "Adel",
    "Smith", "Johnson", "Brown", "Wilson", "Taylor", "Anderson", "Thomas",
    "Patel", "Sharma", "Chen", "Tanaka", "Garcia", "Lopez", "Muller", "Schmidt",
]
COUNTRIES = [
    ("Egypt", "North", 0.22), ("UAE", "East", 0.14), ("KSA", "East", 0.16),
    ("USA", "West", 0.18), ("Germany", "West", 0.10), ("France", "West", 0.08),
    ("India", "South", 0.08), ("UK", "North", 0.04),
]
SEGMENTS = [("Consumer", 0.70), ("SMB", 0.22), ("Enterprise", 0.08)]
TIERS = [("Bronze", 0.45), ("Silver", 0.30), ("Gold", 0.18), ("Platinum", 0.07)]
CATEGORIES = [
    ("Electronics", 0.40), ("Accessories", 0.25), ("Home", 0.15),
    ("Office", 0.12), ("Software", 0.08),
]
PRODUCT_WORDS = {
    "Electronics": ["Laptop Pro", "Ultra Phone", "4K Monitor", "Tablet Air", "Smart Watch", "Wireless Buds"],
    "Accessories": ["USB-C Hub", "Mechanical Keyboard", "Ergonomic Mouse", "Laptop Sleeve", "Charging Dock"],
    "Home": ["Smart Lamp", "Air Purifier", "Robot Vacuum", "Coffee Maker", "Bluetooth Speaker"],
    "Office": ["Standing Desk", "Office Chair", "Whiteboard", "Desk Organizer", "Label Printer"],
    "Software": ["Analytics Suite", "Security Suite", "Design Studio", "Backup Pro", "Developer IDE"],
}
CHANNELS = [("Web", 0.42), ("Mobile", 0.33), ("Store", 0.15), ("Partner", 0.10)]
STATUSES = [("Completed", 0.85), ("Cancelled", 0.07), ("Returned", 0.05), ("Pending", 0.03)]
LOG_PATHS = ["/", "/products", "/cart", "/checkout", "/orders", "/search", "/login", "/api/orders"]
LOG_METHODS = ["GET", "GET", "GET", "POST"]
HTTP_STATUS = [("200", 0.85), ("201", 0.03), ("304", 0.05), ("404", 0.04), ("500", 0.03)]
USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1) Mobile/15E148",
]


def weighted(rng: random.Random, pairs: list[tuple[str, float]]) -> str:
    values, weights = zip(*pairs)
    return rng.choices(values, weights=weights, k=1)[0]


def weighted_country(rng: random.Random) -> tuple[str, str]:
    country, region, _ = rng.choices(COUNTRIES, weights=[c[2] for c in COUNTRIES], k=1)[0]
    return country, region


def random_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


def write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def generate_products(rng: random.Random) -> list[dict]:
    products = []
    for product_id in range(1, N_PRODUCTS + 1):
        if product_id == 1:
            category, name = "Electronics", "Laptop Pro 15"
            price = 1499.0
        elif product_id == 2:
            category, name = "Electronics", "Ultra Phone X"
            price = 899.0
        else:
            category = weighted(rng, CATEGORIES)
            name = f"{rng.choice(PRODUCT_WORDS[category])} {rng.choice(['Lite', 'Plus', 'Max', 'Mini', '2025'])}"
            price = round(rng.uniform(15, 1600), 2)
        cost = round(price * rng.uniform(0.45, 0.78), 2)
        products.append({
            "product_id": product_id,
            "product_name": name,
            "category": category,
            "list_price": price,
            "cost": cost,
            "launch_date": random_date(rng, date(2022, 1, 1), date(2025, 6, 30)).isoformat(),
            "active": "true" if rng.random() > 0.08 else "false",
        })
    return products


def generate_customers(rng: random.Random) -> list[dict]:
    customers = []
    for customer_id in range(1, N_CUSTOMERS + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        country, region = weighted_country(rng)
        signup = random_date(rng, date(2023, 1, 1), date(2025, 9, 30))
        customers.append({
            "customer_id": customer_id,
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower().replace('-', '')}{customer_id}@example.com",
            "country": country,
            "region": region,
            "segment": weighted(rng, SEGMENTS),
            "signup_date": signup.isoformat(),
            "loyalty_tier": weighted(rng, TIERS),
        })
    return customers


def generate_orders(rng: random.Random, products: list[dict], customers: list[dict]) -> list[dict]:
    product_ids = [p["product_id"] for p in products]
    product_by_id = {p["product_id"]: p for p in products}
    customer_ids = [c["customer_id"] for c in customers]
    start = datetime(2025, 1, 1, 0, 0, 0)
    end = datetime(2025, 12, 31, 23, 59, 59)
    span_seconds = int((end - start).total_seconds())

    # Monthly weights produce a realistic Q4 peak.
    month_weights = [0.06, 0.06, 0.07, 0.07, 0.08, 0.08, 0.07, 0.07, 0.08, 0.09, 0.13, 0.14]
    months = [datetime(2025, m, 1) for m in range(1, 13)]

    orders = []
    for order_id in range(1, N_ORDERS + 1):
        # Deliberate skew: product 1 gets ~35% of orders, product 2 ~20%.
        draw = rng.random()
        if draw < 0.35:
            product_id = 1
        elif draw < 0.55:
            product_id = 2
        else:
            product_id = rng.choice(product_ids[2:])

        product = product_by_id[product_id]
        order_month = rng.choices(months, weights=month_weights, k=1)[0]
        month_days = (datetime(order_month.year + (order_month.month == 12), (order_month.month % 12) + 1, 1) - order_month).days
        ts = order_month + timedelta(
            days=rng.randint(0, month_days - 1),
            seconds=rng.randint(0, span_seconds) % 86_400,
        )
        quantity = rng.choices([1, 2, 3, 4, 5, 6, 8, 10], weights=[0.34, 0.24, 0.14, 0.10, 0.07, 0.05, 0.03, 0.03], k=1)[0]
        # ~1% of orders have no customer (guest checkout / stale reference).
        customer_id = "" if rng.random() < 0.01 else rng.choice(customer_ids)
        unit_price = round(product["list_price"] * rng.uniform(0.90, 1.10), 2)
        discount = rng.choices([0.0, 0.05, 0.10, 0.15, 0.20], weights=[0.55, 0.20, 0.14, 0.08, 0.03], k=1)[0]
        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": quantity,
            "unit_price": f"{unit_price:.2f}",
            "discount": f"{discount:.2f}",
            "status": weighted(rng, STATUSES),
            "channel": weighted(rng, CHANNELS),
        })
    return orders


def write_nested_json(path: Path, rng: random.Random, orders: list[dict], products: list[dict], customers: list[dict]) -> None:
    """Write JSON Lines with nested structs and arrays for JSON/streaming demos."""
    p_by_id = {p["product_id"]: p for p in products}
    c_by_id = {c["customer_id"]: c for c in customers}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for order in orders[:N_JSON_ORDERS]:
            customer = c_by_id.get(int(order["customer_id"])) if order["customer_id"] else None
            product = p_by_id[int(order["product_id"])]
            event = {
                "event_id": f"evt-{int(order['order_id']):07d}",
                "event_type": rng.choice(["order_created", "order_paid", "order_shipped"]),
                "event_time": order["order_ts"],
                "order": {
                    "order_id": int(order["order_id"]),
                    "status": order["status"],
                    "channel": order["channel"],
                    "items": [{
                        "product_id": product["product_id"],
                        "product_name": product["product_name"],
                        "category": product["category"],
                        "quantity": int(order["quantity"]),
                        "unit_price": float(order["unit_price"]),
                    }],
                },
                "customer": None if customer is None else {
                    "customer_id": customer["customer_id"],
                    "segment": customer["segment"],
                    "region": customer["region"],
                    "country": customer["country"],
                },
            }
            fh.write(json.dumps(event) + "\n")


def write_dirty_orders(path: Path, rng: random.Random, orders: list[dict]) -> None:
    """Take clean orders and inject realistic data-quality defects."""
    rows = [dict(o) for o in orders[:N_DIRTY_ORDERS]]
    # Duplicate 3% of the rows.
    for i in range(0, 150):
        rows.append(dict(rows[rng.randrange(len(rows))]))
    # Null / invalid values.
    for i in rng.sample(range(len(rows)), 120):
        rows[i]["customer_id"] = ""
    for i in rng.sample(range(len(rows)), 80):
        rows[i]["quantity"] = "-1"
    for i in rng.sample(range(len(rows)), 60):
        rows[i]["order_ts"] = "not-a-date"
    for i in rng.sample(range(len(rows)), 50):
        rows[i]["status"] = "UNKNOWN"
    for i in rng.sample(range(len(rows)), 40):
        rows[i]["unit_price"] = "N/A"
    for i in rng.sample(range(len(rows)), 30):
        rows[i]["discount"] = ""
    for i in rng.sample(range(len(rows)), 25):
        rows[i]["product_id"] = ""
    rng.shuffle(rows)
    write_csv(path, list(rows[0].keys()), [list(r.values()) for r in rows])


def write_access_log(path: Path, rng: random.Random) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    base = datetime(2025, 11, 1, 0, 0, 0)
    with path.open("w", encoding="utf-8") as fh:
        for _ in range(N_ACCESS_LOGS):
            ts = base + timedelta(seconds=rng.randint(0, 30 * 86_400))
            ip = f"192.168.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
            method = rng.choice(LOG_METHODS)
            path_value = rng.choice(LOG_PATHS)
            status = weighted(rng, HTTP_STATUS)
            size = rng.randint(120, 45_000)
            agent = rng.choice(USER_AGENTS)
            fh.write(
                f'{ip} - - [{ts.strftime("%d/%b/%Y:%H:%M:%S +0000")}] '
                f'"{method} {path_value} HTTP/1.1" {status} {size} "-" "{agent}"\n'
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    out = args.output
    rng = random.Random(SEED)

    products = generate_products(rng)
    customers = generate_customers(rng)
    orders = generate_orders(rng, products, customers)

    write_csv(out / "retail" / "products.csv", list(products[0].keys()), [list(p.values()) for p in products])
    write_csv(out / "retail" / "customers.csv", list(customers[0].keys()), [list(c.values()) for c in customers])
    write_csv(out / "retail" / "orders.csv", list(orders[0].keys()), [list(o.values()) for o in orders])
    write_nested_json(out / "retail" / "order_events.json", rng, orders, products, customers)
    write_dirty_orders(out / "quality" / "orders_dirty.csv", rng, orders)
    write_access_log(out / "logs" / "access.log", rng)

    streaming_dir = out / "streaming" / "events"
    streaming_dir.mkdir(parents=True, exist_ok=True)
    (streaming_dir / ".gitkeep").touch()

    print(f"Generated datasets under {out}:")
    print(f"  retail/products.csv       {N_PRODUCTS:,} rows")
    print(f"  retail/customers.csv      {N_CUSTOMERS:,} rows")
    print(f"  retail/orders.csv         {N_ORDERS:,} rows (product 1 ~35%, product 2 ~20%)")
    print(f"  retail/order_events.json  {N_JSON_ORDERS:,} nested JSON lines")
    print(f"  quality/orders_dirty.csv  ~{N_DIRTY_ORDERS + 150:,} rows with injected defects")
    print(f"  logs/access.log           {N_ACCESS_LOGS:,} lines")
    print(f"  streaming/events/         empty landing directory")


if __name__ == "__main__":
    main()
