"""
Periodic data generator for the retail transactional source.

Simulates a live OLTP system by:
  - Inserting new customers, products, orders, and order_items
  - Advancing order statuses over time (pending -> paid -> shipped -> delivered)
  - Updating customer attributes (segment, city)
  - Soft-deleting some pending orders (cancellations)

Each run represents one batch of transactional activity.
Run repeatedly on a schedule or manually to accumulate realistic history.

Usage:
    python src/generation/generate_changes.py
    python src/generation/generate_changes.py --new-orders 30
"""

import argparse
import logging
import os
import random
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Reference data ─────────────────────────────────────────────────────────────

FIRST_NAMES = ["Alice", "Bob", "Carlos", "Diana", "Eduardo", "Fatima",
               "George", "Hannah", "Ivan", "Julia", "Kevin", "Laura"]
LAST_NAMES  = ["Smith", "Johnson", "Garcia", "Martinez", "Brown",
               "Taylor", "Wilson", "Anderson", "Thomas", "Jackson"]
CITIES      = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
               "Dallas", "Austin", "San Diego", "Philadelphia", "San Antonio"]
SEGMENTS    = ["retail", "wholesale", "online"]
CATEGORIES  = {
    "Electronics": ["Phones", "Laptops", "Tablets", "Accessories"],
    "Clothing":    ["Shirts", "Pants", "Shoes", "Jackets"],
    "Food":        ["Snacks", "Beverages", "Dairy", "Bakery"],
    "Home":        ["Furniture", "Kitchen", "Bedding", "Decor"],
}

# Each key transitions to its value
STATUS_TRANSITIONS = {
    "pending":  "paid",
    "paid":     "shipped",
    "shipped":  "delivered",
}


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def new_id() -> str:
    return str(uuid.uuid4())


def existing_ids(conn: sqlite3.Connection, table: str, id_col: str) -> list[str]:
    return [r[0] for r in conn.execute(
        f"SELECT {id_col} FROM {table} WHERE is_deleted = 0"
    ).fetchall()]


# ── Inserts ────────────────────────────────────────────────────────────────────

def insert_customers(conn: sqlite3.Connection, n: int) -> None:
    ts = now_utc()
    rows = [
        (new_id(), random.choice(FIRST_NAMES), random.choice(LAST_NAMES),
         f"user_{new_id()[:8]}@example.com",
         random.choice(CITIES), "US", random.choice(SEGMENTS), ts, ts)
        for _ in range(n)
    ]
    conn.executemany(
        "INSERT INTO customers "
        "(customer_id, first_name, last_name, email, city, country, segment, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    logger.info(f"  Inserted {n} customers")


def insert_products(conn: sqlite3.Connection, n: int) -> None:
    ts = now_utc()
    rows = []
    for _ in range(n):
        cat   = random.choice(list(CATEGORIES))
        sub   = random.choice(CATEGORIES[cat])
        cost  = round(random.uniform(5.0, 300.0), 2)
        price = round(cost * random.uniform(1.2, 2.5), 2)
        rows.append((new_id(), f"{sub} {new_id()[:6]}", cat, sub, price, cost, ts, ts))
    conn.executemany(
        "INSERT INTO products "
        "(product_id, name, category, subcategory, unit_price, cost_price, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    logger.info(f"  Inserted {n} products")


def insert_orders(conn: sqlite3.Connection, n: int) -> None:
    ts          = now_utc()
    customer_ids = existing_ids(conn, "customers", "customer_id")
    product_ids  = existing_ids(conn, "products", "product_id")

    if not customer_ids or not product_ids:
        logger.warning("  No customers or products available — skipping order insert")
        return

    orders_inserted = 0
    items_inserted  = 0

    for _ in range(n):
        oid   = new_id()
        cid   = random.choice(customer_ids)
        total = 0.0
        items = []

        for _ in range(random.randint(1, 5)):
            pid   = random.choice(product_ids)
            price = conn.execute(
                "SELECT unit_price FROM products WHERE product_id = ?", (pid,)
            ).fetchone()[0]
            qty   = random.randint(1, 10)
            lt    = round(price * qty, 2)
            total += lt
            items.append((new_id(), oid, pid, qty, price, lt, ts, ts))

        conn.execute(
            "INSERT INTO orders "
            "(order_id, customer_id, order_status, order_ts, total_amount, shipping_city, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (oid, cid, "pending", ts, round(total, 2), random.choice(CITIES), ts, ts),
        )
        conn.executemany(
            "INSERT INTO order_items "
            "(order_item_id, order_id, product_id, quantity, unit_price, line_total, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            items,
        )
        orders_inserted += 1
        items_inserted  += len(items)

    logger.info(f"  Inserted {orders_inserted} orders with {items_inserted} order items")


# ── Updates ────────────────────────────────────────────────────────────────────

def advance_order_statuses(conn: sqlite3.Connection, fraction: float = 0.3) -> None:
    """Move a fraction of transitional orders to the next status."""
    ts      = now_utc()
    updated = 0
    for current, next_status in STATUS_TRANSITIONS.items():
        rows   = conn.execute(
            "SELECT order_id FROM orders WHERE order_status = ? AND is_deleted = 0",
            (current,),
        ).fetchall()
        subset = random.sample(rows, max(1, int(len(rows) * fraction))) if rows else []
        for (oid,) in subset:
            conn.execute(
                "UPDATE orders SET order_status = ?, updated_at = ? WHERE order_id = ?",
                (next_status, ts, oid),
            )
            updated += 1
    logger.info(f"  Advanced {updated} order statuses")


def update_customer_segments(conn: sqlite3.Connection, n: int = 3) -> None:
    """Randomly change segment or city for a few customers (simulates SCD2 trigger)."""
    ts   = now_utc()
    ids  = existing_ids(conn, "customers", "customer_id")
    for cid in random.sample(ids, min(n, len(ids))):
        conn.execute(
            "UPDATE customers SET segment = ?, city = ?, updated_at = ? WHERE customer_id = ?",
            (random.choice(SEGMENTS), random.choice(CITIES), ts, cid),
        )
    logger.info(f"  Updated up to {n} customer records")


def cancel_orders(conn: sqlite3.Connection, n: int = 2) -> None:
    """Soft-delete a small number of pending orders (cancellations)."""
    ts   = now_utc()
    rows = conn.execute(
        "SELECT order_id FROM orders WHERE order_status = 'pending' AND is_deleted = 0"
    ).fetchall()
    for (oid,) in random.sample(rows, min(n, len(rows))):
        conn.execute(
            "UPDATE orders SET is_deleted = 1, order_status = 'cancelled', updated_at = ? WHERE order_id = ?",
            (ts, oid),
        )
    logger.info(f"  Cancelled up to {n} orders (soft-delete)")


# ── Main batch ─────────────────────────────────────────────────────────────────

def run_batch(
    db_path: str,
    new_customers: int = 3,
    new_products: int  = 2,
    new_orders: int    = 15,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        with conn:
            insert_customers(conn, new_customers)
            insert_products(conn, new_products)
            insert_orders(conn, new_orders)
            advance_order_statuses(conn)
            update_customer_segments(conn)
            cancel_orders(conn)
        logger.info("Batch complete")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate incremental transactional data in the SQLite source.")
    parser.add_argument("--db-path",       default=None,  help="Override SOURCE_DB_PATH from .env")
    parser.add_argument("--new-customers", type=int, default=3)
    parser.add_argument("--new-products",  type=int, default=2)
    parser.add_argument("--new-orders",    type=int, default=15)
    args = parser.parse_args()

    db = args.db_path or os.getenv("SOURCE_DB_PATH", "data/source/retail_source.db")
    run_batch(db, args.new_customers, args.new_products, args.new_orders)
