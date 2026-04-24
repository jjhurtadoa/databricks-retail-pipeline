"""
Initialize the SQLite source database and create all transactional tables.
Run once to set up the source system. Safe to re-run (uses IF NOT EXISTS).

Usage:
    python src/generation/init_source_db.py
"""

import logging
import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id   TEXT PRIMARY KEY,
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT NOT NULL UNIQUE,
    city          TEXT NOT NULL,
    country       TEXT NOT NULL DEFAULT 'US',
    segment       TEXT NOT NULL CHECK (segment IN ('retail', 'wholesale', 'online')),
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    is_deleted    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS products (
    product_id    TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    category      TEXT NOT NULL,
    subcategory   TEXT,
    unit_price    REAL NOT NULL CHECK (unit_price > 0),
    cost_price    REAL NOT NULL CHECK (cost_price > 0),
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    is_deleted    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      TEXT PRIMARY KEY,
    customer_id   TEXT NOT NULL REFERENCES customers(customer_id),
    order_status  TEXT NOT NULL CHECK (order_status IN ('pending','paid','shipped','delivered','cancelled')),
    order_ts      TEXT NOT NULL,
    total_amount  REAL NOT NULL CHECK (total_amount >= 0),
    shipping_city TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    is_deleted    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id      TEXT NOT NULL REFERENCES orders(order_id),
    product_id    TEXT NOT NULL REFERENCES products(product_id),
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    unit_price    REAL NOT NULL CHECK (unit_price > 0),
    line_total    REAL NOT NULL CHECK (line_total > 0),
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    is_deleted    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_customers_updated_at    ON customers(updated_at);
CREATE INDEX IF NOT EXISTS idx_products_updated_at     ON products(updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at       ON orders(updated_at);
CREATE INDEX IF NOT EXISTS idx_order_items_updated_at  ON order_items(updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id      ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id    ON order_items(order_id);
"""


def init_db(db_path: str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(DDL)
        conn.commit()
        logger.info(f"Source database ready: {db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    db_path = os.getenv("SOURCE_DB_PATH", "data/source/retail_source.db")
    init_db(db_path)
