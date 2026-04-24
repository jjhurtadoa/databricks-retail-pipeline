"""
Quick database viewer — prints row counts and sample rows for all source tables.

Usage:
    python src/generation/view_db.py
"""

import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")


def view(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    tables = ["customers", "products", "orders", "order_items"]

    for table in tables:
        count = conn.execute(f"SELECT COUNT(1) FROM {table}").fetchone()[0]
        rows  = conn.execute(f"SELECT * FROM {table} LIMIT 5").fetchall()
        print(f"\n{'='*60}")
        print(f"  {table.upper()}  ({count} total rows)")
        print(f"{'='*60}")
        if rows:
            print("  " + " | ".join(rows[0].keys()))
            print("  " + "-" * 56)
            for r in rows:
                print("  " + " | ".join(str(v) for v in tuple(r)))
        else:
            print("  (empty)")

    conn.close()


if __name__ == "__main__":
    db_path = os.getenv("SOURCE_DB_PATH", "data/source/retail_source.db")
    view(db_path)
