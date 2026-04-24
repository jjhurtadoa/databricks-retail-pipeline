"""
Automated scheduler for incremental source data generation.

Runs three job tiers on different frequencies:
  - orders job  : every 2 hours  (new orders + status advances + cancellations)
  - customers job: every 6 hours (above + new customers)
  - products job : every 24 hours (above + new products)

Usage:
    python src/generation/scheduler.py            # runs forever
    python src/generation/scheduler.py --once     # runs one full cycle immediately (for testing)
"""

import argparse
import logging
import os
from pathlib import Path

import schedule
import time

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

# Import generator functions
import sys
sys.path.insert(0, str(Path(__file__).parent))
from generate_changes import run_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_db_path() -> str:
    return os.getenv("SOURCE_DB_PATH", "data/source/retail_source.db")


# ── Job definitions ────────────────────────────────────────────────────────────

def orders_job() -> None:
    """Every 2 hours: new orders, status advances, cancellations. No new customers or products."""
    logger.info("Running orders job (2h tick)")
    run_batch(
        db_path=get_db_path(),
        new_customers=0,
        new_products=0,
        new_orders=15,
    )


def customers_job() -> None:
    """Every 6 hours: includes orders job + a few new customers."""
    logger.info("Running customers job (6h tick)")
    run_batch(
        db_path=get_db_path(),
        new_customers=3,
        new_products=0,
        new_orders=20,
    )


def products_job() -> None:
    """Every 24 hours: includes customers + orders + new products."""
    logger.info("Running products job (24h tick)")
    run_batch(
        db_path=get_db_path(),
        new_customers=5,
        new_products=2,
        new_orders=30,
    )


# ── Schedule setup ─────────────────────────────────────────────────────────────

def setup_schedule() -> None:
    # Orders: every 2 hours
    schedule.every(2).hours.do(orders_job)

    # Customers: every 6 hours
    schedule.every(6).hours.do(customers_job)

    # Products: every 24 hours
    schedule.every(24).hours.do(products_job)

    logger.info("Scheduler ready:")
    logger.info("  orders_job   → every 2 hours")
    logger.info("  customers_job → every 6 hours")
    logger.info("  products_job  → every 24 hours")


def run_once() -> None:
    """Run all jobs once immediately. Useful for testing."""
    logger.info("Running all jobs once (--once mode)")
    orders_job()
    customers_job()
    products_job()


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Source data generation scheduler.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run all job tiers once immediately and exit (for testing)",
    )
    args = parser.parse_args()

    if args.once:
        run_once()
    else:
        setup_schedule()
        logger.info("Scheduler running. Press Ctrl+C to stop.")
        while True:
            schedule.run_pending()
            time.sleep(30)
