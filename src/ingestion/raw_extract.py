"""
Bronze extraction: SQLite → local JSONL files (data/raw/).

Incremental strategy: watermark on updated_at.
- First run: extracts ALL rows (watermark defaults to epoch).
- Subsequent runs: extracts only rows changed since last run.

Watermark state is persisted in data/raw/watermarks.json.

Usage:
    python src/ingestion/bronze_extract.py           # incremental
    python src/ingestion/bronze_extract.py --full    # force full reload
"""

import argparse
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TABLES = ["customers", "products", "orders", "order_items"]
EPOCH  = "1970-01-01T00:00:00"


# ── Watermark helpers ──────────────────────────────────────────────────────────

def load_watermarks(watermark_file: Path) -> dict:
    """Load last extracted timestamp per table. Returns epoch if file missing."""
    if watermark_file.exists():
        return json.loads(watermark_file.read_text())
    return {t: EPOCH for t in TABLES}


def save_watermarks(watermark_file: Path, watermarks: dict) -> None:
    watermark_file.parent.mkdir(parents=True, exist_ok=True)
    watermark_file.write_text(json.dumps(watermarks, indent=2))


# ── Extraction ─────────────────────────────────────────────────────────────────

def extract_table(
    conn: sqlite3.Connection,
    table: str,
    since: str,
    out_dir: Path,
) -> int:
    """
    Extract rows updated after `since`, write to a timestamped JSONL file.
    Returns the number of rows extracted.
    """
    # Watermark query — this is the core incremental load pattern
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE updated_at > ? ORDER BY updated_at",
        (since,),
    ).fetchall()

    if not rows:
        logger.info("  %s: no new rows since %s", table, since)
        return 0

    # Add extraction metadata to every record for lineage tracking
    extracted_at = datetime.now(timezone.utc).isoformat()
    batch_file   = out_dir / f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.jsonl"
    out_dir.mkdir(parents=True, exist_ok=True)

    with batch_file.open("w") as f:
        for row in rows:
            record = dict(row)
            record["_extracted_at"] = extracted_at   # when we pulled it
            record["_source_table"] = table           # which table it came from
            f.write(json.dumps(record) + "\n")

    logger.info("  %s: %d rows → %s", table, len(rows), batch_file)
    return len(rows)


# ── Main run ───────────────────────────────────────────────────────────────────

def run_extraction(db_path: str, raw_path: str, full: bool = False) -> None:
    raw_dir        = Path(raw_path)
    watermark_file = raw_dir / "watermarks.json"

    watermarks = {t: EPOCH for t in TABLES} if full else load_watermarks(watermark_file)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    new_watermarks = dict(watermarks)

    try:
        for table in TABLES:
            since   = watermarks[table]
            out_dir = raw_dir / table

            count = extract_table(conn, table, since, out_dir)

            if count > 0:
                # Advance watermark to the max updated_at in this table
                max_ts = conn.execute(f"SELECT MAX(updated_at) FROM {table}").fetchone()[0]
                new_watermarks[table] = max_ts
    finally:
        conn.close()

    save_watermarks(watermark_file, new_watermarks)
    logger.info("Watermarks saved → %s", watermark_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract SQLite source tables to local JSONL files.")
    parser.add_argument("--db-path",  default=None,  help="Override SOURCE_DB_PATH from .env")
    parser.add_argument("--raw-path", default=None,  help="Override RAW_OUTPUT_PATH from .env")
    parser.add_argument("--full",     action="store_true", help="Force full reload (ignore watermarks)")
    args = parser.parse_args()

    db_path  = args.db_path  or os.getenv("SOURCE_DB_PATH",  "data/source/retail_source.db")
    raw_path = args.raw_path or os.getenv("RAW_OUTPUT_PATH", "data/raw")

    run_extraction(db_path, raw_path, full=args.full)
