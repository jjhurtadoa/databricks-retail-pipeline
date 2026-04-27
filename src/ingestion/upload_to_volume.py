"""
Incremental upload to Databricks Volume.

Tracks which JSONL files have already been uploaded using data/upload_state.json.
Only uploads new files, never re-uploads existing ones.

This enables true incremental Bronze append: each run adds only new batch files.

Usage:
    python src/ingestion/upload_to_volume.py           # upload new files only
    python src/ingestion/upload_to_volume.py --full    # re-upload everything
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TABLES = ["customers", "products", "orders", "order_items"]
UPLOAD_STATE_FILE = Path("data/upload_state.json")


# ── State helpers ──────────────────────────────────────────────────────────────

def load_upload_state() -> set:
    """Return set of already-uploaded relative file paths."""
    if UPLOAD_STATE_FILE.exists():
        return set(json.loads(UPLOAD_STATE_FILE.read_text()))
    return set()


def save_upload_state(uploaded: set) -> None:
    UPLOAD_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    UPLOAD_STATE_FILE.write_text(json.dumps(sorted(uploaded), indent=2))


# ── Upload helpers ─────────────────────────────────────────────────────────────

def upload_file(local_path: Path, remote_path: str, profile: str) -> bool:
    """Upload a single file using Databricks CLI. Returns True on success."""
    # Use Databricks CLI profile from ~/.databrickscfg and ignore placeholder env vars.
    clean_env = os.environ.copy()
    clean_env.pop("DATABRICKS_HOST", None)
    clean_env.pop("DATABRICKS_TOKEN", None)
    clean_env.pop("DATABRICKS_ACCOUNT_ID", None)

    result = subprocess.run(
        [
            "databricks",
            "--profile",
            profile,
            "fs",
            "cp",
            str(local_path),
            remote_path,
            "--overwrite",
        ],
        capture_output=True,
        text=True,
        env=clean_env,
    )
    if result.returncode != 0:
        logger.error("  Failed to upload %s: %s", local_path, result.stderr.strip())
        return False
    return True


# ── Main upload ────────────────────────────────────────────────────────────────

def run_upload(raw_path: str, volume_path: str, profile: str, full: bool = False) -> int:
    raw_dir = Path(raw_path)
    uploaded = set() if full else load_upload_state()

    new_uploads = set()
    total = 0
    failed = 0

    for table in TABLES:
        table_dir = raw_dir / table
        if not table_dir.exists():
            logger.warning("  %s: local folder not found, skipping", table)
            continue

        jsonl_files = sorted(table_dir.glob("*.jsonl"))

        for file in jsonl_files:
            relative = f"{table}/{file.name}"

            if relative in uploaded:
                logger.debug("  %s: already uploaded, skipping", relative)
                continue

            remote = f"{volume_path}/{table}/{file.name}"
            logger.info("  Uploading %s → %s", relative, remote)

            if upload_file(file, remote, profile):
                new_uploads.add(relative)
                total += 1
            else:
                logger.error("  Aborting upload for %s", relative)
                failed += 1

    if new_uploads:
        save_upload_state(uploaded | new_uploads)
        logger.info("Upload complete: %d new file(s) uploaded", total)
        logger.info("Upload state saved → %s", UPLOAD_STATE_FILE)
    else:
        logger.info("Nothing new to upload.")

    if failed > 0:
        logger.error("Upload finished with %d failed file(s)", failed)
        return 1

    return 0


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload new raw JSONL files to Databricks Volume.")
    parser.add_argument("--raw-path",    default=None, help="Override RAW_OUTPUT_PATH from .env")
    parser.add_argument("--volume-path", default=None, help="Override VOLUME_BRONZE_PATH from .env")
    parser.add_argument("--profile",     default=None, help="Databricks CLI profile name (default: DATABRICKS_PROFILE or DEFAULT)")
    parser.add_argument("--full", action="store_true", help="Re-upload all files ignoring upload state")
    args = parser.parse_args()

    raw_path    = args.raw_path    or os.getenv("RAW_OUTPUT_PATH",    "data/raw")
    volume_path = args.volume_path or os.getenv("VOLUME_BRONZE_PATH", "dbfs:/Volumes/main/retail/bronze/raw")
    profile     = args.profile     or os.getenv("DATABRICKS_PROFILE", "DEFAULT")

    exit_code = run_upload(raw_path, volume_path, profile=profile, full=args.full)
    sys.exit(exit_code)
