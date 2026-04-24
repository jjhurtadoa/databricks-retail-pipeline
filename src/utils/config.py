"""Configuration loader for source and Databricks settings."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")


class Config:
    """Configuration from environment variables."""

    # Source database (SQLite)
    SOURCE_DB_PATH = os.getenv("SOURCE_DB_PATH", "data/source/retail_source.db")

    # Raw output path — local JSONL staging area before upload to Databricks
    RAW_OUTPUT_PATH = os.getenv("RAW_OUTPUT_PATH", "data/raw")

    # Extraction batch size (rows per query)
    INGESTION_BATCH_SIZE = int(os.getenv("INGESTION_BATCH_SIZE", "500"))

    # Databricks (fill when workspace is ready)
    DATABRICKS_HOST         = os.getenv("DATABRICKS_HOST", "")
    DATABRICKS_TOKEN        = os.getenv("DATABRICKS_TOKEN", "")
    DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
