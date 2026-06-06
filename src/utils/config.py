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

    # Databricks connection
    DATABRICKS_HOST         = os.getenv("DATABRICKS_HOST", "")
    DATABRICKS_TOKEN        = os.getenv("DATABRICKS_TOKEN", "")
    DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

    # Unity Catalog — layer schemas
    UNITY_CATALOG    = os.getenv("UNITY_CATALOG", "retail")
    BRONZE_SCHEMA    = os.getenv("BRONZE_SCHEMA", "bronze")
    SILVER_SCHEMA    = os.getenv("SILVER_SCHEMA", "silver")
    GOLD_SCHEMA      = os.getenv("GOLD_SCHEMA", "gold")

    # Volume path for raw source JSONL (used by Bronze)
    # Strip dbfs: prefix — Spark/UC uses the /Volumes/... form directly
    VOLUME_SOURCE_PATH = os.getenv(
        "VOLUME_SOURCE_PATH",
        "/Volumes/retail/raw/source_data",
    )
