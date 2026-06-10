"""
Bronze pipeline — raw ingestion layer.

Reads JSONL from the Unity Catalog Volume, normalizes timestamps,
and appends records to Bronze Delta tables. No business logic lives here.

Entry point for Databricks Jobs: run `bronze.py` as a Python task.
"""

import sys
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Utilities shared across all pipeline layers
from src.utils.config import Config
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Table configuration
# Each entry defines the source sub-folder name, the target Bronze table,
# and the primary key column used for null validation.
# ---------------------------------------------------------------------------
TABLES: List[dict] = [
    {"source": "customers", "table": "customers", "pk": "customer_id"},
    {"source": "products", "table": "products", "pk": "product_id"},
    {"source": "orders", "table": "orders", "pk": "order_id"},
    {"source": "order_items", "table": "order_items", "pk": "order_item_id"},
]

# Timestamp columns present across source tables — normalized to TimestampType
_TIMESTAMP_COLS = ["_extracted_at", "created_at", "updated_at", "order_ts"]


# ---------------------------------------------------------------------------
# Transformation helpers
# ---------------------------------------------------------------------------


def normalize_timestamps(df: DataFrame) -> DataFrame:
    """Cast known timestamp columns from string to TimestampType.

    Only columns that exist in the DataFrame are processed; others are skipped.
    """
    for col in _TIMESTAMP_COLS:
        if col in df.columns:
            df = df.withColumn(col, F.to_timestamp(col))
    return df


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def read_source(spark: SparkSession, source_path: str, table_name: str) -> DataFrame:
    """Read all JSONL files from a Volume sub-folder into a DataFrame."""
    logger.info("Reading source: %s -> %s", source_path, table_name)
    df = spark.read.json(source_path)
    logger.info("  Rows read: %d", df.count())
    return df


def write_bronze(df: DataFrame, full_table_name: str) -> None:
    """Append DataFrame to a Bronze Delta table (append preserves history)."""
    logger.info("Writing to %s ...", full_table_name)
    df.write.format("delta").mode("append").saveAsTable(full_table_name)
    logger.info("  Write complete.")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_bronze(spark: SparkSession) -> bool:
    """Check primary key nulls on all Bronze tables.

    Returns True when all checks pass; raises RuntimeError on critical failure.
    """
    schema = Config.BRONZE_SCHEMA
    all_ok = True

    for tbl in TABLES:
        full_name = f"{schema}.{tbl['table']}"
        pk = tbl["pk"]

        null_count = spark.sql(
            f"SELECT COUNT(*) AS n FROM {full_name} WHERE {pk} IS NULL"
        ).collect()[0]["n"]

        if null_count > 0:
            logger.error(
                "PK null check FAILED: %s.%s has %d null PKs", full_name, pk, null_count
            )
            all_ok = False
        else:
            logger.info("PK null check OK: %s", full_name)

    return all_ok


def reconcile_counts(spark: SparkSession, base_path: str) -> None:
    """Log source file count vs Bronze table count for each table.

    This is an informational check — a mismatch signals re-ingestion may be needed.
    """
    schema = Config.BRONZE_SCHEMA

    for tbl in TABLES:
        source_path = f"{base_path}/{tbl['source']}"
        full_name = f"{schema}.{tbl['table']}"

        source_count = spark.sql(
            f"SELECT COUNT(*) AS n FROM json.`{source_path}`"
        ).collect()[0]["n"]
        bronze_count = spark.sql(f"SELECT COUNT(*) AS n FROM {full_name}").collect()[0][
            "n"
        ]

        status = "OK" if bronze_count >= source_count else "MISMATCH"
        logger.info(
            "Reconciliation [%s] %s — source: %d | bronze: %d",
            status,
            tbl["table"],
            source_count,
            bronze_count,
        )


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------


def run(spark: SparkSession) -> None:
    """Execute the full Bronze pipeline: read -> normalize -> write -> validate."""
    catalog = Config.UNITY_CATALOG
    schema = Config.BRONZE_SCHEMA
    base_path = Config.VOLUME_SOURCE_PATH

    logger.info("=== Bronze pipeline START ===")
    logger.info("Catalog: %s | Schema: %s | Source: %s", catalog, schema, base_path)

    # Activate catalog and create schema if it doesn't exist
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"USE SCHEMA {schema}")

    # Process each table: read, normalize, write
    for tbl in TABLES:
        source_path = f"{base_path}/{tbl['source']}"
        full_table_name = f"{schema}.{tbl['table']}"

        df = read_source(spark, source_path, tbl["table"])
        df = normalize_timestamps(df)
        write_bronze(df, full_table_name)

    # Post-load checks
    logger.info("--- Running validation checks ---")
    passed = validate_bronze(spark)

    logger.info("--- Running source-to-bronze reconciliation ---")
    reconcile_counts(spark, base_path)

    if not passed:
        raise RuntimeError(
            "Bronze pipeline completed with validation errors. Check logs above."
        )

    logger.info("=== Bronze pipeline END — all checks passed ===")


# ---------------------------------------------------------------------------
# Entry point — used when running as a Databricks Jobs Python task
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # In Databricks, `spark` is injected into the global scope automatically.
    # This guard allows running the file as a standalone task.
    try:
        spark  # noqa: F821 — available in Databricks runtime
    except NameError:
        logger.error("No SparkSession found. This module must run inside Databricks.")
        sys.exit(1)

    run(spark)  # noqa: F821
