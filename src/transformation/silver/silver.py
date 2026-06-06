"""
Silver pipeline — conformed layer.

Reads from Bronze Delta tables, applies type casting, deduplication, MERGE upserts,
and quality checks. Silver is the system of record for business semantics.

Entry point for Databricks Jobs: run `silver.py` as a Python task.
"""

import sys
from typing import List

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.config import Config
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Table configuration
# Each table defines bronze source, silver target, PK, and key columns for validation.
# ---------------------------------------------------------------------------
TABLES: List[dict] = [
    {
        "bronze_table": "bronze.customers",
        "silver_table": "silver.customers",
        "primary_key": "customer_id",
        "key_columns": ["customer_id", "email", "created_at"],
    },
    {
        "bronze_table": "bronze.orders",
        "silver_table": "silver.orders",
        "primary_key": "order_id",
        "key_columns": ["order_id", "customer_id", "order_ts", "order_status", "created_at"],
    },
    {
        "bronze_table": "bronze.order_items",
        "silver_table": "silver.order_items",
        "primary_key": "order_item_id",
        "key_columns": ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
    },
    {
        "bronze_table": "bronze.products",
        "silver_table": "silver.products",
        "primary_key": "product_id",
        "key_columns": ["product_id", "name", "cost_price", "unit_price", "category", "subcategory", "created_at"],
    },
]


# ---------------------------------------------------------------------------
# DDL — Silver table schemas
# ---------------------------------------------------------------------------

def create_silver_tables(spark: SparkSession) -> None:
    """Create target Silver tables if they don't exist."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.customers (
            _extracted_at  TIMESTAMP,
            _source_table  STRING,
            city           STRING,
            country        STRING,
            created_at     TIMESTAMP,
            customer_id    STRING,
            email          STRING,
            first_name     STRING,
            is_deleted     BOOLEAN,
            last_name      STRING,
            segment        STRING,
            updated_at     TIMESTAMP
        ) USING DELTA
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.orders (
            _extracted_at  TIMESTAMP,
            _source_table  STRING,
            created_at     TIMESTAMP,
            customer_id    STRING,
            is_deleted     BOOLEAN,
            order_id       STRING,
            order_status   STRING,
            order_ts       TIMESTAMP,
            shipping_city  STRING,
            total_amount   DOUBLE,
            updated_at     TIMESTAMP
        ) USING DELTA
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.order_items (
            _extracted_at  TIMESTAMP,
            _source_table  STRING,
            created_at     TIMESTAMP,
            is_deleted     BOOLEAN,
            line_total     DOUBLE,
            order_id       STRING,
            order_item_id  STRING,
            product_id     STRING,
            quantity       BIGINT,
            unit_price     DOUBLE,
            updated_at     TIMESTAMP
        ) USING DELTA
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.products (
            _extracted_at  TIMESTAMP,
            _source_table  STRING,
            category       STRING,
            cost_price     DOUBLE,
            created_at     TIMESTAMP,
            is_deleted     BOOLEAN,
            name           STRING,
            product_id     STRING,
            subcategory    STRING,
            unit_price     DOUBLE,
            updated_at     TIMESTAMP
        ) USING DELTA
    """)

    logger.info("Silver tables created/verified")


# ---------------------------------------------------------------------------
# Transformation helpers
# ---------------------------------------------------------------------------

def cast_boolean_columns(df: DataFrame, columns: List[str] = None) -> DataFrame:
    """Cast specified columns to boolean type."""
    if columns is None:
        columns = ["is_deleted"]

    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("boolean"))
    return df


def deduplicate(df: DataFrame, primary_key: str) -> DataFrame:
    """Keep the latest record per primary key.
    
    Tie-breaker: updated_at DESC, then _extracted_at DESC.
    """
    window = Window.partitionBy(primary_key).orderBy(
        F.col("updated_at").desc(),
        F.col("_extracted_at").desc()
    )
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def upsert_to_silver(df_source: DataFrame, silver_table: str, primary_key: str) -> None:
    """MERGE deduped source into Silver table.
    
    - Match found + record is newer  → UPDATE all columns
    - No match + record is active    → INSERT
    """
    col_mapping = {col: f"source.{col}" for col in df_source.columns}
    delta_table = DeltaTable.forName(spark, silver_table)

    (
        delta_table.alias("target")
        .merge(
            df_source.alias("source"),
            f"target.{primary_key} = source.{primary_key}"
        )
        .whenMatchedUpdate(
            condition="source.updated_at > target.updated_at",
            set=col_mapping
        )
        .whenNotMatchedInsert(
            condition="source.is_deleted = false",
            values=col_mapping
        )
        .execute()
    )


def validate_silver(spark: SparkSession, silver_table: str, primary_key: str, key_columns: List[str]) -> bool:
    """Run quality checks on a Silver table.
    
    Checks:
    1. Duplicates on primary key
    2. Nulls in key columns
    3. Record count summary (total / active / deleted)
    
    Returns True if all checks pass.
    """
    df = spark.table(silver_table)
    table_name = silver_table.split(".")[-1].upper()
    all_ok = True

    logger.info("")
    logger.info("=" * 50)
    logger.info("Validation — %s", table_name)
    logger.info("=" * 50)

    # 1. Duplicates
    dup_count = df.groupBy(primary_key).count().filter("count > 1").count()
    if dup_count > 0:
        logger.warning("Duplicates on %s: %d found", primary_key, dup_count)
        all_ok = False
    else:
        logger.info("Duplicates check OK")

    # 2. Nulls on key columns
    for col in key_columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            logger.warning("Nulls in %s: %d found", col, null_count)
            all_ok = False
        else:
            logger.info("Nulls check OK: %s", col)

    # 3. Summary
    total = df.count()
    deleted = df.filter(F.col("is_deleted")).count()
    active = total - deleted
    logger.info("Total records  : %d", total)
    logger.info("Active records : %d", active)
    logger.info("Deleted records: %d", deleted)

    return all_ok


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run(spark: SparkSession) -> None:
    """Execute the full Silver pipeline: read → cast → dedup → upsert → validate."""
    catalog = Config.UNITY_CATALOG
    silver_schema = Config.SILVER_SCHEMA

    logger.info("=== Silver pipeline START ===")
    logger.info("Catalog: %s | Schema: %s", catalog, silver_schema)

    # Set context
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
    spark.sql(f"USE SCHEMA {silver_schema}")

    # Create target tables
    create_silver_tables(spark)

    # Process each table
    all_ok = True
    for tbl_config in TABLES:
        bronze_table = f"bronze.{tbl_config['bronze_table'].split('.')[-1]}"
        silver_table = f"{silver_schema}.{tbl_config['silver_table'].split('.')[-1]}"
        primary_key = tbl_config["primary_key"]
        key_columns = tbl_config["key_columns"]

        logger.info("")
        logger.info("Processing %s...", silver_table)

        # Read Bronze
        df_bronze = spark.table(bronze_table)
        bronze_count = df_bronze.count()
        logger.info("  Bronze records: %d", bronze_count)

        # Cast types
        df_cast = cast_boolean_columns(df_bronze)

        # Dedup
        df_deduped = deduplicate(df_cast, primary_key)
        dedup_count = df_deduped.count()
        logger.info("  After dedup   : %d", dedup_count)

        # Upsert
        upsert_to_silver(df_deduped, silver_table, primary_key)
        logger.info("  MERGE complete")

        # Validate
        passed = validate_silver(spark, silver_table, primary_key, key_columns)
        if not passed:
            all_ok = False

    if not all_ok:
        raise RuntimeError("Silver pipeline completed with validation errors. Check logs above.")

    logger.info("")
    logger.info("=== Silver pipeline END — all checks passed ===")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        spark  # noqa: F821 — available in Databricks runtime
    except NameError:
        logger.error("No SparkSession found. This module must run inside Databricks.")
        sys.exit(1)

    run(spark)  # noqa: F821
