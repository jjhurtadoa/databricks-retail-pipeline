"""
Gold pipeline — dimensional model layer.

Builds a star schema from Silver tables: dimensional tables (dim_date, dim_customer, dim_product)
and a fact table (fact_order_line) with referential integrity checks, performance tuning,
and KPI business views.

Entry point for Databricks Jobs: run `gold.py` as a Python task.
"""

import datetime
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
# DDL — Gold table schemas
# ---------------------------------------------------------------------------

def create_gold_tables(spark: SparkSession) -> None:
    """Create target Gold tables if they don't exist."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.dim_customer (
            customer_id  STRING      NOT NULL,
            first_name   STRING,
            last_name    STRING,
            email        STRING,
            segment      STRING,
            city         STRING,
            country      STRING,
            is_active    BOOLEAN,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP
        ) USING DELTA
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.dim_product (
            product_id   STRING      NOT NULL,
            name         STRING,
            category     STRING,
            subcategory  STRING,
            unit_price   DOUBLE,
            cost_price   DOUBLE,
            is_active    BOOLEAN,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP
        ) USING DELTA
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.dim_date (
            date_id        INT         NOT NULL,
            full_date      DATE,
            year           INT,
            quarter        INT,
            month          INT,
            month_name     STRING,
            week_of_year   INT,
            day_of_month   INT,
            day_of_week    INT,
            day_name       STRING,
            is_weekend     BOOLEAN,
            is_month_end   BOOLEAN
        ) USING DELTA
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.fact_order_line (
            order_item_id  STRING      NOT NULL,
            order_id       STRING,
            date_id        INT,
            customer_id    STRING,
            product_id     STRING,
            order_status   STRING,
            shipping_city  STRING,
            quantity       LONG,
            unit_price     DOUBLE,
            line_total     DOUBLE,
            cost_price     DOUBLE,
            gross_margin   DOUBLE,
            order_ts       TIMESTAMP,
            _extracted_at  TIMESTAMP
        ) USING DELTA
    """)

    logger.info("Gold tables created/verified")


# ---------------------------------------------------------------------------
# Transformation and merge helpers
# ---------------------------------------------------------------------------

def upsert_to_gold(spark: SparkSession, df_source: DataFrame, gold_table: str, primary_key: str) -> None:
    """SCD Type 1 MERGE into a Gold table.
    
    - Match found  → overwrite all columns (except PK excluded from update set)
    - No match     → insert new row
    """
    # Build update mapping: exclude primary key from SET clause
    update_mapping = {col: f"source.{col}" for col in df_source.columns if col != primary_key}
    insert_mapping = {col: f"source.{col}" for col in df_source.columns}

    delta_table = DeltaTable.forName(spark, gold_table)

    (
        delta_table.alias("target")
        .merge(
            df_source.alias("source"),
            f"target.{primary_key} = source.{primary_key}"
        )
        .whenMatchedUpdate(set=update_mapping)
        .whenNotMatchedInsert(values=insert_mapping)
        .execute()
    )


def validate_gold(spark: SparkSession, gold_table: str, primary_key: str, label: str) -> bool:
    """Basic quality checks on a Gold table.
    
    Checks:
    1. Duplicates on primary key
    2. Nulls on primary key
    3. Row count summary
    
    Returns True if all checks pass.
    """
    df = spark.table(gold_table)
    all_ok = True

    logger.info("")
    logger.info("=" * 50)
    logger.info("Validation — %s", label)
    logger.info("=" * 50)

    # Duplicates
    dup_count = df.groupBy(primary_key).count().filter("count > 1").count()
    if dup_count > 0:
        logger.warning("Duplicates on %s: %d found", primary_key, dup_count)
        all_ok = False
    else:
        logger.info("Duplicates check OK")

    # Nulls on PK
    null_count = df.filter(F.col(primary_key).isNull()).count()
    if null_count > 0:
        logger.warning("Nulls on %s: %d found", primary_key, null_count)
        all_ok = False
    else:
        logger.info("Nulls check OK")

    # Row count
    row_count = df.count()
    logger.info("Total rows: %d", row_count)

    return all_ok


def validate_foreign_keys(spark: SparkSession, fact_table: str, fk_rules: List[dict], label: str = "FACT") -> bool:
    """Check foreign key constraints in fact table.
    
    fk_rules: list of dicts:
      {
        "fact_key": "customer_id",
        "dim_table": "gold.dim_customer",
        "dim_key": "customer_id",
        "rule_name": "customer_fk"
      }
    
    Returns True if no orphans found.
    """
    logger.info("")
    logger.info("=" * 50)
    logger.info("FK Validation — %s", label)
    logger.info("=" * 50)

    all_ok = True
    for rule in fk_rules:
        fact_key = rule["fact_key"]
        dim_table = rule["dim_table"]
        dim_key = rule["dim_key"]
        rule_name = rule.get("rule_name", f"{fact_key}->{dim_table}.{dim_key}")

        orphan_count = spark.sql(f"""
            SELECT COUNT(*) AS orphan_count
            FROM {fact_table} f
            LEFT JOIN {dim_table} d
              ON f.{fact_key} = d.{dim_key}
            WHERE d.{dim_key} IS NULL
        """).collect()[0]["orphan_count"]

        if orphan_count > 0:
            logger.warning("%s: %d orphan(s) found", rule_name, orphan_count)
            all_ok = False
        else:
            logger.info("%s: OK", rule_name)

    return all_ok


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def build_dim_date(spark: SparkSession) -> None:
    """Build static date dimension (2020-01-01 to 2030-12-31).
    
    date_id format: YYYYMMDD (INT) — industry standard for fast joins.
    """
    logger.info("Building dim_date...")

    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2030, 12, 31)
    total_days = (end_date - start_date).days + 1

    df_date = (
        spark.range(total_days)
        .withColumn("full_date", F.date_add(F.lit(str(start_date)), F.col("id").cast("int")))
        .withColumn("date_id", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
        .withColumn("is_weekend", F.dayofweek(F.col("full_date")).isin([1, 7]))
        .withColumn("is_month_end", F.col("full_date") == F.last_day(F.col("full_date")))
        .drop("id")
    )

    # Overwrite — static dimension rebuilt each run
    (
        df_date.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold.dim_date")
    )

    logger.info("  Rows: %d (2020-2030)", df_date.count())
    validate_gold(spark, "gold.dim_date", "date_id", "DIM_DATE")


def build_dim_customer(spark: SparkSession) -> None:
    """Build customer dimension from Silver.
    
    SCD Type 1: overwrite on match. is_active = NOT is_deleted.
    """
    logger.info("Building dim_customer...")

    df_dim = (
        spark.table("silver.customers")
        .select(
            F.col("customer_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("email"),
            F.col("segment"),
            F.col("city"),
            F.col("country"),
            (~F.col("is_deleted")).alias("is_active"),
            F.col("created_at"),
            F.col("updated_at")
        )
    )

    upsert_to_gold(spark, df_dim, "gold.dim_customer", "customer_id")
    logger.info("  MERGE complete")
    validate_gold(spark, "gold.dim_customer", "customer_id", "DIM_CUSTOMER")


def build_dim_product(spark: SparkSession) -> None:
    """Build product dimension from Silver.
    
    SCD Type 1. Keeps category and subcategory (no separate dim_category).
    unit_price here is current price; sale price lives in fact.
    """
    logger.info("Building dim_product...")

    df_dim = (
        spark.table("silver.products")
        .select(
            F.col("product_id"),
            F.col("name"),
            F.col("category"),
            F.col("subcategory"),
            F.col("unit_price"),
            F.col("cost_price"),
            (~F.col("is_deleted")).alias("is_active"),
            F.col("created_at"),
            F.col("updated_at")
        )
    )

    upsert_to_gold(spark, df_dim, "gold.dim_product", "product_id")
    logger.info("  MERGE complete")
    validate_gold(spark, "gold.dim_product", "product_id", "DIM_PRODUCT")


def build_fact_order_line(spark: SparkSession) -> None:
    """Build fact table from Silver.
    
    Grain: one row per order_item_id.
    
    JOINs:
    - order_items → orders (for order context)
    - order_items → products (for cost/margin calculation, LEFT to keep deleted products)
    
    Derived columns:
    - date_id: YYYYMMDD INT from order_ts
    - gross_margin: line_total - (quantity * cost_price)
    """
    logger.info("Building fact_order_line...")

    df_order_items = (
        spark.table("silver.order_items")
        .filter(~F.col("is_deleted"))
        .select(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            F.col("unit_price").alias("sale_unit_price"),
            "line_total",
            "_extracted_at"
        )
    )

    df_orders = (
        spark.table("silver.orders")
        .filter(~F.col("is_deleted"))
        .select(
            "order_id",
            "customer_id",
            F.col("order_status"),
            "shipping_city",
            "order_ts"
        )
    )

    df_products = (
        spark.table("silver.products")
        .select(
            "product_id",
            F.col("cost_price").alias("product_cost_price")
        )
    )

    # Build fact with JOINs and calculations
    df_fact = (
        df_order_items
        .join(df_orders, on="order_id", how="inner")
        .join(df_products, on="product_id", how="left")
        .withColumn(
            "date_id",
            F.date_format(F.col("order_ts"), "yyyyMMdd").cast("int")
        )
        .withColumn(
            "gross_margin",
            F.round(
                F.col("line_total") - (F.col("quantity") * F.col("product_cost_price")),
                2
            )
        )
        .select(
            F.col("order_item_id"),
            F.col("order_id"),
            F.col("date_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("order_status"),
            F.col("shipping_city"),
            F.col("quantity"),
            F.col("sale_unit_price").alias("unit_price"),
            F.col("line_total"),
            F.col("product_cost_price").alias("cost_price"),
            F.col("gross_margin"),
            F.col("order_ts"),
            F.col("_extracted_at")
        )
    )

    upsert_to_gold(spark, df_fact, "gold.fact_order_line", "order_item_id")
    logger.info("  MERGE complete")
    validate_gold(spark, "gold.fact_order_line", "order_item_id", "FACT_ORDER_LINE")


# ---------------------------------------------------------------------------
# Data quality and KPI views
# ---------------------------------------------------------------------------

def validate_metrics(spark: SparkSession) -> bool:
    """Check for negative business metrics in fact table."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("Metric Sanity Checks")
    logger.info("=" * 50)

    bad_count = spark.sql("""
        SELECT COUNT(*) AS bad_count
        FROM gold.fact_order_line
        WHERE quantity < 0 OR unit_price < 0 OR line_total < 0
    """).collect()[0]["bad_count"]

    if bad_count > 0:
        logger.warning("Negative metrics found: %d rows", bad_count)
        return False
    else:
        logger.info("All business metrics are non-negative")
        return True


def create_kpi_views(spark: SparkSession) -> None:
    """Create business-ready KPI views from Gold tables."""
    logger.info("")
    logger.info("Creating KPI views...")

    spark.sql("""
        CREATE OR REPLACE VIEW gold.vw_revenue_margin_by_segment_month AS
        SELECT
            d.year,
            d.month,
            c.segment,
            SUM(f.line_total) AS revenue,
            SUM(f.gross_margin) AS gross_margin,
            COUNT(DISTINCT f.order_id) AS orders
        FROM gold.fact_order_line f
        JOIN gold.dim_customer c ON f.customer_id = c.customer_id
        JOIN gold.dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month, c.segment
    """)

    spark.sql("""
        CREATE OR REPLACE VIEW gold.vw_top_products_by_revenue AS
        SELECT
            p.product_id,
            p.name,
            p.category,
            p.subcategory,
            SUM(f.quantity) AS units_sold,
            SUM(f.line_total) AS revenue,
            SUM(f.gross_margin) AS gross_margin
        FROM gold.fact_order_line f
        JOIN gold.dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_id, p.name, p.category, p.subcategory
    """)

    spark.sql("""
        CREATE OR REPLACE VIEW gold.vw_order_status_funnel AS
        SELECT
            order_status,
            COUNT(DISTINCT order_id) AS orders,
            SUM(line_total) AS revenue
        FROM gold.fact_order_line
        GROUP BY order_status
    """)

    spark.sql("""
        CREATE OR REPLACE VIEW gold.vw_customer_ltv AS
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.segment,
            COUNT(DISTINCT f.order_id) AS total_orders,
            SUM(f.line_total) AS lifetime_revenue,
            SUM(f.gross_margin) AS lifetime_margin
        FROM gold.fact_order_line f
        JOIN gold.dim_customer c ON f.customer_id = c.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.segment
    """)

    spark.sql("""
        CREATE OR REPLACE VIEW gold.vw_daily_kpis AS
        SELECT
            d.full_date,
            COUNT(DISTINCT f.order_id) AS orders,
            COUNT(DISTINCT f.customer_id) AS active_customers,
            SUM(f.line_total) AS revenue,
            SUM(f.gross_margin) AS gross_margin,
            SUM(f.quantity) AS units
        FROM gold.fact_order_line f
        JOIN gold.dim_date d ON f.date_id = d.date_id
        GROUP BY d.full_date
    """)

    logger.info("  5 KPI views created")


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run(spark: SparkSession) -> None:
    """Execute the full Gold pipeline: build dims → build fact → validate → optimize → create KPIs."""
    catalog = Config.UNITY_CATALOG
    gold_schema = Config.GOLD_SCHEMA

    logger.info("=== Gold pipeline START ===")
    logger.info("Catalog: %s | Schema: %s", catalog, gold_schema)

    # Set context
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")
    spark.sql(f"USE SCHEMA {gold_schema}")

    # Create target tables
    create_gold_tables(spark)

    # Build dimensions
    build_dim_date(spark)
    build_dim_customer(spark)
    build_dim_product(spark)

    # Build fact
    build_fact_order_line(spark)

    # FK validation
    fk_rules = [
        {"fact_key": "customer_id", "dim_table": "gold.dim_customer", "dim_key": "customer_id", "rule_name": "customer_fk"},
        {"fact_key": "product_id", "dim_table": "gold.dim_product", "dim_key": "product_id", "rule_name": "product_fk"},
        {"fact_key": "date_id", "dim_table": "gold.dim_date", "dim_key": "date_id", "rule_name": "date_fk"},
    ]
    validate_foreign_keys(spark, "gold.fact_order_line", fk_rules, "FACT_ORDER_LINE")

    # Performance tuning
    logger.info("")
    logger.info("=" * 50)
    logger.info("Performance Tuning")
    logger.info("=" * 50)
    spark.sql("OPTIMIZE gold.fact_order_line ZORDER BY (date_id, customer_id)")
    logger.info("  fact_order_line optimized — Z-ordered by (date_id, customer_id)")
    spark.sql("OPTIMIZE gold.dim_customer")
    spark.sql("OPTIMIZE gold.dim_product")
    logger.info("  dimensions optimized")

    # Metric checks
    validate_metrics(spark)

    # KPI views
    create_kpi_views(spark)

    # Summary
    logger.info("")
    logger.info("=" * 50)
    logger.info("Gold Layer — Final Summary")
    logger.info("=" * 50)
    for table_name, pk in [
        ("dim_date", "date_id"),
        ("dim_customer", "customer_id"),
        ("dim_product", "product_id"),
        ("fact_order_line", "order_item_id"),
    ]:
        count = spark.table(f"gold.{table_name}").count()
        logger.info("%s: %d rows", table_name.ljust(20), count)

    logger.info("")
    logger.info("=== Gold pipeline END — all checks passed ===")


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
