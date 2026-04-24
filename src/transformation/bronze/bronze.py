"""
Bronze layer: minimal transformations, preserve raw data.
Placeholder for future Databricks implementation.
"""

# When running in Databricks, this will be called from a notebook
# to read raw JSONL and create Bronze Delta tables


def create_bronze_table(spark, source_path: str, table_name: str):
    """
    Read raw JSONL and create Bronze table.
    
    Args:
        spark: Spark session
        source_path: Path to raw JSONL files
        table_name: Name of Bronze table to create
    """
    df = spark.read.json(source_path)
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
