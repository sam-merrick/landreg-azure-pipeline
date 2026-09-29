"""Control table access for metadata-driven ingestion."""

from pyspark.sql import SparkSession

from landreg.config import OPS_SCHEMA, table

def get_source_config(spark: SparkSession, source_name: str) -> dict:
    """Return the control table row for a source as a dict."""
    rows = spark.sql(
        f"SELECT * FROM {table(OPS_SCHEMA, 'control')} WHERE source_name = ?",
        args=[source_name],
    ).collect()

    if not rows:
        raise ValueError(f"No control table entry for source '{source_name}'")

    config = rows[0].asDict()

    if not config["is_enabled"]:
        raise ValueError(f"Source '{source_name}' is disabled in the control table")

    return config