"""
Bronze layer ingestion for Land Registry Price Paid data.

Reads source files from landing and appends them to bronze with provenance
columns. Bronze is append-only and preserves the source as-is; cleaning and
typing happen in silver.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

from landreg.config import COMPLETE_PATH, BRONZE_SCHEMA, table
from landreg.schema import SOURCE_SCHEMA

def add_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Add provenance columns to a source DataFrame."""
    return df.withColumns({
        "source_filename": F.col("_metadata.file_path"),
        "ingestion_timestamp": F.current_timestamp(),
        "run_id": F.lit(run_id)
    })


def load_complete_file(spark: SparkSession, run_id: str) -> None:
    """Read the complete file from landing and append to bronze."""
    print(f"[{run_id}] Loading {COMPLETE_PATH}")
    df = spark.read.csv(COMPLETE_PATH, header=False, schema=SOURCE_SCHEMA)
    df = add_ingestion_metadata(df, run_id)
    df.write.mode("append").saveAsTable(table(BRONZE_SCHEMA, "price_paid_complete"))