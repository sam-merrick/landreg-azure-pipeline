"""
Bronze layer ingestion for Land Registry Price Paid data.

Reads source files from landing and appends them to bronze with provenance
columns. Bronze is append-only and preserves the source as-is; cleaning and
typing happen in silver.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

from landreg.config import COMPLETE_PATH, BRONZE_SCHEMA, MONTHLY_PATH, CHECKPOINT_ROOT, table
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


def load_monthly_files(spark: SparkSession, run_id: str) -> None:
    """Read the monthly file from landing and append to bronze."""
    print(f"[{run_id}] Loading {MONTHLY_PATH}")
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "false")
        .schema(SOURCE_SCHEMA)
        .load(MONTHLY_PATH)
    )
    df = add_ingestion_metadata(df, run_id)
    (
        df.writeStream
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}price_paid_monthly")
        .trigger(availableNow=True)
        .toTable(table(BRONZE_SCHEMA, "price_paid_monthly"))
        .awaitTermination()
    )