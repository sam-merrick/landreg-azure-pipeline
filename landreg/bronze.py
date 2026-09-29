"""
Bronze layer ingestion for Land Registry Price Paid data.

Reads source files from landing and appends them to bronze with provenance
columns. Bronze is append-only and preserves the source as-is; cleaning and
typing happen in silver.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

from landreg.config import BRONZE_SCHEMA, CHECKPOINT_ROOT, LANDING_CONTAINER, table, abfss
from landreg.schema import SOURCE_SCHEMA
from landreg.control import get_source_config
from landreg.runlog import start_run, complete_run


def add_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Add provenance columns to a source DataFrame."""
    return df.withColumns({
        "source_filename": F.col("_metadata.file_path"),
        "ingestion_timestamp": F.current_timestamp(),
        "run_id": F.lit(run_id)
    })


def load_batch_source(spark: SparkSession, source_name: str, run_id: str) -> None:
    """Read a batch source from landing and append it to bronze."""
    config = get_source_config(spark, source_name)
    source_path = abfss(LANDING_CONTAINER, config["source_path"])
    target = table(BRONZE_SCHEMA, config["target_table"])

    start_run(spark, run_id, source_name)
    print(f"[{run_id}] Loading {source_path} into {target}")

    try:
        df = spark.read.csv(source_path, header=False, schema=SOURCE_SCHEMA)
        df = add_ingestion_metadata(df, run_id)
        df.write.mode("append").saveAsTable(target)

        history = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").collect()
        rows_written = int(history[0]["operationMetrics"]["numOutputRows"])

        complete_run(spark, run_id, "succeeded", rows_written=rows_written)

    except Exception as e:
        complete_run(spark, run_id, "failed", error_message=str(e))
        raise


def load_stream_source(spark: SparkSession, source_name: str, run_id: str) -> None:
    """Read a stream source from landing and append it to bronze."""
    config = get_source_config(spark, source_name)
    source_path = abfss(LANDING_CONTAINER, config["source_path"])
    target = table(BRONZE_SCHEMA, config["target_table"])

    start_run(spark, run_id, source_name)
    print(f"[{run_id}] Loading {source_path} into {target}")

    try:
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "false")
            .schema(SOURCE_SCHEMA)
            .load(source_path)
        )
        df = add_ingestion_metadata(df, run_id)
        query = (
            df.writeStream
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}{source_name}")
            .trigger(availableNow=True)
            .toTable(target)
        )
        query.awaitTermination()

        rows_written = sum(p["numInputRows"] for p in query.recentProgress)
        complete_run(spark, run_id, "succeeded", rows_written=rows_written)

    except Exception as e:
        complete_run(spark, run_id, "failed", error_message=str(e))
        raise