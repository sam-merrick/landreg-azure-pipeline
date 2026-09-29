"""Run logging for pipeline observability.

Each pipeline execution writes a row at start and updates it on completion,
recording timings, row counts and failure detail.
"""

from pyspark.sql import SparkSession

from config import OPS_SCHEMA, table

def start_run(spark: SparkSession, run_id: str, source_name: str) -> None:
    """Record the start of a pipeline run."""
    spark.sql(
        f"""
        INSERT INTO {table(OPS_SCHEMA, 'run_log')} 
            (run_id, source_name, started_at, status, ended_at, rows_written, error_message)
        VALUES (?, ?, current_timestamp(), 'running', NULL, NULL, NULL),
        """,
        args=[run_id, source_name],
    )


def complete_run(spark: SparkSession, run_id: str, status: str, rows_written: int | None = None, 
                 error_message: str | None = None) -> None:
    """Record the completion of a pipeline run."""
    spark.sql(
        f"""
        UPDATE {table(OPS_SCHEMA, 'run_log')}
        SET status = ?, ended_at = current_timestamp(), error_message = ?, rows_written = ?
        WHERE run_id = ?
        """,
        args=[status, error_message, rows_written, run_id]
    )