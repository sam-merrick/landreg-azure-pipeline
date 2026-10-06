"""
Gold layer dimensional model for Price Paid data.

Builds the star schema from silver: a transaction fact table with
property and date dimensions. Property is a hybrid SCD — Type 2 on
attributes, Type 1 on address fields.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from landreg.config import GOLD_SCHEMA, table
from landreg.runlog import start_run, complete_run


def build_dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    """Generate one row per calendar date between start and end inclusive.

    Fiscal year follows the UK convention: 6 April to 5 April, so dates
    before 6 April belong to the previous fiscal year.
    """
    df = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS full_date"
        )

    return df.withColumns({
            "date_key": F.date_format("full_date", "yyyyMMdd").cast("int"),
            "day_number": F.dayofmonth("full_date"),
            "day_name": F.date_format("full_date", "EEEE"),
            "day_of_week": F.dayofweek("full_date"),
            "month_number": F.month("full_date"),
            "month_name": F.date_format("full_date", "MMMM"),
            "calendar_quarter": F.quarter("full_date"), 
            "calendar_year": F.year("full_date"),
            "fiscal_year": F.when(
                (F.month("full_date") < 4)
                | ((F.month("full_date") == 4) & (F.dayofmonth("full_date") < 6)), 
                F.year("full_date") - 1
                ).otherwise(F.year("full_date")),
            "is_weekend": F.dayofweek("full_date").isin([1, 7])
    })

def load_dim_date(spark: SparkSession, run_id: str, start: str, end: str) -> None:
    """Generate and write the date dimension. Overwrites on each run."""
    start_run(spark, run_id, "gold_dim_date")
    print(f"[{run_id}] Building dim_date from {start} to {end}")

    try:
        df = build_dim_date(spark, start, end)
        df.write.mode("overwrite").saveAsTable(table(GOLD_SCHEMA, "dim_date"))

        history = spark.sql(
            f"DESCRIBE HISTORY {table(GOLD_SCHEMA, 'dim_date')} LIMIT 1"
        ).collect()
        rows_written = int(history[0]["operationMetrics"]["numOutputRows"])

        complete_run(spark, run_id, "succeeded", rows_written=rows_written)

    except Exception as e:
        complete_run(spark, run_id, "failed", error_message=str(e))
        raise