"""
Silver layer transformation for Price Paid data.

Silver applies typing and quality flags. Fatal failures route to
silver quarantine rather than failing the load.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DateType

from landreg.config import BRONZE_SCHEMA, SILVER_SCHEMA, table
from landreg.runlog import start_run, complete_run

VALID_PROPERTY_TYPES = {"D", "S", "T", "F", "O"}
VALID_OLD_OR_NEW = {"Y", "N"}
VALID_DURATION = {"F", "L", "U"}  # U is undocumented but present in source
VALID_CATEGORY_TYPES = {"A", "B"}

MIN_PLAUSIBLE_PRICE = 100
MAX_PLAUSIBLE_PRICE = 100_000_000

POSTCODE_SENTINEL = "UNKNOWN"


def cast_source_types(df: DataFrame) -> DataFrame:
    """Cast price to long and date_of_transfer to date.

    Uses try_cast so malformed values become null rather than raising.
    Databricks runs in ANSI mode, where a plain cast would fail the entire
    load on a single bad value.

    The original strings are preserved as `price_raw` and
    `date_of_transfer_raw` so quarantined rows retain what actually arrived.
    """
    return df.withColumns({
        "price_raw": F.col("price"),
        "date_of_transfer_raw": F.col("date_of_transfer"),
        "price": F.col("price").try_cast(LongType()),
        "date_of_transfer": F.col("date_of_transfer").try_cast(DateType()),
    })


def normalise_sentinels(df: DataFrame) -> DataFrame:
    """Replace the UNKNOWN postcode placeholder with NULL."""
    return df.replace(POSTCODE_SENTINEL, None, subset=["postcode"])


def flag_price_outliers(df: DataFrame) -> DataFrame:
    """Flag prices outside the plausible range. Flagged rows are still valid."""
    return (
        df.withColumn(
            "is_price_outlier", 
            (F.col("price") <= MIN_PLAUSIBLE_PRICE) |
            (F.col("price") >= MAX_PLAUSIBLE_PRICE)
        )
    )


def flag_unknown_categories(df: DataFrame) -> DataFrame:
    """Flag rows with a coded value outside its accepted set."""
    return (
        df.withColumn(
            "has_unknown_category",
            ~F.col("category_type").isin(list(VALID_CATEGORY_TYPES)) |
            ~F.col("duration").isin(list(VALID_DURATION)) |
            ~F.col("property_type").isin(list(VALID_PROPERTY_TYPES)) |
            ~F.col("old_or_new").isin(list(VALID_OLD_OR_NEW))
        )
    )


def add_silver_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Add metadata columns to silver delta."""
    return df.withColumns({
            "run_id": F.lit(run_id),
            "first_seen": F.current_timestamp(),
            "last_seen": F.current_timestamp()
    })


def transform_to_silver(df: DataFrame, run_id: str) -> DataFrame:
    """Apply the full silver transform. Order matters: casting precedes
    outlier flagging, which compares numeric values."""
    df = cast_source_types(df)
    df = normalise_sentinels(df)
    df = flag_price_outliers(df)
    df = flag_unknown_categories(df)
    return add_silver_metadata(df, run_id)


def split_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (clean, quarantined) based on the three fatal rules."""
    reason = F.concat_ws(
        ", ",
        F.when(F.col("transaction_id").isNull(), "missing transaction id"),
        F.when(F.col("price").isNull(), "invalid or missing price"),
        F.when(F.col("date_of_transfer").isNull(), "invalid or missing date_of_transfer")
    )

    is_bad = (
        F.col("transaction_id").isNull()
        | F.col("price").isNull()
        | F.col("date_of_transfer").isNull()
    )

    clean = df.filter(~is_bad)
    quarantined = (
        df.filter(is_bad)
        .withColumn("quarantine_reason", reason)
        .withColumn("quarantined_at", F.current_timestamp())
    )

    return clean, quarantined


def select_clean_cols(df: DataFrame) -> DataFrame:
    """Shape clean rows to the silver transactions table schema."""
    return df.select(
            "transaction_id",
            "price",
            "date_of_transfer",
            "postcode",
            "property_type",
            "old_or_new",
            "duration",
            "paon",
            "saon",
            "street",
            "locality",
            "town_city",
            "district",
            "county",
            "category_type",
            F.lit(False).alias("is_deleted"),
            "is_price_outlier",
            "has_unknown_category",
            "run_id",
            "first_seen",
            "last_seen"
    )


def select_quarantine_cols(df: DataFrame) -> DataFrame:
    """Shape quarantined rows to the quarantine table schema.

    Uses the preserved raw strings for price and date, since the cast
    versions are null — which is why the row was quarantined.
    """
    return df.select(
            "transaction_id",
            F.col("price_raw").alias("price"),
            F.col("date_of_transfer_raw").alias("date_of_transfer"),
            "postcode",
            "property_type",
            "old_or_new",
            "duration",
            "paon",
            "saon",
            "street",
            "locality",
            "town_city",
            "district",
            "county",
            "category_type",
            "record_status",
            "run_id",
            "source_filename",
            "ingestion_timestamp",
            "quarantine_reason",
            "quarantined_at"
    )


def deduplicate_by_latest(df: DataFrame) -> DataFrame:
    """Return one row per transaction for correct silver MERGE.

    One row per transaction, latest ingestion wins, because Delta errors when a
    MERGE matches the same target row more than once.
    """
    rn_window_spec = Window.partitionBy("transaction_id").orderBy(F.desc("ingestion_timestamp"))
    return (df
             .withColumn("row_num", F.row_number().over(rn_window_spec))
             .filter(F.col("row_num") == 1)
             .drop("row_num")
            )


def merge_into_silver(spark: SparkSession, df: DataFrame) -> None:
    """Merge transformed monthly records into the silver transactions table.

    Clause order is significant: the deletion case must precede the general
    matched case, since Delta takes the first clause that matches.

    `first_seen` is deliberately absent from the update clause so an existing
    row retains when it was first observed. Deletions set only the flag and
    timestamp, leaving the row's values as last known.

    The source must be deduplicated to one row per transaction_id — Delta
    errors if a merge matches the same target row more than once.
    """
    df.createOrReplaceTempView("silver_source")
    
    spark.sql(f"""            
        MERGE INTO {table(SILVER_SCHEMA, 'price_paid_transactions')} AS t
        USING silver_source AS s
        ON t.transaction_id = s.transaction_id
        
        WHEN MATCHED AND s.record_status = 'D' THEN UPDATE SET 
            t.is_deleted = TRUE, 
            t.last_seen = current_timestamp()
        WHEN MATCHED THEN UPDATE SET 
            t.price = s.price,
            t.date_of_transfer = s.date_of_transfer,
            t.postcode = s.postcode,
            t.property_type = s.property_type,
            t.old_or_new = s.old_or_new,
            t.duration = s.duration,
            t.paon = s.paon,
            t.saon = s.saon,
            t.street = s.street,
            t.locality = s.locality,
            t.town_city = s.town_city,
            t.district = s.district,
            t.county = s.county,
            t.category_type = s.category_type,
            t.is_deleted = FALSE,
            t.is_price_outlier = s.is_price_outlier,
            t.has_unknown_category = s.has_unknown_category,
            t.run_id = s.run_id,
            t.last_seen = s.last_seen
        WHEN NOT MATCHED AND s.record_status != 'D' THEN INSERT (
            transaction_id, price, date_of_transfer, postcode, property_type, old_or_new,
            duration, paon, saon, street, locality, town_city, district, county, category_type,
            is_deleted, is_price_outlier, has_unknown_category, run_id, first_seen, last_seen
            ) VALUES (
            s.transaction_id, s.price, s.date_of_transfer, s.postcode, s.property_type, s.old_or_new,
            s.duration, s.paon, s.saon, s.street, s.locality, s.town_city, s.district, s.county, s.category_type,
            FALSE, s.is_price_outlier, s.has_unknown_category, s.run_id, s.first_seen, s.last_seen
            )
    """
    )


def load_silver_backfill(spark: SparkSession, run_id: str) -> None:
    """Read the bronze backfill, transform, and write to silver."""
    start_run(spark, run_id, "silver_backfill")
    print(f"[{run_id}] Loading bronze.price_paid_complete into silver")

    try:
        df = spark.read.table(table(BRONZE_SCHEMA, "price_paid_complete"))
        df = transform_to_silver(df, run_id)

        clean, quarantined = split_quarantine(df)

        select_clean_cols(clean).write.mode("append").saveAsTable(
            table(SILVER_SCHEMA, "price_paid_transactions")
        )

        select_quarantine_cols(quarantined).write.mode("append").saveAsTable(
            table(SILVER_SCHEMA, "price_paid_quarantine")
        )

        history = spark.sql(
            f"DESCRIBE HISTORY {table(SILVER_SCHEMA, 'price_paid_transactions')} LIMIT 1"
        ).collect()
        metrics = history[0]["operationMetrics"]
        rows_written = int(metrics["numOutputRows"])

        complete_run(spark, run_id, "succeeded", rows_written=rows_written)

    except Exception as e:
        complete_run(spark, run_id, "failed", error_message=str(e))
        raise


def load_silver_incremental(spark: SparkSession, run_id: str) -> None:
    """Read the bronze incremental load, transform, and MERGE into silver."""
    start_run(spark, run_id, "silver_incremental")
    print(f"[{run_id}] Loading bronze.price_paid_monthly into silver")

    try:
        df = spark.read.table(table(BRONZE_SCHEMA, "price_paid_monthly"))
        df = transform_to_silver(df, run_id)

        clean, quarantined = split_quarantine(df)

        select_quarantine_cols(quarantined).write.mode("append").saveAsTable(
            table(SILVER_SCHEMA, "price_paid_quarantine")
        )

        clean = deduplicate_by_latest(clean)

        merge_into_silver(spark, clean)

        history = spark.sql(
            f"DESCRIBE HISTORY {table(SILVER_SCHEMA, 'price_paid_transactions')} LIMIT 1"
        ).collect()
        metrics = history[0]["operationMetrics"]
        rows_written = int(metrics["numTargetRowsInserted"]) + int(metrics["numTargetRowsUpdated"])

        complete_run(spark, run_id, "succeeded", rows_written=rows_written)
    except Exception as e:
        complete_run(spark, run_id, "failed", error_message=str(e))
        raise