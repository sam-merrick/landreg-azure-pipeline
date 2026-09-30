"""
Silver layer transformation for Price Paid data.

Silver applies typing and quality flags. Fatal failures route to
silver quarantine rather than failing the load.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DateType

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