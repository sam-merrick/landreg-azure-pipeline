from datetime import date
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType, BooleanType
)

from landreg.gold import build_staged_source, build_property_source, build_fact_transaction

PROPERTY_SOURCE_SCHEMA = StructType([
    StructField("postcode", StringType()),
    StructField("paon", StringType()),
    StructField("saon", StringType()),
    StructField("property_type", StringType()),
    StructField("duration", StringType()),
    StructField("date_of_transfer", DateType()),
    StructField("is_deleted", BooleanType()),
])

SILVER_TEST_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("postcode", StringType()),
    StructField("paon", StringType()),
    StructField("saon", StringType()),
    StructField("price", LongType()),
    StructField("date_of_transfer", DateType()),
    StructField("old_or_new", StringType()),
    StructField("category_type", StringType()),
    StructField("is_deleted", BooleanType()),
    StructField("is_price_outlier", BooleanType()),
    StructField("has_unknown_category", BooleanType()),
])

DIM_TEST_SCHEMA = StructType([
    StructField("property_nk", StringType()),
    StructField("property_sk", LongType()),
    StructField("valid_from", DateType()),
    StructField("valid_to", DateType()),
])


def test_build_staged_source(spark):
    source = spark.createDataFrame(
        [
            ("001", "D", "F", date(2020, 2, 1)),
        ],
        ["property_nk", "property_type", "duration", "valid_from"]
    )

    current_dim = spark.createDataFrame(
        [
            ("001", "S", "F"),
        ],
        ["property_nk", "property_type", "duration"]
    )

    df = build_staged_source(source, current_dim)

    rows = df.collect()
    merge_keys = sorted([r["merge_key"] for r in rows], key=lambda x: (x is None, x))

    assert df.count() == 2
    assert merge_keys == ["001", None]


def test_unchanged_property_emits_one_row(spark):
    source = spark.createDataFrame(
        [("001", "D", "F", date(2020, 2, 1))],
        ["property_nk", "property_type", "duration", "valid_from"],
    )
    current_dim = spark.createDataFrame(
        [("001", "D", "F")],
        ["property_nk", "property_type", "duration"],
    )

    df = build_staged_source(source, current_dim)

    assert df.count() == 1
    assert df.collect()[0]["merge_key"] == "001"


def test_new_property_opens_at_sentinel(spark):
    source = spark.createDataFrame(
        [("002", "D", "F", date(2020, 2, 1))],
        ["property_nk", "property_type", "duration", "valid_from"],
    )
    current_dim = spark.createDataFrame(
        [("001", "D", "F")],
        ["property_nk", "property_type", "duration"],
    )

    df = build_staged_source(source, current_dim)
    row = df.collect()[0]

    assert df.count() == 1
    assert row["valid_from"] == date(1900, 1, 1)
    assert row["merge_key"] == "002"


def test_property_source_takes_latest_transaction(spark):
    df = spark.createDataFrame(
        [
            ("SW1 1AA", "12", None, "D", "F", date(2005, 3, 1), False),
            ("SW1 1AA", "12", None, "S", "F", date(2015, 8, 1), False),
        ], schema=PROPERTY_SOURCE_SCHEMA
    )

    result = build_property_source(df)
    row = result.collect()[0]

    assert result.count() == 1
    assert row["property_type"] == "S"
    assert row["valid_from"] == date(2015, 8, 1)


def test_fact_resolves_historical_property_version(spark):
    silver = spark.createDataFrame(
        [("T1", "SW1 1AA", "12", None, 250000, date(2005, 3, 1), "N", "A", False, False, False)],
        schema=SILVER_TEST_SCHEMA
    )

    dim = spark.createDataFrame(
        [
            ("SW1 1AA|12|", 1001, date(1900, 1, 1), date(2010, 6, 1)),
            ("SW1 1AA|12|", 1002, date(2010, 6, 1), None),
        ], schema=DIM_TEST_SCHEMA
    )

    result = build_fact_transaction(silver, dim, "test_run")
    row = result.collect()[0]

    assert result.count() == 1
    assert row["property_sk"] == 1001
    assert row["date_key"] == 20050301
    assert row["run_id"] == "test_run"