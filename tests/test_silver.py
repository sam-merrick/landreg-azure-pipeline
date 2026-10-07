from datetime import datetime

from landreg.silver import cast_source_types, split_quarantine, flag_price_outliers, flag_unknown_categories, deduplicate_by_latest


def test_split_quarantine_separates_bad_rows(spark):
    df = spark.createDataFrame(
        [
            ("001", "250000", "1995-01-31 00:00"),
            (None, "250000", "1995-01-31 00:00"),
            ("003", "not-a-number", "1995-01-31 00:00"),
        ],
        ["transaction_id", "price", "date_of_transfer"],
    )
    df = cast_source_types(df)

    clean, quarantined = split_quarantine(df)

    assert clean.count() == 1
    assert quarantined.count() == 2

    reasons = {
        row["transaction_id"]: row["quarantine_reason"]
        for row in quarantined.collect()
    }

    assert reasons[None] == "missing transaction id"
    assert reasons["003"] == "invalid or missing price"

def test_source_value_casting(spark):
    df = spark.createDataFrame(
        [
            ("001", "250000", "1995-01-31 00:00"),
            ("002", "not-a-number", "1995-01-31 00:00"),
        ],
        ["transaction_id", "price", "date_of_transfer"]
    )
    result = cast_source_types(df)

    assert result.count() == 2

    rows = {row["transaction_id"]: row for row in result.collect()}

    assert rows["001"]["price"] == 250000
    assert rows["002"]["price"] is None
    assert rows["002"]["price_raw"] == "not-a-number"

def test_price_outlier_boundaries(spark):
    df = spark.createDataFrame(
        [
            ("001", "250000", "1995-01-31 00:00"),
            ("002", "100", "1995-01-31 00:00"),
            ("003", "100000000", "1995-01-31 00:00"),
        ],
        ["transaction_id", "price", "date_of_transfer"],
    )
    result = flag_price_outliers(cast_source_types(df))

    rows = {row["transaction_id"]: row for row in result.collect()}

    assert rows["001"]["is_price_outlier"] is False
    assert rows["002"]["is_price_outlier"] is True
    assert rows["003"]["is_price_outlier"] is True


def test_unknown_category_flag(spark):
    df = spark.createDataFrame(
        [
            ("001", "D", "N", "F", "A"),
            ("002", "Z", "N", "F", "A"),
            ("003", "D", "N", "U", "A"),
        ],
        ["transaction_id", "property_type", "old_or_new", "duration", "category_type"],
    )
    result = flag_unknown_categories(df)

    rows = {row["transaction_id"]: row for row in result.collect()}

    assert rows["001"]["has_unknown_category"] is False
    assert rows["002"]["has_unknown_category"] is True
    # U is undocumented but accepted, so it must not flag
    assert rows["003"]["has_unknown_category"] is False


def test_deduplicate_keeps_latest(spark):
    df = spark.createDataFrame(
        [
            ("001", "A", datetime(2026, 7, 1, 12, 0)),
            ("001", "C", datetime(2026, 8, 1, 12, 0)),
            ("002", "A", datetime(2026, 7, 1, 12, 0)),
        ],
        ["transaction_id", "record_status", "ingestion_timestamp"],
    )
    result = deduplicate_by_latest(df)

    rows = {row["transaction_id"]: row for row in result.collect()}

    assert result.count() == 2
    assert rows["001"]["record_status"] == "C"