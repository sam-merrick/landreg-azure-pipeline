from landreg.bronze import add_ingestion_metadata


def test_adds_metadata_columns(spark):
    df = spark.createDataFrame(
        [("abc", "100"), ("def", "200"), ("ghi", "300")],
        ["transaction_id", "price"]
    )
    result = add_ingestion_metadata(df, "00001")

    assert "run_id" in result.columns
    assert result.count() == 3
    assert result.select("run_id").distinct().collect()[0]["run_id"] == "00001"