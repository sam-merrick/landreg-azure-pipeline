""""""
import pytest
from pyspark.sql import SparkSession

from landreg.bronze import add_ingestion_metadata

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_adds_metadata_columns(spark):
    df = spark.createDataFrame(
        [("abc", "100"), ("def", "200"), ("ghi", "300")],
        ["transaction_id", "price"]
    )
    result = add_ingestion_metadata(df, "00001")

    assert "run_id" in result.columns
    assert result.count() == 3
    assert result.select("run_id").distinct().collect()[0]["run_id"] == "00001"