"""
Gold layer dimensional model for Price Paid data.

Builds the star schema from silver: a transaction fact table with
property and date dimensions. Property is a hybrid SCD — Type 2 on
attributes, Type 1 on address fields.
"""

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from landreg.config import GOLD_SCHEMA, SILVER_SCHEMA, table
from landreg.runlog import start_run, complete_run

SCD_START_DATE = "1900-01-01"


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


def property_natural_key() -> Column:
    """Build the property natural key from its address components.

    Null components are coalesced to empty strings so the separator count
    stays fixed. Without this, concat_ws drops nulls entirely and a
    property with no SAON would collide with one that has no PAON.
    """
    return F.concat_ws(
        "|",
        F.coalesce(F.col("postcode"), F.lit("")),
        F.coalesce(F.col("paon"), F.lit("")),
        F.coalesce(F.col("saon"), F.lit("")),
    )


def build_property_source(df: DataFrame) -> DataFrame:
    """Collapse silver transactions to one row per property.

    Attributes are taken from the property's most recent transaction, and
    valid_from is that transaction's date. For a property not yet in the
    dimension this is overridden to SCD_START_DATE in build_staged_source,
    so historical transactions resolve against its first version.

    All transactions are considered, including withdrawn ones, so that 
    every property appearing in the fact has a dimension row to resolve against.
    """
    df = df.withColumn("property_nk", property_natural_key())

    latest = Window.partitionBy("property_nk").orderBy(F.desc("date_of_transfer"))
    return (
        df.withColumn("row_num", F.row_number().over(latest))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumns({
            "valid_from": F.col("date_of_transfer"),
            "valid_to": F.lit(None).cast("date"),
            "is_current": F.lit(True),
        })
    )


def build_staged_source(source: DataFrame, current_dim: DataFrame) -> DataFrame:
    """Build the merge source for an SCD Type 2 load.

    Every source row is emitted with `merge_key` set to its natural key.
    Rows whose tracked attributes differ from the current dimension version
    are emitted a second time with a NULL `merge_key`, which cannot match on
    the merge join and therefore inserts as a new version.

    A property not yet in the dimension opens at SCD_START_DATE rather than
    its transaction date, so historical transactions resolve against it.
    """
    dim = current_dim.select(
        "property_nk",
        F.col("property_type").alias("dim_property_type"),
        F.col("duration").alias("dim_duration"),
        F.lit(True).alias("exists_in_dim"),
    )

    joined = (
        source.join(dim, on="property_nk", how="left")
        .withColumn(
            "valid_from",
            F.when(F.col("exists_in_dim").isNull(), F.lit(SCD_START_DATE).cast("date"))
            .otherwise(F.col("valid_from")),
        )
        .withColumn(
            "property_sk", F.xxhash64(F.col("property_nk"), F.col("valid_from"))
        )
    )

    changed = joined.filter(
        F.col("exists_in_dim").isNotNull()
        & (
            ~F.col("property_type").eqNullSafe(F.col("dim_property_type"))
            | ~F.col("duration").eqNullSafe(F.col("dim_duration"))
        )
    )

    joined = joined.drop("dim_property_type", "dim_duration", "exists_in_dim")
    changed = changed.drop("dim_property_type", "dim_duration", "exists_in_dim")

    all_rows = joined.withColumn("merge_key", F.col("property_nk"))
    new_versions = changed.withColumn("merge_key", F.lit(None).cast("string"))

    return all_rows.unionByName(new_versions)


def merge_dim_property(spark: SparkSession, df: DataFrame) -> None:
    """Merge property versions into dim_property using SCD Type 2.

    The source must carry a `merge_key` column holding either `property_nk`
    or NULL. Rows with NULL cannot match on the join and therefore fall
    through to the insert clause, which is how a changed property produces
    both a closed old version and a new current one from a single merge.

    Type 2 applies to `property_type` and `duration`; address fields are
    Type 1 and updated in place without creating a version.
    """
    df.createOrReplaceTempView("staged")
    
    spark.sql(f"""
        MERGE INTO {table(GOLD_SCHEMA, 'dim_property')} AS t
        USING staged AS s
        ON t.property_nk = s.merge_key AND t.is_current = true
        
        WHEN MATCHED AND (NOT (t.property_type <=> s.property_type) OR NOT (t.duration <=> s.duration))
            THEN UPDATE SET t.valid_to = s.valid_from, t.is_current = false
        WHEN MATCHED THEN UPDATE SET
            t.postcode = s.postcode,
            t.paon = s.paon,
            t.saon = s.saon,
            t.street = s.street,
            t.locality = s.locality,
            t.town_city = s.town_city,
            t.district = s.district,
            t.county = s.county
        WHEN NOT MATCHED THEN INSERT (
            property_sk, property_nk, postcode, property_type, duration,
            paon, saon, street, locality, town_city, district, county,
            valid_from, valid_to, is_current
        ) VALUES (
            s.property_sk, s.property_nk, s.postcode, s.property_type, s.duration,
            s.paon, s.saon, s.street, s.locality, s.town_city, s.district, s.county,
            s.valid_from, NULL, TRUE
        )
    """
    )


def build_fact_transaction(df: DataFrame, dim_property: DataFrame, run_id: str) -> DataFrame:
    """Build fact rows from silver, resolving dimension surrogate keys.

    Each transaction joins to the property version that was current on its
    transfer date, so a fact row references the property as it was recorded
    at the time rather than as it is now. The upper bound is exclusive:
    a closed version's valid_to equals the next version's valid_from.

    The join is a left join by design. A transaction that fails to resolve
    keeps a null property_sk and fails the table's NOT NULL constraint at
    write time, which surfaces the problem rather than silently dropping
    the row as an inner join would.

    date_key is derived from date_of_transfer rather than joined from
    dim_date, since the key is computable and the join would be wasteful
    at this volume.
    """
    df = df.withColumn("property_nk", property_natural_key())
    dim = dim_property.select(
        "property_nk",
        "property_sk",
        "valid_from",
        "valid_to"
    )

    joined = (
        df.join(
        dim,
        (df.property_nk == dim.property_nk)
        & (df.date_of_transfer >= dim.valid_from)
        & ((df.date_of_transfer < dim.valid_to) | dim.valid_to.isNull()), 
        how="left")
        .drop(dim.property_nk)
        .withColumn("date_key", F.date_format("date_of_transfer", "yyyyMMdd").cast("int"))
    )

    return joined.select(
            "transaction_id",
            "property_sk",
            "date_key",
            "date_of_transfer",
            "price",
            "old_or_new",
            "category_type",
            "is_deleted",
            "is_price_outlier",
            "has_unknown_category",
            F.lit(run_id).alias("run_id")
    )


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


def load_dim_property(spark: SparkSession, run_id: str) -> None:
    """Generate and write the property dimension."""
    start_run(spark, run_id, "gold_dim_property")
    print(f"[{run_id}] Building dim_property")

    try:
        df = spark.read.table(table(SILVER_SCHEMA, "price_paid_transactions"))
        df = build_property_source(df)
        current_dim = spark.read.table(table(GOLD_SCHEMA, "dim_property")).filter("is_current")
        df = build_staged_source(df, current_dim)
        
        merge_dim_property(spark, df)

        history = spark.sql(
            f"DESCRIBE HISTORY {table(GOLD_SCHEMA, 'dim_property')} LIMIT 1"
        ).collect()
        metrics = history[0]["operationMetrics"]
        rows_written = int(metrics["numTargetRowsInserted"]) + int(metrics["numTargetRowsUpdated"])

        complete_run(spark, run_id, "succeeded", rows_written=rows_written)

    except Exception as e:
        complete_run(spark, run_id, "failed", error_message=str(e))
        raise


def load_fact_transaction(spark: SparkSession, run_id: str) -> None:
    """Rebuild the transaction fact table from silver.

    Overwrites on each run: the fact is derived entirely from silver, so a
    full rebuild keeps it consistent without needing merge logic.
    """
    start_run(spark, run_id, "gold_fact_transaction")
    print(f"[{run_id}] Building fact_transaction")

    try:
        df = spark.read.table(table(SILVER_SCHEMA, "price_paid_transactions"))
        dim = spark.read.table(table(GOLD_SCHEMA, "dim_property"))

        df = build_fact_transaction(df, dim, run_id)

        df.write.mode("overwrite").saveAsTable(table(GOLD_SCHEMA, "fact_transaction"))

        history = spark.sql(
            f"DESCRIBE HISTORY {table(GOLD_SCHEMA, 'fact_transaction')} LIMIT 1"
        ).collect()
        rows_written = int(history[0]["operationMetrics"]["numOutputRows"])

        complete_run(spark, run_id, "succeeded", rows_written=rows_written)

    except Exception as e:
        complete_run(spark, run_id, "failed", error_message=str(e))
        raise