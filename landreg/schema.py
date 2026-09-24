"""
Price Paid source schema. 

Verified against the raw files rather than taken from documentation. Every
field is StringType: bronze reads faithfully, typing happens in silver
where bad values can be quarantined rather than failing the load.
"""

from pyspark.sql.types import StructType, StructField, StringType

SOURCE_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=True),
    StructField("price", StringType(), nullable=True),
    StructField("date_of_transfer", StringType(), nullable=True),
    StructField("postcode", StringType(), nullable=True),
    StructField("property_type", StringType(), nullable=True),
    StructField("old_or_new", StringType(), nullable=True),
    StructField("duration", StringType(), nullable=True),
    StructField("paon", StringType(), nullable=True),
    StructField("saon", StringType(), nullable=True),
    StructField("street", StringType(), nullable=True),
    StructField("locality", StringType(), nullable=True),
    StructField("town_city", StringType(), nullable=True),
    StructField("district", StringType(), nullable=True),
    StructField("county", StringType(), nullable=True),
    StructField("category_type", StringType(), nullable=True),
    StructField("record_status", StringType(), nullable=True),
])