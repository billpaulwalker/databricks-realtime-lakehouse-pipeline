from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

event_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("driver_id", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("event_time", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed_mph", DoubleType(), True),
        StructField("heading", IntegerType(), True),
        StructField("odometer_miles", DoubleType(), True),
        StructField("ingest_source", StringType(), True),
    ]
)
