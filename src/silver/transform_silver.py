from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, to_date

from src.config.settings import (
    BRONZE_TABLE,
    SILVER_TABLE,
    SILVER_CHECKPOINT_PATH,
)
from src.utils.data_quality import apply_telematics_quality_rules


def create_silver_table_if_needed(spark: SparkSession):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            event_id STRING,
            vehicle_id STRING,
            driver_id STRING,
            trip_id STRING,
            event_time STRING,
            event_type STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            speed_mph DOUBLE,
            heading INT,
            odometer_miles DOUBLE,
            ingest_source STRING,
            _ingested_at TIMESTAMP,
            _source_file STRING,
            event_timestamp TIMESTAMP,
            event_date DATE
        )
        USING DELTA
        """
    )


def upsert_silver_batch(batch_df: DataFrame, batch_id: int):
    """
    Process one streaming micro-batch into the silver Delta table.

    Why MERGE:
    - Allows idempotent writes.
    - Prevents duplicate event_id records.
    - Makes reruns safer.
    """
    spark = batch_df.sparkSession

    if batch_df.isEmpty():
        return

    create_silver_table_if_needed(spark)

    cleaned_df = (
        batch_df
        .withColumn("event_timestamp", to_timestamp(col("event_time")))
        .withColumn("event_date", to_date(col("event_timestamp")))
    )

    quality_df = apply_telematics_quality_rules(cleaned_df)

    deduped_df = quality_df.dropDuplicates(["event_id"])

    silver_table = DeltaTable.forName(spark, SILVER_TABLE)

    (
        silver_table.alias("target")
        .merge(
            deduped_df.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def start_silver_stream(spark: SparkSession):
    """
    Transform bronze events into cleaned silver events using foreachBatch.

    This is more production-style because each micro-batch can use:
    - Delta MERGE
    - custom validation
    - quarantine handling
    - audit logging
    - idempotent write logic
    """
    create_silver_table_if_needed(spark)

    bronze_df = spark.readStream.table(BRONZE_TABLE)

    query = (
        bronze_df.writeStream
        .foreachBatch(upsert_silver_batch)
        .outputMode("append")
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )

    return query