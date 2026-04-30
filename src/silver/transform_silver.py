from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

from src.config.settings import (
    BRONZE_TABLE,
    SILVER_TABLE,
    SILVER_CHECKPOINT_PATH,
    LATE_EVENT_WATERMARK,
)
from src.utils.data_quality import apply_telematics_quality_rules


def start_silver_stream(spark: SparkSession):
    """
    Transform bronze events into cleaned silver events.

    Silver responsibilities:
    - Standardize timestamps.
    - Validate required fields.
    - Remove duplicates.
    - Handle late-arriving records with watermarking.
    """
    bronze_df = spark.readStream.table(BRONZE_TABLE)

    cleaned_df = (
        bronze_df
        .withColumn("event_timestamp", to_timestamp(col("event_time")))
        .withColumn("event_date", to_date(col("event_timestamp")))
    )

    quality_df = apply_telematics_quality_rules(cleaned_df)

    deduped_df = (
        quality_df
        .withWatermark("event_timestamp", LATE_EVENT_WATERMARK)
        .dropDuplicates(["event_id"])
    )

    query = (
        deduped_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(SILVER_TABLE)
    )

    return query
