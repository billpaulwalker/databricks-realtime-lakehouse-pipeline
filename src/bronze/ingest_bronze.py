from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

from src.config.settings import RAW_EVENTS_PATH, BRONZE_CHECKPOINT_PATH, AUTOLOADER_SCHEMA_LOCATION, BRONZE_TABLE
from src.schemas.telematics_schema import event_schema


def start_bronze_stream(spark: SparkSession):
    """
    Ingest raw JSON telematics events into a bronze Delta table using Auto Loader.

    Bronze design principles:
    - Use Auto Loader for production-style incremental file ingestion.
    - Preserve raw source fields.
    - Add ingestion metadata.
    - Append only.
    - Avoid heavy cleansing here.
    """

    raw_events_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", AUTOLOADER_SCHEMA_LOCATION)
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(event_schema)
        .load(RAW_EVENTS_PATH)
    )

    bronze_df = (
        raw_events_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )

    return query
