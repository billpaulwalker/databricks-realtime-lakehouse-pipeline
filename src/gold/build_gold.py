from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    countDistinct,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
    col,
)

from src.config.settings import (
    SILVER_TABLE,
    GOLD_VEHICLE_DAILY_TABLE,
    GOLD_DRIVER_DAILY_TABLE,
)


def build_gold_vehicle_activity_by_day(spark: SparkSession):
    """
    Build daily vehicle-level metrics for reporting.

    This is batch over the current silver table. In production, you could use:
    - Databricks Workflows
    - Delta Live Tables
    - MERGE-based incremental processing
    - Structured Streaming foreachBatch
    """
    silver_df = spark.read.table(SILVER_TABLE)

    vehicle_daily_df = (
        silver_df
        .groupBy("event_date", "vehicle_id")
        .agg(
            count("*").alias("event_count"),
            countDistinct("trip_id").alias("trip_count"),
            avg("speed_mph").alias("avg_speed_mph"),
            spark_max("speed_mph").alias("max_speed_mph"),
            spark_min("event_timestamp").alias("first_event_timestamp"),
            spark_max("event_timestamp").alias("last_event_timestamp"),
            spark_max("odometer_miles").alias("max_odometer_miles"),
        )
    )

    (
        vehicle_daily_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_VEHICLE_DAILY_TABLE)
    )


def build_gold_driver_behavior_by_day(spark: SparkSession):
    """
    Build daily driver-level behavior metrics.

    Example business rule:
    - speeding_event_count = speed above 75 mph
    """
    silver_df = spark.read.table(SILVER_TABLE)

    driver_daily_df = (
        silver_df
        .filter(col("driver_id").isNotNull())
        .groupBy("event_date", "driver_id")
        .agg(
            count("*").alias("event_count"),
            countDistinct("trip_id").alias("trip_count"),
            avg("speed_mph").alias("avg_speed_mph"),
            spark_max("speed_mph").alias("max_speed_mph"),
            spark_sum(when(col("speed_mph") > 75, 1).otherwise(0)).alias("speeding_event_count"),
            spark_sum(when(col("event_type") == "hard_brake", 1).otherwise(0)).alias("hard_brake_count"),
            spark_sum(when(col("event_type") == "rapid_acceleration", 1).otherwise(0)).alias("rapid_acceleration_count"),
        )
    )

    (
        driver_daily_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_DRIVER_DAILY_TABLE)
    )


def build_all_gold_tables(spark: SparkSession):
    build_gold_vehicle_activity_by_day(spark)
    build_gold_driver_behavior_by_day(spark)
