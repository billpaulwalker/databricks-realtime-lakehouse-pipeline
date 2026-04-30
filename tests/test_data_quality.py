import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

from src.utils.data_quality import apply_telematics_quality_rules


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("telematics-tests")
        .getOrCreate()
    )


def test_quality_rules_remove_invalid_records(spark):
    rows = [
        ("e1", "v1", "2026-04-30T10:00:00", 55.0, 32.7, -117.1),
        (None, "v2", "2026-04-30T10:01:00", 60.0, 32.7, -117.1),
        ("e3", "v3", "2026-04-30T10:02:00", 200.0, 32.7, -117.1),
    ]

    df = spark.createDataFrame(
        rows,
        ["event_id", "vehicle_id", "event_time", "speed_mph", "latitude", "longitude"],
    ).withColumn("event_timestamp", to_timestamp(col("event_time")))

    result = apply_telematics_quality_rules(df)

    assert result.count() == 1
    assert result.collect()[0]["event_id"] == "e1"
