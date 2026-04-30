from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def require_not_null(df: DataFrame, columns: list[str]) -> DataFrame:
    """Keep only records where required columns are not null."""
    for column_name in columns:
        df = df.filter(col(column_name).isNotNull())
    return df


def apply_telematics_quality_rules(df: DataFrame) -> DataFrame:
    """
    Apply practical quality rules for telematics events.

    Rules:
    - Required identifiers must exist.
    - Timestamp must be valid.
    - Speed must be realistic.
    - Coordinates must be within valid ranges when present.
    """
    return (
        df.transform(lambda d: require_not_null(d, ["event_id", "vehicle_id", "event_timestamp"]))
        .filter((col("speed_mph").isNull()) | ((col("speed_mph") >= 0) & (col("speed_mph") <= 120)))
        .filter((col("latitude").isNull()) | ((col("latitude") >= -90) & (col("latitude") <= 90)))
        .filter((col("longitude").isNull()) | ((col("longitude") >= -180) & (col("longitude") <= 180)))
    )
