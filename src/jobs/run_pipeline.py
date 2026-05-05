from pyspark.sql import SparkSession

from src.config.settings import CATALOG, SCHEMA
from src.bronze.ingest_bronze import start_bronze_stream
from src.silver.transform_silver import start_silver_stream
from src.gold.build_gold import build_all_gold_tables


def create_database_if_needed(spark: SparkSession):
    """
    The Unity Catalog catalog should already exist and be configured
    with a managed location or external storage.

    This pipeline only creates the schema if needed.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")


def main():
    spark = SparkSession.builder.appName("databricks-realtime-lakehouse-pipeline").getOrCreate()

    create_database_if_needed(spark)

    bronze_query = start_bronze_stream(spark)
    bronze_query.awaitTermination()

    silver_query = start_silver_stream(spark)
    silver_query.awaitTermination()

    build_all_gold_tables(spark)


if __name__ == "__main__":
    main()