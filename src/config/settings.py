"""
Centralized configuration for the Databricks telematics pipeline.

For a portfolio/demo project, keeping paths here is fine.
In production, move environment-specific values to Databricks job parameters,
secrets, or bundle variables.
"""

CATALOG = "main"
SCHEMA = "telematics_demo"

RAW_EVENTS_PATH = "/Volumes/main/telematics_demo/raw/events"
BRONZE_CHECKPOINT_PATH = "/Volumes/main/telematics_demo/checkpoints/bronze_events"
SILVER_CHECKPOINT_PATH = "/Volumes/main/telematics_demo/checkpoints/silver_events"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_telematics_events"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_telematics_events"
GOLD_VEHICLE_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.gold_vehicle_activity_by_day"
GOLD_DRIVER_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.gold_driver_behavior_by_day"

# Watermark controls how long Spark keeps state for late-arriving events.
LATE_EVENT_WATERMARK = "30 minutes"
