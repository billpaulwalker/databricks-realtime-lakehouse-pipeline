# Real-Time Data Pipeline with Databricks

## Overview

This project demonstrates a production-style, end-to-end real-time data pipeline using Databricks, PySpark Structured Streaming, Delta Lake, and Unity Catalog.

The pipeline processes simulated telematics event data such as GPS location, vehicle speed, driver behavior, and trip activity. It follows a medallion architecture (bronze, silver, gold) and implements modern ingestion and transformation patterns used in real-world data platforms.

The goal of this project is to showcase how modern data engineering systems ingest, transform, validate, and serve high-volume event data for analytics in a scalable and reliable way.

---

## What This Demonstrates

- Production-grade ingestion using Databricks Auto Loader (cloudFiles)
- Medallion architecture: bronze → silver → gold
- Unity Catalog with external volumes and governed storage
- Incremental processing with Structured Streaming
- Idempotent transformations using foreachBatch + Delta MERGE
- Data quality validation and deduplication
- Handling of late-arriving and out-of-order data
- Analytics-ready gold layer for reporting and BI
- Git-based development workflow

## Problem Statement

Telematics platforms generate large volumes of event data from vehicles, sensors, and mobile devices.

Common challenges include:

- High-volume event ingestion
- Late-arriving records
- Duplicate events
- Schema changes over time
- Need for near real-time analytics
- Reliable downstream reporting

This project addresses these challenges using Auto Loader, Delta Lake, and Structured Streaming.

---

## Architecture


JSON Files (External Volume)
        |
        v
Bronze Layer - Auto Loader Incremental Ingestion
        |
        v
Silver Layer - foreachBatch + Cleaned and Deduplicated Events + Delta MERGE
        |
        v
Gold Layer - Aggregated Analytics Tables
        |
        v
BI / SQL / Dashboard Layer

---

## Tech Stack

- Databricks
- PySpark
- Spark Structured Streaming
- Delta Lake
- Unity Catalog
- SQL
- Git / GitHub

## Pipeline Flow

1. Raw JSON telematics files land in a Unity Catalog external volume
2. Bronze layer ingests data incrementally using Auto Loader
3. Silver layer:
- Cleans and standardizes data
- Applies data quality rules
- Deduplicates using event_id
- Uses foreachBatch + Delta MERGE for idempotent processing
4. Gold layer builds aggregated, analytics-ready tables
5. Data is served via Databricks SQL or BI tools

## Pipeline Layers

### Bronze Layer (Auto Loader)

The bronze layer ingests raw data using Databricks Auto Loader.

Responsibilities:

- Incrementally ingest new files from cloud storage
- Preserve original event structure
- Capture ingestion metadata
- Support schema evolution
- Maintain append-only history

### Silver Layer (foreachBatch + MERGE)

The silver layer cleans and standardizes the data.

Responsibilities:

- Standardize timestamps
- Apply data quality validation
- Remove duplicates using event_id
- Handle late-arriving data
- Use Delta MERGE for idempotent writes

### Gold Layer (Analytics)

The gold layer contains business-ready aggregated data.

Responsibilities:

Calculate trip-level metrics
Aggregate driver and vehicle activity
Create analytics-ready tables
Optimize for reporting and dashboards

## Key Features

- Auto Loader for scalable file ingestion
- Medallion architecture design
- Incremental processing with checkpointing
- Data quality enforcement
- Deduplication using business keys
- Idempotent processing with foreachBatch
- Delta Lake ACID guarantees
- Unity Catalog governance

## Example PySpark Streaming Pattern

raw_events_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .load(raw_path)
)

## Example Transformation Pattern

def upsert_silver_batch(batch_df, batch_id):
        silver_table.alias("target").merge(
                batch_df.alias("source"),
                "target.event_id = source.event_id"
        ).whenNotMatchedInsertAll().execute()

## Data Quality Checks

Example validation rules:

- event_id must not be null
- vehicle_id must not be null
- event_timestamp must be valid
- duplicate events should be removed
- speed values should be within expected range

## Setup

Unity Catalog Objects

Create:
- Catalog: telematics
- Schema: demo
- Volumes: raw and checkpoints

Expected Paths

/Volumes/telematics/demo/raw/events
/Volumes/telematics/demo/checkpoints/bronze_events
/Volumes/telematics/demo/checkpoints/bronze_events_schema
/Volumes/telematics/demo/checkpoints/silver_events

## Run the Pipeline

import sys repo_root = "/Workspace/Users/<your-email>/databricks-realtime-lakehouse-pipeline"

if repo_root not in sys.path:
        sys.path.append(repo_root)

from src.jobs.run_pipeline import main main()

main()

## Validate Results

SELECT COUNT(*) FROM telematics.demo.bronze_telematics_events;
SELECT COUNT(*) FROM telematics.demo.silver_telematics_events;
SELECT * FROM telematics.demo.gold_vehicle_activity_by_day;
SELECT * FROM telematics.demo.gold_driver_behavior_by_day;

## Performance Considerations

This project applies common lakehouse performance patterns:

- Partitioning by event date
- Delta table optimization
- File compaction
- Incremental processing
- Checkpointing
- Avoiding unnecessary full reloads

## Design Tradeoffs

Decision	                Benefit	                                                Tradeoff
Auto Loader                     Incremental, scalable ingestion                         Slight setup complexity
Delta Lake	                ACID transactions and schema evolution	                Slight overhead compared to raw Parquet
Medallion architecture	        Clear separation of raw, cleaned, and business data	More layers to manage
foreachBatch + MERGE	        Idempotent processing                                   More complex logic
External volumes                Governed storage                                        requires cloud setup

## Future Enhancements

- Add Kafka as a streaming source
- Add Great Expectations or Deequ for data quality
- Add Databricks Workflows for orchestration
- Add Unity Catalog governance
- Add dashboard layer using Power BI or Databricks SQL
- Add CI/CD deployment using GitHub Actions

## Key Takeaways

This project demonstrates:
- End-to-end data pipeline design
- Lakehouse architecture
- Streaming ingestion concepts
- Data quality enforcement
- Incremental processing
- Senior-level system design thinking


## Author

Bill Walker
Senior Data Engineer