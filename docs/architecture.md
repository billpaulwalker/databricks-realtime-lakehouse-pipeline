# Architecture

## Source

The source is simulated telematics event data written as JSON files. This mimics a real streaming landing zone.

In a production environment, this source could be:

- Kafka
- Event Hubs
- Kinesis
- Auto Loader over cloud object storage
- IoT platform event feeds

## Bronze layer

The bronze layer captures raw events with minimal transformation.

### Why

Bronze should preserve the source-of-truth event stream so data can be replayed or reprocessed.

### Key design choices

- Append-only
- Delta format
- Ingestion metadata
- Checkpointing
- Explicit schema

## Silver layer

The silver layer turns raw events into trusted events.

### Responsibilities

- Parse timestamps
- Validate required columns
- Enforce reasonable speed and coordinate ranges
- Deduplicate by `event_id`
- Use watermarking for late data

## Gold layer

The gold layer creates reporting-ready aggregations.

### Tables

- Vehicle activity by day
- Driver behavior by day

## Design tradeoffs

| Decision | Benefit | Tradeoff |
|---|---|---|
| Structured Streaming | Near real-time processing | More operational complexity |
| Delta Lake | ACID transactions and schema evolution | Slight overhead versus raw Parquet |
| Medallion architecture | Clear separation of concerns | More tables to manage |
| Silver deduplication | Better data quality | Requires reliable event IDs |
| Date partitioning | Faster date-filtered queries | Poor choices can create small files |

## Production enhancements

- Auto Loader for cloud files
- Kafka source
- Unity Catalog permissions
- Delta Live Tables
- Databricks Workflows
- Data quality expectations
- Alerting and observability
- CI/CD deployment
