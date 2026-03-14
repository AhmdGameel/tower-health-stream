# Tower Health Stream - Technical Architecture

## Overview
Tower Health Stream implements a **Real-Time Lakehouse** pattern (Hot Path + Cold Analytical Path) for monitoring 10,192 Egyptian cell towers from OpenCelliD (`MCC=602`).

## Data Flow

### 1. Ingestion (`simulator/tower_simulator.py`)
- Reads `data/towers_clean.csv` and emits telemetry keyed by `tower_id` to Kafka topic `tower_events`.
- Emits UTC timestamps in SQL `TIMESTAMP(3)` format (`yyyy-MM-dd HH:mm:ss.SSS`) under field `ts` so Flink event-time watermarks parse deterministically.
- Injects configurable anomaly (~5%) and corrupted (~1%) rates to continuously exercise both alerting and the Dead Letter Queue.

### 2. Stream Quality & Anomaly Detection (`flink_jobs/`)
- **`data_quality.py` (PyFlink Table API, `StatementSet`, `EXACTLY_ONCE` checkpointing every 30s)**:
  - Consumes `tower_events` once and routes valid records (`signal_strength BETWEEN -120 AND 0`, `latency_ms > 0`, `connected_users >= 0`, non-null keys/timestamps) to `valid_events`, and invalid/null records to `dead_letter_queue`.
- **`anomaly_detection.py` (PyFlink Table API, `StatementSet`, `EXACTLY_ONCE` checkpointing every 30s)**:
  - Parses `event_time AS TO_TIMESTAMP(ts, 'yyyy-MM-dd HH:mm:ss.SSS')` with a 10-second bounded out-of-orderness watermark.
  - Persists stateful window checkpoints to `/opt/flink/checkpoints` so in-flight tumbling windows survive TaskManager restarts.
  - Routes tower-level alerts (`OVERLOADED`, `HIGH_DROP_RATE`, `WEAK_SIGNAL`, `HIGH_LATENCY`) to `tower_alerts`.
  - Runs a tumbling event-time window (`OUTAGE_WINDOW_MINUTES`, default 2m for local demo / configurable to 10m) grouped by `(area, operator)` to emit `REGIONAL_OUTAGE` events to a dedicated `regional_outages` topic when 3+ distinct towers degrade in the same window.

### 3. Hot Path: Weather Enrichment & Serving (`services/`)
- **`weather_enrichment.py`**:
  - Consumes `tower_alerts`, rounds `(lat, lon)` to a spatial grid (`WEATHER_GRID_PRECISION=1`, ~11km), and caches Open-Meteo responses for 15 minutes (`WEATHER_CACHE_TTL_SEC=900`).
  - Classifies `weather_impact` (`HIGH`, `MEDIUM`, `LOW`) and emits a flat JSON payload to `enriched_alerts`.
- **`postgres_sink.py`**:
  - Subscribes to both `enriched_alerts` and `regional_outages`, ensures tables exist with `CREATE TABLE IF NOT EXISTS` (never drops historical data), and batch-inserts via `psycopg2.extras.execute_values` into a dedicated `telecom_analytics` PostgreSQL 16 database.

### 4. Cold Path: Lakehouse & Batch Analytics (`services/` + `dbt_project/`)
- **`iceberg_sink.py`**:
  - Consumes `valid_events` and appends to `telecom.valid_events` in Apache Iceberg (REST Catalog), partitioned by `(event_date, operator)`.
  - Buffers up to `5,000` records or `30` seconds before committing to avoid small-file proliferation.
- **`analytics_runner.py` + `dbt_project/`**:
  - Reads the latest ACID Iceberg snapshot via PyIceberg into DuckDB (`raw_iceberg_telemetry`).
  - Executes `dbt run` (2 staging views + 3 telemetry-driven mart tables) and `dbt test` (18 data quality assertions).
  - Exports the 3 mart tables into PostgreSQL using staging tables and an atomic rename swap so Grafana queries never see partial refreshes.
  - Enforces a configurable hot-serving retention window (`POSTGRES_RETENTION_HOURS=168`, 7 days) on PostgreSQL `realtime_alerts` and `regional_outages`, while Apache Iceberg retains full historical telemetry.

### 5. Visualization (`docker/grafana/provisioning/`)
- Grafana 11.4 auto-provisions the PostgreSQL datasource (`telecom-postgres`) and a 10-panel NOC dashboard (4 top-level KPI stat cards, live OpenStreetMap geo-mapped alerts, regional outages, enriched alert logs, operator performance, lowest-health towers, and regional risk index).
