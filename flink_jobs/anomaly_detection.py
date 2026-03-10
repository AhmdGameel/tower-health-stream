import os
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

os.environ.setdefault("PYFLINK_PYTHON", "/usr/bin/python3")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
OUTAGE_WINDOW_MINUTES = int(os.getenv("OUTAGE_WINDOW_MINUTES", "5"))

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=settings)
t_env.get_config().set("pipeline.name", "Tower Health - Anomaly Detection")
t_env.get_config().set("parallelism.default", "1")
t_env.get_config().set(
    "pipeline.jars",
    "file:///opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar",
)

# Source: valid_events with proper event-time watermark
t_env.execute_sql(f"""
    CREATE TABLE valid_events (
        tower_id        BIGINT,
        operator        STRING,
        radio           STRING,
        lat             DOUBLE,
        lon             DOUBLE,
        area            BIGINT,
        ts              STRING,
        signal_strength DOUBLE,
        latency_ms      DOUBLE,
        connected_users INT,
        call_drop_rate  DOUBLE,
        is_peak_hour    BOOLEAN,
        is_anomaly      BOOLEAN,
        event_time      AS TO_TIMESTAMP(ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'valid_events',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'properties.group.id'          = 'flink-anomaly-detection-group',
        'scan.startup.mode'            = 'latest-offset',
        'format'                       = 'json',
        'json.ignore-parse-errors'     = 'true'
    )
""")

# Sink 1: Tower-level alerts (dedicated topic with clean schema)
t_env.execute_sql(f"""
    CREATE TABLE tower_alerts (
        tower_id        BIGINT,
        operator        STRING,
        lat             DOUBLE,
        lon             DOUBLE,
        area            BIGINT,
        ts              STRING,
        signal_strength DOUBLE,
        latency_ms      DOUBLE,
        connected_users INT,
        call_drop_rate  DOUBLE,
        alert_type      STRING
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'tower_alerts',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'format'                       = 'json'
    )
""")

# Sink 2: Area-level regional outages (separate topic to avoid schema pollution)
t_env.execute_sql(f"""
    CREATE TABLE regional_outages (
        area            BIGINT,
        operator        STRING,
        window_start    TIMESTAMP(3),
        window_end      TIMESTAMP(3),
        affected_towers BIGINT,
        avg_signal      DOUBLE,
        avg_drop_rate   DOUBLE,
        alert_type      STRING
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'regional_outages',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'format'                       = 'json'
    )
""")

# Use StatementSet so valid_events is read ONCE and routed to both outputs in one Flink job
stmt_set = t_env.create_statement_set()

stmt_set.add_insert_sql("""
    INSERT INTO tower_alerts
    SELECT
        tower_id,
        operator,
        lat,
        lon,
        area,
        ts,
        signal_strength,
        latency_ms,
        connected_users,
        call_drop_rate,
        CASE
            WHEN signal_strength < -90 AND connected_users > 800 THEN 'OVERLOADED'
            WHEN call_drop_rate > 0.05 THEN 'HIGH_DROP_RATE'
            WHEN signal_strength < -90 THEN 'WEAK_SIGNAL'
            WHEN latency_ms > 200 THEN 'HIGH_LATENCY'
            ELSE 'DEGRADED'
        END AS alert_type
    FROM valid_events
    WHERE signal_strength < -90
       OR call_drop_rate > 0.05
       OR latency_ms > 200
""")

stmt_set.add_insert_sql(f"""
    INSERT INTO regional_outages
    SELECT
        area,
        operator,
        window_start,
        window_end,
        COUNT(DISTINCT tower_id) AS affected_towers,
        ROUND(AVG(signal_strength), 2) AS avg_signal,
        ROUND(AVG(call_drop_rate), 4)  AS avg_drop_rate,
        'REGIONAL_OUTAGE'              AS alert_type
    FROM TABLE(
        TUMBLE(TABLE valid_events, DESCRIPTOR(event_time), INTERVAL '{OUTAGE_WINDOW_MINUTES}' MINUTE)
    )
    WHERE signal_strength < -90
       OR call_drop_rate > 0.05
    GROUP BY
        area,
        operator,
        window_start,
        window_end
    HAVING COUNT(DISTINCT tower_id) >= 3
""")

print(f"Submitting Anomaly Detection Flink job (outage window={OUTAGE_WINDOW_MINUTES}m)...")
stmt_set.execute()
