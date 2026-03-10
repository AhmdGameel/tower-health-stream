import os
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

os.environ.setdefault("PYFLINK_PYTHON", "/usr/bin/python3")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=settings)
t_env.get_config().set("pipeline.name", "Tower Health - Data Quality")
t_env.get_config().set("parallelism.default", "1")
t_env.get_config().set(
    "pipeline.jars",
    "file:///opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar",
)

COMMON_SCHEMA = """
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
    is_anomaly      BOOLEAN
"""

t_env.execute_sql(f"""
    CREATE TABLE tower_events (
        {COMMON_SCHEMA}
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'tower_events',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'properties.group.id'          = 'flink-data-quality-group',
        'scan.startup.mode'            = 'latest-offset',
        'format'                       = 'json',
        'json.ignore-parse-errors'     = 'true'
    )
""")

t_env.execute_sql(f"""
    CREATE TABLE valid_events (
        {COMMON_SCHEMA}
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'valid_events',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'format'                       = 'json'
    )
""")

t_env.execute_sql(f"""
    CREATE TABLE dead_letter (
        {COMMON_SCHEMA}
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'dead_letter_queue',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'format'                       = 'json'
    )
""")

# Use StatementSet so Kafka is consumed ONCE and fanned out to both sinks in a single Flink job
stmt_set = t_env.create_statement_set()

stmt_set.add_insert_sql("""
    INSERT INTO valid_events
    SELECT * FROM tower_events
    WHERE tower_id IS NOT NULL
      AND ts IS NOT NULL
      AND signal_strength IS NOT NULL
      AND signal_strength >= -120
      AND signal_strength <= 0
      AND latency_ms IS NOT NULL
      AND latency_ms > 0
      AND connected_users IS NOT NULL
      AND connected_users >= 0
""")

stmt_set.add_insert_sql("""
    INSERT INTO dead_letter
    SELECT * FROM tower_events
    WHERE tower_id IS NULL
       OR ts IS NULL
       OR signal_strength IS NULL
       OR signal_strength < -120
       OR signal_strength > 0
       OR latency_ms IS NULL
       OR latency_ms <= 0
       OR connected_users IS NULL
       OR connected_users < 0
""")

print("Submitting Data Quality Flink job...")
stmt_set.execute()
