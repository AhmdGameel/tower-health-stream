import json
import logging
import os
import signal
import time

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ENRICHED_TOPIC = os.getenv("ENRICHED_ALERTS_TOPIC", "enriched_alerts")
OUTAGES_TOPIC = os.getenv("REGIONAL_OUTAGES_TOPIC", "regional_outages")
FLUSH_BATCH_SIZE = int(os.getenv("PG_FLUSH_BATCH_SIZE", "50"))
FLUSH_INTERVAL_SEC = float(os.getenv("PG_FLUSH_INTERVAL_SEC", "2.0"))

DB_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5434")),
    dbname=os.getenv("POSTGRES_DB", "telecom_analytics"),
    user=os.getenv("POSTGRES_USER", "telecom"),
    password=os.getenv("POSTGRES_PASSWORD", "telecom"),
)

running = True


def _handle_shutdown(signum, _frame):
    global running
    log.info("Received signal %s, shutting down postgres sink...", signum)
    running = False


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


def connect_postgres():
    for attempt in range(1, 31):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            log.info(
                "Connected to PostgreSQL at %s:%s/%s",
                DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"],
            )
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Postgres not ready (attempt %d/30): %s", attempt, exc)
            time.sleep(2)
    raise RuntimeError("Could not connect to PostgreSQL")


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realtime_alerts (
                id              BIGSERIAL PRIMARY KEY,
                tower_id        BIGINT NOT NULL,
                operator        TEXT,
                area            BIGINT,
                lat             DOUBLE PRECISION,
                lon             DOUBLE PRECISION,
                event_ts        TIMESTAMP,
                alert_type      TEXT,
                signal_strength DOUBLE PRECISION,
                latency_ms      DOUBLE PRECISION,
                connected_users INT,
                call_drop_rate  DOUBLE PRECISION,
                weather_impact  TEXT,
                conclusion      TEXT,
                temperature     DOUBLE PRECISION,
                rain            DOUBLE PRECISION,
                wind_speed      DOUBLE PRECISION,
                cloud_cover     INT,
                created_at      TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_realtime_alerts_created_at
            ON realtime_alerts (created_at DESC)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS regional_outages (
                id              BIGSERIAL PRIMARY KEY,
                area            BIGINT NOT NULL,
                operator        TEXT,
                window_start    TIMESTAMP,
                window_end      TIMESTAMP,
                affected_towers BIGINT,
                avg_signal      DOUBLE PRECISION,
                avg_drop_rate   DOUBLE PRECISION,
                alert_type      TEXT,
                created_at      TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_regional_outages_created_at
            ON regional_outages (created_at DESC)
        """)
    conn.commit()
    log.info("Ensured PostgreSQL tables (realtime_alerts, regional_outages) exist")


def connect_kafka() -> KafkaConsumer:
    for attempt in range(1, 31):
        try:
            consumer = KafkaConsumer(
                ENRICHED_TOPIC,
                OUTAGES_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="postgres-serving-sink-group",
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            log.info("Subscribed to Kafka topics: %s, %s", ENRICHED_TOPIC, OUTAGES_TOPIC)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/30), retrying in 2s...", attempt)
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP}")


def flush_buffers(conn, alerts_buf: list, outages_buf: list) -> None:
    if not alerts_buf and not outages_buf:
        return
    try:
        with conn.cursor() as cur:
            if alerts_buf:
                execute_values(
                    cur,
                    """
                    INSERT INTO realtime_alerts (
                        tower_id, operator, area, lat, lon, event_ts, alert_type,
                        signal_strength, latency_ms, connected_users, call_drop_rate,
                        weather_impact, conclusion, temperature, rain, wind_speed, cloud_cover
                    ) VALUES %s
                    """,
                    alerts_buf,
                )
            if outages_buf:
                execute_values(
                    cur,
                    """
                    INSERT INTO regional_outages (
                        area, operator, window_start, window_end,
                        affected_towers, avg_signal, avg_drop_rate, alert_type
                    ) VALUES %s
                    """,
                    outages_buf,
                )
        conn.commit()
        log.info(
            "Flushed to PostgreSQL: %d alerts, %d regional outages",
            len(alerts_buf), len(outages_buf),
        )
        alerts_buf.clear()
        outages_buf.clear()
    except Exception as exc:
        log.error("Failed to flush batch to PostgreSQL: %s", exc)
        conn.rollback()


def main() -> None:
    conn = connect_postgres()
    ensure_tables(conn)
    consumer = connect_kafka()

    alerts_buf: list[tuple] = []
    outages_buf: list[tuple] = []
    last_flush = time.monotonic()

    try:
        while running:
            for msg in consumer:
                if not running:
                    break
                payload = msg.value

                if msg.topic == ENRICHED_TOPIC:
                    alerts_buf.append((
                        payload.get("tower_id"),
                        payload.get("operator"),
                        payload.get("area"),
                        payload.get("lat"),
                        payload.get("lon"),
                        payload.get("ts"),
                        payload.get("alert_type"),
                        payload.get("signal_strength"),
                        payload.get("latency_ms"),
                        payload.get("connected_users"),
                        payload.get("call_drop_rate"),
                        payload.get("weather_impact"),
                        payload.get("conclusion"),
                        payload.get("temperature"),
                        payload.get("rain"),
                        payload.get("wind_speed"),
                        payload.get("cloud_cover"),
                    ))
                elif msg.topic == OUTAGES_TOPIC:
                    outages_buf.append((
                        payload.get("area"),
                        payload.get("operator"),
                        payload.get("window_start"),
                        payload.get("window_end"),
                        payload.get("affected_towers"),
                        payload.get("avg_signal"),
                        payload.get("avg_drop_rate"),
                        payload.get("alert_type"),
                    ))

                now = time.monotonic()
                if (
                    len(alerts_buf) + len(outages_buf) >= FLUSH_BATCH_SIZE
                    or (now - last_flush) >= FLUSH_INTERVAL_SEC
                ):
                    flush_buffers(conn, alerts_buf, outages_buf)
                    last_flush = now

            # Flush any remaining buffered records after consumer_timeout_ms
            if alerts_buf or outages_buf:
                flush_buffers(conn, alerts_buf, outages_buf)
                last_flush = time.monotonic()
    finally:
        flush_buffers(conn, alerts_buf, outages_buf)
        consumer.close()
        conn.close()
        log.info("PostgreSQL serving sink stopped.")


if __name__ == "__main__":
    main()
