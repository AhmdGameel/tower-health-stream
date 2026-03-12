import json
import logging
import os
import signal
import time

import pyarrow as pa
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import BooleanType, DoubleType, LongType, NestedField, StringType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VALID_TOPIC = os.getenv("VALID_EVENTS_TOPIC", "valid_events")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://localhost:8182")
BATCH_SIZE = int(os.getenv("ICEBERG_BATCH_SIZE", "5000"))
FLUSH_INTERVAL_SEC = float(os.getenv("ICEBERG_FLUSH_INTERVAL_SEC", "30"))

running = True


def _handle_shutdown(signum, _frame):
    global running
    log.info("Received signal %s, shutting down Iceberg sink...", signum)
    running = False


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)

SCHEMA = Schema(
    NestedField(1, "tower_id", LongType(), required=True),
    NestedField(2, "operator", StringType(), required=False),
    NestedField(3, "radio", StringType(), required=False),
    NestedField(4, "lat", DoubleType(), required=False),
    NestedField(5, "lon", DoubleType(), required=False),
    NestedField(6, "area", LongType(), required=False),
    NestedField(7, "ts", StringType(), required=False),
    NestedField(8, "event_date", StringType(), required=False),
    NestedField(9, "signal_strength", DoubleType(), required=False),
    NestedField(10, "latency_ms", DoubleType(), required=False),
    NestedField(11, "connected_users", LongType(), required=False),
    NestedField(12, "call_drop_rate", DoubleType(), required=False),
    NestedField(13, "is_peak_hour", BooleanType(), required=False),
    NestedField(14, "is_anomaly", BooleanType(), required=False),
)

PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=8, field_id=1000, transform=IdentityTransform(), name="event_date"),
    PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="operator"),
)


def connect_iceberg():
    for attempt in range(1, 31):
        try:
            catalog = RestCatalog(name="rest", uri=ICEBERG_REST_URI)
            try:
                catalog.create_namespace("telecom")
            except NamespaceAlreadyExistsError:
                pass

            try:
                table = catalog.create_table(
                    identifier="telecom.valid_events",
                    schema=SCHEMA,
                    partition_spec=PARTITION_SPEC,
                )
                log.info("Created partitioned Iceberg table: telecom.valid_events")
            except TableAlreadyExistsError:
                table = catalog.load_table("telecom.valid_events")
                log.info("Loaded existing Iceberg table: telecom.valid_events")

            return table
        except Exception as exc:
            log.warning("Iceberg REST catalog not ready (attempt %d/30): %s", attempt, exc)
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Iceberg REST catalog at {ICEBERG_REST_URI}")


def connect_kafka() -> KafkaConsumer:
    for attempt in range(1, 31):
        try:
            consumer = KafkaConsumer(
                VALID_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="iceberg-lakehouse-sink-group",
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=2000,
            )
            log.info("Connected to Kafka topic '%s' at %s", VALID_TOPIC, KAFKA_BOOTSTRAP)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/30), retrying in 2s...", attempt)
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP}")


def flush_to_iceberg(table, batch: list[dict]) -> None:
    if not batch:
        return
    try:
        arrow_table = pa.Table.from_pylist(batch, schema=SCHEMA.as_arrow())
        table.append(arrow_table)
        log.info("Committed %d records to Iceberg table telecom.valid_events", len(batch))
        batch.clear()
    except Exception as exc:
        log.error("Failed to commit batch to Iceberg: %s", exc)
        batch.clear()


def main() -> None:
    table = connect_iceberg()
    consumer = connect_kafka()

    log.info(
        "Iceberg sink started (batch_size=%d, flush_interval=%.0fs)",
        BATCH_SIZE, FLUSH_INTERVAL_SEC,
    )

    batch: list[dict] = []
    last_flush = time.monotonic()

    try:
        while running:
            for msg in consumer:
                if not running:
                    break
                event = msg.value
                ts = event.get("ts") or ""
                event_date = ts[:10] if len(ts) >= 10 else "unknown"

                batch.append({
                    "tower_id": event.get("tower_id"),
                    "operator": event.get("operator"),
                    "radio": event.get("radio"),
                    "lat": event.get("lat"),
                    "lon": event.get("lon"),
                    "area": event.get("area"),
                    "ts": ts,
                    "event_date": event_date,
                    "signal_strength": event.get("signal_strength"),
                    "latency_ms": event.get("latency_ms"),
                    "connected_users": event.get("connected_users"),
                    "call_drop_rate": event.get("call_drop_rate"),
                    "is_peak_hour": event.get("is_peak_hour"),
                    "is_anomaly": event.get("is_anomaly"),
                })

                now = time.monotonic()
                if len(batch) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL_SEC:
                    flush_to_iceberg(table, batch)
                    last_flush = now

            if batch and (time.monotonic() - last_flush) >= FLUSH_INTERVAL_SEC:
                flush_to_iceberg(table, batch)
                last_flush = time.monotonic()
    finally:
        flush_to_iceberg(table, batch)
        consumer.close()
        log.info("Iceberg sink stopped.")


if __name__ == "__main__":
    main()
