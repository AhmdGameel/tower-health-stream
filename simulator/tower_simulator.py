import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("RAW_EVENTS_TOPIC", "tower_events")
TOWERS_CSV = os.getenv("TOWERS_CSV_PATH", "data/towers_clean.csv")
INTERVAL_SEC = float(os.getenv("SIMULATOR_INTERVAL_SEC", "5"))
BATCH_LIMIT = int(os.getenv("SIMULATOR_BATCH_LIMIT", "2000"))
ANOMALY_RATE = float(os.getenv("SIMULATOR_ANOMALY_RATE", "0.05"))
BAD_DATA_RATE = float(os.getenv("SIMULATOR_BAD_DATA_RATE", "0.01"))

running = True


def _handle_shutdown(signum, _frame):
    global running
    log.info("Received signal %s, shutting down simulator...", signum)
    running = False


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    for attempt in range(1, 31):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: str(k).encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
                batch_size=32768,
            )
            log.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/30), retrying in 2s...", attempt)
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Kafka at {bootstrap_servers}")


def is_peak_hour() -> bool:
    hour = datetime.now(timezone.utc).hour + 2  # Africa/Cairo UTC+2
    return (7 <= hour <= 9) or (17 <= hour <= 19)


def generate_event(tower: dict) -> dict:
    peak = is_peak_hour()
    anomaly = random.random() < ANOMALY_RATE
    corrupted = random.random() < BAD_DATA_RATE

    signal_strength = random.uniform(-88, -60)
    latency_ms = random.uniform(20, 60)
    users = random.randint(50, 300)
    drop_rate = random.uniform(0.01, 0.04)

    if peak:
        signal_strength -= random.uniform(5, 12)
        latency_ms += random.uniform(10, 40)
        users += random.randint(150, 550)

    if anomaly:
        signal_strength -= random.uniform(15, 25)
        latency_ms += random.uniform(120, 220)
        drop_rate += random.uniform(0.05, 0.25)

    if corrupted:
        # Inject realistic sensor/telemetry corruption to exercise the DLQ
        fault = random.choice(["signal_out_of_range", "negative_latency", "negative_users"])
        if fault == "signal_out_of_range":
            signal_strength = random.choice([-150.0, 25.0])
        elif fault == "negative_latency":
            latency_ms = -10.0
        else:
            users = -5

    # Standard SQL TIMESTAMP(3) format so Flink TO_TIMESTAMP(ts) parses cleanly
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    return {
        "tower_id": int(tower["tower_id"]),
        "operator": str(tower["operator"]),
        "radio": str(tower["radio"]),
        "lat": float(tower["lat"]),
        "lon": float(tower["lon"]),
        "area": int(tower["area"]),
        "ts": ts,
        "signal_strength": round(signal_strength, 2),
        "latency_ms": round(latency_ms, 2),
        "connected_users": int(users),
        "call_drop_rate": round(drop_rate, 4),
        "is_peak_hour": peak,
        "is_anomaly": anomaly,
    }


def main() -> None:
    if not os.path.exists(TOWERS_CSV):
        log.error("Towers file not found: %s", TOWERS_CSV)
        sys.exit(1)

    df = pd.read_csv(TOWERS_CSV)
    towers = df.to_dict("records")
    log.info("Loaded %d towers from %s", len(towers), TOWERS_CSV)

    producer = create_producer(KAFKA_BOOTSTRAP)
    log.info(
        "Simulator started (topic=%s, batch_limit=%d, interval=%.1fs)",
        TOPIC, BATCH_LIMIT, INTERVAL_SEC,
    )

    try:
        while running:
            batch = random.sample(towers, min(BATCH_LIMIT, len(towers))) if BATCH_LIMIT > 0 else towers
            for tower in batch:
                event = generate_event(tower)
                producer.send(TOPIC, key=event["tower_id"], value=event)

            producer.flush()
            log.info("Sent %d events to '%s'", len(batch), TOPIC)

            slept = 0.0
            while running and slept < INTERVAL_SEC:
                time.sleep(0.5)
                slept += 0.5
    finally:
        producer.flush()
        producer.close()
        log.info("Simulator stopped.")


if __name__ == "__main__":
    main()
