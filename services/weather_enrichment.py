import json
import logging
import os
import signal
import time
import urllib.request

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = os.getenv("TOWER_ALERTS_TOPIC", "tower_alerts")
OUTPUT_TOPIC = os.getenv("ENRICHED_ALERTS_TOPIC", "enriched_alerts")
WEATHER_CACHE_TTL_SEC = int(os.getenv("WEATHER_CACHE_TTL_SEC", "900"))  # 15 minutes
GRID_PRECISION = int(os.getenv("WEATHER_GRID_PRECISION", "1"))  # ~11km grid to avoid rate limits

running = True


def _handle_shutdown(signum, _frame):
    global running
    log.info("Received signal %s, shutting down weather enrichment...", signum)
    running = False


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)

# In-memory TTL cache: {(grid_lat, grid_lon): (expires_at, weather_dict)}
_weather_cache: dict[tuple[float, float], tuple[float, dict]] = {}


def fetch_open_meteo(lat: float, lon: float) -> dict:
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&current=temperature_2m,wind_speed_10m,rain,cloud_cover"
        f"&timezone=Africa%2FCairo"
    )
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
            current = data.get("current", {})
            return {
                "temperature": current.get("temperature_2m"),
                "wind_speed": current.get("wind_speed_10m"),
                "rain": current.get("rain"),
                "cloud_cover": current.get("cloud_cover"),
            }
    except Exception as exc:
        log.warning("Open-Meteo lookup failed for (%.2f, %.2f): %s", lat, lon, exc)
        return {
            "temperature": None,
            "wind_speed": None,
            "rain": None,
            "cloud_cover": None,
        }


def get_weather_cached(lat: float, lon: float) -> tuple[dict, bool]:
    key = (round(lat, GRID_PRECISION), round(lon, GRID_PRECISION))
    now = time.monotonic()
    cached = _weather_cache.get(key)
    if cached and cached[0] > now:
        return cached[1], True

    weather = fetch_open_meteo(key[0], key[1])
    _weather_cache[key] = (now + WEATHER_CACHE_TTL_SEC, weather)
    return weather, False


def classify_weather_impact(weather: dict) -> tuple[str, str]:
    rain = weather.get("rain") or 0
    wind_speed = weather.get("wind_speed") or 0
    cloud_cover = weather.get("cloud_cover") or 0

    if rain > 5 or wind_speed > 40:
        return "HIGH", "Weather is likely the primary cause"
    if rain > 1 or wind_speed > 20 or cloud_cover > 80:
        return "MEDIUM", "Weather may be a partial contributing factor"
    return "LOW", "Technical issue - not weather related"


def connect_kafka() -> tuple[KafkaConsumer, KafkaProducer]:
    for attempt in range(1, 31):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="weather-enrichment-group",
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                key_serializer=lambda k: str(k).encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return consumer, producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/30), retrying in 2s...", attempt)
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP}")


def main() -> None:
    consumer, producer = connect_kafka()
    log.info(
        "Weather enrichment started (in=%s, out=%s, cache_ttl=%ds)",
        INPUT_TOPIC, OUTPUT_TOPIC, WEATHER_CACHE_TTL_SEC,
    )

    processed = 0
    cache_hits = 0

    try:
        while running:
            for msg in consumer:
                if not running:
                    break
                alert = msg.value
                lat = alert.get("lat")
                lon = alert.get("lon")
                if lat is None or lon is None:
                    continue

                weather, hit = get_weather_cached(float(lat), float(lon))
                impact, conclusion = classify_weather_impact(weather)

                # Produce a clean flat schema so downstream sinks can consume directly
                enriched = {
                    "tower_id": alert.get("tower_id"),
                    "operator": alert.get("operator"),
                    "area": alert.get("area"),
                    "lat": lat,
                    "lon": lon,
                    "ts": alert.get("ts"),
                    "signal_strength": alert.get("signal_strength"),
                    "latency_ms": alert.get("latency_ms"),
                    "connected_users": alert.get("connected_users"),
                    "call_drop_rate": alert.get("call_drop_rate"),
                    "alert_type": alert.get("alert_type"),
                    "weather_impact": impact,
                    "conclusion": conclusion,
                    "temperature": weather.get("temperature"),
                    "rain": weather.get("rain"),
                    "wind_speed": weather.get("wind_speed"),
                    "cloud_cover": weather.get("cloud_cover"),
                }

                producer.send(OUTPUT_TOPIC, key=enriched["tower_id"], value=enriched)
                processed += 1
                if hit:
                    cache_hits += 1

                if processed % 100 == 0:
                    hit_rate = (cache_hits / processed) * 100
                    log.info(
                        "Enriched %d alerts (cache_entries=%d, cache_hit_rate=%.1f%%)",
                        processed, len(_weather_cache), hit_rate,
                    )
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        log.info("Weather enrichment stopped. Total processed: %d", processed)


if __name__ == "__main__":
    main()
