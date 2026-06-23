"""End-to-end integration check for the streaming + CDC stack.

Assumes the stack is up: `docker compose --profile stream up -d`.
Run by CI (GitHub Actions, Linux + Docker) and usable locally once Docker works.

  streaming: produce -> Redpanda -> consume -> land to bronze/stream (assert counts)
  CDC:       seed Postgres -> register Debezium -> change rows -> read events (assert)
"""

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.config import SETTINGS


def wait_postgres(timeout=150):
    import psycopg2
    for _ in range(timeout // 3):
        try:
            psycopg2.connect(SETTINGS.pg_dsn).close()
            return
        except Exception:
            time.sleep(3)
    raise SystemExit("Postgres not reachable")


def wait_kafka(timeout=150):
    from kafka import KafkaProducer
    for _ in range(timeout // 3):
        try:
            KafkaProducer(bootstrap_servers=SETTINGS.kafka_bootstrap).close()
            return
        except Exception:
            time.sleep(3)
    raise SystemExit("Redpanda (Kafka) not reachable")


def main():
    print("waiting for services...")
    wait_postgres()
    wait_kafka()

    print("\n== streaming ==")
    from streaming.producer import produce
    from streaming.consumer import consume
    produced = produce(5000)
    consumed = consume(max_messages=5000, batch_size=2500, idle_ms=20000)
    assert consumed >= produced * 0.9, f"stream loss: {consumed}/{produced}"
    print(f"STREAMING OK: produced {produced}, consumed {consumed}")

    print("\n== CDC ==")
    from cdc.setup_source import setup
    from cdc.register_connector import register
    from cdc.demo import make_changes, read_cdc
    setup()
    register()
    time.sleep(10)          # let Debezium snapshot + start streaming
    make_changes()
    time.sleep(5)
    events = read_cdc(idle_ms=30000)
    assert events >= 3, f"expected >=3 CDC events, got {events}"
    print(f"CDC OK: {events} change events captured")

    print("\nINTEGRATION OK")


if __name__ == "__main__":
    main()
