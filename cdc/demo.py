"""Demonstrate CDC: change rows in Postgres, then read the change events off Kafka.

    python -m cdc.demo

Debezium first snapshots existing rows (op='r'), then streams live changes
(op='c' insert, 'u' update, 'd' delete) with before/after images.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import psycopg2
from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.config import SETTINGS

TOPIC = "cdc.public.drivers"
OPS = {"r": "snapshot", "c": "insert", "u": "update", "d": "delete"}


def make_changes() -> None:
    con = psycopg2.connect(SETTINGS.pg_dsn)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("update public.drivers set status='inactive', updated_at=now() where driver_id=1")
    cur.execute("insert into public.drivers (driver_id, name, status) values (4, 'Dave', 'active') "
                "on conflict (driver_id) do update set status=excluded.status, updated_at=now()")
    cur.execute("delete from public.drivers where driver_id=3")
    con.close()
    print("applied to public.drivers: update #1 -> inactive, insert #4 (Dave), delete #3")


def read_cdc(idle_ms: int = 15000) -> int:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=SETTINGS.kafka_bootstrap,
        auto_offset_reset="earliest",
        group_id="cdc-demo",
        value_deserializer=lambda b: json.loads(b) if b else None,
        consumer_timeout_ms=idle_ms,
    )
    n = 0
    for msg in consumer:
        v = msg.value
        if not v:
            continue
        op = OPS.get(v.get("op"), v.get("op"))
        after, before = v.get("after"), v.get("before")
        print(f"  [{op:8}] before={before} after={after}")
        n += 1
    consumer.close()
    print(f"read {n} change events from {TOPIC}")
    return n


if __name__ == "__main__":
    make_changes()
    time.sleep(3)
    read_cdc()
