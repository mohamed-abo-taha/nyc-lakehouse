"""Replay landed NYC trips into the `trips` topic as a simulated live feed.

    python -m streaming.producer [n]      # default 50,000 events
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.config import SETTINGS


def _synthetic(limit: int) -> list[dict]:
    """Fallback feed when no Parquet is landed (used in CI so no download is needed)."""
    import random
    rng = random.Random(42)
    rows = []
    for _ in range(limit):
        h, m = rng.randint(0, 23), rng.randint(0, 58)
        dur = rng.randint(3, 40)
        rows.append({
            "vendor_id": rng.choice([1, 2]),
            "pickup_at": f"2023-01-15 {h:02d}:{m:02d}:00",
            "dropoff_at": f"2023-01-15 {h:02d}:{min(m + dur, 59):02d}:00",
            "pickup_zone_id": rng.randint(1, 263),
            "dropoff_zone_id": rng.randint(1, 263),
            "payment_type": rng.choice([1, 2]),
            "rate_code_id": 1,
            "passenger_count": rng.randint(1, 4),
            "trip_distance": round(rng.uniform(0.5, 15), 2),
            "fare_amount": round(rng.uniform(5, 60), 2),
            "tip_amount": round(rng.uniform(0, 15), 2),
            "tolls_amount": 0.0,
            "total_amount": round(rng.uniform(6, 80), 2),
        })
    return rows


def _rows(limit: int) -> list[dict]:
    files = sorted(SETTINGS.landing.glob(f"{SETTINGS.dataset}_tripdata_*.parquet"))
    if not files:
        print("no landed Parquet; producing synthetic trips")
        return _synthetic(limit)
    sql = f"""
        select
            VendorID as vendor_id,
            strftime(tpep_pickup_datetime,  '%Y-%m-%d %H:%M:%S') as pickup_at,
            strftime(tpep_dropoff_datetime, '%Y-%m-%d %H:%M:%S') as dropoff_at,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            payment_type,
            RatecodeID as rate_code_id,
            passenger_count, trip_distance,
            fare_amount, tip_amount, tolls_amount, total_amount
        from read_parquet('{files[0].as_posix()}')
        limit {limit}
    """
    return duckdb.connect().execute(sql).fetch_arrow_table().to_pylist()


def produce(limit: int = 50000) -> int:
    rows = _rows(limit)
    producer = KafkaProducer(
        bootstrap_servers=SETTINGS.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
        linger_ms=50,
    )
    for i, row in enumerate(rows, 1):
        producer.send(SETTINGS.topic_trips, row)
        if i % 10000 == 0:
            print(f"  produced {i:,}")
    producer.flush()
    print(f"produced {len(rows):,} events to topic '{SETTINGS.topic_trips}'")
    return len(rows)


if __name__ == "__main__":
    produce(int(sys.argv[1]) if len(sys.argv) > 1 else 50000)
