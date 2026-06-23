"""Consume the `trips` stream and land micro-batches into the bronze/stream lake.

A streaming counterpart to the batch ingest: the same lake, a `bronze/stream/`
prefix. dbt could union it with the batch bronze; here it proves the live path.

    python -m streaming.consumer
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.config import SETTINGS
from pipeline.storage import ensure_bucket, duckdb_s3, upload


def _flush(rows: list[dict], batch_no: int) -> str:
    tmp = SETTINGS.landing / f"_stream_batch_{batch_no}.parquet"
    pd.DataFrame(rows).to_parquet(tmp, index=False)
    key = f"{SETTINGS.bronze_prefix}/stream/trips_{batch_no:05d}.parquet"
    upload(tmp, key)
    tmp.unlink(missing_ok=True)
    return key


def consume(max_messages: int = 50000, batch_size: int = 10000, idle_ms: int = 8000) -> int:
    ensure_bucket()
    consumer = KafkaConsumer(
        SETTINGS.topic_trips,
        bootstrap_servers=SETTINGS.kafka_bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="trips-bronze",
        value_deserializer=lambda b: json.loads(b),
        consumer_timeout_ms=idle_ms,
    )
    buf, total, batch_no = [], 0, 0
    for msg in consumer:
        buf.append(msg.value)
        total += 1
        if len(buf) >= batch_size:
            batch_no += 1
            key = _flush(buf, batch_no)
            buf = []
            print(f"  landed batch {batch_no} -> s3://{SETTINGS.s3_bucket}/{key} ({total:,} total)")
        if total >= max_messages:
            break
    if buf:
        batch_no += 1
        _flush(buf, batch_no)
    consumer.close()

    glob = f"s3://{SETTINGS.s3_bucket}/{SETTINGS.bronze_prefix}/stream/*.parquet"
    n = duckdb_s3().execute(f"select count(*) from read_parquet('{glob}')").fetchone()[0]
    print(f"consumed {total:,} events; bronze/stream now holds {n:,} rows")
    return total


if __name__ == "__main__":
    consume()
