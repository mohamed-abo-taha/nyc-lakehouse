"""Batch ingestion: NYC TLC monthly Parquet -> bronze layer in the lake.

Bronze is raw and immutable: the source files land in object storage untouched,
so the pipeline can always be replayed from them. Cleaning and modeling happen
later in dbt (silver/gold).
"""

from __future__ import annotations

from pathlib import Path

import requests

from .config import SETTINGS, Settings
from .storage import duckdb_s3, ensure_bucket, upload

CHUNK = 1 << 20


def _download(month: str, s: Settings = SETTINGS) -> Path:
    fn = f"{s.dataset}_tripdata_{month}.parquet"
    dest = s.landing / fn
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    url = f"{s.tlc_base_url}/{fn}"
    tmp = dest.with_suffix(".part")
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                f.write(chunk)
    tmp.replace(dest)
    return dest


def ingest(s: Settings = SETTINGS) -> list[str]:
    ensure_bucket(s)
    keys = []
    for month in s.months:
        p = _download(month, s)
        key = f"{s.bronze_prefix}/{p.name}"
        uri = upload(p, key, s)
        print(f"  bronze <- {uri} ({p.stat().st_size / 1e6:.1f} MB)")
        keys.append(key)

    con = duckdb_s3(s)
    glob = f"s3://{s.s3_bucket}/{s.bronze_prefix}/*.parquet"
    n = con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]
    print(f"bronze total rows: {n:,} across {len(keys)} file(s)")
    return keys


if __name__ == "__main__":
    ingest()
