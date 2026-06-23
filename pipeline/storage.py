"""Lake (MinIO/S3) helpers and a DuckDB connection wired to read from it.

DuckDB is the lakehouse query engine: with the httpfs extension it reads Parquet
straight out of MinIO over the S3 API, so the same engine powers ingestion checks,
dbt, the dashboard, and the data-quality audit.
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
from minio import Minio

from .config import SETTINGS, Settings


def minio_client(s: Settings = SETTINGS) -> Minio:
    return Minio(s.s3_endpoint, access_key=s.s3_access_key,
                 secret_key=s.s3_secret_key, secure=s.s3_secure)


def ensure_bucket(s: Settings = SETTINGS, retries: int = 30) -> None:
    last = None
    for _ in range(retries):
        try:
            c = minio_client(s)
            if not c.bucket_exists(s.s3_bucket):
                c.make_bucket(s.s3_bucket)
            return
        except Exception as e:  # MinIO may still be starting
            last = e
            time.sleep(2)
    raise RuntimeError(f"MinIO not reachable at {s.s3_endpoint}: {last}")


def upload(local: Path, key: str, s: Settings = SETTINGS) -> str:
    minio_client(s).fput_object(s.s3_bucket, key, str(local))
    return f"s3://{s.s3_bucket}/{key}"


def duckdb_s3(s: Settings = SETTINGS) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{s.s3_endpoint}';")
    con.execute(f"SET s3_use_ssl={'true' if s.s3_secure else 'false'};")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{s.s3_access_key}';")
    con.execute(f"SET s3_secret_access_key='{s.s3_secret_key}';")
    con.execute("SET s3_region='us-east-1';")
    return con


def s3_glob(pattern: str, s: Settings = SETTINGS) -> str:
    return f"s3://{s.s3_bucket}/{pattern}"
