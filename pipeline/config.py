"""Project settings, read from the environment (.env) with docker-compose defaults.

Keeping config in one place means the ingest scripts, the Dagster assets, the
dashboard, and the dbt profile all point at the same lake and warehouse.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


_load_dotenv(ROOT / ".env")


@dataclass
class Settings:
    # object store (the lake)
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "localhost:9000")
    s3_access_key: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    s3_secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    s3_bucket: str = os.getenv("S3_BUCKET", "lake")
    s3_secure: bool = os.getenv("S3_SECURE", "false").lower() == "true"

    # serving warehouse
    pg_host: str = os.getenv("PGHOST", "localhost")
    pg_port: int = int(os.getenv("PGPORT", "5432"))
    pg_db: str = os.getenv("POSTGRES_DB", "warehouse")
    pg_user: str = os.getenv("POSTGRES_USER", "warehouse")
    pg_password: str = os.getenv("POSTGRES_PASSWORD", "warehouse")

    # source
    tlc_base_url: str = os.getenv("TLC_BASE_URL",
                                  "https://d37ci6vzurychx.cloudfront.net/trip-data")
    dataset: str = os.getenv("TLC_DATASET", "yellow")
    months: list = field(default_factory=lambda: [
        m.strip() for m in os.getenv("TLC_MONTHS", "2023-01,2023-02,2023-03").split(",")
        if m.strip()
    ])

    # streaming (Kafka API via Redpanda) + CDC
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
    topic_trips: str = os.getenv("TOPIC_TRIPS", "trips")
    connect_url: str = os.getenv("CONNECT_URL", "http://localhost:8083")

    landing: Path = ROOT / "data" / "landing"
    warehouse_db: Path = ROOT / "data" / "warehouse" / "warehouse.duckdb"

    @property
    def bronze_prefix(self) -> str:
        return f"bronze/{self.dataset}"

    @property
    def pg_dsn(self) -> str:
        return (f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
                f"user={self.pg_user} password={self.pg_password}")


SETTINGS = Settings()
