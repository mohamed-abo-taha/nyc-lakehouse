"""Register the Debezium Postgres connector with Kafka Connect.

Captures changes on public.drivers and publishes them to the topic
`cdc.public.drivers`. Uses the built-in pgoutput plugin (no Postgres extensions).

    python -m cdc.register_connector
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.config import SETTINGS

NAME = "drivers-cdc"


def _config() -> dict:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",   # Connect reaches Postgres by service name
        "database.port": "5432",
        "database.user": SETTINGS.pg_user,
        "database.password": SETTINGS.pg_password,
        "database.dbname": SETTINGS.pg_db,
        "topic.prefix": "cdc",
        "table.include.list": "public.drivers",
        "plugin.name": "pgoutput",
        "slot.name": "drivers_slot",
        "publication.autocreate.mode": "filtered",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
    }


def register() -> None:
    base = SETTINGS.connect_url
    for _ in range(60):  # Connect can take a while to come up
        try:
            if requests.get(f"{base}/connectors", timeout=3).ok:
                break
        except Exception:
            time.sleep(2)
    else:
        raise SystemExit(f"Kafka Connect not reachable at {base}")

    r = requests.put(f"{base}/connectors/{NAME}/config", json=_config(), timeout=20)
    print(f"register {NAME}: HTTP {r.status_code}")
    print(r.text[:300])
    r.raise_for_status()


if __name__ == "__main__":
    register()
