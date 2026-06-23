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


def _wait_rest_ready(base: str, attempts: int = 120) -> None:
    # Connect's REST port accepts connections before it's ready and returns 503
    # while workers start, so sleep every attempt and accept only 200.
    for _ in range(attempts):
        try:
            if requests.get(f"{base}/connectors", timeout=5).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit(f"Kafka Connect REST not ready at {base}")


def _wait_running(base: str, attempts: int = 60) -> None:
    for _ in range(attempts):
        try:
            st = requests.get(f"{base}/connectors/{NAME}/status", timeout=5).json()
            conn = st.get("connector", {}).get("state")
            tasks = [t.get("state") for t in st.get("tasks", [])]
            if "FAILED" in [conn, *tasks]:
                raise SystemExit(f"connector FAILED: {st}")
            if conn == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks):
                print(f"connector RUNNING (tasks: {tasks})")
                return
        except SystemExit:
            raise
        except Exception:
            pass
        time.sleep(2)
    print("warning: connector not confirmed RUNNING; proceeding anyway")


def register() -> None:
    base = SETTINGS.connect_url
    _wait_rest_ready(base)
    r = requests.put(f"{base}/connectors/{NAME}/config", json=_config(), timeout=30)
    print(f"register {NAME}: HTTP {r.status_code}")
    r.raise_for_status()
    _wait_running(base)


if __name__ == "__main__":
    register()
