"""An operational `drivers` table in the warehouse DuckDB.

Stands in for the OLTP / CDC source so the SCD2 dbt snapshot has something that
changes between runs (mirrors the Postgres `drivers` table used by the CDC layer).
"""

from __future__ import annotations

import duckdb

from .config import SETTINGS, Settings

SEED = [(1, "Alice", "active"), (2, "Bob", "active"), (3, "Carol", "inactive")]


def seed(s: Settings = SETTINGS) -> None:
    con = duckdb.connect(str(s.warehouse_db))
    con.execute("create schema if not exists raw_oltp")
    con.execute("create or replace table raw_oltp.drivers "
                "(driver_id integer, name varchar, status varchar, updated_at timestamp)")
    con.executemany("insert into raw_oltp.drivers values (?, ?, ?, now())", SEED)
    con.close()


def apply_changes(s: Settings = SETTINGS) -> None:
    """A status change and a new driver — the events SCD2 should version."""
    con = duckdb.connect(str(s.warehouse_db))
    con.execute("update raw_oltp.drivers set status='inactive', updated_at=now() where driver_id=1")
    con.execute("insert into raw_oltp.drivers values (4, 'Dave', 'active', now())")
    con.close()


if __name__ == "__main__":
    seed()
    print("seeded raw_oltp.drivers")
