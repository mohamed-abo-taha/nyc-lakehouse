"""Create and seed an OLTP source table (public.drivers) in Postgres.

This stands in for an operational database that a CDC pipeline would track.
`replica identity full` makes Postgres emit the old row values on updates/deletes,
so the change events carry both before and after.

    python -m cdc.setup_source
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.config import SETTINGS

DDL = """
create table if not exists public.drivers (
    driver_id  int primary key,
    name       text,
    status     text,
    updated_at timestamp default now()
);
alter table public.drivers replica identity full;
"""

SEED = [(1, "Alice", "active"), (2, "Bob", "active"), (3, "Carol", "inactive")]


def setup() -> None:
    con = psycopg2.connect(SETTINGS.pg_dsn)
    con.autocommit = True
    cur = con.cursor()
    cur.execute(DDL)
    cur.execute("delete from public.drivers")
    cur.executemany(
        "insert into public.drivers (driver_id, name, status) values (%s, %s, %s)", SEED)
    cur.execute("select count(*) from public.drivers")
    print(f"seeded public.drivers with {cur.fetchone()[0]} rows")
    con.close()


if __name__ == "__main__":
    setup()
