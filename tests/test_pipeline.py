"""Offline tests: settings + the silver cleaning rules (no Docker, no network)."""

from __future__ import annotations

import duckdb

from pipeline.config import Settings


def test_settings_defaults():
    s = Settings()
    assert s.bronze_prefix == "bronze/yellow"
    assert f"dbname={s.pg_db}" in s.pg_dsn
    assert len(s.months) >= 1


def test_cleaning_rules_filter_and_coalesce():
    """Mirrors stg_yellow_trips: drop impossible/out-of-window rows, coalesce
    null foreign keys to their dimension's unknown member."""
    con = duckdb.connect()
    con.execute("""
        create table raw (
            vendor_id integer, pickup timestamp, dropoff timestamp,
            pu_loc integer, do_loc integer, payment_type integer, ratecode integer,
            trip_distance double, fare_amount double, total_amount double
        )
    """)
    con.execute("""
        insert into raw values
          (1,    timestamp '2023-01-15 10:00', timestamp '2023-01-15 10:20', 100, 200, 1, 1, 2.0, 10.0, 12.0),
          (NULL, timestamp '2023-01-15 11:00', timestamp '2023-01-15 11:10', NULL, NULL, NULL, NULL, 1.0, 5.0, 5.0),
          (2,    timestamp '2023-01-15 12:00', timestamp '2023-01-15 11:00', 1, 2, 1, 1, 1.0, 5.0, 5.0),
          (2,    timestamp '2023-01-15 13:00', timestamp '2023-01-15 13:10', 1, 2, 1, 1, 1.0, -5.0, 5.0),
          (2,    timestamp '2022-06-01 09:00', timestamp '2022-06-01 09:10', 1, 2, 1, 1, 1.0, 5.0, 5.0)
    """)
    out = con.execute("""
        select coalesce(vendor_id, 0)     as vendor_id,
               coalesce(pu_loc, 264)      as pickup_zone_id,
               coalesce(payment_type, 5)  as payment_type_id,
               coalesce(ratecode, 99)     as rate_code_id
        from raw
        where dropoff >= pickup and fare_amount >= 0 and total_amount >= 0
          and pickup >= timestamp '2023-01-01' and pickup < timestamp '2023-04-01'
        order by pickup
    """).fetchall()
    assert len(out) == 2                 # dropoff<pickup, negative fare, out-of-window dropped
    assert out[0] == (1, 100, 1, 1)
    assert out[1] == (0, 264, 5, 99)     # null FKs coalesced to unknown members
