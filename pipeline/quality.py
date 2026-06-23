"""Data-quality audit on the gold layer.

dbt tests cover schema-level contracts (not_null, unique, relationships, ranges).
This adds operational checks a pipeline should fail on: non-empty facts, no
orphaned foreign keys, no duplicate keys, and a freshness read. In production this
is where Great Expectations or Soda would plug in; the checks here are the same
idea without the extra dependency.
"""

from __future__ import annotations

import duckdb

from .config import SETTINGS, Settings


def audit(s: Settings = SETTINGS) -> list:
    con = duckdb.connect(str(s.warehouse_db), read_only=True)
    checks = []

    n = con.execute("select count(*) from gold.fct_trips").fetchone()[0]
    checks.append(("fct_trips_rowcount", n, n > 0))

    dup = con.execute(
        "select count(*) from (select trip_id from gold.fct_trips "
        "group by trip_id having count(*) > 1)").fetchone()[0]
    checks.append(("duplicate_trip_ids", dup, dup == 0))

    orphans = con.execute(
        "select count(*) from gold.fct_trips f "
        "left join gold.dim_zone z on f.pickup_zone_id = z.zone_id "
        "where z.zone_id is null").fetchone()[0]
    checks.append(("orphan_pickup_zones", orphans, orphans == 0))

    neg = con.execute(
        "select count(*) from gold.fct_trips where total_amount < 0").fetchone()[0]
    checks.append(("negative_total_amount", neg, neg == 0))

    latest = con.execute("select max(pickup_at) from gold.fct_trips").fetchone()[0]
    checks.append(("latest_pickup_at", latest, latest is not None))

    con.close()
    ok = True
    for name, val, passed in checks:
        ok = ok and passed
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}: {val}")
    if not ok:
        raise SystemExit("data-quality audit failed")
    print("data-quality audit: all checks passed")
    return checks


if __name__ == "__main__":
    audit()
