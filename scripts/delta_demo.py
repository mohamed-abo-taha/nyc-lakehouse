"""Delta Lake table format on the lakehouse: ACID writes + time travel.

Uses delta-rs (no Spark): write an initial version, append a second, then read the
latest and time-travel back to version 0. DuckDB reads the same Delta table too.
Works on a local path here; the same call takes an s3://lake/... URI for MinIO.

  python scripts/delta_demo.py   (needs the warehouse built: gold.fct_trips)
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import duckdb
from deltalake import DeltaTable, write_deltalake

from pipeline.config import SETTINGS

DELTA = ROOT / "data" / "delta" / "trips"
COLS = "date_key, pickup_zone_id, dropoff_zone_id, total_amount"


def main():
    src = duckdb.connect(str(SETTINGS.warehouse_db), read_only=True)
    df0 = src.execute(f"select {COLS} from gold.fct_trips limit 2000").fetchdf()
    df1 = src.execute(f"select {COLS} from gold.fct_trips limit 1000 offset 2000").fetchdf()

    DELTA.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(DELTA), df0, mode="overwrite")   # version 0
    write_deltalake(str(DELTA), df1, mode="append")      # version 1 (ACID append)

    dt = DeltaTable(str(DELTA))
    print(f"delta current version: {dt.version()}  (0 = initial, 1 = after append)")
    print(f"latest rows: {len(dt.to_pandas()):,}")

    v0 = DeltaTable(str(DELTA), version=0)               # time travel
    print(f"time-travel to v0 rows: {len(v0.to_pandas()):,}")

    duck = duckdb.connect()
    duck.execute("install delta; load delta;")
    n = duck.execute(f"select count(*) from delta_scan('{DELTA.as_posix()}')").fetchone()[0]
    print(f"DuckDB delta_scan rows: {n:,}")

    assert dt.version() == 1 and len(dt.to_pandas()) == 3000 and len(v0.to_pandas()) == 2000
    print("\nDELTA OK: ACID append created v1 (3,000 rows); v0 (2,000) still readable via time travel")


if __name__ == "__main__":
    main()
