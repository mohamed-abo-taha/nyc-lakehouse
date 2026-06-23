"""SCD2 demo: a dbt snapshot versions a changing dimension (Type 2 history).

  seed drivers -> dbt snapshot (v1) -> change a driver -> dbt snapshot (v2)
  -> the snapshot now holds both versions, the old one closed (dbt_valid_to set).

Local + DuckDB only (no Docker).  python scripts/scd2_demo.py
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import duckdb

from pipeline.config import SETTINGS
from pipeline.load_drivers import apply_changes, seed

DBT = str(Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt"))


def snapshot():
    subprocess.run([DBT, "snapshot", "--project-dir", "dbt", "--profiles-dir", "dbt"],
                   cwd=ROOT, check=True)


def main():
    seed()
    snapshot()           # initial versions for drivers 1-3
    apply_changes()      # driver 1 status change + new driver 4
    snapshot()           # SCD2 closes driver 1's old row, opens a new one

    con = duckdb.connect(str(SETTINGS.warehouse_db), read_only=True)
    rows = con.execute("""
        select driver_id, status, dbt_valid_from, dbt_valid_to
        from snapshots.drivers_snapshot
        order by driver_id, dbt_valid_from
    """).fetchdf()
    print("\n" + rows.to_string(index=False))

    versions_d1 = con.execute(
        "select count(*) from snapshots.drivers_snapshot where driver_id=1").fetchone()[0]
    current = con.execute(
        "select count(*) from snapshots.drivers_snapshot where dbt_valid_to is null").fetchone()[0]
    assert versions_d1 == 2, f"expected 2 versions for driver 1, got {versions_d1}"
    print(f"\nSCD2 OK: driver 1 has {versions_d1} versions; {current} rows are current")


if __name__ == "__main__":
    main()
