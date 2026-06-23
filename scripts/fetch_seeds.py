"""Download the official TLC taxi-zone lookup into dbt/seeds/ (small, static)."""

import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEST = ROOT / "dbt" / "seeds" / "taxi_zone_lookup.csv"


def main():
    if DEST.exists() and DEST.stat().st_size > 0:
        print(f"seed already present: {DEST}")
        return
    DEST.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    DEST.write_bytes(r.content)
    print(f"wrote {DEST} ({len(r.content)} bytes)")


if __name__ == "__main__":
    sys.exit(main())
