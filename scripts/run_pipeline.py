"""Run the whole batch pipeline end to end.

    fetch seeds -> ingest (bronze) -> dbt deps/seed/run/test -> data-quality audit

Assumes the stack is up (docker compose up -d) and you're using the project venv.
    python scripts/run_pipeline.py
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DBT = str(Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt"))


def sh(cmd: list):
    print(f"\n$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)


def dbt(*args: str):
    sh([DBT, *args, "--project-dir", "dbt", "--profiles-dir", "dbt"])


def main():
    from scripts.fetch_seeds import main as fetch_seeds
    from pipeline.ingest import ingest
    from pipeline.quality import audit

    fetch_seeds()
    ingest()
    dbt("deps")
    dbt("seed")
    dbt("run")
    dbt("test")
    audit()
    print("\npipeline complete.")


if __name__ == "__main__":
    main()
