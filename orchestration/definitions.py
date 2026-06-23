"""Dagster orchestration: ingest -> dbt build -> data-quality audit, as one asset graph.

The dbt source (bronze.yellow_trips) is mapped to the ingestion asset, so Dagster
knows the dbt models depend on the bronze load and schedules them in order. Run the
UI with:  dagster dev -f orchestration/definitions.py

(Airflow is the heavier-weight alternative; the same graph maps to a DAG of tasks.)
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from dagster import AssetKey, Definitions, ScheduleDefinition, asset, define_asset_job
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt"
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))

# resolve dbt from PATH (activated venv) or fall back to the venv next to python
DBT_EXE = shutil.which("dbt") or str(
    Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt"))

dbt_project = DbtProject(project_dir=DBT_DIR)
dbt_project.prepare_if_dev()


@asset(key=AssetKey(["bronze", "yellow_trips"]), group_name="ingestion",
       compute_kind="python")
def bronze_yellow_trips() -> None:
    """Download TLC months and land them in the MinIO bronze layer."""
    from pipeline.ingest import ingest
    ingest()


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(deps=[AssetKey("fct_trips")], group_name="quality", compute_kind="python")
def data_quality_audit() -> None:
    """Operational DQ checks on the gold layer (fail-fast)."""
    from pipeline.quality import audit
    audit()


daily_refresh = define_asset_job("daily_refresh", selection="*")

defs = Definitions(
    assets=[bronze_yellow_trips, dbt_models, data_quality_audit],
    jobs=[daily_refresh],
    schedules=[ScheduleDefinition(job=daily_refresh, cron_schedule="0 6 * * *")],
    resources={"dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXE)},
)
