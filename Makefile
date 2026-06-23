.PHONY: up down seeds ingest dbt test pipeline dagster dashboard bi clean

up:                 ## start the lake (MinIO) + warehouse (Postgres)
	docker compose up -d

bi:                 ## add Metabase
	docker compose --profile bi up -d

down:
	docker compose down

seeds:
	python scripts/fetch_seeds.py

ingest:
	python -m pipeline.ingest

dbt:
	dbt deps --project-dir dbt --profiles-dir dbt
	dbt seed --project-dir dbt --profiles-dir dbt
	dbt run  --project-dir dbt --profiles-dir dbt
	dbt test --project-dir dbt --profiles-dir dbt

pipeline:           ## full run: seeds -> ingest -> dbt -> data quality
	python scripts/run_pipeline.py

dagster:
	dagster dev -f orchestration/definitions.py

dashboard:
	streamlit run dashboard/app.py

test:
	pytest -q

clean:
	rm -rf dbt/target dbt/dbt_packages data/warehouse/*.duckdb data/landing/*.parquet
