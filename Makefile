.PHONY: up down seeds ingest dbt test pipeline dagster dashboard bi stream produce consume cdc clean

up:                 ## start the lake (MinIO) + warehouse (Postgres)
	docker compose up -d

bi:                 ## add Metabase
	docker compose --profile bi up -d

stream:             ## start lake + warehouse + Redpanda + Debezium Connect
	docker compose --profile stream up -d

produce:            ## stream trips into the `trips` topic
	python -m streaming.producer

consume:            ## land the stream into bronze/stream in the lake
	python -m streaming.consumer

cdc:                ## seed OLTP source, register Debezium, run the change demo
	python -m cdc.setup_source
	python -m cdc.register_connector
	python -m cdc.demo

scd2:               ## SCD2 demo: dbt snapshot versions a changing dimension
	python scripts/scd2_demo.py

delta:              ## Delta table format: ACID append + time travel
	python scripts/delta_demo.py

tf-validate:        ## validate the Terraform IaC
	cd infra/terraform && terraform init -backend=false && terraform validate

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
