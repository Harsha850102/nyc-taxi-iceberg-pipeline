# One command per part. Values come from .env (copy .env.example -> .env).
-include .env
export
PY := PYTHONPATH=src python

.PHONY: help install aws-setup polaris-up polaris-bootstrap polaris-down ingest transform features metrics \
        snowflake-sql dq test all app aws-teardown

help:            ## list targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-18s %s\n", $$1, $$2}'

install:         ## install Python dependencies
	pip install -r requirements.txt -r requirements-dev.txt

aws-setup:       ## create bucket + least-privilege IAM (run with an ADMIN profile, not root)
	AWS_PROFILE=$(ADMIN_PROFILE) bash infra/setup_aws.sh

polaris-up:      ## start Apache Polaris in Docker
	docker compose up -d polaris

polaris-bootstrap: ## create the Polaris catalog pointing at s3://$$S3_BUCKET/warehouse
	bash scripts/polaris_bootstrap.sh

polaris-down:    ## stop Polaris
	docker compose down

ingest:          ## Part 1: download TLC files -> s3://$$S3_BUCKET/raw/
	$(PY) -m pipeline.ingest

transform:       ## Part 2 + 3.1 + quarantine: raw -> trips_clean / trips_rejected (MERGE INTO)
	$(PY) -m pipeline.transform_job

features:        ## Part 3.2 + 3.3: schema evolution + time travel report
	$(PY) -m pipeline.iceberg_features

metrics:         ## Part 3.4: build daily_zone_metrics
	$(PY) -m pipeline.metrics_job

dq:              ## Part 5: data quality checks (exit 1 on failure)
	$(PY) -m pipeline.dq_checks

snowflake-sql:   ## Part 4 helper: generate CREATE/REFRESH SQL with current metadata paths
	$(PY) -m pipeline.snowflake_metadata

test:            ## Part 5: pytest unit tests (no AWS needed)
	$(PY) -m pytest -v

all: ingest transform features metrics dq snowflake-sql ## run the whole pipeline end to end

app:             ## run the Streamlit results app locally
	streamlit run app/streamlit_app.py

aws-teardown:    ## delete ALL AWS resources (after review)
	AWS_PROFILE=$(ADMIN_PROFILE) bash infra/teardown_aws.sh
