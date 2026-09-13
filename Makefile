# Vancouver open-data pipeline. Copy .env.example to .env first.
PY ?= python3

.PHONY: install sync sofi-fetch sofi-parse review heights council dbt-deps dbt seed build export all

install:
	$(PY) -m pip install -r requirements.txt

## 1. ingest — the City's open-data portal (every source in config/vancouver.yaml)
sync:
	$(PY) pipeline/sync/sync_ods_dataset.py

## 2. Statements of Financial Information (PDF) from the Internet Archive
sofi-fetch:
	$(PY) pipeline/finance/fetch_sofi.py
sofi-parse:
	$(PY) pipeline/finance/parse_sofi.py --load
review:
	$(PY) pipeline/finance/parse_sofi_review.py --load

## 3. measured / derived sources (need step 1 and core models)
heights:
	$(PY) pipeline/city/measure_building_heights.py
council:
	$(PY) pipeline/city/place_council_items.py

## 4. transform + test
dbt-deps:
	cd dbt && dbt deps
seed:
	cd dbt && dbt seed
build:
	cd dbt && dbt build

## 5. outputs
export:
	$(PY) pipeline/export/export_vancouver.py

all: sync sofi-fetch sofi-parse review dbt-deps seed build council build export
