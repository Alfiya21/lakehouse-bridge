PY ?= python
export PYTHONPATH := src
.PHONY: all data batch stream hourly migrate quality govern dbt eval test clean
all: clean data batch stream hourly migrate quality govern dbt eval
data:     ; $(PY) -m lakehouse_bridge.data_gen
batch:    ; $(PY) -m lakehouse_bridge.pipelines.batch
stream:   ; $(PY) -m lakehouse_bridge.pipelines.streaming
migrate:  ; $(PY) -m lakehouse_bridge.migration.migrate
quality:  ; $(PY) -m lakehouse_bridge.governance.quality
govern:   ; $(PY) -m lakehouse_bridge.governance.security
dbt:      ; cd dbt_project && dbt build --profiles-dir . --target duckdb
eval:     ; $(PY) -m lakehouse_bridge.agent.eval
test:     ; $(PY) -m pytest -q
clean:    ; rm -rf data
