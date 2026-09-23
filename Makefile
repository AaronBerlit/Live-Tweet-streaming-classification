# Every command referenced in the PRD lives here.
#
# Recipes are POSIX sh, run through Git Bash -- the short path avoids the
# space in "Program Files", which GNU make cannot quote in SHELL.
SHELL := C:/PROGRA~1/Git/bin/bash.exe
.SHELLFLAGS := -c

PY       := .venv/Scripts/python.exe
PIP      := .venv/Scripts/pip.exe
# Spark jobs run inside WSL Ubuntu: Hadoop's filesystem layer needs unofficial
# winutils binaries on Windows, and on Linux it needs nothing. MSYS_NO_PATHCONV
# stops Git Bash rewriting the arguments into Windows paths.
SPY      := MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -- bash scripts/wsl_run.sh
COMPOSE  := docker compose
DC_CORE  := -f docker-compose.yml
DC_HDFS  := -f docker-compose.hdfs.yml

.DEFAULT_GOAL := help

# ---------------------------------------------------------------- environment

.PHONY: help
help:  ## Show every target with its purpose
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n",$$1,$$2}'

.PHONY: venv
venv:  ## Create the Python 3.11 venv and install dependencies
	@test -d .venv || py -3.11 -m venv .venv
	@$(PIP) install --quiet --upgrade pip
	@$(PIP) install --quiet -r requirements.txt
	@$(PY) -c "from pipeline.versions import version_report; [print(f'{k:24}{v}') for k,v in version_report().items()]"

.PHONY: wsl-venv
wsl-venv:  ## Build the Spark venv inside WSL (one-time)
	MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -- bash scripts/wsl_setup.sh

.PHONY: up
up:  ## Start Mongo + Kafka + HDFS
	$(COMPOSE) $(DC_CORE) up -d
	$(COMPOSE) $(DC_HDFS) up -d

.PHONY: down
down:  ## Stop all services (volumes preserved)
	-$(COMPOSE) $(DC_CORE) down
	-$(COMPOSE) $(DC_HDFS) down

.PHONY: setup
setup: venv wsl-venv up  ## Full bring-up: venv, services, Mongo indexes, HDFS dirs
	@$(PY) scripts/wait_for_services.py
	@$(PY) scripts/init_mongo.py
	@$(PY) scripts/init_hdfs.py
	@echo "setup complete -- run 'make verify-spark' and 'make verify-mongo'"

.PHONY: verify-spark
verify-spark:  ## C5: Spark 3.x installed and verified (word-count job)
	$(SPY) scripts/verify_spark.py

.PHONY: verify-mongo
verify-mongo:  ## C6: MongoDB reachable through the Spark connector
	$(SPY) scripts/verify_mongo.py

.PHONY: reset
reset:  ## Drop all MongoDB collections (leaves the lake intact)
	$(PY) scripts/reset.py

# ------------------------------------------------------------------ data + ml

.PHONY: ingest
ingest:  ## C1/C2: download Sentiment140, profile it, load to the lake as Parquet
	$(SPY) -m ingest.download
	$(SPY) -m ingest.profile_dataset
	$(SPY) -m ingest.load_raw

.PHONY: clean-batch
clean-batch:  ## C8: preprocess raw -> clean, then the seeded train/test split
	$(SPY) -m pipeline.clean_batch

.PHONY: train
train:  ## C9/C11: train and evaluate the default model (Naive Bayes, 200k rows)
	$(SPY) -m trainer.train --model nb
	$(SPY) -m trainer.evaluate --model nb

.PHONY: train-all
train-all:  ## C10: both models x all three emoji strategies -> docs/experiments.md
	$(SPY) scripts/run_experiments.py

# --------------------------------------------------------------------- demos

.PHONY: demo-seed
demo-seed:  ## Populate MongoDB with seeded aggregates (no pipeline required)
	$(PY) scripts/seed.py

.PHONY: demo-socket
demo-socket:  ## C14: producer -> TCP 9999 -> Spark Structured Streaming
	$(SPY) scripts/demo.py --source socket

.PHONY: demo-kafka
demo-kafka:  ## C13: producer -> Kafka topic tweets.raw -> Spark
	$(SPY) scripts/demo.py --source kafka

.PHONY: api
api:  ## Run the FastAPI backend
	.venv/Scripts/uvicorn.exe api.main:app --host 127.0.0.1 --port 8000 --timeout-graceful-shutdown 3

.PHONY: api-dev
api-dev:  ## Backend with auto-reload (dev only; orphans workers on Windows if killed)
	.venv/Scripts/uvicorn.exe api.main:app --host 127.0.0.1 --port 8000 --reload --reload-dir api --reload-dir pipeline --reload-dir storage --timeout-graceful-shutdown 3

.PHONY: web
web:  ## Run the React dashboard
	cd web && npm run dev

# ------------------------------------------------------------------- evidence

.PHONY: evidence
evidence: evidence-volume evidence-velocity evidence-variety evidence-hdfs \
          evidence-storage-split evidence-query-plan evidence-watermark \
          evidence-reproducible evidence-reconciliation evidence-kafka-resume  ## Regenerate every proof in docs/evidence/
	@echo "all evidence written to docs/evidence/"

.PHONY: evidence-volume
evidence-volume:  ## Three V's -- Volume: lake listing and row count
	$(SPY) -m scripts.evidence.volume

.PHONY: evidence-velocity
evidence-velocity:  ## C23 -- Velocity: observed rows/sec and batch durations
	$(PY) -m scripts.evidence.velocity

.PHONY: evidence-variety
evidence-variety:  ## Three V's -- Variety: schema, null rates, token classes
	$(SPY) -m scripts.evidence.variety

.PHONY: evidence-hdfs
evidence-hdfs:  ## C4/C8: hdfs dfs -ls -R /sentiment
	$(PY) -m scripts.evidence.hdfs_listing

.PHONY: evidence-storage-split
evidence-storage-split:  ## C19: MongoDB holds aggregates, not the corpus
	$(PY) -m scripts.evidence.storage_split

.PHONY: evidence-query-plan
evidence-query-plan:  ## C20: explain() showing index use on /api/windows
	$(PY) -m scripts.evidence.query_plan

.PHONY: evidence-watermark
evidence-watermark:  ## C17: watermark bounds window state; late records counted
	$(PY) -m scripts.evidence.watermark

.PHONY: evidence-kafka-resume
evidence-kafka-resume:  ## C13: crash the Kafka consumer, restart, nothing lost or doubled
	$(SPY) -m scripts.evidence.kafka_resume

.PHONY: evidence-reconciliation
evidence-reconciliation:  ## 12.5: windows and scored agree exactly
	$(PY) -m scripts.evidence.reconciliation

.PHONY: evidence-reproducible
evidence-reproducible:  ## C15: two seeded producer runs, identical checksums
	$(SPY) -m scripts.evidence.reproducible

# ---------------------------------------------------------------------- tests

.PHONY: test
test:  ## Run the full test suite
	$(PY) -m pytest -q -m "not spark"
	$(SPY) -m pytest -q -m spark

.PHONY: test-unit
test-unit:  ## Unit tests only (no services required)
	$(PY) -m pytest -q tests/unit

.PHONY: test-integration
test-integration:  ## Integration tests (requires Mongo)
	$(PY) -m pytest -q tests/integration
