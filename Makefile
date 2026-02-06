# ── Makefile — convenience commands ──────────────────────
.PHONY: db-up db-down db-migrate db-rollback db-reset db-status \
        build ingest compute-features infer-claims export-profiles pipeline \
        tg\:venv tg\:list-dialogs tg\:collect

# ── Database ─────────────────────────────────────────────
db-up:
	docker compose up -d postgres

db-down:
	docker compose down

db-migrate:
	docker compose run --rm dbmate up

db-rollback:
	docker compose run --rm dbmate down

db-status:
	docker compose run --rm dbmate status

db-reset:
	docker compose down -v
	docker compose up -d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 3
	docker compose run --rm dbmate up

# ── Build ────────────────────────────────────────────────
build:
	npx tsc

# ── Pipeline steps ───────────────────────────────────────
ingest: build
	node dist/cli/ingest.js

compute-features: build
	node dist/cli/compute-features.js

infer-claims: build
	node dist/cli/infer-claims.js

export-profiles: build
	node dist/cli/export-profiles.js

export-diagnostics: build
	DIAGNOSTICS_SALT=$${DIAGNOSTICS_SALT} node dist/cli/export-diagnostics.js

pipeline: build ingest compute-features infer-claims export-profiles

# ── Telethon collector ───────────────────────────────────
TG_VENV := tools/telethon_collector/.venv
TG_PY   := $(TG_VENV)/bin/python
GROUP   ?= BD in Web3
OUT     ?= data/exports/telethon_bd_web3.json
LIMIT   ?= 5000
SINCE   ?=

tg\:venv:
	python3 -m venv $(TG_VENV)
	$(TG_VENV)/bin/pip install -r tools/telethon_collector/requirements.txt
	@echo "\n✅ Telethon venv ready: source $(TG_VENV)/bin/activate"

tg\:list-dialogs: | $(TG_VENV)
	$(TG_PY) tools/telethon_collector/list_dialogs.py

tg\:collect: | $(TG_VENV)
	$(TG_PY) tools/telethon_collector/collect_group_export.py \
		--group "$(GROUP)" \
		--out "$(OUT)" \
		--limit $(LIMIT) \
		$(if $(SINCE),--since $(SINCE),)

$(TG_VENV):
	@echo "❌ Venv not found. Run 'make tg:venv' first." && exit 1
