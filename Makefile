# ── Makefile — convenience commands ──────────────────────
.PHONY: db-up db-down db-migrate db-rollback db-reset db-status \
        build ingest compute-features infer-claims export-profiles pipeline

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

pipeline: build ingest compute-features infer-claims export-profiles
