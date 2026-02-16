# ── Makefile — convenience commands ──────────────────────
.PHONY: db-up db-down db-migrate db-rollback db-reset db-status \
        env-remote env-remote-ip env-local db-smoke serve-viewer \
        build pipeline

# ── Environment helpers ──────────────────────────────────
env-remote:
	npm run env:remote

env-remote-ip:
	npm run env:remote:ip

env-local:
	npm run env:local

# ── Database (local docker-compose) ───────────────────────
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

# ── Database smoke test (works for remote or local via DATABASE_URL) ──
db-smoke:
	npm run db:smoke

# ── Build ────────────────────────────────────────────────
build:
	npm run build

# ── Pipeline ─────────────────────────────────────────────
pipeline:
	npm run pipeline

# ── Static viewer ───────────────────────────────────────
serve-viewer:
	python3 -m http.server 4173 --bind 127.0.0.1 --directory .
