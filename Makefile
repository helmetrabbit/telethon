# ── Makefile — convenience commands ──────────────────────
.PHONY: db-up db-down db-migrate db-rollback db-reset db-status \
        env-remote env-remote-ip env-local db-smoke serve-viewer \
        tg-listen-dm tg-ingest-dm-jsonl tg-listen-ingest-dm tg-listen-ingest-dm-profile tg-reconcile-dm-psych tg-live-start tg-live-stop tg-live-status build pipeline

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

# ── DM-only live listener (private chats only) ───────────
tg-listen-dm:
	cd tools/telethon_collector && . .venv/bin/activate && python3 listen-dms.py --out ../../data/exports/telethon_dms_live.jsonl

# ── DM JSONL to Postgres (requires dm tables migration) ───────────
tg-ingest-dm-jsonl:
	@FILE=$${file:-data/exports/telethon_dms_live.jsonl}; \
	npm run ingest-dm-jsonl -- --file "$$FILE"

# ── Continuous DM live loop (capture + periodic ingest) ───────────
tg-listen-ingest-dm:
	@FILE=$${file:-data/exports/telethon_dms_live.jsonl}; \
	INTERVAL=$${interval:-30}; \
	while true; do \
		echo "[$$(date -Is)] ingesting $$FILE"; \
		npm run ingest-dm-jsonl -- --file "$$FILE" || true; \
		sleep $$INTERVAL; \
	done

# ── Continuous DM live loop + automatic profile correction merge ───
tg-listen-ingest-dm-profile:
	@FILE=$${file:-data/exports/telethon_dms_live.jsonl}; \
	INTERVAL=$${interval:-30}; \
	while true; do \
		echo "[$$(date -Is)] ingesting $$FILE + reconciliation"; \
		npm run ingest-dm-jsonl -- --file "$$FILE" || true; \
		npm run reconcile-dm-psych || true; \
		sleep $$INTERVAL; \
	done

# One-shot reconcile for pending DM updates
# Usage: make tg-reconcile-dm-psych [limit=250] [userIds=1,2]
tg-reconcile-dm-psych:
	@LIMIT=$${limit:-0}; \
	USER_IDS=$${userIds:-}; \
	if [ "$$USER_IDS" != "" ]; then \
		npm run reconcile-dm-psych -- --user-ids "$$USER_IDS" --limit $$LIMIT; \
	else \
		npm run reconcile-dm-psych -- --limit $$LIMIT; \
	fi

# One-shot always-running DM pipeline (listener + periodic ingest) ──
# Usage:
#   make tg-live-start FILE=data/exports/telethon_dms_live.jsonl INTERVAL=30
tg-live-start:
	@FILE=$${FILE:-data/exports/telethon_dms_live.jsonl}; \
	INTERVAL=$${INTERVAL:-30}; \
	bash tools/telethon_collector/run-dm-live.sh "$$FILE" "$$INTERVAL"

# Show/stop helpers for the one-shot pipeline
tg-live-status:
	@LISTENER=$$(cat data/.pids/tg-listen-dm.pid 2>/dev/null || true); \
	INGEST=$$(cat data/.pids/tg-ingest-dm.pid 2>/dev/null || true); \
	echo "Listener pid: $$LISTENER"; \
	echo "Ingest pid: $$INGEST"; \
	[ -n "$$LISTENER" ] && kill -0 "$$LISTENER" 2>/dev/null && echo "  listener: running" || echo "  listener: stopped"; \
	[ -n "$$INGEST" ] && kill -0 "$$INGEST" 2>/dev/null && echo "  ingest: running" || echo "  ingest: stopped"

tg-live-stop:
	@bash -lc 'function stop_one() {     pid_file=$$1;     label=$$2;     PID=$$(cat "$$pid_file");     kill "$$PID" 2>/dev/null || true;     rm -f "$$pid_file";     echo "$$label $$PID"; }; if [ -f data/.pids/tg-listen-dm.pid ]; then stop_one data/.pids/tg-listen-dm.pid "stopped listener"; fi; if [ -f data/.pids/tg-ingest-dm.pid ]; then stop_one data/.pids/tg-ingest-dm.pid "stopped ingest"; fi'
