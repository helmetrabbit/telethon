# ── Makefile — convenience commands ──────────────────────
.PHONY: db-up db-down db-migrate db-rollback db-reset db-status \
        env-remote env-remote-ip env-local db-smoke serve-viewer \
        tg-listen-dm tg-ingest-dm-jsonl tg-listen-ingest-dm tg-listen-ingest-dm-profile tg-respond-dm tg-reconcile-dm-psych tg-live-start tg-live-start-ingest tg-live-stop tg-live-status tg-live-state-reset build pipeline

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
	@OUT=$${out:-../../data/exports/telethon_dms_live.jsonl}; \
	SESSION_PATH=$${session_path:-$${SESSION_PATH:-$${TG_SESSION_PATH}}}; \
	if [ -z "$$SESSION_PATH" ]; then SESSION_PATH=../../tools/telethon_collector/telethon.session; fi; \
	cd tools/telethon_collector && . .venv/bin/activate && TG_SESSION_PATH="$$SESSION_PATH" python3 listen-dms.py --out "$$OUT"

# ── DM JSONL to Postgres (requires dm tables migration) ───────────
tg-ingest-dm-jsonl:
	@FILE=$${file:-data/exports/telethon_dms_live.jsonl}; \
	STATE_FILE=$${state_file:-$${FILE}.checkpoint.json}; \
	npm run ingest-dm-jsonl -- --file "$$FILE" --state-file "$$STATE_FILE"

# ── Continuous DM live loop (capture + periodic ingest only) ───────────
tg-listen-ingest-dm:
	@FILE=$${file:-data/exports/telethon_dms_live.jsonl}; \
	STATE_FILE=$${state_file:-$${FILE}.checkpoint.json}; \
	INTERVAL=$${interval:-30}; \
	while true; do \
		echo "[$$(date -Is)] ingesting $$FILE"; \
		npm run ingest-dm-jsonl -- --file "$$FILE" --state-file "$$STATE_FILE" || true; \
		sleep $$INTERVAL; \
	done

# ── Continuous DM live loop + automatic profile correction merge and response worker ───
tg-listen-ingest-dm-profile:
	@FILE=$${file:-data/exports/telethon_dms_live.jsonl}; \
	STATE_FILE=$${state_file:-$${FILE}.checkpoint.json}; \
	INTERVAL=$${interval:-30}; \
	while true; do \
		echo "[$$(date -Is)] ingesting $$FILE + reconciliation"; \
		npm run ingest-dm-jsonl -- --file "$$FILE" --state-file "$$STATE_FILE" || true; \
		npm run reconcile-dm-psych || true; \
		bash tools/telethon_collector/run-dm-response.sh || true; \
		sleep $$INTERVAL; \
	done

# Send outbound replies for pending inbound DM messages
# Usage: make tg-respond-dm [limit=20] [max_retries=3] [dry_run=1]
tg-respond-dm:
	@LIMIT=$${limit:-20}; \
	MAX_RETRIES=$${max_retries:-3}; \
	DRY_RUN=$${dry_run:-0}; \
	DM_RESPONSE_LIMIT="$$LIMIT" DM_MAX_RETRIES="$$MAX_RETRIES" DM_RESPONSE_DRY_RUN="$$DRY_RUN" DM_SESSION_PATH="$${DM_SESSION_PATH:-}" bash tools/telethon_collector/run-dm-response.sh


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

# One-shot always-running DM pipeline (listener + periodic ingest + reconcile + responder) ──
# Usage:
#   make tg-live-start FILE=data/exports/telethon_dms_live.jsonl INTERVAL=30
#   make tg-live-start [STATE_FILE=data/.state/dm-live.state.json]
tg-live-start:
	@FILE=$${FILE:-data/exports/telethon_dms_live.jsonl}; \
	INTERVAL=$${INTERVAL:-30}; \
	STATE_FILE=$${STATE_FILE:-data/.state/dm-live.state.json}; \
	SESSION_PATH=$${SESSION_PATH:-$${TG_SESSION_PATH:-tools/telethon_collector/telethon_openclaw.session}}; \
	bash tools/telethon_collector/run-dm-live.sh "$${FILE}" "$${INTERVAL}" profile "$${STATE_FILE}" "$${SESSION_PATH}"


# Keep legacy naming for old behavior: full ingest loop only
tg-live-start-ingest:
	@FILE=$${FILE:-data/exports/telethon_dms_live.jsonl}; \
	INTERVAL=$${INTERVAL:-30}; \
	STATE_FILE=$${STATE_FILE:-data/.state/dm-live.state.json}; \
	SESSION_PATH=$${SESSION_PATH:-$${TG_SESSION_PATH:-tools/telethon_collector/telethon_openclaw.session}}; \
	bash tools/telethon_collector/run-dm-live.sh "$${FILE}" "$${INTERVAL}" ingest "$${STATE_FILE}" "$${SESSION_PATH}"

# Show/stop helpers for the live pipeline
tg-live-status:
	@FILE_PATH=$${FILE:-data/exports/telethon_dms_live.jsonl}; \
	SUPERVISOR=$$(cat data/.pids/tg-live-supervisor.pid 2>/dev/null || true); \
	LISTENER_PGREP=$$(pgrep -f "listen-dms.py --out $$(pwd)/$$FILE_PATH" | head -n 1); \
	LISTENER=$$(cat data/.pids/tg-listen-dm.pid 2>/dev/null || true); \
	if [ -z "$$LISTENER" ] && [ -n "$$LISTENER_PGREP" ]; then \
		LISTENER="$$LISTENER_PGREP"; \
		echo $$LISTENER > data/.pids/tg-listen-dm.pid; \
	fi; \
	echo "Supervisor pid: $$SUPERVISOR"; \
	echo "Listener pid: $$LISTENER"; \
	SUP_OK=0; \
	if [ -n "$$SUPERVISOR" ] && kill -0 "$$SUPERVISOR" 2>/dev/null; then SUP_OK=1; fi; \
	if [ "$$SUP_OK" -eq 1 ]; then echo "  supervisor: running"; else echo "  supervisor: stopped"; fi; \
	LISTENER_OK=0; \
	if [ -n "$$LISTENER" ] && kill -0 "$$LISTENER" 2>/dev/null; then LISTENER_OK=1; fi; \
	if [ -n "$$LISTENER_PGREP" ]; then LISTENER_OK=1; fi; \
	if [ "$$LISTENER_OK" -eq 1 ]; then echo "  listener: running"; else echo "  listener: stopped"; fi
tg-live-stop:
	@bash -lc 'function stop_one() {     pid_file=$$1;     label=$$2;     if [ -f "$$pid_file" ]; then PID=$$(cat "$$pid_file"); kill "$$PID" 2>/dev/null || true; rm -f "$$pid_file"; echo "$$label $$PID"; fi }; \
	stop_one data/.pids/tg-live-supervisor.pid "stopped supervisor"; \
	stop_one data/.pids/tg-listen-dm.pid "stopped listener"'

# Clean persisted checkpoint state when you want a clean resync
tg-live-state-reset:
	@FILE=$${FILE:-data/exports/telethon_dms_live.jsonl}; \
	STATE_FILE=$${STATE_FILE:-$${FILE}.checkpoint.json}; \
	rm -f "$$STATE_FILE" "data/.state/dm-live.state.json"
