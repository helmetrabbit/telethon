-- migrate:up

-- Backfill bio_source/bio_updated_at for existing users with bios.
-- Use last_msg_at when available; otherwise fall back to now().
UPDATE users
SET
  bio_source = COALESCE(bio_source, 'telegram_export'),
  bio_updated_at = COALESCE(bio_updated_at, last_msg_at, now())
WHERE bio IS NOT NULL AND bio <> '';

-- migrate:down
-- No-op for backfill
