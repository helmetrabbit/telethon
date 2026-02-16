-- migrate:up

ALTER TABLE dm_messages
  ADD COLUMN IF NOT EXISTS response_status TEXT NOT NULL DEFAULT 'pending';

ALTER TABLE dm_messages
  ADD COLUMN IF NOT EXISTS response_attempts INTEGER NOT NULL DEFAULT 0;

ALTER TABLE dm_messages
  ADD COLUMN IF NOT EXISTS response_attempted_at TIMESTAMPTZ;

ALTER TABLE dm_messages
  ADD COLUMN IF NOT EXISTS response_last_error TEXT;

ALTER TABLE dm_messages
  ADD COLUMN IF NOT EXISTS response_message_external_id TEXT;

ALTER TABLE dm_messages
  ADD COLUMN IF NOT EXISTS responded_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_dm_messages_response_status
  ON dm_messages (response_status)
  WHERE response_status IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dm_messages_response_pending
  ON dm_messages (conversation_id, direction, sent_at)
  WHERE response_status = 'pending' OR response_status = 'failed';


UPDATE dm_messages
  SET response_status = 'not_applicable';

-- Ensure only known response states are stored.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dm_messages_response_status_check'
  ) THEN
    ALTER TABLE dm_messages
      ADD CONSTRAINT dm_messages_response_status_check
      CHECK (response_status IN ('pending','sending','responded','failed','not_applicable'));
  END IF;
END;
$$;

-- migrate:down

ALTER TABLE dm_messages
  DROP COLUMN IF EXISTS responded_at;

ALTER TABLE dm_messages
  DROP COLUMN IF EXISTS response_message_external_id;

ALTER TABLE dm_messages
  DROP COLUMN IF EXISTS response_last_error;

ALTER TABLE dm_messages
  DROP COLUMN IF EXISTS response_attempted_at;

ALTER TABLE dm_messages
  DROP COLUMN IF EXISTS response_attempts;

ALTER TABLE dm_messages
  DROP COLUMN IF EXISTS response_status;

DROP INDEX IF EXISTS idx_dm_messages_response_status;
DROP INDEX IF EXISTS idx_dm_messages_response_pending;
ALTER TABLE dm_messages DROP CONSTRAINT IF EXISTS dm_messages_response_status_check;

