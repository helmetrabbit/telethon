-- migrate:up

-- ── Idempotent message ingestion ────────────────────────
-- Ensure (group_id, external_message_id) is unique so re-importing
-- the same group from a different export file (different SHA) is safe.
-- Drop the old non-unique index first, then create the unique one.

DROP INDEX IF EXISTS idx_messages_ext_id;
CREATE UNIQUE INDEX idx_messages_ext_id ON messages (group_id, external_message_id);


-- migrate:down

DROP INDEX IF EXISTS idx_messages_ext_id;
CREATE INDEX idx_messages_ext_id ON messages (group_id, external_message_id);
