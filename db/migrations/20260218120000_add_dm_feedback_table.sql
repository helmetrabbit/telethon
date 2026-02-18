-- migrate:up

CREATE TABLE IF NOT EXISTS dm_feedback (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  conversation_id BIGINT REFERENCES dm_conversations(id) ON DELETE SET NULL,
  source_message_id BIGINT,
  source_external_message_id TEXT,
  kind TEXT NOT NULL,
  text TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dm_feedback_user_created
  ON dm_feedback (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dm_feedback_conversation_created
  ON dm_feedback (conversation_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dm_feedback_kind_created
  ON dm_feedback (kind, created_at DESC);

-- migrate:down

DROP INDEX IF EXISTS idx_dm_feedback_kind_created;
DROP INDEX IF EXISTS idx_dm_feedback_conversation_created;
DROP INDEX IF EXISTS idx_dm_feedback_user_created;
DROP TABLE IF EXISTS dm_feedback;

