-- migrate:up
-- Add dedicated DM/conversation/event tables for one-on-one Telegram outreach tracking

CREATE TABLE IF NOT EXISTS dm_conversations (
  id BIGSERIAL PRIMARY KEY,
  account_user_id BIGINT NOT NULL,
  subject_user_id BIGINT NOT NULL,
  platform TEXT NOT NULL DEFAULT 'telegram',
  external_chat_id TEXT NOT NULL,
  title TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  source TEXT NOT NULL DEFAULT 'listener',
  priority INTEGER NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  next_followup_at TIMESTAMPTZ,
  last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT dm_conversations_account_user_fk FOREIGN KEY (account_user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT dm_conversations_subject_user_fk FOREIGN KEY (subject_user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT dm_conversations_unique UNIQUE (platform, account_user_id, external_chat_id)
);

CREATE INDEX IF NOT EXISTS dm_conversations_account_user_idx
  ON dm_conversations (account_user_id, status, last_activity_at DESC);

CREATE INDEX IF NOT EXISTS dm_conversations_subject_user_idx
  ON dm_conversations (subject_user_id, status, last_activity_at DESC);

CREATE INDEX IF NOT EXISTS dm_conversations_followup_idx
  ON dm_conversations (next_followup_at)
  WHERE next_followup_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS dm_messages (
  id BIGSERIAL PRIMARY KEY,
  conversation_id BIGINT NOT NULL,
  external_message_id TEXT NOT NULL,
  direction TEXT NOT NULL CHECK (direction IN ('inbound','outbound')),
  message_text TEXT,
  text_hash TEXT NOT NULL,
  sent_at TIMESTAMPTZ NOT NULL,
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  response_to_external_message_id TEXT,
  has_links BOOLEAN NOT NULL DEFAULT false,
  has_mentions BOOLEAN NOT NULL DEFAULT false,
  extracted_handles TEXT[] NOT NULL DEFAULT '{}'::text[],
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT dm_messages_conversation_fk FOREIGN KEY (conversation_id)
    REFERENCES dm_conversations(id) ON DELETE CASCADE,
  CONSTRAINT dm_messages_unique UNIQUE (conversation_id, external_message_id)
);

CREATE INDEX IF NOT EXISTS dm_messages_conversation_idx
  ON dm_messages (conversation_id, sent_at DESC);

CREATE INDEX IF NOT EXISTS dm_messages_direction_idx
  ON dm_messages (direction, sent_at DESC);

CREATE TABLE IF NOT EXISTS dm_interpretations (
  id BIGSERIAL PRIMARY KEY,
  dm_message_id BIGINT NOT NULL,
  kind TEXT NOT NULL,
  summary TEXT NOT NULL,
  sentiment_score REAL,
  confidence REAL,
  requires_followup BOOLEAN NOT NULL DEFAULT false,
  followup_reason TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT dm_interpretations_dm_message_fk FOREIGN KEY (dm_message_id)
    REFERENCES dm_messages(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS dm_interpretations_message_idx
  ON dm_interpretations (dm_message_id, created_at DESC);

CREATE TABLE IF NOT EXISTS dm_followups (
  id BIGSERIAL PRIMARY KEY,
  conversation_id BIGINT NOT NULL,
  planned_for TIMESTAMPTZ NOT NULL,
  action_type TEXT NOT NULL,
  body_template TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'scheduled',
  assigned_to_user_id BIGINT,
  executed_at TIMESTAMPTZ,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT dm_followups_conversation_fk FOREIGN KEY (conversation_id)
    REFERENCES dm_conversations(id) ON DELETE CASCADE,
  CONSTRAINT dm_followups_assignee_fk FOREIGN KEY (assigned_to_user_id)
    REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS dm_followups_conversation_idx
  ON dm_followups (conversation_id, planned_for);

CREATE INDEX IF NOT EXISTS dm_followups_status_idx
  ON dm_followups (status, planned_for);

-- migrate:down
DROP TABLE IF EXISTS dm_followups;
DROP TABLE IF EXISTS dm_interpretations;
DROP TABLE IF EXISTS dm_messages;
DROP TABLE IF EXISTS dm_conversations;