-- migrate:up

CREATE TABLE IF NOT EXISTS dm_conversations (
  id BIGSERIAL PRIMARY KEY,
  platform TEXT NOT NULL DEFAULT 'telegram',
  external_chat_id TEXT NOT NULL,
  user_a_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  user_b_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  last_message_at TIMESTAMPTZ,
  message_count BIGINT DEFAULT 0 NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_conversations_chat_unique
  ON dm_conversations (platform, external_chat_id);

CREATE INDEX IF NOT EXISTS idx_dm_conversations_user_a ON dm_conversations (user_a_id);
CREATE INDEX IF NOT EXISTS idx_dm_conversations_user_b ON dm_conversations (user_b_id);
CREATE INDEX IF NOT EXISTS idx_dm_conversations_last_message ON dm_conversations (last_message_at DESC);

CREATE TABLE IF NOT EXISTS dm_messages (
  id BIGSERIAL PRIMARY KEY,
  conversation_id BIGINT NOT NULL REFERENCES dm_conversations(id) ON DELETE CASCADE,
  external_message_id TEXT NOT NULL,
  sender_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
  text TEXT,
  text_len INTEGER NOT NULL DEFAULT 0,
  sent_at TIMESTAMPTZ NOT NULL,
  reply_to_external_message_id TEXT,
  views INTEGER DEFAULT 0,
  forwards INTEGER DEFAULT 0,
  has_links BOOLEAN DEFAULT FALSE NOT NULL,
  has_mentions BOOLEAN DEFAULT FALSE NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  raw_payload JSONB
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_messages_unique
  ON dm_messages (conversation_id, external_message_id);

CREATE INDEX IF NOT EXISTS idx_dm_messages_conv_sent_at
  ON dm_messages (conversation_id, sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_dm_messages_sender
  ON dm_messages (sender_id);

-- migrate:down

DROP INDEX IF EXISTS idx_dm_messages_conv_sent_at;
DROP INDEX IF EXISTS idx_dm_messages_sender;
DROP INDEX IF EXISTS idx_dm_messages_unique;
DROP TABLE IF EXISTS dm_messages;

DROP INDEX IF EXISTS idx_dm_conversations_last_message;
DROP INDEX IF EXISTS idx_dm_conversations_user_a;
DROP INDEX IF EXISTS idx_dm_conversations_user_b;
DROP INDEX IF EXISTS idx_dm_conversations_chat_unique;
DROP TABLE IF EXISTS dm_conversations;
