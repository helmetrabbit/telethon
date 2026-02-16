-- migrate:up

CREATE TABLE IF NOT EXISTS dm_profile_update_events (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  conversation_id BIGINT REFERENCES dm_conversations(id) ON DELETE SET NULL,
  source_message_id BIGINT REFERENCES dm_messages(id) ON DELETE SET NULL,
  source_external_message_id TEXT,
  event_type TEXT NOT NULL,
  event_source TEXT NOT NULL DEFAULT 'dm_listener',
  actor_role TEXT NOT NULL DEFAULT 'user',
  event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  extracted_facts JSONB NOT NULL DEFAULT '[]'::jsonb,
  confidence REAL NOT NULL DEFAULT 0.5 CHECK (confidence >= 0 AND confidence <= 1),
  processed BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dm_profile_update_events_message_type
  ON dm_profile_update_events (source_message_id, event_type)
  WHERE source_message_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dm_profile_update_events_user
  ON dm_profile_update_events (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dm_profile_update_events_unprocessed
  ON dm_profile_update_events (id)
  WHERE processed = false;

CREATE TABLE IF NOT EXISTS dm_profile_state (
  user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  last_profile_event_id BIGINT REFERENCES dm_profile_update_events(id) ON DELETE SET NULL,
  user_psychographics_id BIGINT REFERENCES user_psychographics(id) ON DELETE SET NULL,
  snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- migrate:down

DROP INDEX IF EXISTS idx_dm_profile_update_events_unprocessed;
DROP INDEX IF EXISTS idx_dm_profile_update_events_user;
DROP INDEX IF EXISTS ux_dm_profile_update_events_message_type;
DROP TABLE IF EXISTS dm_profile_update_events;

DROP TABLE IF EXISTS dm_profile_state;
