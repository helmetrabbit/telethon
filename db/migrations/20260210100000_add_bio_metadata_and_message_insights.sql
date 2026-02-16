-- migrate:up

-- 1) Add bio metadata to users
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS bio_source TEXT,
  ADD COLUMN IF NOT EXISTS bio_updated_at TIMESTAMPTZ;

-- 2) Message distillation cache
CREATE TABLE IF NOT EXISTS message_insights (
  message_id BIGINT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
  user_id BIGINT NOT NULL,
  sent_at TIMESTAMPTZ NOT NULL,
  group_id BIGINT NOT NULL,
  text_hash TEXT NOT NULL,
  classifier_version TEXT NOT NULL DEFAULT 'v1',
  is_noise BOOLEAN DEFAULT FALSE,
  signal_types TEXT[] DEFAULT '{}'::text[],
  extracted_urls TEXT[] DEFAULT '{}'::text[],
  extracted_handles TEXT[] DEFAULT '{}'::text[],
  extracted_orgs TEXT[] DEFAULT '{}'::text[],
  extracted_roles TEXT[] DEFAULT '{}'::text[],
  first_person_score REAL DEFAULT 0,
  llm_confidence REAL,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_message_insights_user_sent_at ON message_insights (user_id, sent_at);
CREATE INDEX IF NOT EXISTS idx_message_insights_signal_types ON message_insights USING GIN (signal_types);
CREATE INDEX IF NOT EXISTS idx_message_insights_extracted_urls ON message_insights USING GIN (extracted_urls);
CREATE INDEX IF NOT EXISTS idx_message_insights_extracted_orgs ON message_insights USING GIN (extracted_orgs);
CREATE INDEX IF NOT EXISTS idx_message_insights_extracted_roles ON message_insights USING GIN (extracted_roles);

-- migrate:down

ALTER TABLE users
  DROP COLUMN IF EXISTS bio_source,
  DROP COLUMN IF EXISTS bio_updated_at;

DROP INDEX IF EXISTS idx_message_insights_extracted_urls;
DROP INDEX IF EXISTS idx_message_insights_signal_types;
DROP INDEX IF EXISTS idx_message_insights_user_sent_at;
DROP TABLE IF EXISTS message_insights;
