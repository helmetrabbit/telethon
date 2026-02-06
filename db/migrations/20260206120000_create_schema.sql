-- migrate:up

-- ============================================================
-- ENUMS (taxonomies — controlled vocabularies)
-- ============================================================

CREATE TYPE group_kind AS ENUM (
  'bd', 'work', 'general_chat', 'unknown'
);

CREATE TYPE role_label AS ENUM (
  'bd', 'builder', 'founder_exec', 'investor_analyst',
  'recruiter', 'vendor_agency', 'community', 'unknown'
);

CREATE TYPE intent_label AS ENUM (
  'networking', 'evaluating', 'selling', 'hiring',
  'support_seeking', 'support_giving', 'broadcasting', 'unknown'
);

CREATE TYPE evidence_type AS ENUM (
  'bio', 'message', 'feature', 'membership'
);

CREATE TYPE claim_status AS ENUM (
  'tentative', 'supported'
);

CREATE TYPE predicate_label AS ENUM (
  'has_role', 'has_intent', 'has_topic_affinity', 'affiliated_with'
);

-- ============================================================
-- LAYER 1: Raw traceability
-- ============================================================

-- Every import run is recorded so we can trace any row back to its source file.
CREATE TABLE raw_imports (
  id            BIGSERIAL PRIMARY KEY,
  source_path   TEXT        NOT NULL,
  imported_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  sha256        TEXT        NOT NULL
);

CREATE INDEX idx_raw_imports_sha256 ON raw_imports (sha256);

-- Individual rows extracted from the source JSON.
-- row_type: 'message' | 'user' | 'group_meta' etc.
CREATE TABLE raw_import_rows (
  id            BIGSERIAL PRIMARY KEY,
  raw_import_id BIGINT      NOT NULL REFERENCES raw_imports(id) ON DELETE CASCADE,
  row_type      TEXT        NOT NULL,
  external_id   TEXT,                         -- Telegram's id for the entity
  raw_json      JSONB       NOT NULL
);

CREATE INDEX idx_raw_import_rows_import ON raw_import_rows (raw_import_id);
CREATE INDEX idx_raw_import_rows_ext    ON raw_import_rows (row_type, external_id);

-- ============================================================
-- LAYER 2: Normalized ontology-lite
-- ============================================================

CREATE TABLE users (
  id            BIGSERIAL   PRIMARY KEY,
  platform      TEXT        NOT NULL DEFAULT 'telegram',
  external_id   TEXT        NOT NULL,
  handle        TEXT,
  display_name  TEXT,
  bio           TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (platform, external_id)
);

CREATE INDEX idx_users_handle ON users (handle);

CREATE TABLE groups (
  id            BIGSERIAL   PRIMARY KEY,
  platform      TEXT        NOT NULL DEFAULT 'telegram',
  external_id   TEXT        NOT NULL,
  title         TEXT,
  kind          group_kind  NOT NULL DEFAULT 'unknown',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (platform, external_id)
);

CREATE TABLE memberships (
  group_id      BIGINT      NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  user_id       BIGINT      NOT NULL REFERENCES users(id)  ON DELETE CASCADE,
  first_seen_at TIMESTAMPTZ,
  last_seen_at  TIMESTAMPTZ,
  msg_count     INT         NOT NULL DEFAULT 0,
  PRIMARY KEY (group_id, user_id)
);

CREATE TABLE messages (
  id                          BIGSERIAL   PRIMARY KEY,
  group_id                    BIGINT      NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  user_id                     BIGINT               REFERENCES users(id)  ON DELETE SET NULL,
  external_message_id         TEXT        NOT NULL,
  sent_at                     TIMESTAMPTZ NOT NULL,
  text                        TEXT,
  text_len                    INT         NOT NULL DEFAULT 0,
  reply_to_external_message_id TEXT,
  has_links                   BOOLEAN     NOT NULL DEFAULT FALSE,
  has_mentions                BOOLEAN     NOT NULL DEFAULT FALSE,
  raw_ref_row_id              BIGINT               REFERENCES raw_import_rows(id),
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_messages_group     ON messages (group_id);
CREATE INDEX idx_messages_user      ON messages (user_id);
CREATE INDEX idx_messages_sent_at   ON messages (sent_at);
CREATE INDEX idx_messages_ext_id    ON messages (group_id, external_message_id);

CREATE TABLE message_mentions (
  message_id        BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  mentioned_handle  TEXT   NOT NULL,
  mentioned_user_id BIGINT          REFERENCES users(id) ON DELETE SET NULL,
  PRIMARY KEY (message_id, mentioned_handle)
);

-- ============================================================
-- LAYER 3: Derived features + Inferred claims
-- ============================================================

CREATE TABLE user_features_daily (
  user_id             BIGINT  NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  day                 DATE    NOT NULL,
  msg_count           INT     NOT NULL DEFAULT 0,
  reply_count         INT     NOT NULL DEFAULT 0,
  mention_count       INT     NOT NULL DEFAULT 0,
  avg_msg_len         REAL    NOT NULL DEFAULT 0,
  groups_active_count INT     NOT NULL DEFAULT 0,
  bd_group_msg_share  REAL    NOT NULL DEFAULT 0,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, day)
);

CREATE TABLE claims (
  id               BIGSERIAL       PRIMARY KEY,
  subject_user_id  BIGINT          NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  predicate        predicate_label NOT NULL,
  object_value     TEXT            NOT NULL,
  status           claim_status    NOT NULL DEFAULT 'tentative',
  confidence       REAL            NOT NULL DEFAULT 0,
  model_version    TEXT            NOT NULL,
  generated_at     TIMESTAMPTZ     NOT NULL DEFAULT now(),
  notes            TEXT
);

CREATE INDEX idx_claims_user      ON claims (subject_user_id);
CREATE INDEX idx_claims_predicate ON claims (predicate, object_value);

CREATE TABLE claim_evidence (
  claim_id      BIGINT        NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
  evidence_type evidence_type NOT NULL,
  evidence_ref  TEXT          NOT NULL,  -- human-readable pointer (e.g. "message:12345", "bio:keyword:founder")
  weight        REAL          NOT NULL DEFAULT 1.0,
  PRIMARY KEY (claim_id, evidence_type, evidence_ref)
);


-- migrate:down

DROP TABLE IF EXISTS claim_evidence   CASCADE;
DROP TABLE IF EXISTS claims           CASCADE;
DROP TABLE IF EXISTS user_features_daily CASCADE;
DROP TABLE IF EXISTS message_mentions CASCADE;
DROP TABLE IF EXISTS messages         CASCADE;
DROP TABLE IF EXISTS memberships      CASCADE;
DROP TABLE IF EXISTS groups           CASCADE;
DROP TABLE IF EXISTS users            CASCADE;
DROP TABLE IF EXISTS raw_import_rows  CASCADE;
DROP TABLE IF EXISTS raw_imports      CASCADE;

DROP TYPE IF EXISTS predicate_label;
DROP TYPE IF EXISTS claim_status;
DROP TYPE IF EXISTS evidence_type;
DROP TYPE IF EXISTS intent_label;
DROP TYPE IF EXISTS role_label;
DROP TYPE IF EXISTS group_kind;
