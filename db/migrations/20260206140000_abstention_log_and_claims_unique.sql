-- migrate:up

-- ============================================================
-- Phase D: Abstention log + claims uniqueness
--
-- 1. abstention_log: persists WHY a user got no claim for a
--    given predicate (evidence gating, low confidence, etc.)
--    Enables post-hoc analysis of "unknown" users.
--
-- 2. UNIQUE index on claims(subject_user_id, predicate,
--    object_value, model_version) enables idempotent upserts.
--    Re-running inference with the same model_version replaces
--    existing claims rather than duplicating them.
-- ============================================================

-- ── 1. Abstention log ─────────────────────────────────────

CREATE TABLE abstention_log (
  id               BIGSERIAL       PRIMARY KEY,
  subject_user_id  BIGINT          NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  predicate        predicate_label NOT NULL,
  reason_code      TEXT            NOT NULL,
  details          TEXT,
  model_version    TEXT            NOT NULL,
  generated_at     TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX idx_abstention_user      ON abstention_log (subject_user_id);
CREATE INDEX idx_abstention_version   ON abstention_log (model_version);
CREATE INDEX idx_abstention_predicate ON abstention_log (predicate, reason_code);

COMMENT ON TABLE abstention_log IS
  'Records why a claim was NOT emitted for a user — evidence gating, '
  'low confidence, or insufficient data. Enables audit of "unknown" assignments.';

COMMENT ON COLUMN abstention_log.reason_code IS
  'Machine-readable code: insufficient_evidence, low_confidence, no_data';


-- ── 2. Unique constraint for idempotent claim upserts ─────

CREATE UNIQUE INDEX idx_claims_unique_per_version
  ON claims (subject_user_id, predicate, object_value, model_version);

COMMENT ON INDEX idx_claims_unique_per_version IS
  'Enables ON CONFLICT upsert: re-running inference with the same '
  'model_version replaces confidence/status/evidence rather than duplicating.';


-- migrate:down

DROP INDEX IF EXISTS idx_claims_unique_per_version;
DROP TABLE IF EXISTS abstention_log CASCADE;
