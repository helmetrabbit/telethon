-- migrate:up

ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS conflict_notes JSONB DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS evidence_summary_json JSONB DEFAULT '{}'::jsonb;

COMMENT ON COLUMN user_psychographics.conflict_notes IS
  'Structured conflict notes (e.g., bio vs message evidence).';
COMMENT ON COLUMN user_psychographics.evidence_summary_json IS
  'Per-user evidence pack summary: counts, recency share, ranges.';

-- migrate:down

ALTER TABLE user_psychographics
  DROP COLUMN IF EXISTS conflict_notes,
  DROP COLUMN IF EXISTS evidence_summary_json;
