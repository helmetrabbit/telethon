-- migrate:up

ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS role_company_timeline JSONB DEFAULT '[]'::jsonb;

COMMENT ON COLUMN user_psychographics.role_company_timeline IS
  'Timeline of role/company changes with evidence: [{org, role, start_hint, end_hint, is_current, evidence_message_ids[], confidence}]';

-- migrate:down

ALTER TABLE user_psychographics
  DROP COLUMN IF EXISTS role_company_timeline;
