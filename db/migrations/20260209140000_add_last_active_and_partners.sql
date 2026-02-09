-- migrate:up
-- Add last_active_days, top_conversation_partners

ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS last_active_days integer,
  ADD COLUMN IF NOT EXISTS top_conversation_partners jsonb DEFAULT '[]'::jsonb;

COMMENT ON COLUMN user_psychographics.last_active_days IS 'Days since last message (integer for sorting)';
COMMENT ON COLUMN user_psychographics.top_conversation_partners IS 'Top people this user interacts with via replies';

-- migrate:down
ALTER TABLE user_psychographics
  DROP COLUMN IF EXISTS last_active_days,
  DROP COLUMN IF EXISTS top_conversation_partners;
