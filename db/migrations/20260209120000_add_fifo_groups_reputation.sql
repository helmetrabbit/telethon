-- migrate:up
-- Add FIFO (first/last message dates) and group_tags to psychographics

ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS fifo text,
  ADD COLUMN IF NOT EXISTS group_tags text[] DEFAULT '{}'::text[],
  ADD COLUMN IF NOT EXISTS reputation_summary text;

COMMENT ON COLUMN user_psychographics.fifo IS 'First In First Out: MM/YY - MM/YY of first and last message';
COMMENT ON COLUMN user_psychographics.group_tags IS 'List of group names the user is a member of';
COMMENT ON COLUMN user_psychographics.reputation_summary IS 'Human-readable summary of reputation stats';

-- migrate:down
ALTER TABLE user_psychographics
  DROP COLUMN IF EXISTS fifo,
  DROP COLUMN IF EXISTS group_tags,
  DROP COLUMN IF EXISTS reputation_summary;
