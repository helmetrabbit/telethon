-- migrate:up
-- Add activity_window for messaging pattern analysis

ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS activity_window text;

COMMENT ON COLUMN user_psychographics.activity_window IS 'Summary of messaging activity patterns: peak hours, active days, msg count, avg size';

-- migrate:down
ALTER TABLE user_psychographics DROP COLUMN IF EXISTS activity_window;
