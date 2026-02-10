-- migrate:up
-- Split activity_window string into separate sortable/filterable columns
ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS total_msgs      integer,
  ADD COLUMN IF NOT EXISTS avg_msg_length  integer,
  ADD COLUMN IF NOT EXISTS peak_hours      integer[],
  ADD COLUMN IF NOT EXISTS active_days     text[];

-- Drop the old string column
ALTER TABLE user_psychographics DROP COLUMN IF EXISTS activity_window;

-- migrate:down
ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS activity_window text;
ALTER TABLE user_psychographics
  DROP COLUMN IF EXISTS total_msgs,
  DROP COLUMN IF EXISTS avg_msg_length,
  DROP COLUMN IF EXISTS peak_hours,
  DROP COLUMN IF EXISTS active_days;
