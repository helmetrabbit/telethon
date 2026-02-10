-- migrate:up
ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS total_reactions integer,
  ADD COLUMN IF NOT EXISTS avg_reactions_per_msg numeric(5,1),
  ADD COLUMN IF NOT EXISTS total_replies_received integer,
  ADD COLUMN IF NOT EXISTS avg_replies_per_msg numeric(5,1),
  ADD COLUMN IF NOT EXISTS engagement_rate numeric(5,1);

-- migrate:down
ALTER TABLE user_psychographics
  DROP COLUMN IF EXISTS total_reactions,
  DROP COLUMN IF EXISTS avg_reactions_per_msg,
  DROP COLUMN IF EXISTS total_replies_received,
  DROP COLUMN IF EXISTS avg_replies_per_msg,
  DROP COLUMN IF EXISTS engagement_rate;
