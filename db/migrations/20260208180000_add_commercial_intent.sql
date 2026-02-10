-- migrate:up
ALTER TABLE user_psychographics
  ADD COLUMN commercial_archetype text,
  ADD COLUMN pain_points jsonb DEFAULT '[]'::jsonb,
  ADD COLUMN crypto_values jsonb DEFAULT '[]'::jsonb;

-- migrate:down
ALTER TABLE user_psychographics
  DROP COLUMN commercial_archetype,
  DROP COLUMN pain_points,
  DROP COLUMN crypto_values;
