-- migrate:up
ALTER TABLE user_psychographics
  ADD COLUMN connection_requests jsonb DEFAULT '[]'::jsonb,
  ADD COLUMN fingerprint_tags text[] DEFAULT '{}';

-- migrate:down
ALTER TABLE user_psychographics
  DROP COLUMN connection_requests,
  DROP COLUMN fingerprint_tags;
