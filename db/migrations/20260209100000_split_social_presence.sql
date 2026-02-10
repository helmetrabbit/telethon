-- migrate:up
-- Split social_presence into social_platforms (platform names) + social_urls (full links/handles)

ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS social_urls text[] DEFAULT '{}'::text[];

-- Rename social_presence → social_platforms for clarity
ALTER TABLE user_psychographics
  RENAME COLUMN social_presence TO social_platforms;

COMMENT ON COLUMN user_psychographics.social_platforms IS 'Platform names only (e.g. Twitter, LinkedIn, Farcaster)';
COMMENT ON COLUMN user_psychographics.social_urls IS 'Full social URLs or handles (e.g. twitter.com/user, linkedin.com/in/user)';

-- migrate:down
ALTER TABLE user_psychographics RENAME COLUMN social_platforms TO social_presence;
ALTER TABLE user_psychographics DROP COLUMN IF EXISTS social_urls;
