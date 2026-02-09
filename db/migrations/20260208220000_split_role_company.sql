-- migrate:up

-- 1. Split Role and Company for cleaner CRM data
ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS primary_role text,
  ADD COLUMN IF NOT EXISTS primary_company text;

-- migrate:down
ALTER TABLE user_psychographics
  DROP COLUMN IF EXISTS primary_role,
  DROP COLUMN IF EXISTS primary_company;
