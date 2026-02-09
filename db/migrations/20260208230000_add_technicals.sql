-- migrate:up

-- 1. Add Technicals and Business Focus for Researchers/Devs/Funds
ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS technical_specifics text[],
  ADD COLUMN IF NOT EXISTS business_focus text[];

-- migrate:down
ALTER TABLE user_psychographics
  DROP COLUMN IF EXISTS technical_specifics,
  DROP COLUMN IF EXISTS business_focus;
