-- migrate:up
-- Add importance_weight to groups for proportional message sampling

ALTER TABLE groups
  ADD COLUMN IF NOT EXISTS importance_weight real DEFAULT 5.0 NOT NULL;

COMMENT ON COLUMN groups.importance_weight IS 'Weight for proportional message sampling during enrichment (1-10 scale)';

-- Set initial weights based on known group relevance
-- BD/professional groups get highest weight, large general chats lower
UPDATE groups SET importance_weight = 9.0 WHERE title ILIKE '%BD in Web3%';
UPDATE groups SET importance_weight = 8.0 WHERE title ILIKE '%Avalanche Builders%';
UPDATE groups SET importance_weight = 8.0 WHERE title ILIKE '%ETH Magicians%';
UPDATE groups SET importance_weight = 7.0 WHERE title ILIKE '%BTC Connect%';
UPDATE groups SET importance_weight = 7.0 WHERE title ILIKE '%Savvy Conferences%';
UPDATE groups SET importance_weight = 7.0 WHERE title ILIKE '%gmBD%';
UPDATE groups SET importance_weight = 6.0 WHERE title ILIKE '%The Best Event%';
UPDATE groups SET importance_weight = 6.0 WHERE title ILIKE '%The Trenches%';
UPDATE groups SET importance_weight = 3.0 WHERE title ILIKE '%LobsterDAO%';

-- migrate:down
ALTER TABLE groups DROP COLUMN IF EXISTS importance_weight;
