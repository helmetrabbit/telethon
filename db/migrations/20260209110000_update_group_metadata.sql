-- migrate:up
-- Add group_description column and update group metadata

ALTER TABLE groups ADD COLUMN IF NOT EXISTS group_description text;

-- Update all groups with correct importance_weight, kind, and group_description
UPDATE groups SET importance_weight = 8, kind = 'bd', group_description = 'Business Networking' WHERE title LIKE 'BD in Web3%';
UPDATE groups SET importance_weight = 5, kind = 'work', group_description = 'Ecosystem Developers' WHERE title LIKE 'Avalanche Builders%';
UPDATE groups SET importance_weight = 6, kind = 'work', group_description = 'Ethereum Contributors' WHERE title LIKE 'ETH Magicians%';
UPDATE groups SET importance_weight = 4, kind = 'bd', group_description = 'Bitcoin Networking' WHERE title LIKE 'BTC Connect%';
UPDATE groups SET importance_weight = 3, kind = 'bd', group_description = 'Conference Networking' WHERE title LIKE 'Savvy Conferences%';
UPDATE groups SET importance_weight = 7, kind = 'bd', group_description = 'Business Networking' WHERE title LIKE 'gmBD%';
UPDATE groups SET importance_weight = 3, kind = 'bd', group_description = 'Event Networking' WHERE title LIKE 'The Best Event%';
UPDATE groups SET importance_weight = 3, kind = 'bd', group_description = 'Business Networking' WHERE title LIKE 'The Trenches%';
UPDATE groups SET importance_weight = 8, kind = 'general_chat', group_description = 'Research, Technical, and Political Discussion' WHERE title LIKE 'LobsterDAO%';

-- migrate:down
ALTER TABLE groups DROP COLUMN IF EXISTS group_description;
UPDATE groups SET importance_weight = 5, kind = 'unknown' WHERE TRUE;
