-- migrate:up

-- Add new role values to role_label enum
ALTER TYPE role_label ADD VALUE IF NOT EXISTS 'media_kol';
ALTER TYPE role_label ADD VALUE IF NOT EXISTS 'market_maker';

-- Add display_name evidence type
ALTER TYPE evidence_type ADD VALUE IF NOT EXISTS 'display_name';

-- migrate:down
-- Note: PostgreSQL does not support removing values from an existing enum.
-- To reverse, you'd need to recreate the enum — skipped here for safety.
