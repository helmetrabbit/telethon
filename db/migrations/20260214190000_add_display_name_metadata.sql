-- migrate:up

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS display_name_source TEXT,
  ADD COLUMN IF NOT EXISTS display_name_updated_at TIMESTAMPTZ;

-- migrate:down

ALTER TABLE users
  DROP COLUMN IF EXISTS display_name_source,
  DROP COLUMN IF EXISTS display_name_updated_at;
