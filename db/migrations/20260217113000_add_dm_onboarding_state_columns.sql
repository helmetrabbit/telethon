-- migrate:up

ALTER TABLE dm_profile_state
  ADD COLUMN IF NOT EXISTS onboarding_status TEXT NOT NULL DEFAULT 'not_started',
  ADD COLUMN IF NOT EXISTS onboarding_required_fields JSONB NOT NULL DEFAULT '["primary_role","primary_company","notable_topics","preferred_contact_style"]'::jsonb,
  ADD COLUMN IF NOT EXISTS onboarding_missing_fields JSONB NOT NULL DEFAULT '["primary_role","primary_company","notable_topics","preferred_contact_style"]'::jsonb,
  ADD COLUMN IF NOT EXISTS onboarding_last_prompted_field TEXT,
  ADD COLUMN IF NOT EXISTS onboarding_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS onboarding_completed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS onboarding_turns INTEGER NOT NULL DEFAULT 0;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dm_profile_state_onboarding_status_check'
  ) THEN
    ALTER TABLE dm_profile_state
      ADD CONSTRAINT dm_profile_state_onboarding_status_check
      CHECK (onboarding_status IN ('not_started', 'collecting', 'completed', 'paused'));
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_dm_profile_state_onboarding_status
  ON dm_profile_state (onboarding_status);


-- migrate:down

DROP INDEX IF EXISTS idx_dm_profile_state_onboarding_status;
ALTER TABLE dm_profile_state DROP CONSTRAINT IF EXISTS dm_profile_state_onboarding_status_check;
ALTER TABLE dm_profile_state
  DROP COLUMN IF EXISTS onboarding_turns,
  DROP COLUMN IF EXISTS onboarding_completed_at,
  DROP COLUMN IF EXISTS onboarding_started_at,
  DROP COLUMN IF EXISTS onboarding_last_prompted_field,
  DROP COLUMN IF EXISTS onboarding_missing_fields,
  DROP COLUMN IF EXISTS onboarding_required_fields,
  DROP COLUMN IF EXISTS onboarding_status;
