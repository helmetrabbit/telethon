-- migrate:up

-- 1. ADD STALENESS TRACKING (The "Sync" Engine)
ALTER TABLE users ADD COLUMN IF NOT EXISTS needs_enrichment boolean DEFAULT false;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_msg_at timestamp with time zone;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_enriched_at timestamp with time zone;

-- 2. CREATE TRIGGER FOR AUTO-FLAGGING
CREATE OR REPLACE FUNCTION trg_flag_user_for_enrichment() RETURNS TRIGGER AS $$
BEGIN
    UPDATE users 
    SET needs_enrichment = true, last_msg_at = NEW.sent_at 
    WHERE id = NEW.user_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS flag_user_dirty ON messages;
CREATE TRIGGER flag_user_dirty
AFTER INSERT ON messages
FOR EACH ROW
EXECUTE FUNCTION trg_flag_user_for_enrichment();

-- 3. EXPAND USERS (Total Capture)
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS is_verified boolean DEFAULT false,
  ADD COLUMN IF NOT EXISTS is_scam boolean DEFAULT false,
  ADD COLUMN IF NOT EXISTS is_fake boolean DEFAULT false,
  ADD COLUMN IF NOT EXISTS is_premium boolean DEFAULT false,
  ADD COLUMN IF NOT EXISTS lang_code text;

-- 4. EXPAND MESSAGES (Viral Metrics)
ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS views integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS forwards integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS media_type text,
  ADD COLUMN IF NOT EXISTS topic_id bigint,
  ADD COLUMN IF NOT EXISTS reply_count integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reaction_count integer DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reactions jsonb DEFAULT '[]'::jsonb;

-- Ensure uniqueness for incremental sync (ON CONFLICT DO NOTHING support)
-- Note: Check if index exists first to avoid errors, though CREATE UNIQUE INDEX IF NOT EXISTS is PG 9.5+ feature which is standard now.
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_dedup ON messages(group_id, external_message_id);

-- 5. EXPAND PSYCHOGRAPHICS (Omni-Profile)
ALTER TABLE user_psychographics
  ADD COLUMN IF NOT EXISTS generated_bio_professional text,
  ADD COLUMN IF NOT EXISTS generated_bio_personal text,
  ADD COLUMN IF NOT EXISTS affiliations text[],
  ADD COLUMN IF NOT EXISTS deep_skills text[],
  ADD COLUMN IF NOT EXISTS social_presence text[],
  ADD COLUMN IF NOT EXISTS buying_power text,
  ADD COLUMN IF NOT EXISTS languages text[],
  ADD COLUMN IF NOT EXISTS scam_risk_score integer,
  ADD COLUMN IF NOT EXISTS confidence_score real,
  ADD COLUMN IF NOT EXISTS career_stage text,
  ADD COLUMN IF NOT EXISTS tribe_affiliations text[],
  ADD COLUMN IF NOT EXISTS reputation_score integer,
  ADD COLUMN IF NOT EXISTS driving_values text[];

-- 6. CLEANUP (Deprecate Old Tables)
DROP TABLE IF EXISTS llm_enrichments;   
-- NOTE: abstention_log is still used by infer-claims, do NOT drop it

-- 7. DATA BACKFILL (Reply Counts)
-- Update existing messages so they have correct reply counts immediately
WITH ReplyCounts AS (
    SELECT reply_to_external_message_id, group_id, COUNT(*) as cnt
    FROM messages WHERE reply_to_external_message_id IS NOT NULL
    GROUP BY reply_to_external_message_id, group_id
)
UPDATE messages m
SET reply_count = rc.cnt
FROM ReplyCounts rc
WHERE m.external_message_id = rc.reply_to_external_message_id
  AND m.group_id = rc.group_id;

-- migrate:down
Alter TABLE users DROP COLUMN IF EXISTS is_verified, DROP COLUMN IF EXISTS is_scam, DROP COLUMN IF EXISTS is_fake, DROP COLUMN IF EXISTS is_premium, DROP COLUMN IF EXISTS lang_code, DROP COLUMN IF EXISTS needs_enrichment, DROP COLUMN IF EXISTS last_msg_at, DROP COLUMN IF EXISTS last_enriched_at;
DROP TRIGGER IF EXISTS flag_user_dirty ON messages;
DROP FUNCTION IF EXISTS trg_flag_user_for_enrichment();
ALTER TABLE messages DROP COLUMN IF EXISTS views, DROP COLUMN IF EXISTS forwards, DROP COLUMN IF EXISTS media_type, DROP COLUMN IF EXISTS topic_id, DROP COLUMN IF EXISTS reply_count, DROP COLUMN IF EXISTS reaction_count, DROP COLUMN IF EXISTS reactions;
ALTER TABLE user_psychographics DROP COLUMN IF EXISTS generated_bio_professional, DROP COLUMN IF EXISTS generated_bio_personal, DROP COLUMN IF EXISTS affiliations, DROP COLUMN IF EXISTS deep_skills, DROP COLUMN IF EXISTS social_presence, DROP COLUMN IF EXISTS buying_power, DROP COLUMN IF EXISTS languages, DROP COLUMN IF EXISTS scam_risk_score, DROP COLUMN IF EXISTS confidence_score, DROP COLUMN IF EXISTS career_stage, DROP COLUMN IF EXISTS tribe_affiliations, DROP COLUMN IF EXISTS reputation_score, DROP COLUMN IF EXISTS driving_values;
