-- migrate:up

-- ── Track whether a user is currently a member of the group ─────
-- Set to TRUE for users found in the participant list (Telethon exports),
-- FALSE for users known only from their messages (i.e. they left/were kicked).
-- NULL means unknown (e.g. Desktop exports that lack participant data).

ALTER TABLE memberships
  ADD COLUMN is_current_member BOOLEAN DEFAULT NULL;


-- migrate:down

ALTER TABLE memberships
  DROP COLUMN IF EXISTS is_current_member;
