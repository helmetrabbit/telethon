-- migrate:up

DROP INDEX IF EXISTS ux_dm_profile_update_events_message_type;
DROP INDEX IF EXISTS ux_dm_profile_update_events_message_type_all;
CREATE UNIQUE INDEX IF NOT EXISTS ux_dm_profile_update_events_message_type
  ON dm_profile_update_events (source_message_id, event_type)
  WHERE source_message_id IS NOT NULL;

-- migrate:down

DROP INDEX IF EXISTS ux_dm_profile_update_events_message_type;
CREATE UNIQUE INDEX IF NOT EXISTS ux_dm_profile_update_events_message_type_all
  ON dm_profile_update_events (source_message_id, event_type);
