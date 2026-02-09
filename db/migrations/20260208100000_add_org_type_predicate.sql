-- migrate:up
-- Migration: Add has_org_type predicate
-- Supports fix #2: separate function-role from organization-type classification.
-- A user can be "BD" (has_role) at a "market_maker" firm (has_org_type).

-- Add new predicate value
ALTER TYPE predicate_label ADD VALUE IF NOT EXISTS 'has_org_type';

-- migrate:down
-- Cannot remove enum value in Postgres safely without dropping type.
-- No-op or complex migration required. We leave it.

