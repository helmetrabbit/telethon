-- migrate:up

-- ============================================================
-- Phase B: Harden claim constraints
--
-- Fixes three gaps found in the Phase A audit:
--
-- Fix 1: Trigger Rule 2 now covers has_topic_affinity
--         (was only has_role, has_intent).
--
-- Fix 2: Both constraint triggers now fire on INSERT OR UPDATE
--         (was INSERT only). Prevents bypassing via UPDATE
--         of predicate or evidence_type after initial insert.
--
-- Fix 3: New trigger validates that object_value is a valid
--         member of the relevant ENUM for has_role/has_intent.
--         affiliated_with and has_topic_affinity are free-text
--         (no ENUM validation, but must be non-empty).
-- ============================================================

-- ── Fix 1 + 2: Replace Rule 1 trigger (now INSERT OR UPDATE) ─

DROP TRIGGER IF EXISTS claim_must_have_evidence ON claims;

CREATE CONSTRAINT TRIGGER claim_must_have_evidence
  AFTER INSERT OR UPDATE ON claims
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION trg_claim_must_have_evidence();
  -- Function body unchanged — still checks ≥1 claim_evidence row.


-- ── Fix 1 + 2: Replace Rule 2 trigger + function ─────────────

DROP TRIGGER IF EXISTS claim_role_intent_needs_real_evidence ON claims;
DROP FUNCTION IF EXISTS trg_claim_role_intent_needs_real_evidence();

CREATE OR REPLACE FUNCTION trg_claim_needs_real_evidence()
RETURNS TRIGGER AS $$
DECLARE
  non_membership_count INT;
BEGIN
  -- Enforce for has_role, has_intent, AND has_topic_affinity
  IF NEW.predicate IN ('has_role', 'has_intent', 'has_topic_affinity') THEN
    SELECT count(*) INTO non_membership_count
      FROM claim_evidence
     WHERE claim_id = NEW.id
       AND evidence_type != 'membership';

    IF non_membership_count = 0 THEN
      RAISE EXCEPTION
        'claim id=% (predicate=%) cannot be committed with only membership evidence — '
        'at least one bio/message/feature evidence row is required',
        NEW.id, NEW.predicate;
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER claim_needs_real_evidence
  AFTER INSERT OR UPDATE ON claims
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION trg_claim_needs_real_evidence();


-- ── Fix 3: Validate object_value against relevant ENUM ────────

CREATE OR REPLACE FUNCTION trg_claim_validate_object_value()
RETURNS TRIGGER AS $$
BEGIN
  -- For has_role: object_value must be a valid role_label
  IF NEW.predicate = 'has_role' THEN
    BEGIN
      PERFORM NEW.object_value::role_label;
    EXCEPTION WHEN invalid_text_representation THEN
      RAISE EXCEPTION
        'claim id=%: object_value ''%'' is not a valid role_label for predicate has_role',
        NEW.id, NEW.object_value;
    END;

  -- For has_intent: object_value must be a valid intent_label
  ELSIF NEW.predicate = 'has_intent' THEN
    BEGIN
      PERFORM NEW.object_value::intent_label;
    EXCEPTION WHEN invalid_text_representation THEN
      RAISE EXCEPTION
        'claim id=%: object_value ''%'' is not a valid intent_label for predicate has_intent',
        NEW.id, NEW.object_value;
    END;

  -- For affiliated_with / has_topic_affinity: must be non-empty
  ELSE
    IF NEW.object_value IS NULL OR trim(NEW.object_value) = '' THEN
      RAISE EXCEPTION
        'claim id=%: object_value cannot be empty for predicate %',
        NEW.id, NEW.predicate;
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER claim_validate_object_value
  AFTER INSERT OR UPDATE ON claims
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION trg_claim_validate_object_value();


-- ── Fix 2 (evidence side): re-validate claim when evidence changes ─
-- If someone deletes or updates evidence rows, the parent claim might
-- become invalid. Add a trigger on claim_evidence that re-fires the
-- checks on the parent claim.

CREATE OR REPLACE FUNCTION trg_evidence_change_revalidate_claim()
RETURNS TRIGGER AS $$
DECLARE
  the_claim_id BIGINT;
  the_predicate predicate_label;
  non_membership_count INT;
  evidence_count INT;
BEGIN
  -- Determine which claim_id was affected
  IF TG_OP = 'DELETE' THEN
    the_claim_id := OLD.claim_id;
  ELSE
    the_claim_id := NEW.claim_id;
  END IF;

  -- Look up the claim
  SELECT predicate INTO the_predicate
    FROM claims WHERE id = the_claim_id;

  IF NOT FOUND THEN
    RETURN COALESCE(NEW, OLD);
  END IF;

  -- Re-check Rule 1: must have ≥1 evidence row
  SELECT count(*) INTO evidence_count
    FROM claim_evidence WHERE claim_id = the_claim_id;

  IF evidence_count = 0 THEN
    RAISE EXCEPTION
      'claim id=% has no claim_evidence rows — every claim must be backed by evidence',
      the_claim_id;
  END IF;

  -- Re-check Rule 2: role/intent/topic must have non-membership evidence
  IF the_predicate IN ('has_role', 'has_intent', 'has_topic_affinity') THEN
    SELECT count(*) INTO non_membership_count
      FROM claim_evidence
     WHERE claim_id = the_claim_id
       AND evidence_type != 'membership';

    IF non_membership_count = 0 THEN
      RAISE EXCEPTION
        'claim id=% (predicate=%) cannot exist with only membership evidence — '
        'at least one bio/message/feature evidence row is required',
        the_claim_id, the_predicate;
    END IF;
  END IF;

  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER evidence_change_revalidate_claim
  AFTER UPDATE OR DELETE ON claim_evidence
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION trg_evidence_change_revalidate_claim();


-- migrate:down

DROP TRIGGER IF EXISTS evidence_change_revalidate_claim ON claim_evidence;
DROP FUNCTION IF EXISTS trg_evidence_change_revalidate_claim();

DROP TRIGGER IF EXISTS claim_validate_object_value ON claims;
DROP FUNCTION IF EXISTS trg_claim_validate_object_value();

DROP TRIGGER IF EXISTS claim_needs_real_evidence ON claims;
DROP FUNCTION IF EXISTS trg_claim_needs_real_evidence();

-- Restore original triggers (from migration 20260206120100)
CREATE CONSTRAINT TRIGGER claim_must_have_evidence
  AFTER INSERT ON claims
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION trg_claim_must_have_evidence();

CREATE OR REPLACE FUNCTION trg_claim_role_intent_needs_real_evidence()
RETURNS TRIGGER AS $$
DECLARE
  non_membership_count INT;
BEGIN
  IF NEW.predicate IN ('has_role', 'has_intent') THEN
    SELECT count(*) INTO non_membership_count
      FROM claim_evidence
     WHERE claim_id = NEW.id
       AND evidence_type != 'membership';
    IF non_membership_count = 0 THEN
      RAISE EXCEPTION
        'claim id=% (predicate=%) cannot be committed with only membership evidence — '
        'at least one bio/message/feature evidence row is required',
        NEW.id, NEW.predicate;
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER claim_role_intent_needs_real_evidence
  AFTER INSERT ON claims
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION trg_claim_role_intent_needs_real_evidence();
