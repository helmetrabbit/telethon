-- migrate:up

-- ============================================================
-- CONSTRAINT TRIGGERS: Evidence gating for claims
--
-- These fire at COMMIT time (DEFERRABLE INITIALLY DEFERRED) so
-- that claim + claim_evidence rows can be inserted in the same
-- transaction before the check runs.
--
-- Rule 1: Every claim MUST have >= 1 claim_evidence row.
-- Rule 2: For has_role / has_intent, at least one evidence row
--          must have evidence_type != 'membership'.
-- ============================================================

-- ── Rule 1: claim must have at least one evidence row ───────

CREATE OR REPLACE FUNCTION trg_claim_must_have_evidence()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM claim_evidence WHERE claim_id = NEW.id
  ) THEN
    RAISE EXCEPTION
      'claim id=% has no claim_evidence rows — every claim must be backed by evidence',
      NEW.id;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER claim_must_have_evidence
  AFTER INSERT ON claims
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION trg_claim_must_have_evidence();


-- ── Rule 2: role/intent claims need non-membership evidence ─

CREATE OR REPLACE FUNCTION trg_claim_role_intent_needs_real_evidence()
RETURNS TRIGGER AS $$
DECLARE
  non_membership_count INT;
BEGIN
  -- Only enforce for has_role and has_intent predicates
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


-- migrate:down

DROP TRIGGER IF EXISTS claim_role_intent_needs_real_evidence ON claims;
DROP FUNCTION IF EXISTS trg_claim_role_intent_needs_real_evidence();

DROP TRIGGER IF EXISTS claim_must_have_evidence ON claims;
DROP FUNCTION IF EXISTS trg_claim_must_have_evidence();
