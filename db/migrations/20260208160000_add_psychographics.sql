-- migrate:up

-- 1. Create table for psychographic profiles (dossiers)
CREATE TABLE public.user_psychographics (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES public.users(id),
    model_name      TEXT NOT NULL,
    prompt_hash     TEXT NOT NULL,

    -- Structured dimensions (communication style)
    tone            TEXT,           -- formal | casual | blunt | diplomatic | enthusiastic | dry
    professionalism TEXT,           -- corporate | professional | relaxed | street
    verbosity       TEXT,           -- terse | concise | moderate | verbose | walls_of_text
    responsiveness  TEXT,           -- fast_responder | deliberate | sporadic | lurker
    decision_style  TEXT,           -- data_driven | relationship_driven | authority_driven | consensus_seeker
    seniority_signal TEXT,          -- junior | mid | senior | executive | unclear
    
    approachability REAL,           -- 0.0 to 1.0

    -- Freeform / Lists (intelligence)
    quirks          JSONB DEFAULT '[]',   -- string[]: "Uses Russian often", "Emojis in every sentence"
    notable_topics  JSONB DEFAULT '[]',   -- string[]: "ZK proofs", "EthDenver", "Skiing"
    
    -- New fields for Location / Events
    based_in        TEXT,                 -- "New York", "Bangkok", "Remote"
    attended_events JSONB DEFAULT '[]',   -- string[]: "Token2049", "Devcon 7"

    preferred_contact_style TEXT,         -- 1-sentence actionable advice
    reasoning       TEXT,                 -- grounding

    raw_response    TEXT,
    latency_ms      INT,
    created_at      TIMESTAMPTZ DEFAULT now(),

    UNIQUE(user_id, model_name, prompt_hash)
);

CREATE INDEX idx_psychographics_user ON public.user_psychographics(user_id);
CREATE INDEX idx_psychographics_model ON public.user_psychographics(model_name);

-- migrate:down
DROP TABLE IF EXISTS public.user_psychographics;
