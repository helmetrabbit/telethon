-- migrate:up

-- New evidence type for LLM-generated claims
ALTER TYPE public.evidence_type ADD VALUE IF NOT EXISTS 'llm';

-- Store raw LLM responses for audit/debugging
CREATE TABLE public.llm_enrichments (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES public.users(id),
    model_name TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    raw_response TEXT NOT NULL,
    parsed_json JSONB,
    latency_ms INT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(user_id, model_name, prompt_hash)
);

CREATE INDEX idx_llm_enrichments_user ON public.llm_enrichments(user_id);
CREATE INDEX idx_llm_enrichments_model ON public.llm_enrichments(model_name);

-- migrate:down
DROP TABLE IF EXISTS public.llm_enrichments;
