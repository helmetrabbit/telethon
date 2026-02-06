\restrict dbmate

-- Dumped from database version 16.11
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: claim_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.claim_status AS ENUM (
    'tentative',
    'supported'
);


--
-- Name: evidence_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.evidence_type AS ENUM (
    'bio',
    'message',
    'feature',
    'membership'
);


--
-- Name: group_kind; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.group_kind AS ENUM (
    'bd',
    'work',
    'general_chat',
    'unknown'
);


--
-- Name: intent_label; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.intent_label AS ENUM (
    'networking',
    'evaluating',
    'selling',
    'hiring',
    'support_seeking',
    'support_giving',
    'broadcasting',
    'unknown'
);


--
-- Name: predicate_label; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.predicate_label AS ENUM (
    'has_role',
    'has_intent',
    'has_topic_affinity',
    'affiliated_with'
);


--
-- Name: role_label; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.role_label AS ENUM (
    'bd',
    'builder',
    'founder_exec',
    'investor_analyst',
    'recruiter',
    'vendor_agency',
    'community',
    'unknown'
);


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: claim_evidence; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.claim_evidence (
    claim_id bigint NOT NULL,
    evidence_type public.evidence_type NOT NULL,
    evidence_ref text NOT NULL,
    weight real DEFAULT 1.0 NOT NULL
);


--
-- Name: claims; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.claims (
    id bigint NOT NULL,
    subject_user_id bigint NOT NULL,
    predicate public.predicate_label NOT NULL,
    object_value text NOT NULL,
    status public.claim_status DEFAULT 'tentative'::public.claim_status NOT NULL,
    confidence real DEFAULT 0 NOT NULL,
    model_version text NOT NULL,
    generated_at timestamp with time zone DEFAULT now() NOT NULL,
    notes text
);


--
-- Name: claims_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.claims_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: claims_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.claims_id_seq OWNED BY public.claims.id;


--
-- Name: groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.groups (
    id bigint NOT NULL,
    platform text DEFAULT 'telegram'::text NOT NULL,
    external_id text NOT NULL,
    title text,
    kind public.group_kind DEFAULT 'unknown'::public.group_kind NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: groups_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.groups_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: groups_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.groups_id_seq OWNED BY public.groups.id;


--
-- Name: memberships; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.memberships (
    group_id bigint NOT NULL,
    user_id bigint NOT NULL,
    first_seen_at timestamp with time zone,
    last_seen_at timestamp with time zone,
    msg_count integer DEFAULT 0 NOT NULL
);


--
-- Name: message_mentions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.message_mentions (
    message_id bigint NOT NULL,
    mentioned_handle text NOT NULL,
    mentioned_user_id bigint
);


--
-- Name: messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.messages (
    id bigint NOT NULL,
    group_id bigint NOT NULL,
    user_id bigint,
    external_message_id text NOT NULL,
    sent_at timestamp with time zone NOT NULL,
    text text,
    text_len integer DEFAULT 0 NOT NULL,
    reply_to_external_message_id text,
    has_links boolean DEFAULT false NOT NULL,
    has_mentions boolean DEFAULT false NOT NULL,
    raw_ref_row_id bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: messages_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.messages_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: messages_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.messages_id_seq OWNED BY public.messages.id;


--
-- Name: raw_import_rows; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_import_rows (
    id bigint NOT NULL,
    raw_import_id bigint NOT NULL,
    row_type text NOT NULL,
    external_id text,
    raw_json jsonb NOT NULL
);


--
-- Name: raw_import_rows_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.raw_import_rows_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_import_rows_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.raw_import_rows_id_seq OWNED BY public.raw_import_rows.id;


--
-- Name: raw_imports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_imports (
    id bigint NOT NULL,
    source_path text NOT NULL,
    imported_at timestamp with time zone DEFAULT now() NOT NULL,
    sha256 text NOT NULL
);


--
-- Name: raw_imports_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.raw_imports_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_imports_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.raw_imports_id_seq OWNED BY public.raw_imports.id;


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


--
-- Name: user_features_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_features_daily (
    user_id bigint NOT NULL,
    day date NOT NULL,
    msg_count integer DEFAULT 0 NOT NULL,
    reply_count integer DEFAULT 0 NOT NULL,
    mention_count integer DEFAULT 0 NOT NULL,
    avg_msg_len real DEFAULT 0 NOT NULL,
    groups_active_count integer DEFAULT 0 NOT NULL,
    bd_group_msg_share real DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id bigint NOT NULL,
    platform text DEFAULT 'telegram'::text NOT NULL,
    external_id text NOT NULL,
    handle text,
    display_name text,
    bio text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: claims id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.claims ALTER COLUMN id SET DEFAULT nextval('public.claims_id_seq'::regclass);


--
-- Name: groups id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.groups ALTER COLUMN id SET DEFAULT nextval('public.groups_id_seq'::regclass);


--
-- Name: messages id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.messages ALTER COLUMN id SET DEFAULT nextval('public.messages_id_seq'::regclass);


--
-- Name: raw_import_rows id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_import_rows ALTER COLUMN id SET DEFAULT nextval('public.raw_import_rows_id_seq'::regclass);


--
-- Name: raw_imports id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_imports ALTER COLUMN id SET DEFAULT nextval('public.raw_imports_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: claim_evidence claim_evidence_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.claim_evidence
    ADD CONSTRAINT claim_evidence_pkey PRIMARY KEY (claim_id, evidence_type, evidence_ref);


--
-- Name: claims claims_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.claims
    ADD CONSTRAINT claims_pkey PRIMARY KEY (id);


--
-- Name: groups groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.groups
    ADD CONSTRAINT groups_pkey PRIMARY KEY (id);


--
-- Name: groups groups_platform_external_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.groups
    ADD CONSTRAINT groups_platform_external_id_key UNIQUE (platform, external_id);


--
-- Name: memberships memberships_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.memberships
    ADD CONSTRAINT memberships_pkey PRIMARY KEY (group_id, user_id);


--
-- Name: message_mentions message_mentions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_mentions
    ADD CONSTRAINT message_mentions_pkey PRIMARY KEY (message_id, mentioned_handle);


--
-- Name: messages messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_pkey PRIMARY KEY (id);


--
-- Name: raw_import_rows raw_import_rows_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_import_rows
    ADD CONSTRAINT raw_import_rows_pkey PRIMARY KEY (id);


--
-- Name: raw_imports raw_imports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_imports
    ADD CONSTRAINT raw_imports_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: user_features_daily user_features_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_features_daily
    ADD CONSTRAINT user_features_daily_pkey PRIMARY KEY (user_id, day);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_platform_external_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_platform_external_id_key UNIQUE (platform, external_id);


--
-- Name: idx_claims_predicate; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_claims_predicate ON public.claims USING btree (predicate, object_value);


--
-- Name: idx_claims_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_claims_user ON public.claims USING btree (subject_user_id);


--
-- Name: idx_messages_ext_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_ext_id ON public.messages USING btree (group_id, external_message_id);


--
-- Name: idx_messages_group; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_group ON public.messages USING btree (group_id);


--
-- Name: idx_messages_sent_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_sent_at ON public.messages USING btree (sent_at);


--
-- Name: idx_messages_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_user ON public.messages USING btree (user_id);


--
-- Name: idx_raw_import_rows_ext; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_raw_import_rows_ext ON public.raw_import_rows USING btree (row_type, external_id);


--
-- Name: idx_raw_import_rows_import; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_raw_import_rows_import ON public.raw_import_rows USING btree (raw_import_id);


--
-- Name: idx_raw_imports_sha256; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_raw_imports_sha256 ON public.raw_imports USING btree (sha256);


--
-- Name: idx_users_handle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_users_handle ON public.users USING btree (handle);


--
-- Name: claim_evidence claim_evidence_claim_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.claim_evidence
    ADD CONSTRAINT claim_evidence_claim_id_fkey FOREIGN KEY (claim_id) REFERENCES public.claims(id) ON DELETE CASCADE;


--
-- Name: claims claims_subject_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.claims
    ADD CONSTRAINT claims_subject_user_id_fkey FOREIGN KEY (subject_user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: memberships memberships_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.memberships
    ADD CONSTRAINT memberships_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.groups(id) ON DELETE CASCADE;


--
-- Name: memberships memberships_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.memberships
    ADD CONSTRAINT memberships_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: message_mentions message_mentions_mentioned_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_mentions
    ADD CONSTRAINT message_mentions_mentioned_user_id_fkey FOREIGN KEY (mentioned_user_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: message_mentions message_mentions_message_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_mentions
    ADD CONSTRAINT message_mentions_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.messages(id) ON DELETE CASCADE;


--
-- Name: messages messages_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.groups(id) ON DELETE CASCADE;


--
-- Name: messages messages_raw_ref_row_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_raw_ref_row_id_fkey FOREIGN KEY (raw_ref_row_id) REFERENCES public.raw_import_rows(id);


--
-- Name: messages messages_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: raw_import_rows raw_import_rows_raw_import_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_import_rows
    ADD CONSTRAINT raw_import_rows_raw_import_id_fkey FOREIGN KEY (raw_import_id) REFERENCES public.raw_imports(id) ON DELETE CASCADE;


--
-- Name: user_features_daily user_features_daily_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_features_daily
    ADD CONSTRAINT user_features_daily_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict dbmate


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20260206120000');
