--
-- PostgreSQL database cluster dump
--

\restrict LI6iEJQIM4bcEJshhDeiH1gd1vIAqJKTXV7HnuRVJBZTENj43vIilM3EfDMHjtq

SET default_transaction_read_only = off;

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Roles
--

CREATE ROLE tgprofile;
ALTER ROLE tgprofile WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS PASSWORD 'SCRAM-SHA-256$4096:GZgQL9NOE6Q1JjFQN9SA3A==$Li9yLzJQ4zACDw1BTbF4AS3Oh9BPyM3dAIfqH2HZj9E=:MT+WXj9EE4hH4uR3wEoTbIqJai32iE+rODApfM7XNFM=';

--
-- User Configurations
--








\unrestrict LI6iEJQIM4bcEJshhDeiH1gd1vIAqJKTXV7HnuRVJBZTENj43vIilM3EfDMHjtq

--
-- PostgreSQL database cluster dump complete
--

