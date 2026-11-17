-- Row-level security in the public schema: the functions it relies on, then each table's RLS state and
-- policies.
-- Generated from a database at alembic head by `python -m mavedb.scripts.write_policy_snapshot`. Don't edit
-- by hand: change policies in a migration and regenerate this file.

CREATE OR REPLACE FUNCTION public.mavedb_roles()
 RETURNS text[]
 LANGUAGE sql
 STABLE PARALLEL SAFE
AS $function$ SELECT coalesce(nullif(current_setting('app.active_roles', true), '')::text[], '{}') $function$;

CREATE OR REPLACE FUNCTION public.mavedb_user_id()
 RETURNS integer
 LANGUAGE sql
 STABLE PARALLEL SAFE
AS $function$ SELECT nullif(current_setting('app.user_id', true), '')::integer $function$;
