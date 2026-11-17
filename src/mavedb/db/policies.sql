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

-- collections: row-level security not enabled
CREATE POLICY collections_rule ON collections AS PERMISSIVE FOR ALL TO public
  USING (((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin}'::text[]) OR (id IN ( SELECT cua.collection_id
   FROM collection_user_associations cua
  WHERE ((cua.user_id = mavedb_user_id()) AND ((cua.contribution_role)::text = ANY ((ARRAY['admin'::character varying, 'editor'::character varying, 'viewer'::character varying])::text[])))))))
  WITH CHECK (((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin}'::text[]) OR (id IN ( SELECT cua.collection_id
   FROM collection_user_associations cua
  WHERE ((cua.user_id = mavedb_user_id()) AND ((cua.contribution_role)::text = ANY ((ARRAY['admin'::character varying, 'editor'::character varying, 'viewer'::character varying])::text[])))))));
CREATE POLICY collections_worker ON collections AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- experiment_sets: row-level security not enabled
CREATE POLICY experiment_sets_rule ON experiment_sets AS PERMISSIVE FOR ALL TO public
  USING (((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin,mapper}'::text[]) OR (id IN ( SELECT esc.experiment_set_id
   FROM ((experiment_set_contributors esc
     JOIN contributors c ON ((c.id = esc.contributor_id)))
     JOIN users u ON (((u.username)::text = (c.orcid_id)::text)))
  WHERE (u.id = mavedb_user_id()))) OR (id IN ( SELECT experiments.experiment_set_id
   FROM experiments))))
  WITH CHECK (((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin,mapper}'::text[]) OR (id IN ( SELECT esc.experiment_set_id
   FROM ((experiment_set_contributors esc
     JOIN contributors c ON ((c.id = esc.contributor_id)))
     JOIN users u ON (((u.username)::text = (c.orcid_id)::text)))
  WHERE (u.id = mavedb_user_id()))) OR (id IN ( SELECT experiments.experiment_set_id
   FROM experiments))));
CREATE POLICY experiment_sets_worker ON experiment_sets AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- experiments: row-level security not enabled
CREATE POLICY experiments_rule ON experiments AS PERMISSIVE FOR ALL TO public
  USING (((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin,mapper}'::text[]) OR (id IN ( SELECT ec.experiment_id
   FROM ((experiment_contributors ec
     JOIN contributors c ON ((c.id = ec.contributor_id)))
     JOIN users u ON (((u.username)::text = (c.orcid_id)::text)))
  WHERE (u.id = mavedb_user_id()))) OR (id IN ( SELECT scoresets.experiment_id
   FROM scoresets))))
  WITH CHECK (((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin,mapper}'::text[]) OR (id IN ( SELECT ec.experiment_id
   FROM ((experiment_contributors ec
     JOIN contributors c ON ((c.id = ec.contributor_id)))
     JOIN users u ON (((u.username)::text = (c.orcid_id)::text)))
  WHERE (u.id = mavedb_user_id()))) OR (id IN ( SELECT scoresets.experiment_id
   FROM scoresets))));
CREATE POLICY experiments_worker ON experiments AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- score_calibrations: row-level security not enabled
CREATE POLICY score_calibrations_rule ON score_calibrations AS PERMISSIVE FOR ALL TO public
  USING (((EXISTS ( SELECT 1
   FROM scoresets s
  WHERE (s.id = score_calibrations.score_set_id))) AND ((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin}'::text[]) OR (investigator_provided AND (EXISTS ( SELECT 1
   FROM scoresets ss
  WHERE ((ss.id = score_calibrations.score_set_id) AND ((ss.created_by_id = mavedb_user_id()) OR (ss.id IN ( SELECT sc.scoreset_id
           FROM ((scoreset_contributors sc
             JOIN contributors c ON ((c.id = sc.contributor_id)))
             JOIN users u ON (((u.username)::text = (c.orcid_id)::text)))
          WHERE (u.id = mavedb_user_id())))))))))))
  WITH CHECK (((EXISTS ( SELECT 1
   FROM scoresets s
  WHERE (s.id = score_calibrations.score_set_id))) AND ((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin}'::text[]) OR (investigator_provided AND (EXISTS ( SELECT 1
   FROM scoresets ss
  WHERE ((ss.id = score_calibrations.score_set_id) AND ((ss.created_by_id = mavedb_user_id()) OR (ss.id IN ( SELECT sc.scoreset_id
           FROM ((scoreset_contributors sc
             JOIN contributors c ON ((c.id = sc.contributor_id)))
             JOIN users u ON (((u.username)::text = (c.orcid_id)::text)))
          WHERE (u.id = mavedb_user_id())))))))))));
CREATE POLICY score_calibrations_worker ON score_calibrations AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- scoresets: row-level security not enabled
CREATE POLICY scoresets_rule ON scoresets AS PERMISSIVE FOR ALL TO public
  USING (((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin,mapper}'::text[]) OR (id IN ( SELECT sc.scoreset_id
   FROM ((scoreset_contributors sc
     JOIN contributors c ON ((c.id = sc.contributor_id)))
     JOIN users u ON (((u.username)::text = (c.orcid_id)::text)))
  WHERE (u.id = mavedb_user_id())))))
  WITH CHECK (((private = false) OR (created_by_id = mavedb_user_id()) OR (mavedb_roles() && '{admin,mapper}'::text[]) OR (id IN ( SELECT sc.scoreset_id
   FROM ((scoreset_contributors sc
     JOIN contributors c ON ((c.id = sc.contributor_id)))
     JOIN users u ON (((u.username)::text = (c.orcid_id)::text)))
  WHERE (u.id = mavedb_user_id())))));
CREATE POLICY scoresets_worker ON scoresets AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);
