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

-- calibration_controls: row-level security not enabled
CREATE POLICY calibration_controls_rule ON calibration_controls AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM score_calibrations p
  WHERE (p.id = calibration_controls.calibration_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM score_calibrations p
  WHERE (p.id = calibration_controls.calibration_id))));
CREATE POLICY calibration_controls_worker ON calibration_controls AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- collection_experiments: row-level security not enabled
CREATE POLICY collection_experiments_rule ON collection_experiments AS PERMISSIVE FOR ALL TO public
  USING (((EXISTS ( SELECT 1
   FROM collections p
  WHERE (p.id = collection_experiments.collection_id))) AND (EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = collection_experiments.experiment_id)))))
  WITH CHECK (((EXISTS ( SELECT 1
   FROM collections p
  WHERE (p.id = collection_experiments.collection_id))) AND (EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = collection_experiments.experiment_id)))));
CREATE POLICY collection_experiments_worker ON collection_experiments AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- collection_score_sets: row-level security not enabled
CREATE POLICY collection_score_sets_rule ON collection_score_sets AS PERMISSIVE FOR ALL TO public
  USING (((EXISTS ( SELECT 1
   FROM collections p
  WHERE (p.id = collection_score_sets.collection_id))) AND (EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = collection_score_sets.score_set_id)))))
  WITH CHECK (((EXISTS ( SELECT 1
   FROM collections p
  WHERE (p.id = collection_score_sets.collection_id))) AND (EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = collection_score_sets.score_set_id)))));
CREATE POLICY collection_score_sets_worker ON collection_score_sets AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

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

-- experiment_controlled_keywords: row-level security not enabled
CREATE POLICY experiment_controlled_keywords_rule ON experiment_controlled_keywords AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = experiment_controlled_keywords.experiment_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = experiment_controlled_keywords.experiment_id))));
CREATE POLICY experiment_controlled_keywords_worker ON experiment_controlled_keywords AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- experiment_doi_identifiers: row-level security not enabled
CREATE POLICY experiment_doi_identifiers_rule ON experiment_doi_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = experiment_doi_identifiers.experiment_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = experiment_doi_identifiers.experiment_id))));
CREATE POLICY experiment_doi_identifiers_worker ON experiment_doi_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- experiment_publication_identifiers: row-level security not enabled
CREATE POLICY experiment_publication_identifiers_rule ON experiment_publication_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = experiment_publication_identifiers.experiment_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = experiment_publication_identifiers.experiment_id))));
CREATE POLICY experiment_publication_identifiers_worker ON experiment_publication_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- experiment_set_doi_identifiers: row-level security not enabled
CREATE POLICY experiment_set_doi_identifiers_rule ON experiment_set_doi_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM experiment_sets p
  WHERE (p.id = experiment_set_doi_identifiers.experiment_set_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM experiment_sets p
  WHERE (p.id = experiment_set_doi_identifiers.experiment_set_id))));
CREATE POLICY experiment_set_doi_identifiers_worker ON experiment_set_doi_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- experiment_set_sra_identifiers: row-level security not enabled
CREATE POLICY experiment_set_sra_identifiers_rule ON experiment_set_sra_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM experiment_sets p
  WHERE (p.id = experiment_set_sra_identifiers.experiment_set_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM experiment_sets p
  WHERE (p.id = experiment_set_sra_identifiers.experiment_set_id))));
CREATE POLICY experiment_set_sra_identifiers_worker ON experiment_set_sra_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
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

-- experiment_sra_identifiers: row-level security not enabled
CREATE POLICY experiment_sra_identifiers_rule ON experiment_sra_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = experiment_sra_identifiers.experiment_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM experiments p
  WHERE (p.id = experiment_sra_identifiers.experiment_id))));
CREATE POLICY experiment_sra_identifiers_worker ON experiment_sra_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
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

-- job_dependencies: row-level security not enabled
CREATE POLICY job_dependencies_rule ON job_dependencies AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM job_runs p
  WHERE (p.id = job_dependencies.id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM job_runs p
  WHERE (p.id = job_dependencies.id))));
CREATE POLICY job_dependencies_worker ON job_dependencies AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- job_runs: row-level security not enabled
CREATE POLICY job_runs_rule ON job_runs AS PERMISSIVE FOR ALL TO public
  USING (((EXISTS ( SELECT 1
   FROM pipelines p
  WHERE (p.id = job_runs.pipeline_id))) OR (mavedb_roles() && '{admin}'::text[])))
  WITH CHECK (((EXISTS ( SELECT 1
   FROM pipelines p
  WHERE (p.id = job_runs.pipeline_id))) OR (mavedb_roles() && '{admin}'::text[])));
CREATE POLICY job_runs_worker ON job_runs AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- mapping_record_alleles: row-level security not enabled
CREATE POLICY mapping_record_alleles_rule ON mapping_record_alleles AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = mapping_record_alleles.score_set_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = mapping_record_alleles.score_set_id))));
CREATE POLICY mapping_record_alleles_worker ON mapping_record_alleles AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- mapping_records: row-level security not enabled
CREATE POLICY mapping_records_rule ON mapping_records AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = mapping_records.score_set_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = mapping_records.score_set_id))));
CREATE POLICY mapping_records_worker ON mapping_records AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- pipelines: row-level security not enabled
CREATE POLICY pipelines_rule ON pipelines AS PERMISSIVE FOR ALL TO public
  USING (((created_by_user_id = mavedb_user_id()) OR (mavedb_roles() && '{admin}'::text[])))
  WITH CHECK (((created_by_user_id = mavedb_user_id()) OR (mavedb_roles() && '{admin}'::text[])));
CREATE POLICY pipelines_worker ON pipelines AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- projection_groups: row-level security not enabled
CREATE POLICY projection_groups_rule ON projection_groups AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = projection_groups.score_set_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = projection_groups.score_set_id))));
CREATE POLICY projection_groups_worker ON projection_groups AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- score_calibration_functional_classification_variants: row-level security not enabled
CREATE POLICY score_calibration_functional_classification_variants_rule ON score_calibration_functional_classification_variants AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM score_calibration_functional_classifications p
  WHERE (p.id = score_calibration_functional_classification_variants.functional_classification_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM score_calibration_functional_classifications p
  WHERE (p.id = score_calibration_functional_classification_variants.functional_classification_id))));
CREATE POLICY score_calibration_functional_classification_variants_worker ON score_calibration_functional_classification_variants AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- score_calibration_functional_classifications: row-level security not enabled
CREATE POLICY score_calibration_functional_classifications_rule ON score_calibration_functional_classifications AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM score_calibrations p
  WHERE (p.id = score_calibration_functional_classifications.calibration_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM score_calibrations p
  WHERE (p.id = score_calibration_functional_classifications.calibration_id))));
CREATE POLICY score_calibration_functional_classifications_worker ON score_calibration_functional_classifications AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- score_calibration_publication_identifiers: row-level security not enabled
CREATE POLICY score_calibration_publication_identifiers_rule ON score_calibration_publication_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM score_calibrations p
  WHERE (p.id = score_calibration_publication_identifiers.score_calibration_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM score_calibrations p
  WHERE (p.id = score_calibration_publication_identifiers.score_calibration_id))));
CREATE POLICY score_calibration_publication_identifiers_worker ON score_calibration_publication_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
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

-- scoreset_doi_identifiers: row-level security not enabled
CREATE POLICY scoreset_doi_identifiers_rule ON scoreset_doi_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_doi_identifiers.scoreset_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_doi_identifiers.scoreset_id))));
CREATE POLICY scoreset_doi_identifiers_worker ON scoreset_doi_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- scoreset_meta_analysis_sources: row-level security not enabled
CREATE POLICY scoreset_meta_analysis_sources_rule ON scoreset_meta_analysis_sources AS PERMISSIVE FOR ALL TO public
  USING (((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_meta_analysis_sources.source_scoreset_id))) AND (EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_meta_analysis_sources.meta_analysis_scoreset_id)))))
  WITH CHECK (((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_meta_analysis_sources.source_scoreset_id))) AND (EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_meta_analysis_sources.meta_analysis_scoreset_id)))));
CREATE POLICY scoreset_meta_analysis_sources_worker ON scoreset_meta_analysis_sources AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- scoreset_publication_identifiers: row-level security not enabled
CREATE POLICY scoreset_publication_identifiers_rule ON scoreset_publication_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_publication_identifiers.scoreset_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_publication_identifiers.scoreset_id))));
CREATE POLICY scoreset_publication_identifiers_worker ON scoreset_publication_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- scoreset_sra_identifiers: row-level security not enabled
CREATE POLICY scoreset_sra_identifiers_rule ON scoreset_sra_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_sra_identifiers.scoreset_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = scoreset_sra_identifiers.scoreset_id))));
CREATE POLICY scoreset_sra_identifiers_worker ON scoreset_sra_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
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

-- target_accessions: row-level security not enabled
CREATE POLICY target_accessions_rule ON target_accessions AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = target_accessions.score_set_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = target_accessions.score_set_id))));
CREATE POLICY target_accessions_worker ON target_accessions AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- target_gene_external_identifiers: row-level security not enabled
CREATE POLICY target_gene_external_identifiers_rule ON target_gene_external_identifiers AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM target_genes p
  WHERE (p.id = target_gene_external_identifiers.target_gene_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM target_genes p
  WHERE (p.id = target_gene_external_identifiers.target_gene_id))));
CREATE POLICY target_gene_external_identifiers_worker ON target_gene_external_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- target_gene_mappings: row-level security not enabled
CREATE POLICY target_gene_mappings_rule ON target_gene_mappings AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM target_genes p
  WHERE (p.id = target_gene_mappings.target_gene_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM target_genes p
  WHERE (p.id = target_gene_mappings.target_gene_id))));
CREATE POLICY target_gene_mappings_worker ON target_gene_mappings AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- target_genes: row-level security not enabled
CREATE POLICY target_genes_rule ON target_genes AS PERMISSIVE FOR ALL TO public
  USING ((scoreset_id IN ( SELECT scoresets.id
   FROM scoresets)))
  WITH CHECK ((scoreset_id IN ( SELECT scoresets.id
   FROM scoresets)));
CREATE POLICY target_genes_worker ON target_genes AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- target_sequences: row-level security not enabled
CREATE POLICY target_sequences_rule ON target_sequences AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = target_sequences.score_set_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE (p.id = target_sequences.score_set_id))));
CREATE POLICY target_sequences_worker ON target_sequences AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- urn_redirects: row-level security not enabled
CREATE POLICY urn_redirects_rule ON urn_redirects AS PERMISSIVE FOR ALL TO public
  USING (((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE ((p.urn)::text = (urn_redirects.new_urn)::text))) OR (EXISTS ( SELECT 1
   FROM experiments p
  WHERE ((p.urn)::text = (urn_redirects.new_urn)::text))) OR (EXISTS ( SELECT 1
   FROM experiment_sets p
  WHERE ((p.urn)::text = (urn_redirects.new_urn)::text)))))
  WITH CHECK (((EXISTS ( SELECT 1
   FROM scoresets p
  WHERE ((p.urn)::text = (urn_redirects.new_urn)::text))) OR (EXISTS ( SELECT 1
   FROM experiments p
  WHERE ((p.urn)::text = (urn_redirects.new_urn)::text))) OR (EXISTS ( SELECT 1
   FROM experiment_sets p
  WHERE ((p.urn)::text = (urn_redirects.new_urn)::text)))));
CREATE POLICY urn_redirects_worker ON urn_redirects AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- variants: row-level security not enabled
CREATE POLICY variants_rule ON variants AS PERMISSIVE FOR ALL TO public
  USING ((scoreset_id IN ( SELECT scoresets.id
   FROM scoresets)))
  WITH CHECK ((scoreset_id IN ( SELECT scoresets.id
   FROM scoresets)));
CREATE POLICY variants_worker ON variants AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);
