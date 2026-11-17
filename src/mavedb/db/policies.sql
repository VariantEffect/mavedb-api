-- Row-level security in the public schema: the functions and triggers it relies on, then each table's RLS
-- state and policies.
-- Generated from a database at alembic head by `python -m mavedb.scripts.write_policy_snapshot`. Don't edit
-- by hand: change policies in a migration and regenerate this file.

CREATE OR REPLACE FUNCTION public.mavedb_mark_alleles_published_by_links()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    UPDATE alleles a
       SET first_published_at = coalesce(s.published_date, current_date)
      FROM new_links l
      JOIN scoresets s ON s.id = l.score_set_id
     WHERE a.id = l.allele_id
       AND NOT s.private
       AND a.first_published_at IS NULL;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION public.mavedb_mark_alleles_published_by_score_set()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    UPDATE alleles a
       SET first_published_at = coalesce(NEW.published_date, current_date)
      FROM mapping_record_alleles m
     WHERE m.score_set_id = NEW.id
       AND a.id = m.allele_id
       AND a.first_published_at IS NULL;
    RETURN NULL;
END
$function$;

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

CREATE TRIGGER mapping_record_alleles_mark_alleles_published AFTER INSERT ON public.mapping_record_alleles REFERENCING NEW TABLE AS new_links FOR EACH STATEMENT EXECUTE FUNCTION mavedb_mark_alleles_published_by_links();
CREATE TRIGGER scoresets_mark_alleles_published AFTER UPDATE OF private ON public.scoresets FOR EACH ROW WHEN ((old.private AND (NOT new.private))) EXECUTE FUNCTION mavedb_mark_alleles_published_by_score_set();

-- alleles: row-level security not enabled
CREATE POLICY alleles_rule ON alleles AS PERMISSIVE FOR ALL TO public
  USING (((first_published_at IS NOT NULL) OR (EXISTS ( SELECT 1
   FROM mapping_record_alleles m
  WHERE (m.allele_id = alleles.id)))))
  WITH CHECK (((first_published_at IS NOT NULL) OR (EXISTS ( SELECT 1
   FROM mapping_record_alleles m
  WHERE (m.allele_id = alleles.id)))));
CREATE POLICY alleles_worker ON alleles AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

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

-- clinvar_allele_links: row-level security not enabled
CREATE POLICY clinvar_allele_links_rule ON clinvar_allele_links AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM alleles a
  WHERE (a.id = clinvar_allele_links.allele_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM alleles a
  WHERE (a.id = clinvar_allele_links.allele_id))));
CREATE POLICY clinvar_allele_links_worker ON clinvar_allele_links AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- clinvar_controls: row-level security not enabled
CREATE POLICY clinvar_controls_rule ON clinvar_controls AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM clinvar_allele_links l
  WHERE (l.clinvar_control_id = clinvar_controls.id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM clinvar_allele_links l
  WHERE (l.clinvar_control_id = clinvar_controls.id))));
CREATE POLICY clinvar_controls_worker ON clinvar_controls AS PERMISSIVE FOR ALL TO mavedb_worker
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

-- gnomad_allele_links: row-level security not enabled
CREATE POLICY gnomad_allele_links_rule ON gnomad_allele_links AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM alleles a
  WHERE (a.id = gnomad_allele_links.allele_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM alleles a
  WHERE (a.id = gnomad_allele_links.allele_id))));
CREATE POLICY gnomad_allele_links_worker ON gnomad_allele_links AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);

-- gnomad_variants: row-level security not enabled
CREATE POLICY gnomad_variants_rule ON gnomad_variants AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM gnomad_allele_links l
  WHERE (l.gnomad_variant_id = gnomad_variants.id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM gnomad_allele_links l
  WHERE (l.gnomad_variant_id = gnomad_variants.id))));
CREATE POLICY gnomad_variants_worker ON gnomad_variants AS PERMISSIVE FOR ALL TO mavedb_worker
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

-- publication_identifiers: row-level security not enabled
CREATE POLICY publication_identifiers_rule ON publication_identifiers AS PERMISSIVE FOR ALL TO public
  USING (((EXISTS ( SELECT 1
   FROM scoreset_publication_identifiers c
  WHERE (((c.db_name)::text = (publication_identifiers.db_name)::text) AND ((c.identifier)::text = (publication_identifiers.identifier)::text)))) OR (EXISTS ( SELECT 1
   FROM experiment_publication_identifiers c
  WHERE (((c.db_name)::text = (publication_identifiers.db_name)::text) AND ((c.identifier)::text = (publication_identifiers.identifier)::text)))) OR (EXISTS ( SELECT 1
   FROM score_calibration_publication_identifiers c
  WHERE (((c.db_name)::text = (publication_identifiers.db_name)::text) AND ((c.identifier)::text = (publication_identifiers.identifier)::text))))))
  WITH CHECK (true);
CREATE POLICY publication_identifiers_worker ON publication_identifiers AS PERMISSIVE FOR ALL TO mavedb_worker
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

-- variant_translations: row-level security not enabled
CREATE POLICY variant_translations_rule ON variant_translations AS PERMISSIVE FOR ALL TO public
  USING (((EXISTS ( SELECT 1
   FROM alleles a
  WHERE ((a.clingen_allele_id)::text = (variant_translations.aa_clingen_id)::text))) AND (EXISTS ( SELECT 1
   FROM alleles a
  WHERE ((a.clingen_allele_id)::text = (variant_translations.nt_clingen_id)::text)))))
  WITH CHECK (((EXISTS ( SELECT 1
   FROM alleles a
  WHERE ((a.clingen_allele_id)::text = (variant_translations.aa_clingen_id)::text))) AND (EXISTS ( SELECT 1
   FROM alleles a
  WHERE ((a.clingen_allele_id)::text = (variant_translations.nt_clingen_id)::text)))));
CREATE POLICY variant_translations_worker ON variant_translations AS PERMISSIVE FOR ALL TO mavedb_worker
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

-- vep_allele_consequences: row-level security not enabled
CREATE POLICY vep_allele_consequences_rule ON vep_allele_consequences AS PERMISSIVE FOR ALL TO public
  USING ((EXISTS ( SELECT 1
   FROM alleles a
  WHERE (a.id = vep_allele_consequences.allele_id))))
  WITH CHECK ((EXISTS ( SELECT 1
   FROM alleles a
  WHERE (a.id = vep_allele_consequences.allele_id))));
CREATE POLICY vep_allele_consequences_worker ON vep_allele_consequences AS PERMISSIVE FOR ALL TO mavedb_worker
  USING (true)
  WITH CHECK (true);
