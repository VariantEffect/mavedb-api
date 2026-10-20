--
-- PostgreSQL database dump
--


-- Dumped from database version 15.17
-- Dumped by pg_dump version 15.17 (Homebrew)


--
-- Name: pg_stat_statements; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public;


--
-- Name: EXTENSION pg_stat_statements; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_stat_statements IS 'track planning and execution statistics of all SQL statements executed';


--
-- Name: reset_sequence(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reset_sequence(tablename text, columnname text, sequence_name text) RETURNS void
    LANGUAGE plpgsql
    AS $$  
      DECLARE 
      BEGIN 
    
      EXECUTE 'SELECT setval( ''' || sequence_name  || ''', ' || '(SELECT MAX(' || columnname || 
          ') FROM ' || tablename || ')' || '+1)';
    
      END;  
    
    $$;




--
-- Name: access_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.access_keys (
    id integer NOT NULL,
    user_id integer NOT NULL,
    key_id character varying NOT NULL,
    public_key character varying NOT NULL,
    name character varying,
    expiration_date date,
    creation_time timestamp without time zone,
    role_id integer
);


--
-- Name: access_keys_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.access_keys_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: access_keys_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.access_keys_id_seq OWNED BY public.access_keys.id;


--
-- Name: acmg_classifications; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.acmg_classifications (
    id integer NOT NULL,
    criterion character varying(32),
    evidence_strength character varying(32),
    points integer,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: acmg_classifications_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.acmg_classifications_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: acmg_classifications_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.acmg_classifications_id_seq OWNED BY public.acmg_classifications.id;


--
-- Name: alleles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alleles (
    id integer NOT NULL,
    vrs_digest character varying NOT NULL,
    level character varying(16) NOT NULL,
    hgvs_g character varying,
    hgvs_c character varying,
    hgvs_p character varying,
    clingen_allele_id character varying,
    post_mapped jsonb,
    created_at date DEFAULT CURRENT_DATE NOT NULL,
    updated_at date DEFAULT CURRENT_DATE NOT NULL
);


--
-- Name: alleles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.alleles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: alleles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.alleles_id_seq OWNED BY public.alleles.id;


--
-- Name: annotation_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.annotation_event (
    id integer NOT NULL,
    annotation_type character varying(50) NOT NULL,
    variant_id integer,
    allele_id integer,
    disposition character varying(50) NOT NULL,
    reason character varying(50) NOT NULL,
    source_version character varying(50),
    metadata jsonb,
    job_run_id integer,
    score_set_id integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_annotation_event_subject CHECK (((((annotation_type)::text = ANY ((ARRAY['vrs_mapping'::character varying, 'cross_level_translation'::character varying, 'variant_translation'::character varying, 'ldh_submission'::character varying])::text[])) AND (variant_id IS NOT NULL) AND (allele_id IS NULL)) OR (((annotation_type)::text = ANY ((ARRAY['clingen_allele_id'::character varying, 'gnomad_allele_frequency'::character varying, 'vep_functional_consequence'::character varying, 'clinvar_control'::character varying, 'mapped_hgvs'::character varying])::text[])) AND (allele_id IS NOT NULL) AND (variant_id IS NULL))))
);


--
-- Name: annotation_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.annotation_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: annotation_event_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.annotation_event_id_seq OWNED BY public.annotation_event.id;


--
-- Name: calibration_controls; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.calibration_controls (
    id integer NOT NULL,
    calibration_id integer NOT NULL,
    variant_id integer NOT NULL,
    clinical_status character varying(32) NOT NULL,
    created_by_id integer NOT NULL,
    modified_by_id integer NOT NULL,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: calibration_controls_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.calibration_controls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: calibration_controls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.calibration_controls_id_seq OWNED BY public.calibration_controls.id;


--
-- Name: clinvar_controls; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.clinvar_controls (
    id integer NOT NULL,
    db_identifier character varying NOT NULL,
    gene_symbol character varying NOT NULL,
    clinical_significance character varying NOT NULL,
    clinical_review_status character varying NOT NULL,
    db_version character varying NOT NULL,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    db_name character varying NOT NULL,
    clinvar_variation_id character varying
);


--
-- Name: clinical_controls_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.clinical_controls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: clinical_controls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.clinical_controls_id_seq OWNED BY public.clinvar_controls.id;


--
-- Name: clinvar_allele_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.clinvar_allele_links (
    id integer NOT NULL,
    allele_id integer NOT NULL,
    clinvar_control_id integer NOT NULL,
    valid_from timestamp with time zone DEFAULT now() NOT NULL,
    valid_to timestamp with time zone
);


--
-- Name: clinvar_allele_links_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.clinvar_allele_links_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: clinvar_allele_links_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.clinvar_allele_links_id_seq OWNED BY public.clinvar_allele_links.id;


--
-- Name: collection_experiments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.collection_experiments (
    collection_id integer NOT NULL,
    experiment_id integer NOT NULL,
    "position" integer NOT NULL
);


--
-- Name: collection_score_sets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.collection_score_sets (
    collection_id integer NOT NULL,
    score_set_id integer NOT NULL,
    "position" integer NOT NULL
);


--
-- Name: collection_user_associations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.collection_user_associations (
    collection_id integer NOT NULL,
    user_id integer NOT NULL,
    contribution_role character varying(32) NOT NULL,
    CONSTRAINT contributionrole CHECK (((contribution_role)::text = ANY ((ARRAY['admin'::character varying, 'editor'::character varying, 'viewer'::character varying])::text[])))
);


--
-- Name: collections; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.collections (
    id integer NOT NULL,
    urn character varying(64),
    private boolean NOT NULL,
    name character varying NOT NULL,
    badge_name character varying,
    description character varying,
    created_by_id integer,
    modified_by_id integer,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: collections_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.collections_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: collections_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.collections_id_seq OWNED BY public.collections.id;


--
-- Name: contributors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contributors (
    id integer NOT NULL,
    orcid_id character varying NOT NULL,
    given_name character varying,
    family_name character varying
);


--
-- Name: contributors_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.contributors_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: contributors_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.contributors_id_seq OWNED BY public.contributors.id;


--
-- Name: controlled_keywords; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.controlled_keywords (
    id integer NOT NULL,
    key character varying NOT NULL,
    label character varying NOT NULL,
    system character varying,
    special boolean,
    description character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    code character varying,
    version character varying
);


--
-- Name: controlled_keywords_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.controlled_keywords_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: controlled_keywords_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.controlled_keywords_id_seq OWNED BY public.controlled_keywords.id;


--
-- Name: doi_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.doi_identifiers (
    id integer NOT NULL,
    identifier character varying NOT NULL,
    db_name character varying NOT NULL,
    db_version character varying,
    url character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: doi_identifiers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.doi_identifiers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: doi_identifiers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.doi_identifiers_id_seq OWNED BY public.doi_identifiers.id;


--
-- Name: ensembl_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ensembl_identifiers (
    id integer NOT NULL,
    identifier character varying NOT NULL,
    db_name character varying NOT NULL,
    db_version character varying,
    url character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: ensembl_identifiers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ensembl_identifiers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ensembl_identifiers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ensembl_identifiers_id_seq OWNED BY public.ensembl_identifiers.id;


--
-- Name: ensembl_offsets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ensembl_offsets (
    identifier_id integer NOT NULL,
    target_gene_id integer NOT NULL,
    "offset" integer NOT NULL
);


--
-- Name: experiment_contributors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_contributors (
    experiment_id integer NOT NULL,
    contributor_id integer NOT NULL
);


--
-- Name: experiment_controlled_keywords; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_controlled_keywords (
    experiment_id integer NOT NULL,
    controlled_keyword_id integer NOT NULL,
    description character varying
);


--
-- Name: experiment_doi_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_doi_identifiers (
    experiment_id integer NOT NULL,
    doi_identifier_id integer NOT NULL
);


--
-- Name: experiment_keywords; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_keywords (
    experiment_id integer NOT NULL,
    keyword_id integer NOT NULL
);


--
-- Name: experiment_publication_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_publication_identifiers (
    experiment_id integer NOT NULL,
    publication_identifier_id integer NOT NULL,
    "primary" boolean
);


--
-- Name: experiment_set_contributors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_set_contributors (
    experiment_set_id integer NOT NULL,
    contributor_id integer NOT NULL
);


--
-- Name: experiment_set_doi_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_set_doi_identifiers (
    experiment_set_id integer NOT NULL,
    doi_identifier_id integer NOT NULL
);


--
-- Name: experiment_set_keywords; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_set_keywords (
    experiment_set_id integer NOT NULL,
    keyword_id integer NOT NULL
);


--
-- Name: experiment_set_publication_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_set_publication_identifiers (
    experiment_set_id integer NOT NULL,
    publication_identifier_id integer NOT NULL
);


--
-- Name: experiment_set_sra_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_set_sra_identifiers (
    experiment_set_id integer NOT NULL,
    sra_identifier_id integer NOT NULL
);


--
-- Name: experiment_sets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_sets (
    id integer NOT NULL,
    urn character varying(64),
    extra_metadata jsonb NOT NULL,
    private boolean NOT NULL,
    approved boolean NOT NULL,
    published_date date,
    processing_state character varying,
    num_experiments integer NOT NULL,
    created_by_id integer,
    modified_by_id integer,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: experiment_sets_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.experiment_sets_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: experiment_sets_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.experiment_sets_id_seq OWNED BY public.experiment_sets.id;


--
-- Name: experiment_sra_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_sra_identifiers (
    experiment_id integer NOT NULL,
    sra_identifier_id integer NOT NULL
);


--
-- Name: experiments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiments (
    id integer NOT NULL,
    urn character varying(64),
    title character varying NOT NULL,
    short_description character varying NOT NULL,
    abstract_text character varying NOT NULL,
    method_text character varying NOT NULL,
    extra_metadata jsonb NOT NULL,
    private boolean NOT NULL,
    approved boolean NOT NULL,
    published_date date,
    processing_state character varying,
    num_scoresets integer NOT NULL,
    experiment_set_id integer,
    created_by_id integer,
    modified_by_id integer,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    external_links jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: experiments_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.experiments_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: experiments_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.experiments_id_seq OWNED BY public.experiments.id;


--
-- Name: genome_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.genome_identifiers (
    id integer NOT NULL,
    identifier character varying NOT NULL,
    db_name character varying NOT NULL,
    db_version character varying,
    url character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: genome_identifiers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.genome_identifiers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: genome_identifiers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.genome_identifiers_id_seq OWNED BY public.genome_identifiers.id;


--
-- Name: gnomad_allele_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gnomad_allele_links (
    id integer NOT NULL,
    allele_id integer NOT NULL,
    gnomad_variant_id integer NOT NULL,
    valid_from timestamp with time zone DEFAULT now() NOT NULL,
    valid_to timestamp with time zone
);


--
-- Name: gnomad_allele_links_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.gnomad_allele_links_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gnomad_allele_links_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.gnomad_allele_links_id_seq OWNED BY public.gnomad_allele_links.id;


--
-- Name: gnomad_variants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gnomad_variants (
    id integer NOT NULL,
    db_name character varying NOT NULL,
    db_identifier character varying NOT NULL,
    db_version character varying NOT NULL,
    allele_count integer NOT NULL,
    allele_number integer NOT NULL,
    allele_frequency double precision NOT NULL,
    faf95_max double precision,
    faf95_max_ancestry character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: gnomad_variants_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.gnomad_variants_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gnomad_variants_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.gnomad_variants_id_seq OWNED BY public.gnomad_variants.id;


--
-- Name: gnomad_variants_mapped_variants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gnomad_variants_mapped_variants (
    mapped_variant_id integer NOT NULL,
    gnomad_variant_id integer NOT NULL
);


--
-- Name: job_dependencies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job_dependencies (
    id integer NOT NULL,
    depends_on_job_id integer NOT NULL,
    dependency_type character varying(50) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    metadata jsonb,
    CONSTRAINT ck_job_dependencies_type_valid CHECK (((dependency_type IS NULL) OR ((dependency_type)::text = ANY ((ARRAY['success_required'::character varying, 'completion_required'::character varying])::text[]))))
);


--
-- Name: job_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job_runs (
    id integer NOT NULL,
    urn character varying(255),
    job_type character varying(100) NOT NULL,
    job_function character varying(255) NOT NULL,
    job_params jsonb,
    status character varying(50) NOT NULL,
    pipeline_id integer,
    max_retries integer NOT NULL,
    retry_count integer NOT NULL,
    retry_delay_seconds integer,
    scheduled_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    error_message text,
    error_traceback text,
    failure_category character varying(100),
    progress_current integer,
    progress_total integer,
    progress_message character varying(500),
    correlation_id character varying(255),
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    mavedb_version character varying(50),
    progress_updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_job_runs_max_retries_positive CHECK ((max_retries >= 0)),
    CONSTRAINT ck_job_runs_retry_count_positive CHECK ((retry_count >= 0)),
    CONSTRAINT ck_job_runs_status_valid CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'queued'::character varying, 'running'::character varying, 'succeeded'::character varying, 'failed'::character varying, 'errored'::character varying, 'cancelled'::character varying, 'skipped'::character varying])::text[])))
);


--
-- Name: job_runs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.job_runs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: job_runs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.job_runs_id_seq OWNED BY public.job_runs.id;


--
-- Name: keywords; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.keywords (
    id integer NOT NULL,
    text character varying NOT NULL,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: keywords_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.keywords_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: keywords_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.keywords_id_seq OWNED BY public.keywords.id;


--
-- Name: licenses; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.licenses (
    id integer NOT NULL,
    long_name character varying NOT NULL,
    short_name character varying NOT NULL,
    text character varying NOT NULL,
    link character varying,
    version character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    active boolean DEFAULT true NOT NULL
);


--
-- Name: licenses_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.licenses_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: licenses_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.licenses_id_seq OWNED BY public.licenses.id;


--
-- Name: mapped_variants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mapped_variants (
    id integer NOT NULL,
    pre_mapped jsonb,
    post_mapped jsonb,
    variant_id integer NOT NULL,
    vrs_version character varying DEFAULT '1.3'::character varying,
    error_message character varying,
    modification_date date DEFAULT CURRENT_DATE NOT NULL,
    mapped_date date DEFAULT '2024-01-01'::date NOT NULL,
    mapping_api_version character varying DEFAULT '0.0.0'::character varying NOT NULL,
    current boolean DEFAULT false NOT NULL,
    clingen_allele_id character varying,
    vep_functional_consequence character varying,
    vep_access_date date,
    hgvs_assay_level character varying,
    hgvs_g character varying,
    hgvs_c character varying,
    hgvs_p character varying,
    target_gene_mapping_id integer,
    alignment_level character varying(16),
    at_mismatched_locus boolean,
    near_gap boolean,
    CONSTRAINT ck_mapped_variants_alignment_level_valid CHECK (((alignment_level IS NULL) OR ((alignment_level)::text = ANY ((ARRAY['protein'::character varying, 'cdna'::character varying, 'genomic'::character varying])::text[]))))
);


--
-- Name: mapped_variants_clinical_controls; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mapped_variants_clinical_controls (
    mapped_variant_id integer NOT NULL,
    clinical_control_id integer NOT NULL
);


--
-- Name: mapped_variants_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.mapped_variants_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mapped_variants_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.mapped_variants_id_seq OWNED BY public.mapped_variants.id;


--
-- Name: mapping_record_alleles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mapping_record_alleles (
    id integer NOT NULL,
    mapping_record_id integer NOT NULL,
    allele_id integer NOT NULL,
    is_authoritative boolean DEFAULT false NOT NULL,
    valid_from timestamp with time zone DEFAULT now() NOT NULL,
    valid_to timestamp with time zone,
    projection_group integer,
    score_set_id integer NOT NULL
);


--
-- Name: mapping_record_alleles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.mapping_record_alleles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mapping_record_alleles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.mapping_record_alleles_id_seq OWNED BY public.mapping_record_alleles.id;


--
-- Name: mapping_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mapping_records (
    id integer NOT NULL,
    variant_id integer NOT NULL,
    vrs_digest character varying,
    pre_mapped jsonb,
    assay_level character varying(16) NOT NULL,
    hgvs_assay_level character varying,
    mapping_api_version character varying NOT NULL,
    mapped_date date NOT NULL,
    vrs_version character varying,
    alignment_level character varying(16),
    at_mismatched_locus boolean,
    near_gap boolean,
    target_gene_mapping_id integer,
    valid_from timestamp with time zone DEFAULT now() NOT NULL,
    valid_to timestamp with time zone,
    score_set_id integer NOT NULL,
    CONSTRAINT ck_mapping_records_assay_level_valid CHECK (((assay_level)::text = ANY ((ARRAY['genomic'::character varying, 'cdna'::character varying, 'protein'::character varying])::text[])))
);


--
-- Name: mapping_records_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.mapping_records_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mapping_records_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.mapping_records_id_seq OWNED BY public.mapping_records.id;


--
-- Name: mavedb_ldh_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mavedb_ldh_records (
    variant_urn text,
    ldh_id bigint,
    ldh_creation_date timestamp without time zone,
    ldh_modification_date timestamp without time zone,
    clingen_allele_id text,
    post_mapped_hgvs_1 text,
    post_mapped_hgvs_2 text,
    post_mapped_hgvs_3 text,
    post_mapped_hgvs_4 text,
    post_mapped_hgvs_5 text,
    post_mapped_hgvs_6 text,
    post_mapped_hgvs_7 text,
    post_mapped_hgvs_8 text
);


--
-- Name: mavedb_ldh_records_audit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mavedb_ldh_records_audit (
    variant_urn text,
    ldh_id bigint,
    ldh_creation_date timestamp without time zone,
    ldh_modification_date timestamp without time zone,
    clingen_allele_id text,
    post_mapped_hgvs_1 text,
    post_mapped_hgvs_2 text,
    post_mapped_hgvs_3 text,
    post_mapped_hgvs_4 text,
    post_mapped_hgvs_5 text,
    post_mapped_hgvs_6 text,
    post_mapped_hgvs_7 text,
    post_mapped_hgvs_8 text,
    num_alleles_from_post_mapped_vrs integer,
    num_alleles_from_hgvs_nt integer,
    num_alleles_from_hgvs_pro integer,
    num_alleles_from_hgvs integer,
    num_clingen_allele_ids integer,
    hgvs_nt text,
    hgvs_pro text
);


--
-- Name: mavedb_staging_mapped_clinvar_allele_ids_1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mavedb_staging_mapped_clinvar_allele_ids_1 (
    urn text,
    clingen_allele_id text
);


--
-- Name: mavedb_staging_mapped_variants_clinical_controls; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mavedb_staging_mapped_variants_clinical_controls (
    variant_urn text,
    clinical_control_id integer
);


--
-- Name: mondo_terms; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mondo_terms (
    id integer NOT NULL,
    code character varying NOT NULL,
    system character varying NOT NULL,
    system_version character varying,
    label character varying NOT NULL,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: mondo_terms_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.mondo_terms_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mondo_terms_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.mondo_terms_id_seq OWNED BY public.mondo_terms.id;


--
-- Name: pipelines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pipelines (
    id integer NOT NULL,
    urn character varying(255),
    name character varying(500) NOT NULL,
    description text,
    status character varying(50) NOT NULL,
    correlation_id character varying(255),
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_by_user_id integer,
    mavedb_version character varying(50),
    CONSTRAINT ck_pipelines_status_valid CHECK (((status)::text = ANY ((ARRAY['created'::character varying, 'running'::character varying, 'succeeded'::character varying, 'failed'::character varying, 'cancelled'::character varying, 'paused'::character varying, 'partial'::character varying])::text[])))
);


--
-- Name: COLUMN pipelines.metadata; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.pipelines.metadata IS 'Flexible metadata storage for pipeline-specific data';


--
-- Name: pipelines_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pipelines_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pipelines_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pipelines_id_seq OWNED BY public.pipelines.id;


--
-- Name: projection_groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.projection_groups (
    id integer NOT NULL,
    mapping_record_id integer NOT NULL,
    score_set_id integer NOT NULL,
    hgvs_c character varying,
    hgvs_g character varying,
    hgvs_p character varying,
    valid_from timestamp with time zone DEFAULT now() NOT NULL,
    valid_to timestamp with time zone
);


--
-- Name: projection_groups_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.projection_groups_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: projection_groups_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.projection_groups_id_seq OWNED BY public.projection_groups.id;


--
-- Name: publication_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.publication_identifiers (
    id integer NOT NULL,
    identifier character varying NOT NULL,
    db_name character varying NOT NULL,
    db_version character varying,
    url character varying,
    reference_html character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    title character varying NOT NULL,
    abstract character varying,
    authors jsonb NOT NULL,
    publication_year integer,
    publication_journal character varying,
    doi character varying
);


--
-- Name: publication_identifiers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.publication_identifiers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: publication_identifiers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.publication_identifiers_id_seq OWNED BY public.publication_identifiers.id;


--
-- Name: scoresets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scoresets (
    id integer NOT NULL,
    urn character varying(64),
    title character varying NOT NULL,
    method_text character varying NOT NULL,
    abstract_text character varying NOT NULL,
    short_description character varying NOT NULL,
    extra_metadata jsonb NOT NULL,
    dataset_columns jsonb NOT NULL,
    normalised boolean NOT NULL,
    private boolean NOT NULL,
    approved boolean NOT NULL,
    published_date date,
    processing_state character varying(32),
    data_usage_policy character varying,
    num_variants integer NOT NULL,
    experiment_id integer NOT NULL,
    licence_id integer NOT NULL,
    replaces_id integer,
    created_by_id integer,
    modified_by_id integer,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    processing_errors jsonb,
    external_links jsonb DEFAULT '{}'::jsonb NOT NULL,
    mapping_state character varying(32),
    mapping_errors jsonb,
    score_distribution jsonb,
    CONSTRAINT mappingstate CHECK (((mapping_state)::text = ANY ((ARRAY['incomplete'::character varying, 'processing'::character varying, 'failed'::character varying, 'complete'::character varying, 'pending_variant_processing'::character varying, 'not_attempted'::character varying, 'queued'::character varying])::text[]))),
    CONSTRAINT processingstate CHECK (((processing_state)::text = ANY (ARRAY[('incomplete'::character varying)::text, ('processing'::character varying)::text, ('failed'::character varying)::text, ('success'::character varying)::text])))
);


--
-- Name: variants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.variants (
    id integer NOT NULL,
    urn character varying(64),
    data jsonb NOT NULL,
    scoreset_id integer NOT NULL,
    hgvs_nt character varying,
    hgvs_pro character varying,
    hgvs_splice character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: published_variants_materialized_view; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.published_variants_materialized_view AS
 SELECT variants.id AS variant_id,
    variants.urn AS variant_urn,
    mapping_records.id AS mapping_record_id,
    scoresets.id AS score_set_id,
    scoresets.urn AS score_set_urn,
    scoresets.published_date,
    (mapping_records.valid_to IS NULL) AS current_mapping_record
   FROM ((public.variants
     LEFT JOIN public.mapping_records ON ((variants.id = mapping_records.variant_id)))
     JOIN public.scoresets ON ((scoresets.id = variants.scoreset_id)))
  WHERE (scoresets.published_date IS NOT NULL)
  WITH NO DATA;


--
-- Name: refseq_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.refseq_identifiers (
    id integer NOT NULL,
    identifier character varying NOT NULL,
    db_name character varying NOT NULL,
    db_version character varying,
    url character varying,
    reference_html character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: refseq_identifiers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.refseq_identifiers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: refseq_identifiers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.refseq_identifiers_id_seq OWNED BY public.refseq_identifiers.id;


--
-- Name: refseq_offsets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.refseq_offsets (
    identifier_id integer NOT NULL,
    target_gene_id integer NOT NULL,
    "offset" integer NOT NULL
);


--
-- Name: roles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.roles (
    id integer NOT NULL,
    name character varying NOT NULL
);


--
-- Name: roles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.roles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: roles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.roles_id_seq OWNED BY public.roles.id;


--
-- Name: score_calibration_functional_classification_variants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.score_calibration_functional_classification_variants (
    functional_classification_id integer NOT NULL,
    variant_id integer NOT NULL
);


--
-- Name: score_calibration_functional_classifications; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.score_calibration_functional_classifications (
    id integer NOT NULL,
    calibration_id integer NOT NULL,
    label character varying NOT NULL,
    description character varying,
    functional_classification character varying(32) NOT NULL,
    range jsonb,
    inclusive_lower_bound boolean,
    inclusive_upper_bound boolean,
    oddspaths_ratio double precision,
    positive_likelihood_ratio double precision,
    acmg_classification_id integer,
    class_ character varying
);


--
-- Name: score_calibration_functional_classifications_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.score_calibration_functional_classifications_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: score_calibration_functional_classifications_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.score_calibration_functional_classifications_id_seq OWNED BY public.score_calibration_functional_classifications.id;


--
-- Name: score_calibration_publication_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.score_calibration_publication_identifiers (
    score_calibration_id integer NOT NULL,
    publication_identifier_id integer NOT NULL,
    relation character varying(32) NOT NULL
);


--
-- Name: score_calibrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.score_calibrations (
    id integer NOT NULL,
    urn character varying(64),
    score_set_id integer NOT NULL,
    title character varying NOT NULL,
    research_use_only boolean NOT NULL,
    "primary" boolean NOT NULL,
    investigator_provided boolean NOT NULL,
    private boolean NOT NULL,
    notes character varying,
    baseline_score double precision,
    baseline_score_description character varying,
    calibration_metadata jsonb,
    created_by_id integer NOT NULL,
    modified_by_id integer NOT NULL,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    disease_term_id integer NOT NULL,
    controls_not_phi boolean
);


--
-- Name: score_calibrations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.score_calibrations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: score_calibrations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.score_calibrations_id_seq OWNED BY public.score_calibrations.id;


--
-- Name: scoreset_contributors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scoreset_contributors (
    scoreset_id integer NOT NULL,
    contributor_id integer NOT NULL
);


--
-- Name: scoreset_doi_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scoreset_doi_identifiers (
    scoreset_id integer NOT NULL,
    doi_identifier_id integer NOT NULL
);


--
-- Name: scoreset_keywords; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scoreset_keywords (
    scoreset_id integer NOT NULL,
    keyword_id integer NOT NULL
);


--
-- Name: scoreset_meta_analysis_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scoreset_meta_analysis_sources (
    source_scoreset_id integer NOT NULL,
    meta_analysis_scoreset_id integer NOT NULL
);


--
-- Name: scoreset_publication_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scoreset_publication_identifiers (
    scoreset_id integer NOT NULL,
    publication_identifier_id integer NOT NULL,
    "primary" boolean
);


--
-- Name: scoreset_sra_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scoreset_sra_identifiers (
    scoreset_id integer NOT NULL,
    sra_identifier_id integer NOT NULL
);


--
-- Name: scoresets_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scoresets_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scoresets_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.scoresets_id_seq OWNED BY public.scoresets.id;


--
-- Name: sra_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sra_identifiers (
    id integer NOT NULL,
    identifier character varying NOT NULL,
    db_name character varying NOT NULL,
    db_version character varying,
    url character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: sra_identifiers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.sra_identifiers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sra_identifiers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.sra_identifiers_id_seq OWNED BY public.sra_identifiers.id;


--
-- Name: target_accessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.target_accessions (
    id integer NOT NULL,
    assembly character varying,
    accession character varying NOT NULL,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    gene character varying,
    is_base_editor boolean DEFAULT false NOT NULL
);


--
-- Name: target_accessions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.target_accessions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: target_accessions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.target_accessions_id_seq OWNED BY public.target_accessions.id;


--
-- Name: target_gene_mappings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.target_gene_mappings (
    id integer NOT NULL,
    target_gene_id integer NOT NULL,
    alignment_level character varying(16) NOT NULL,
    preferred boolean DEFAULT false NOT NULL,
    reference_assembly character varying,
    reference_accession character varying,
    reference_sequence_id character varying,
    alignment_score double precision,
    next_best_alignment_score double precision,
    alignment_length integer,
    alignment_string character varying,
    mismatch_count integer,
    gap_count integer,
    percent_identity double precision,
    total_variants integer,
    variants_failed integer,
    variants_with_alignment_warnings integer,
    variants_mapped_cleanly integer,
    tool_name character varying DEFAULT 'dcd-mapping'::character varying NOT NULL,
    tool_version character varying NOT NULL,
    tool_parameters jsonb,
    alignment_metadata jsonb,
    vrs_version character varying,
    mapped_date date,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    job_run_id integer,
    CONSTRAINT ck_target_gene_mappings_alignment_level_valid CHECK (((alignment_level)::text = ANY ((ARRAY['protein'::character varying, 'cdna'::character varying, 'genomic'::character varying])::text[])))
);


--
-- Name: target_gene_mappings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.target_gene_mappings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: target_gene_mappings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.target_gene_mappings_id_seq OWNED BY public.target_gene_mappings.id;


--
-- Name: target_genes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.target_genes (
    id integer NOT NULL,
    name character varying NOT NULL,
    category character varying(32) NOT NULL,
    scoreset_id integer NOT NULL,
    target_sequence_id integer,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    accession_id integer,
    pre_mapped_metadata jsonb,
    post_mapped_metadata jsonb,
    uniprot_id_from_mapped_metadata character varying,
    mapped_hgnc_name character varying,
    CONSTRAINT targetcategory CHECK (((category)::text = ANY ((ARRAY['protein_coding'::character varying, 'other_noncoding'::character varying, 'regulatory'::character varying])::text[])))
);


--
-- Name: target_genes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.target_genes_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: target_genes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.target_genes_id_seq OWNED BY public.target_genes.id;


--
-- Name: target_sequences; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.target_sequences (
    id integer NOT NULL,
    sequence_type character varying NOT NULL,
    sequence character varying NOT NULL,
    creation_date date NOT NULL,
    modification_date date NOT NULL,
    label character varying,
    taxonomy_id integer
);


--
-- Name: target_sequences_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.target_sequences_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: target_sequences_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.target_sequences_id_seq OWNED BY public.target_sequences.id;


--
-- Name: taxonomies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.taxonomies (
    id integer NOT NULL,
    code integer NOT NULL,
    organism_name character varying,
    common_name character varying,
    rank character varying,
    has_described_species_name boolean,
    url character varying NOT NULL,
    article_reference character varying,
    genome_identifier_id integer,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: taxonomies_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.taxonomies_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: taxonomies_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.taxonomies_id_seq OWNED BY public.taxonomies.id;


--
-- Name: uniprot_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.uniprot_identifiers (
    id integer NOT NULL,
    identifier character varying NOT NULL,
    db_name character varying NOT NULL,
    db_version character varying,
    url character varying,
    reference_html character varying,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: uniprot_identifiers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.uniprot_identifiers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: uniprot_identifiers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.uniprot_identifiers_id_seq OWNED BY public.uniprot_identifiers.id;


--
-- Name: uniprot_offsets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.uniprot_offsets (
    identifier_id integer NOT NULL,
    target_gene_id integer NOT NULL,
    "offset" integer NOT NULL
);


--
-- Name: urn_redirects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.urn_redirects (
    id integer NOT NULL,
    old_urn character varying(64) NOT NULL,
    new_urn character varying(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: urn_redirects_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.urn_redirects_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: urn_redirects_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.urn_redirects_id_seq OWNED BY public.urn_redirects.id;


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id integer NOT NULL,
    username character varying NOT NULL,
    first_name character varying,
    last_name character varying,
    is_superuser boolean NOT NULL,
    is_staff boolean NOT NULL,
    is_active boolean NOT NULL,
    date_joined timestamp without time zone,
    email character varying,
    last_login timestamp without time zone,
    is_first_login boolean DEFAULT false NOT NULL
);


--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.users_id_seq
    AS integer
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
-- Name: users_roles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users_roles (
    user_id integer NOT NULL,
    role_id integer NOT NULL
);


--
-- Name: v_current_annotation_events; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_current_annotation_events AS
 SELECT ranked_annotation_events.id,
    ranked_annotation_events.annotation_type,
    ranked_annotation_events.variant_id,
    ranked_annotation_events.allele_id,
    ranked_annotation_events.disposition,
    ranked_annotation_events.reason,
    ranked_annotation_events.source_version,
    ranked_annotation_events.event_metadata,
    ranked_annotation_events.job_run_id,
    ranked_annotation_events.created_at
   FROM ( SELECT annotation_event.id,
            annotation_event.annotation_type,
            annotation_event.variant_id,
            annotation_event.allele_id,
            annotation_event.disposition,
            annotation_event.reason,
            annotation_event.source_version,
            annotation_event.metadata AS event_metadata,
            annotation_event.job_run_id,
            annotation_event.created_at,
            row_number() OVER (PARTITION BY annotation_event.allele_id, annotation_event.variant_id, annotation_event.annotation_type,
                CASE
                    WHEN ((annotation_event.annotation_type)::text = 'clinvar_control'::text) THEN annotation_event.source_version
                    ELSE NULL::character varying
                END ORDER BY annotation_event.id DESC) AS row_number
           FROM public.annotation_event) ranked_annotation_events
  WHERE (ranked_annotation_events.row_number = 1);


--
-- Name: variant_annotation_status; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.variant_annotation_status (
    id integer NOT NULL,
    variant_id integer NOT NULL,
    annotation_type character varying(50) NOT NULL,
    version character varying(50),
    status character varying(50) NOT NULL,
    error_message text,
    failure_category character varying(100),
    annotation_metadata jsonb,
    current boolean DEFAULT true NOT NULL,
    job_run_id integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_variant_annotation_failure_category_valid CHECK (((failure_category IS NULL) OR ((failure_category)::text = ANY ((ARRAY['missing_identifier'::character varying, 'unsupported_identifier'::character varying, 'external_api_error'::character varying, 'external_service_rejected'::character varying, 'external_reference_not_found'::character varying, 'no_linked_allele'::character varying, 'unknown'::character varying])::text[])))),
    CONSTRAINT ck_variant_annotation_status_valid CHECK (((status)::text = ANY ((ARRAY['success'::character varying, 'failed'::character varying, 'skipped'::character varying])::text[]))),
    CONSTRAINT ck_variant_annotation_type_valid CHECK (((annotation_type)::text = ANY ((ARRAY['vrs_mapping'::character varying, 'cross_level_translation'::character varying, 'clingen_allele_id'::character varying, 'mapped_hgvs'::character varying, 'variant_translation'::character varying, 'gnomad_allele_frequency'::character varying, 'clinvar_control'::character varying, 'vep_functional_consequence'::character varying, 'ldh_submission'::character varying])::text[])))
);


--
-- Name: COLUMN variant_annotation_status.annotation_type; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.variant_annotation_status.annotation_type IS 'Type of annotation: vrs, clinvar, gnomad, etc.';


--
-- Name: COLUMN variant_annotation_status.version; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.variant_annotation_status.version IS 'Version of the annotation source used (if applicable)';


--
-- Name: COLUMN variant_annotation_status.status; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.variant_annotation_status.status IS 'success, failed, skipped, pending';


--
-- Name: COLUMN variant_annotation_status.annotation_metadata; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.variant_annotation_status.annotation_metadata IS 'Structured metadata for the annotation result';


--
-- Name: COLUMN variant_annotation_status.current; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.variant_annotation_status.current IS 'Whether this is the current status for the variant and annotation type';


--
-- Name: vep_allele_consequences; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vep_allele_consequences (
    id integer NOT NULL,
    allele_id integer NOT NULL,
    functional_consequence character varying,
    source_version character varying NOT NULL,
    access_date date NOT NULL,
    valid_from timestamp with time zone DEFAULT now() NOT NULL,
    valid_to timestamp with time zone,
    consequence_terms jsonb,
    consequence_source character varying(32),
    matched_transcript character varying,
    resolver_version character varying,
    CONSTRAINT vepconsequencesource CHECK (((consequence_source)::text = ANY ((ARRAY['transcript'::character varying, 'most_severe'::character varying, 'reference_identical'::character varying])::text[])))
);


--
-- Name: v_variant_annotations; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_variant_annotations AS
 SELECT variants.urn AS variant_urn,
    scoresets.urn AS score_set_urn,
    variants.hgvs_nt,
    variants.hgvs_pro,
    variants.hgvs_splice,
    mapping_records.id AS mapping_record_id,
    ( SELECT alleles.clingen_allele_id
           FROM (public.mapping_record_alleles
             JOIN public.alleles ON ((alleles.id = mapping_record_alleles.allele_id)))
          WHERE ((mapping_record_alleles.mapping_record_id = mapping_records.id) AND (mapping_record_alleles.is_authoritative IS TRUE) AND (mapping_record_alleles.valid_to IS NULL))
         LIMIT 1) AS clingen_allele_id,
    mapping_records.hgvs_assay_level,
    ( SELECT alleles.hgvs_g
           FROM (public.mapping_record_alleles
             JOIN public.alleles ON ((alleles.id = mapping_record_alleles.allele_id)))
          WHERE ((mapping_record_alleles.mapping_record_id = mapping_records.id) AND (mapping_record_alleles.valid_to IS NULL) AND ((alleles.level)::text = 'genomic'::text))
         LIMIT 1) AS hgvs_g,
    ( SELECT alleles.hgvs_c
           FROM (public.mapping_record_alleles
             JOIN public.alleles ON ((alleles.id = mapping_record_alleles.allele_id)))
          WHERE ((mapping_record_alleles.mapping_record_id = mapping_records.id) AND (mapping_record_alleles.valid_to IS NULL) AND ((alleles.level)::text = 'cdna'::text))
         LIMIT 1) AS hgvs_c,
    ( SELECT alleles.hgvs_p
           FROM (public.mapping_record_alleles
             JOIN public.alleles ON ((alleles.id = mapping_record_alleles.allele_id)))
          WHERE ((mapping_record_alleles.mapping_record_id = mapping_records.id) AND (mapping_record_alleles.valid_to IS NULL) AND ((alleles.level)::text = 'protein'::text))
         LIMIT 1) AS hgvs_p,
    ( SELECT vep_allele_consequences.functional_consequence
           FROM ((public.mapping_record_alleles
             JOIN public.alleles ON ((alleles.id = mapping_record_alleles.allele_id)))
             JOIN public.vep_allele_consequences ON (((vep_allele_consequences.allele_id = alleles.id) AND (vep_allele_consequences.valid_to IS NULL))))
          WHERE ((mapping_record_alleles.mapping_record_id = mapping_records.id) AND (mapping_record_alleles.is_authoritative IS TRUE) AND (mapping_record_alleles.valid_to IS NULL))
         LIMIT 1) AS vep_functional_consequence,
    ( SELECT vep_allele_consequences.access_date
           FROM ((public.mapping_record_alleles
             JOIN public.alleles ON ((alleles.id = mapping_record_alleles.allele_id)))
             JOIN public.vep_allele_consequences ON (((vep_allele_consequences.allele_id = alleles.id) AND (vep_allele_consequences.valid_to IS NULL))))
          WHERE ((mapping_record_alleles.mapping_record_id = mapping_records.id) AND (mapping_record_alleles.is_authoritative IS TRUE) AND (mapping_record_alleles.valid_to IS NULL))
         LIMIT 1) AS vep_access_date,
    mapping_records.mapped_date,
    mapping_records.mapping_api_version,
    mapping_records.vrs_version,
    variant_annotation_status.annotation_type,
    variant_annotation_status.status AS annotation_status,
    variant_annotation_status.failure_category,
    variant_annotation_status.error_message AS annotation_error,
    variant_annotation_status.version AS annotation_version,
    variant_annotation_status.annotation_metadata,
    variant_annotation_status.current,
    variant_annotation_status.created_at AS annotation_created_at,
    variant_annotation_status.updated_at AS annotation_updated_at
   FROM (((public.variants
     JOIN public.scoresets ON ((scoresets.id = variants.scoreset_id)))
     LEFT JOIN public.mapping_records ON (((mapping_records.variant_id = variants.id) AND (mapping_records.valid_to IS NULL))))
     LEFT JOIN public.variant_annotation_status ON (((variant_annotation_status.variant_id = variants.id) AND (variant_annotation_status.current = true))));


--
-- Name: variant_annotation_status_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.variant_annotation_status_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: variant_annotation_status_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.variant_annotation_status_id_seq OWNED BY public.variant_annotation_status.id;


--
-- Name: variant_translations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.variant_translations (
    aa_clingen_id character varying NOT NULL,
    nt_clingen_id character varying NOT NULL,
    creation_date date NOT NULL,
    modification_date date NOT NULL
);


--
-- Name: variants_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.variants_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: variants_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.variants_id_seq OWNED BY public.variants.id;


--
-- Name: variants_to_sync; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.variants_to_sync (
    variant_id integer,
    variant_urn text,
    hgvs_nt text,
    hgvs_pro text,
    hgvs_splice text,
    score_set_urn text
);


--
-- Name: vep_allele_consequences_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.vep_allele_consequences_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: vep_allele_consequences_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.vep_allele_consequences_id_seq OWNED BY public.vep_allele_consequences.id;


--
-- Name: vep_functional_consequence_archive; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vep_functional_consequence_archive (
    mapped_variant_id integer NOT NULL,
    vep_functional_consequence character varying,
    vep_access_date date,
    cleared_at timestamp with time zone
);


--
-- Name: access_keys id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_keys ALTER COLUMN id SET DEFAULT nextval('public.access_keys_id_seq'::regclass);


--
-- Name: acmg_classifications id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acmg_classifications ALTER COLUMN id SET DEFAULT nextval('public.acmg_classifications_id_seq'::regclass);


--
-- Name: alleles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alleles ALTER COLUMN id SET DEFAULT nextval('public.alleles_id_seq'::regclass);


--
-- Name: annotation_event id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.annotation_event ALTER COLUMN id SET DEFAULT nextval('public.annotation_event_id_seq'::regclass);


--
-- Name: calibration_controls id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.calibration_controls ALTER COLUMN id SET DEFAULT nextval('public.calibration_controls_id_seq'::regclass);


--
-- Name: clinvar_allele_links id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clinvar_allele_links ALTER COLUMN id SET DEFAULT nextval('public.clinvar_allele_links_id_seq'::regclass);


--
-- Name: clinvar_controls id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clinvar_controls ALTER COLUMN id SET DEFAULT nextval('public.clinical_controls_id_seq'::regclass);


--
-- Name: collections id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collections ALTER COLUMN id SET DEFAULT nextval('public.collections_id_seq'::regclass);


--
-- Name: contributors id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contributors ALTER COLUMN id SET DEFAULT nextval('public.contributors_id_seq'::regclass);


--
-- Name: controlled_keywords id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.controlled_keywords ALTER COLUMN id SET DEFAULT nextval('public.controlled_keywords_id_seq'::regclass);


--
-- Name: doi_identifiers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.doi_identifiers ALTER COLUMN id SET DEFAULT nextval('public.doi_identifiers_id_seq'::regclass);


--
-- Name: ensembl_identifiers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ensembl_identifiers ALTER COLUMN id SET DEFAULT nextval('public.ensembl_identifiers_id_seq'::regclass);


--
-- Name: experiment_sets id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_sets ALTER COLUMN id SET DEFAULT nextval('public.experiment_sets_id_seq'::regclass);


--
-- Name: experiments id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments ALTER COLUMN id SET DEFAULT nextval('public.experiments_id_seq'::regclass);


--
-- Name: genome_identifiers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.genome_identifiers ALTER COLUMN id SET DEFAULT nextval('public.genome_identifiers_id_seq'::regclass);


--
-- Name: gnomad_allele_links id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gnomad_allele_links ALTER COLUMN id SET DEFAULT nextval('public.gnomad_allele_links_id_seq'::regclass);


--
-- Name: gnomad_variants id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gnomad_variants ALTER COLUMN id SET DEFAULT nextval('public.gnomad_variants_id_seq'::regclass);


--
-- Name: job_runs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_runs ALTER COLUMN id SET DEFAULT nextval('public.job_runs_id_seq'::regclass);


--
-- Name: keywords id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.keywords ALTER COLUMN id SET DEFAULT nextval('public.keywords_id_seq'::regclass);


--
-- Name: licenses id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.licenses ALTER COLUMN id SET DEFAULT nextval('public.licenses_id_seq'::regclass);


--
-- Name: mapped_variants id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapped_variants ALTER COLUMN id SET DEFAULT nextval('public.mapped_variants_id_seq'::regclass);


--
-- Name: mapping_record_alleles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapping_record_alleles ALTER COLUMN id SET DEFAULT nextval('public.mapping_record_alleles_id_seq'::regclass);


--
-- Name: mapping_records id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapping_records ALTER COLUMN id SET DEFAULT nextval('public.mapping_records_id_seq'::regclass);


--
-- Name: mondo_terms id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mondo_terms ALTER COLUMN id SET DEFAULT nextval('public.mondo_terms_id_seq'::regclass);


--
-- Name: pipelines id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipelines ALTER COLUMN id SET DEFAULT nextval('public.pipelines_id_seq'::regclass);


--
-- Name: projection_groups id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projection_groups ALTER COLUMN id SET DEFAULT nextval('public.projection_groups_id_seq'::regclass);


--
-- Name: publication_identifiers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.publication_identifiers ALTER COLUMN id SET DEFAULT nextval('public.publication_identifiers_id_seq'::regclass);


--
-- Name: refseq_identifiers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refseq_identifiers ALTER COLUMN id SET DEFAULT nextval('public.refseq_identifiers_id_seq'::regclass);


--
-- Name: roles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.roles ALTER COLUMN id SET DEFAULT nextval('public.roles_id_seq'::regclass);


--
-- Name: score_calibration_functional_classifications id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_functional_classifications ALTER COLUMN id SET DEFAULT nextval('public.score_calibration_functional_classifications_id_seq'::regclass);


--
-- Name: score_calibrations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibrations ALTER COLUMN id SET DEFAULT nextval('public.score_calibrations_id_seq'::regclass);


--
-- Name: scoresets id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoresets ALTER COLUMN id SET DEFAULT nextval('public.scoresets_id_seq'::regclass);


--
-- Name: sra_identifiers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sra_identifiers ALTER COLUMN id SET DEFAULT nextval('public.sra_identifiers_id_seq'::regclass);


--
-- Name: target_accessions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_accessions ALTER COLUMN id SET DEFAULT nextval('public.target_accessions_id_seq'::regclass);


--
-- Name: target_gene_mappings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_gene_mappings ALTER COLUMN id SET DEFAULT nextval('public.target_gene_mappings_id_seq'::regclass);


--
-- Name: target_genes id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_genes ALTER COLUMN id SET DEFAULT nextval('public.target_genes_id_seq'::regclass);


--
-- Name: target_sequences id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_sequences ALTER COLUMN id SET DEFAULT nextval('public.target_sequences_id_seq'::regclass);


--
-- Name: taxonomies id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.taxonomies ALTER COLUMN id SET DEFAULT nextval('public.taxonomies_id_seq'::regclass);


--
-- Name: uniprot_identifiers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.uniprot_identifiers ALTER COLUMN id SET DEFAULT nextval('public.uniprot_identifiers_id_seq'::regclass);


--
-- Name: urn_redirects id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.urn_redirects ALTER COLUMN id SET DEFAULT nextval('public.urn_redirects_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: variant_annotation_status id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variant_annotation_status ALTER COLUMN id SET DEFAULT nextval('public.variant_annotation_status_id_seq'::regclass);


--
-- Name: variants id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variants ALTER COLUMN id SET DEFAULT nextval('public.variants_id_seq'::regclass);


--
-- Name: vep_allele_consequences id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vep_allele_consequences ALTER COLUMN id SET DEFAULT nextval('public.vep_allele_consequences_id_seq'::regclass);


--
-- Name: access_keys access_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_keys
    ADD CONSTRAINT access_keys_pkey PRIMARY KEY (id);


--
-- Name: acmg_classifications acmg_classifications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acmg_classifications
    ADD CONSTRAINT acmg_classifications_pkey PRIMARY KEY (id);


--
-- Name: alleles alleles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alleles
    ADD CONSTRAINT alleles_pkey PRIMARY KEY (id);


--
-- Name: annotation_event annotation_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.annotation_event
    ADD CONSTRAINT annotation_event_pkey PRIMARY KEY (id);


--
-- Name: calibration_controls calibration_controls_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.calibration_controls
    ADD CONSTRAINT calibration_controls_pkey PRIMARY KEY (id);


--
-- Name: clinvar_controls clinical_controls_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clinvar_controls
    ADD CONSTRAINT clinical_controls_pkey PRIMARY KEY (id);


--
-- Name: clinvar_allele_links clinvar_allele_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clinvar_allele_links
    ADD CONSTRAINT clinvar_allele_links_pkey PRIMARY KEY (id);


--
-- Name: collection_experiments collection_experiments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collection_experiments
    ADD CONSTRAINT collection_experiments_pkey PRIMARY KEY (collection_id, experiment_id);


--
-- Name: collection_score_sets collection_score_sets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collection_score_sets
    ADD CONSTRAINT collection_score_sets_pkey PRIMARY KEY (collection_id, score_set_id);


--
-- Name: collection_user_associations collection_user_associations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collection_user_associations
    ADD CONSTRAINT collection_user_associations_pkey PRIMARY KEY (collection_id, user_id);


--
-- Name: collections collections_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collections
    ADD CONSTRAINT collections_pkey PRIMARY KEY (id);


--
-- Name: contributors contributors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contributors
    ADD CONSTRAINT contributors_pkey PRIMARY KEY (id);


--
-- Name: controlled_keywords controlled_keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.controlled_keywords
    ADD CONSTRAINT controlled_keywords_pkey PRIMARY KEY (id);


--
-- Name: doi_identifiers doi_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.doi_identifiers
    ADD CONSTRAINT doi_identifiers_pkey PRIMARY KEY (id);


--
-- Name: ensembl_identifiers ensembl_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ensembl_identifiers
    ADD CONSTRAINT ensembl_identifiers_pkey PRIMARY KEY (id);


--
-- Name: ensembl_offsets ensembl_offsets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ensembl_offsets
    ADD CONSTRAINT ensembl_offsets_pkey PRIMARY KEY (identifier_id, target_gene_id);


--
-- Name: experiment_contributors experiment_contributors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_contributors
    ADD CONSTRAINT experiment_contributors_pkey PRIMARY KEY (experiment_id, contributor_id);


--
-- Name: experiment_controlled_keywords experiment_controlled_keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_controlled_keywords
    ADD CONSTRAINT experiment_controlled_keywords_pkey PRIMARY KEY (experiment_id, controlled_keyword_id);


--
-- Name: experiment_doi_identifiers experiment_doi_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_doi_identifiers
    ADD CONSTRAINT experiment_doi_identifiers_pkey PRIMARY KEY (experiment_id, doi_identifier_id);


--
-- Name: experiment_keywords experiment_keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_keywords
    ADD CONSTRAINT experiment_keywords_pkey PRIMARY KEY (experiment_id, keyword_id);


--
-- Name: experiment_publication_identifiers experiment_publication_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_publication_identifiers
    ADD CONSTRAINT experiment_publication_identifiers_pkey PRIMARY KEY (experiment_id, publication_identifier_id);


--
-- Name: experiment_set_contributors experiment_set_contributors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_contributors
    ADD CONSTRAINT experiment_set_contributors_pkey PRIMARY KEY (experiment_set_id, contributor_id);


--
-- Name: experiment_set_doi_identifiers experiment_set_doi_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_doi_identifiers
    ADD CONSTRAINT experiment_set_doi_identifiers_pkey PRIMARY KEY (experiment_set_id, doi_identifier_id);


--
-- Name: experiment_set_keywords experiment_set_keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_keywords
    ADD CONSTRAINT experiment_set_keywords_pkey PRIMARY KEY (experiment_set_id, keyword_id);


--
-- Name: experiment_set_publication_identifiers experiment_set_publication_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_publication_identifiers
    ADD CONSTRAINT experiment_set_publication_identifiers_pkey PRIMARY KEY (experiment_set_id, publication_identifier_id);


--
-- Name: experiment_set_sra_identifiers experiment_set_sra_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_sra_identifiers
    ADD CONSTRAINT experiment_set_sra_identifiers_pkey PRIMARY KEY (experiment_set_id, sra_identifier_id);


--
-- Name: experiment_sets experiment_sets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_sets
    ADD CONSTRAINT experiment_sets_pkey PRIMARY KEY (id);


--
-- Name: experiment_sra_identifiers experiment_sra_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_sra_identifiers
    ADD CONSTRAINT experiment_sra_identifiers_pkey PRIMARY KEY (experiment_id, sra_identifier_id);


--
-- Name: experiments experiments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiments_pkey PRIMARY KEY (id);


--
-- Name: genome_identifiers genome_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.genome_identifiers
    ADD CONSTRAINT genome_identifiers_pkey PRIMARY KEY (id);


--
-- Name: gnomad_allele_links gnomad_allele_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gnomad_allele_links
    ADD CONSTRAINT gnomad_allele_links_pkey PRIMARY KEY (id);


--
-- Name: gnomad_variants_mapped_variants gnomad_variants_mapped_variants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gnomad_variants_mapped_variants
    ADD CONSTRAINT gnomad_variants_mapped_variants_pkey PRIMARY KEY (mapped_variant_id, gnomad_variant_id);


--
-- Name: gnomad_variants gnomad_variants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gnomad_variants
    ADD CONSTRAINT gnomad_variants_pkey PRIMARY KEY (id);


--
-- Name: controlled_keywords ix_controlled_keywords_key_label; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.controlled_keywords
    ADD CONSTRAINT ix_controlled_keywords_key_label UNIQUE (key, label);


--
-- Name: job_dependencies job_dependencies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_dependencies
    ADD CONSTRAINT job_dependencies_pkey PRIMARY KEY (id, depends_on_job_id);


--
-- Name: job_runs job_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_runs
    ADD CONSTRAINT job_runs_pkey PRIMARY KEY (id);


--
-- Name: job_runs job_runs_urn_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_runs
    ADD CONSTRAINT job_runs_urn_key UNIQUE (urn);


--
-- Name: keywords keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.keywords
    ADD CONSTRAINT keywords_pkey PRIMARY KEY (id);


--
-- Name: keywords keywords_text_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.keywords
    ADD CONSTRAINT keywords_text_key UNIQUE (text);


--
-- Name: licenses licenses_long_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.licenses
    ADD CONSTRAINT licenses_long_name_key UNIQUE (long_name);


--
-- Name: licenses licenses_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.licenses
    ADD CONSTRAINT licenses_pkey PRIMARY KEY (id);


--
-- Name: licenses licenses_short_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.licenses
    ADD CONSTRAINT licenses_short_name_key UNIQUE (short_name);


--
-- Name: mapped_variants_clinical_controls mapped_variants_clinical_controls_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapped_variants_clinical_controls
    ADD CONSTRAINT mapped_variants_clinical_controls_pkey PRIMARY KEY (mapped_variant_id, clinical_control_id);


--
-- Name: mapped_variants mapped_variants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapped_variants
    ADD CONSTRAINT mapped_variants_pkey PRIMARY KEY (id);


--
-- Name: mapping_record_alleles mapping_record_alleles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapping_record_alleles
    ADD CONSTRAINT mapping_record_alleles_pkey PRIMARY KEY (id);


--
-- Name: mapping_records mapping_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapping_records
    ADD CONSTRAINT mapping_records_pkey PRIMARY KEY (id);


--
-- Name: mondo_terms mondo_terms_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mondo_terms
    ADD CONSTRAINT mondo_terms_pkey PRIMARY KEY (id);


--
-- Name: pipelines pipelines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT pipelines_pkey PRIMARY KEY (id);


--
-- Name: pipelines pipelines_urn_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT pipelines_urn_key UNIQUE (urn);


--
-- Name: projection_groups projection_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projection_groups
    ADD CONSTRAINT projection_groups_pkey PRIMARY KEY (id);


--
-- Name: publication_identifiers publication_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.publication_identifiers
    ADD CONSTRAINT publication_identifiers_pkey PRIMARY KEY (id);


--
-- Name: refseq_identifiers refseq_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refseq_identifiers
    ADD CONSTRAINT refseq_identifiers_pkey PRIMARY KEY (id);


--
-- Name: refseq_offsets refseq_offsets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refseq_offsets
    ADD CONSTRAINT refseq_offsets_pkey PRIMARY KEY (identifier_id, target_gene_id);


--
-- Name: roles roles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_pkey PRIMARY KEY (id);


--
-- Name: score_calibration_functional_classification_variants score_calibration_functional_classification_variants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_functional_classification_variants
    ADD CONSTRAINT score_calibration_functional_classification_variants_pkey PRIMARY KEY (functional_classification_id, variant_id);


--
-- Name: score_calibration_functional_classifications score_calibration_functional_classifications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_functional_classifications
    ADD CONSTRAINT score_calibration_functional_classifications_pkey PRIMARY KEY (id);


--
-- Name: score_calibration_publication_identifiers score_calibration_publication_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_publication_identifiers
    ADD CONSTRAINT score_calibration_publication_identifiers_pkey PRIMARY KEY (score_calibration_id, publication_identifier_id, relation);


--
-- Name: score_calibrations score_calibrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibrations
    ADD CONSTRAINT score_calibrations_pkey PRIMARY KEY (id);


--
-- Name: scoreset_contributors scoreset_contributors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_contributors
    ADD CONSTRAINT scoreset_contributors_pkey PRIMARY KEY (scoreset_id, contributor_id);


--
-- Name: scoreset_doi_identifiers scoreset_doi_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_doi_identifiers
    ADD CONSTRAINT scoreset_doi_identifiers_pkey PRIMARY KEY (scoreset_id, doi_identifier_id);


--
-- Name: scoreset_keywords scoreset_keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_keywords
    ADD CONSTRAINT scoreset_keywords_pkey PRIMARY KEY (scoreset_id, keyword_id);


--
-- Name: scoreset_meta_analysis_sources scoreset_meta_analysis_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_meta_analysis_sources
    ADD CONSTRAINT scoreset_meta_analysis_sources_pkey PRIMARY KEY (source_scoreset_id, meta_analysis_scoreset_id);


--
-- Name: scoreset_publication_identifiers scoreset_publication_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_publication_identifiers
    ADD CONSTRAINT scoreset_publication_identifiers_pkey PRIMARY KEY (scoreset_id, publication_identifier_id);


--
-- Name: scoreset_sra_identifiers scoreset_sra_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_sra_identifiers
    ADD CONSTRAINT scoreset_sra_identifiers_pkey PRIMARY KEY (scoreset_id, sra_identifier_id);


--
-- Name: scoresets scoresets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoresets
    ADD CONSTRAINT scoresets_pkey PRIMARY KEY (id);


--
-- Name: sra_identifiers sra_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sra_identifiers
    ADD CONSTRAINT sra_identifiers_pkey PRIMARY KEY (id);


--
-- Name: target_accessions target_accessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_accessions
    ADD CONSTRAINT target_accessions_pkey PRIMARY KEY (id);


--
-- Name: target_gene_mappings target_gene_mappings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_gene_mappings
    ADD CONSTRAINT target_gene_mappings_pkey PRIMARY KEY (id);


--
-- Name: target_genes target_genes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_genes
    ADD CONSTRAINT target_genes_pkey PRIMARY KEY (id);


--
-- Name: target_sequences target_sequences_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_sequences
    ADD CONSTRAINT target_sequences_pkey PRIMARY KEY (id);


--
-- Name: taxonomies taxonomies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.taxonomies
    ADD CONSTRAINT taxonomies_pkey PRIMARY KEY (id);


--
-- Name: uniprot_identifiers uniprot_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.uniprot_identifiers
    ADD CONSTRAINT uniprot_identifiers_pkey PRIMARY KEY (id);


--
-- Name: uniprot_offsets uniprot_offsets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.uniprot_offsets
    ADD CONSTRAINT uniprot_offsets_pkey PRIMARY KEY (identifier_id, target_gene_id);


--
-- Name: alleles uq_alleles_vrs_digest; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alleles
    ADD CONSTRAINT uq_alleles_vrs_digest UNIQUE (vrs_digest);


--
-- Name: calibration_controls uq_calibration_controls_calibration_id_variant_id; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.calibration_controls
    ADD CONSTRAINT uq_calibration_controls_calibration_id_variant_id UNIQUE (calibration_id, variant_id);


--
-- Name: clinvar_controls uq_clinvar_controls_db_name_identifier_version; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clinvar_controls
    ADD CONSTRAINT uq_clinvar_controls_db_name_identifier_version UNIQUE (db_name, db_identifier, db_version);


--
-- Name: mondo_terms uq_mondo_terms_system_code; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mondo_terms
    ADD CONSTRAINT uq_mondo_terms_system_code UNIQUE (system, code);


--
-- Name: urn_redirects urn_redirects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.urn_redirects
    ADD CONSTRAINT urn_redirects_pkey PRIMARY KEY (id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users_roles users_roles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users_roles
    ADD CONSTRAINT users_roles_pkey PRIMARY KEY (user_id, role_id);


--
-- Name: variant_annotation_status variant_annotation_status_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variant_annotation_status
    ADD CONSTRAINT variant_annotation_status_pkey PRIMARY KEY (id);


--
-- Name: variant_translations variant_translations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variant_translations
    ADD CONSTRAINT variant_translations_pkey PRIMARY KEY (aa_clingen_id, nt_clingen_id);


--
-- Name: variants variants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variants
    ADD CONSTRAINT variants_pkey PRIMARY KEY (id);


--
-- Name: vep_allele_consequences vep_allele_consequences_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vep_allele_consequences
    ADD CONSTRAINT vep_allele_consequences_pkey PRIMARY KEY (id);


--
-- Name: vep_functional_consequence_archive vep_functional_consequence_archive_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vep_functional_consequence_archive
    ADD CONSTRAINT vep_functional_consequence_archive_pkey PRIMARY KEY (mapped_variant_id);


--
-- Name: idx_published_variants_materialized_view_ids; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_published_variants_materialized_view_ids ON public.published_variants_materialized_view USING btree (mapping_record_id, variant_id, score_set_id);


--
-- Name: ix_access_keys_key_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_access_keys_key_id ON public.access_keys USING btree (key_id);


--
-- Name: ix_access_keys_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_access_keys_user_id ON public.access_keys USING btree (user_id);


--
-- Name: ix_alleles_clingen_allele_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_alleles_clingen_allele_id ON public.alleles USING btree (clingen_allele_id);


--
-- Name: ix_alleles_level; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_alleles_level ON public.alleles USING btree (level);


--
-- Name: ix_alleles_vrs_digest; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_alleles_vrs_digest ON public.alleles USING btree (vrs_digest);


--
-- Name: ix_annotation_event_allele_type_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_annotation_event_allele_type_id ON public.annotation_event USING btree (allele_id, annotation_type, id DESC);


--
-- Name: ix_annotation_event_allele_type_version; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_annotation_event_allele_type_version ON public.annotation_event USING btree (allele_id, annotation_type, source_version);


--
-- Name: ix_annotation_event_job_run_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_annotation_event_job_run_id ON public.annotation_event USING btree (job_run_id);


--
-- Name: ix_annotation_event_score_set_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_annotation_event_score_set_id ON public.annotation_event USING btree (score_set_id);


--
-- Name: ix_annotation_event_variant_type_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_annotation_event_variant_type_id ON public.annotation_event USING btree (variant_id, annotation_type, id DESC);


--
-- Name: ix_calibration_controls_calibration_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_calibration_controls_calibration_id ON public.calibration_controls USING btree (calibration_id);


--
-- Name: ix_calibration_controls_created_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_calibration_controls_created_by_id ON public.calibration_controls USING btree (created_by_id);


--
-- Name: ix_calibration_controls_modified_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_calibration_controls_modified_by_id ON public.calibration_controls USING btree (modified_by_id);


--
-- Name: ix_calibration_controls_variant_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_calibration_controls_variant_id ON public.calibration_controls USING btree (variant_id);


--
-- Name: ix_clinical_controls_db_identifier; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_clinical_controls_db_identifier ON public.clinvar_controls USING btree (db_identifier);


--
-- Name: ix_clinical_controls_db_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_clinical_controls_db_name ON public.clinvar_controls USING btree (db_name);


--
-- Name: ix_clinical_controls_db_version; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_clinical_controls_db_version ON public.clinvar_controls USING btree (db_version);


--
-- Name: ix_clinical_controls_gene_symbol; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_clinical_controls_gene_symbol ON public.clinvar_controls USING btree (gene_symbol);


--
-- Name: ix_clinvar_allele_links_allele_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_clinvar_allele_links_allele_id ON public.clinvar_allele_links USING btree (allele_id);


--
-- Name: ix_clinvar_allele_links_clinvar_control_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_clinvar_allele_links_clinvar_control_id ON public.clinvar_allele_links USING btree (clinvar_control_id);


--
-- Name: ix_clinvar_variants_allele_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_clinvar_variants_allele_id ON public.clinvar_controls USING btree (db_identifier);


--
-- Name: ix_collections_created_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_collections_created_by_id ON public.collections USING btree (created_by_id);


--
-- Name: ix_collections_modified_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_collections_modified_by_id ON public.collections USING btree (modified_by_id);


--
-- Name: ix_collections_urn; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_collections_urn ON public.collections USING btree (urn);


--
-- Name: ix_contributors_orcid_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_contributors_orcid_id ON public.contributors USING btree (orcid_id);


--
-- Name: ix_controlled_keywords_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_controlled_keywords_id ON public.controlled_keywords USING btree (id);


--
-- Name: ix_experiment_sets_created_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_experiment_sets_created_by_id ON public.experiment_sets USING btree (created_by_id);


--
-- Name: ix_experiment_sets_modified_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_experiment_sets_modified_by_id ON public.experiment_sets USING btree (modified_by_id);


--
-- Name: ix_experiment_sets_urn; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_experiment_sets_urn ON public.experiment_sets USING btree (urn);


--
-- Name: ix_experiments_created_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_experiments_created_by_id ON public.experiments USING btree (created_by_id);


--
-- Name: ix_experiments_experiment_set_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_experiments_experiment_set_id ON public.experiments USING btree (experiment_set_id);


--
-- Name: ix_experiments_modified_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_experiments_modified_by_id ON public.experiments USING btree (modified_by_id);


--
-- Name: ix_experiments_urn; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_experiments_urn ON public.experiments USING btree (urn);


--
-- Name: ix_gnomad_allele_links_allele_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gnomad_allele_links_allele_id ON public.gnomad_allele_links USING btree (allele_id);


--
-- Name: ix_gnomad_allele_links_gnomad_variant_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gnomad_allele_links_gnomad_variant_id ON public.gnomad_allele_links USING btree (gnomad_variant_id);


--
-- Name: ix_gnomad_variants_db_identifier; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gnomad_variants_db_identifier ON public.gnomad_variants USING btree (db_identifier);


--
-- Name: ix_job_dependencies_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_dependencies_created_at ON public.job_dependencies USING btree (created_at);


--
-- Name: ix_job_dependencies_depends_on_job_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_dependencies_depends_on_job_id ON public.job_dependencies USING btree (depends_on_job_id);


--
-- Name: ix_job_runs_correlation_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_runs_correlation_id ON public.job_runs USING btree (correlation_id);


--
-- Name: ix_job_runs_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_runs_created_at ON public.job_runs USING btree (created_at);


--
-- Name: ix_job_runs_job_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_runs_job_type ON public.job_runs USING btree (job_type);


--
-- Name: ix_job_runs_pipeline_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_runs_pipeline_id ON public.job_runs USING btree (pipeline_id);


--
-- Name: ix_job_runs_scheduled_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_runs_scheduled_at ON public.job_runs USING btree (scheduled_at);


--
-- Name: ix_job_runs_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_runs_status ON public.job_runs USING btree (status);


--
-- Name: ix_job_runs_status_progress_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_runs_status_progress_updated ON public.job_runs USING btree (status, progress_updated_at);


--
-- Name: ix_job_runs_status_scheduled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_runs_status_scheduled ON public.job_runs USING btree (status, scheduled_at);


--
-- Name: ix_mapped_variants_clingen_allele_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapped_variants_clingen_allele_id ON public.mapped_variants USING btree (clingen_allele_id);


--
-- Name: ix_mapped_variants_post_mapped_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapped_variants_post_mapped_id ON public.mapped_variants USING btree (((post_mapped ->> 'id'::text)));


--
-- Name: ix_mapped_variants_pre_mapped_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapped_variants_pre_mapped_id ON public.mapped_variants USING btree (((pre_mapped ->> 'id'::text)));


--
-- Name: ix_mapped_variants_target_gene_mapping_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapped_variants_target_gene_mapping_id ON public.mapped_variants USING btree (target_gene_mapping_id);


--
-- Name: ix_mapped_variants_variant_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapped_variants_variant_id ON public.mapped_variants USING btree (variant_id);


--
-- Name: ix_mapping_record_alleles_allele_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapping_record_alleles_allele_id ON public.mapping_record_alleles USING btree (allele_id);


--
-- Name: ix_mapping_record_alleles_mapping_record_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapping_record_alleles_mapping_record_id ON public.mapping_record_alleles USING btree (mapping_record_id);


--
-- Name: ix_mapping_record_alleles_projection_group; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapping_record_alleles_projection_group ON public.mapping_record_alleles USING btree (projection_group);


--
-- Name: ix_mapping_records_target_gene_mapping_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapping_records_target_gene_mapping_id ON public.mapping_records USING btree (target_gene_mapping_id);


--
-- Name: ix_mapping_records_variant_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapping_records_variant_id ON public.mapping_records USING btree (variant_id);


--
-- Name: ix_mapping_records_vrs_digest; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mapping_records_vrs_digest ON public.mapping_records USING btree (vrs_digest);


--
-- Name: ix_pipelines_correlation_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_pipelines_correlation_id ON public.pipelines USING btree (correlation_id);


--
-- Name: ix_pipelines_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_pipelines_created_at ON public.pipelines USING btree (created_at);


--
-- Name: ix_pipelines_created_by_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_pipelines_created_by_user_id ON public.pipelines USING btree (created_by_user_id);


--
-- Name: ix_pipelines_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_pipelines_status ON public.pipelines USING btree (status);


--
-- Name: ix_projection_groups_mapping_record_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_projection_groups_mapping_record_id ON public.projection_groups USING btree (mapping_record_id);


--
-- Name: ix_score_calib_func_classification_variants_variant_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_score_calib_func_classification_variants_variant_id ON public.score_calibration_functional_classification_variants USING btree (variant_id);


--
-- Name: ix_score_calibrations_created_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_score_calibrations_created_by_id ON public.score_calibrations USING btree (created_by_id);


--
-- Name: ix_score_calibrations_disease_term_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_score_calibrations_disease_term_id ON public.score_calibrations USING btree (disease_term_id);


--
-- Name: ix_score_calibrations_modified_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_score_calibrations_modified_by_id ON public.score_calibrations USING btree (modified_by_id);


--
-- Name: ix_score_calibrations_urn; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_score_calibrations_urn ON public.score_calibrations USING btree (urn);


--
-- Name: ix_scoresets_created_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scoresets_created_by_id ON public.scoresets USING btree (created_by_id);


--
-- Name: ix_scoresets_experiment_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scoresets_experiment_id ON public.scoresets USING btree (experiment_id);


--
-- Name: ix_scoresets_licence_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scoresets_licence_id ON public.scoresets USING btree (licence_id);


--
-- Name: ix_scoresets_modified_by_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scoresets_modified_by_id ON public.scoresets USING btree (modified_by_id);


--
-- Name: ix_scoresets_replaces_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_scoresets_replaces_id ON public.scoresets USING btree (replaces_id);


--
-- Name: ix_scoresets_urn; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_scoresets_urn ON public.scoresets USING btree (urn);


--
-- Name: ix_target_gene_mappings_job_run_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_target_gene_mappings_job_run_id ON public.target_gene_mappings USING btree (job_run_id);


--
-- Name: ix_target_gene_mappings_target_alignment; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_target_gene_mappings_target_alignment ON public.target_gene_mappings USING btree (target_gene_id, alignment_level);


--
-- Name: ix_target_gene_mappings_target_gene_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_target_gene_mappings_target_gene_id ON public.target_gene_mappings USING btree (target_gene_id);


--
-- Name: ix_target_genes_accession_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_target_genes_accession_id ON public.target_genes USING btree (accession_id);


--
-- Name: ix_target_genes_scoreset_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_target_genes_scoreset_id ON public.target_genes USING btree (scoreset_id);


--
-- Name: ix_target_genes_target_sequence_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_target_genes_target_sequence_id ON public.target_genes USING btree (target_sequence_id);


--
-- Name: ix_target_sequences_taxonomy_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_target_sequences_taxonomy_id ON public.target_sequences USING btree (taxonomy_id);


--
-- Name: ix_taxonomies_genome_identifier_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_taxonomies_genome_identifier_id ON public.taxonomies USING btree (genome_identifier_id);


--
-- Name: ix_urn_redirects_old_urn; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_urn_redirects_old_urn ON public.urn_redirects USING btree (old_urn);


--
-- Name: ix_users_username; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_users_username ON public.users USING btree (username);


--
-- Name: ix_variant_annotation_status_job_run_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_variant_annotation_status_job_run_id ON public.variant_annotation_status USING btree (job_run_id);


--
-- Name: ix_variant_annotation_status_variant_type_version_current; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_variant_annotation_status_variant_type_version_current ON public.variant_annotation_status USING btree (variant_id, annotation_type, version, current);


--
-- Name: ix_variants_scoreset_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_variants_scoreset_id ON public.variants USING btree (scoreset_id);


--
-- Name: ix_variants_scoreset_number; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_variants_scoreset_number ON public.variants USING btree (scoreset_id, ((split_part((urn)::text, '#'::text, 2))::integer), id);


--
-- Name: ix_variants_urn; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_variants_urn ON public.variants USING btree (urn);


--
-- Name: ix_vep_allele_consequences_allele_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_vep_allele_consequences_allele_id ON public.vep_allele_consequences USING btree (allele_id);


--
-- Name: uq_alleles_hgvs; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_alleles_hgvs ON public.alleles USING btree (COALESCE(hgvs_g, hgvs_c, hgvs_p));


--
-- Name: INDEX uq_alleles_hgvs; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON INDEX public.uq_alleles_hgvs IS 'One allele per HGVS expression. A violation means two writers disagree about the sequence an allele sits on (see mavedb.lib.variant_translations.AlleleIdentityConflictError).';


--
-- Name: uq_clinvar_allele_links_live; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_clinvar_allele_links_live ON public.clinvar_allele_links USING btree (allele_id, clinvar_control_id) WHERE (valid_to IS NULL);


--
-- Name: uq_gnomad_allele_links_live; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_gnomad_allele_links_live ON public.gnomad_allele_links USING btree (allele_id) WHERE (valid_to IS NULL);


--
-- Name: uq_mapping_record_alleles_live; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_mapping_record_alleles_live ON public.mapping_record_alleles USING btree (mapping_record_id, allele_id) WHERE (valid_to IS NULL);


--
-- Name: uq_mapping_record_alleles_live_authoritative; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_mapping_record_alleles_live_authoritative ON public.mapping_record_alleles USING btree (mapping_record_id) WHERE (is_authoritative AND (valid_to IS NULL));


--
-- Name: uq_mapping_records_current; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_mapping_records_current ON public.mapping_records USING btree (variant_id) WHERE (valid_to IS NULL);


--
-- Name: uq_mapping_records_id_score_set; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_mapping_records_id_score_set ON public.mapping_records USING btree (id, score_set_id);


--
-- Name: uq_projection_groups_id_mapping_record; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_projection_groups_id_mapping_record ON public.projection_groups USING btree (id, mapping_record_id);


--
-- Name: uq_variants_id_scoreset; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_variants_id_scoreset ON public.variants USING btree (id, scoreset_id);


--
-- Name: uq_vep_allele_consequences_live; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_vep_allele_consequences_live ON public.vep_allele_consequences USING btree (allele_id) WHERE (valid_to IS NULL);


--
-- Name: access_keys access_key_roles_fk_constraint; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_keys
    ADD CONSTRAINT access_key_roles_fk_constraint FOREIGN KEY (role_id) REFERENCES public.roles(id);


--
-- Name: access_keys access_keys_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_keys
    ADD CONSTRAINT access_keys_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: annotation_event annotation_event_allele_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.annotation_event
    ADD CONSTRAINT annotation_event_allele_id_fkey FOREIGN KEY (allele_id) REFERENCES public.alleles(id) ON DELETE RESTRICT;


--
-- Name: annotation_event annotation_event_job_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.annotation_event
    ADD CONSTRAINT annotation_event_job_run_id_fkey FOREIGN KEY (job_run_id) REFERENCES public.job_runs(id) ON DELETE SET NULL;


--
-- Name: annotation_event annotation_event_score_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.annotation_event
    ADD CONSTRAINT annotation_event_score_set_id_fkey FOREIGN KEY (score_set_id) REFERENCES public.scoresets(id) ON DELETE SET NULL;


--
-- Name: annotation_event annotation_event_variant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.annotation_event
    ADD CONSTRAINT annotation_event_variant_id_fkey FOREIGN KEY (variant_id) REFERENCES public.variants(id) ON DELETE CASCADE;


--
-- Name: calibration_controls calibration_controls_calibration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.calibration_controls
    ADD CONSTRAINT calibration_controls_calibration_id_fkey FOREIGN KEY (calibration_id) REFERENCES public.score_calibrations(id) ON DELETE CASCADE;


--
-- Name: calibration_controls calibration_controls_created_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.calibration_controls
    ADD CONSTRAINT calibration_controls_created_by_id_fkey FOREIGN KEY (created_by_id) REFERENCES public.users(id);


--
-- Name: calibration_controls calibration_controls_modified_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.calibration_controls
    ADD CONSTRAINT calibration_controls_modified_by_id_fkey FOREIGN KEY (modified_by_id) REFERENCES public.users(id);


--
-- Name: calibration_controls calibration_controls_variant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.calibration_controls
    ADD CONSTRAINT calibration_controls_variant_id_fkey FOREIGN KEY (variant_id) REFERENCES public.variants(id);


--
-- Name: collection_experiments collection_experiments_collection_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collection_experiments
    ADD CONSTRAINT collection_experiments_collection_id_fkey FOREIGN KEY (collection_id) REFERENCES public.collections(id);


--
-- Name: collection_experiments collection_experiments_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collection_experiments
    ADD CONSTRAINT collection_experiments_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(id);


--
-- Name: collection_score_sets collection_score_sets_collection_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collection_score_sets
    ADD CONSTRAINT collection_score_sets_collection_id_fkey FOREIGN KEY (collection_id) REFERENCES public.collections(id);


--
-- Name: collection_score_sets collection_score_sets_score_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collection_score_sets
    ADD CONSTRAINT collection_score_sets_score_set_id_fkey FOREIGN KEY (score_set_id) REFERENCES public.scoresets(id);


--
-- Name: collection_user_associations collection_user_associations_collection_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collection_user_associations
    ADD CONSTRAINT collection_user_associations_collection_id_fkey FOREIGN KEY (collection_id) REFERENCES public.collections(id);


--
-- Name: collection_user_associations collection_user_associations_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collection_user_associations
    ADD CONSTRAINT collection_user_associations_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: collections collections_created_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collections
    ADD CONSTRAINT collections_created_by_id_fkey FOREIGN KEY (created_by_id) REFERENCES public.users(id);


--
-- Name: collections collections_modified_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collections
    ADD CONSTRAINT collections_modified_by_id_fkey FOREIGN KEY (modified_by_id) REFERENCES public.users(id);


--
-- Name: ensembl_offsets ensembl_offsets_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ensembl_offsets
    ADD CONSTRAINT ensembl_offsets_identifier_id_fkey FOREIGN KEY (identifier_id) REFERENCES public.ensembl_identifiers(id);


--
-- Name: ensembl_offsets ensembl_offsets_target_gene_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ensembl_offsets
    ADD CONSTRAINT ensembl_offsets_target_gene_id_fkey FOREIGN KEY (target_gene_id) REFERENCES public.target_genes(id);


--
-- Name: experiment_contributors experiment_contributors_contributor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_contributors
    ADD CONSTRAINT experiment_contributors_contributor_id_fkey FOREIGN KEY (contributor_id) REFERENCES public.contributors(id);


--
-- Name: experiment_contributors experiment_contributors_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_contributors
    ADD CONSTRAINT experiment_contributors_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(id);


--
-- Name: experiment_controlled_keywords experiment_controlled_keywords_controlled_keyword_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_controlled_keywords
    ADD CONSTRAINT experiment_controlled_keywords_controlled_keyword_id_fkey FOREIGN KEY (controlled_keyword_id) REFERENCES public.controlled_keywords(id);


--
-- Name: experiment_controlled_keywords experiment_controlled_keywords_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_controlled_keywords
    ADD CONSTRAINT experiment_controlled_keywords_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(id);


--
-- Name: experiment_doi_identifiers experiment_doi_identifiers_doi_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_doi_identifiers
    ADD CONSTRAINT experiment_doi_identifiers_doi_identifier_id_fkey FOREIGN KEY (doi_identifier_id) REFERENCES public.doi_identifiers(id);


--
-- Name: experiment_doi_identifiers experiment_doi_identifiers_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_doi_identifiers
    ADD CONSTRAINT experiment_doi_identifiers_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(id);


--
-- Name: experiment_keywords experiment_keywords_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_keywords
    ADD CONSTRAINT experiment_keywords_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(id);


--
-- Name: experiment_keywords experiment_keywords_keyword_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_keywords
    ADD CONSTRAINT experiment_keywords_keyword_id_fkey FOREIGN KEY (keyword_id) REFERENCES public.keywords(id);


--
-- Name: experiment_publication_identifiers experiment_pub_identifiers_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_publication_identifiers
    ADD CONSTRAINT experiment_pub_identifiers_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(id);


--
-- Name: experiment_publication_identifiers experiment_pub_identifiers_publication_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_publication_identifiers
    ADD CONSTRAINT experiment_pub_identifiers_publication_identifier_id_fkey FOREIGN KEY (publication_identifier_id) REFERENCES public.publication_identifiers(id);


--
-- Name: experiment_set_contributors experiment_set_contributors_contributor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_contributors
    ADD CONSTRAINT experiment_set_contributors_contributor_id_fkey FOREIGN KEY (contributor_id) REFERENCES public.contributors(id);


--
-- Name: experiment_set_contributors experiment_set_contributors_experiment_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_contributors
    ADD CONSTRAINT experiment_set_contributors_experiment_set_id_fkey FOREIGN KEY (experiment_set_id) REFERENCES public.experiment_sets(id);


--
-- Name: experiment_set_doi_identifiers experiment_set_doi_identifiers_doi_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_doi_identifiers
    ADD CONSTRAINT experiment_set_doi_identifiers_doi_identifier_id_fkey FOREIGN KEY (doi_identifier_id) REFERENCES public.doi_identifiers(id);


--
-- Name: experiment_set_doi_identifiers experiment_set_doi_identifiers_experiment_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_doi_identifiers
    ADD CONSTRAINT experiment_set_doi_identifiers_experiment_set_id_fkey FOREIGN KEY (experiment_set_id) REFERENCES public.experiment_sets(id);


--
-- Name: experiment_set_keywords experiment_set_keywords_experiment_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_keywords
    ADD CONSTRAINT experiment_set_keywords_experiment_set_id_fkey FOREIGN KEY (experiment_set_id) REFERENCES public.experiment_sets(id);


--
-- Name: experiment_set_keywords experiment_set_keywords_keyword_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_keywords
    ADD CONSTRAINT experiment_set_keywords_keyword_id_fkey FOREIGN KEY (keyword_id) REFERENCES public.keywords(id);


--
-- Name: experiment_set_publication_identifiers experiment_set_pub_identifiers_experiment_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_publication_identifiers
    ADD CONSTRAINT experiment_set_pub_identifiers_experiment_set_id_fkey FOREIGN KEY (experiment_set_id) REFERENCES public.experiment_sets(id);


--
-- Name: experiment_set_publication_identifiers experiment_set_pub_identifiers_publication_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_publication_identifiers
    ADD CONSTRAINT experiment_set_pub_identifiers_publication_identifier_id_fkey FOREIGN KEY (publication_identifier_id) REFERENCES public.publication_identifiers(id);


--
-- Name: experiment_set_sra_identifiers experiment_set_sra_identifiers_experiment_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_sra_identifiers
    ADD CONSTRAINT experiment_set_sra_identifiers_experiment_set_id_fkey FOREIGN KEY (experiment_set_id) REFERENCES public.experiment_sets(id);


--
-- Name: experiment_set_sra_identifiers experiment_set_sra_identifiers_sra_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_set_sra_identifiers
    ADD CONSTRAINT experiment_set_sra_identifiers_sra_identifier_id_fkey FOREIGN KEY (sra_identifier_id) REFERENCES public.sra_identifiers(id);


--
-- Name: experiment_sets experiment_sets_created_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_sets
    ADD CONSTRAINT experiment_sets_created_by_id_fkey FOREIGN KEY (created_by_id) REFERENCES public.users(id);


--
-- Name: experiment_sets experiment_sets_modified_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_sets
    ADD CONSTRAINT experiment_sets_modified_by_id_fkey FOREIGN KEY (modified_by_id) REFERENCES public.users(id);


--
-- Name: experiment_sra_identifiers experiment_sra_identifiers_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_sra_identifiers
    ADD CONSTRAINT experiment_sra_identifiers_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(id);


--
-- Name: experiment_sra_identifiers experiment_sra_identifiers_sra_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_sra_identifiers
    ADD CONSTRAINT experiment_sra_identifiers_sra_identifier_id_fkey FOREIGN KEY (sra_identifier_id) REFERENCES public.sra_identifiers(id);


--
-- Name: experiments experiments_created_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiments_created_by_id_fkey FOREIGN KEY (created_by_id) REFERENCES public.users(id);


--
-- Name: experiments experiments_experiment_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiments_experiment_set_id_fkey FOREIGN KEY (experiment_set_id) REFERENCES public.experiment_sets(id);


--
-- Name: experiments experiments_modified_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiments_modified_by_id_fkey FOREIGN KEY (modified_by_id) REFERENCES public.users(id);


--
-- Name: clinvar_allele_links fk_clinvar_allele_links_allele_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clinvar_allele_links
    ADD CONSTRAINT fk_clinvar_allele_links_allele_id FOREIGN KEY (allele_id) REFERENCES public.alleles(id) ON DELETE RESTRICT;


--
-- Name: clinvar_allele_links fk_clinvar_allele_links_clinvar_control_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clinvar_allele_links
    ADD CONSTRAINT fk_clinvar_allele_links_clinvar_control_id FOREIGN KEY (clinvar_control_id) REFERENCES public.clinvar_controls(id) ON DELETE RESTRICT;


--
-- Name: gnomad_allele_links fk_gnomad_allele_links_allele_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gnomad_allele_links
    ADD CONSTRAINT fk_gnomad_allele_links_allele_id FOREIGN KEY (allele_id) REFERENCES public.alleles(id) ON DELETE RESTRICT;


--
-- Name: gnomad_allele_links fk_gnomad_allele_links_gnomad_variant_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gnomad_allele_links
    ADD CONSTRAINT fk_gnomad_allele_links_gnomad_variant_id FOREIGN KEY (gnomad_variant_id) REFERENCES public.gnomad_variants(id) ON DELETE RESTRICT;


--
-- Name: mapped_variants fk_mapped_variants_target_gene_mapping_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapped_variants
    ADD CONSTRAINT fk_mapped_variants_target_gene_mapping_id FOREIGN KEY (target_gene_mapping_id) REFERENCES public.target_gene_mappings(id);


--
-- Name: mapping_record_alleles fk_mapping_record_alleles_allele_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapping_record_alleles
    ADD CONSTRAINT fk_mapping_record_alleles_allele_id FOREIGN KEY (allele_id) REFERENCES public.alleles(id) ON DELETE RESTRICT;


--
-- Name: mapping_record_alleles fk_mapping_record_alleles_mapping_record_score_set; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapping_record_alleles
    ADD CONSTRAINT fk_mapping_record_alleles_mapping_record_score_set FOREIGN KEY (mapping_record_id, score_set_id) REFERENCES public.mapping_records(id, score_set_id) ON DELETE CASCADE;


--
-- Name: mapping_record_alleles fk_mapping_record_alleles_projection_group; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapping_record_alleles
    ADD CONSTRAINT fk_mapping_record_alleles_projection_group FOREIGN KEY (projection_group, mapping_record_id) REFERENCES public.projection_groups(id, mapping_record_id) ON DELETE CASCADE;


--
-- Name: mapping_records fk_mapping_records_target_gene_mapping_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapping_records
    ADD CONSTRAINT fk_mapping_records_target_gene_mapping_id FOREIGN KEY (target_gene_mapping_id) REFERENCES public.target_gene_mappings(id);


--
-- Name: mapping_records fk_mapping_records_variant_score_set; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapping_records
    ADD CONSTRAINT fk_mapping_records_variant_score_set FOREIGN KEY (variant_id, score_set_id) REFERENCES public.variants(id, scoreset_id) ON DELETE CASCADE;


--
-- Name: projection_groups fk_projection_groups_mapping_record_score_set; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projection_groups
    ADD CONSTRAINT fk_projection_groups_mapping_record_score_set FOREIGN KEY (mapping_record_id, score_set_id) REFERENCES public.mapping_records(id, score_set_id) ON DELETE CASCADE;


--
-- Name: score_calibrations fk_score_calibrations_disease_term_id_mondo_terms; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibrations
    ADD CONSTRAINT fk_score_calibrations_disease_term_id_mondo_terms FOREIGN KEY (disease_term_id) REFERENCES public.mondo_terms(id);


--
-- Name: target_gene_mappings fk_target_gene_mappings_job_run_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_gene_mappings
    ADD CONSTRAINT fk_target_gene_mappings_job_run_id FOREIGN KEY (job_run_id) REFERENCES public.job_runs(id) ON DELETE SET NULL;


--
-- Name: vep_allele_consequences fk_vep_allele_consequences_allele_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vep_allele_consequences
    ADD CONSTRAINT fk_vep_allele_consequences_allele_id FOREIGN KEY (allele_id) REFERENCES public.alleles(id) ON DELETE RESTRICT;


--
-- Name: gnomad_variants_mapped_variants gnomad_variants_mapped_variants_gnomad_variant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gnomad_variants_mapped_variants
    ADD CONSTRAINT gnomad_variants_mapped_variants_gnomad_variant_id_fkey FOREIGN KEY (gnomad_variant_id) REFERENCES public.gnomad_variants(id);


--
-- Name: gnomad_variants_mapped_variants gnomad_variants_mapped_variants_mapped_variant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gnomad_variants_mapped_variants
    ADD CONSTRAINT gnomad_variants_mapped_variants_mapped_variant_id_fkey FOREIGN KEY (mapped_variant_id) REFERENCES public.mapped_variants(id);


--
-- Name: job_dependencies job_dependencies_depends_on_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_dependencies
    ADD CONSTRAINT job_dependencies_depends_on_job_id_fkey FOREIGN KEY (depends_on_job_id) REFERENCES public.job_runs(id) ON DELETE CASCADE;


--
-- Name: job_dependencies job_dependencies_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_dependencies
    ADD CONSTRAINT job_dependencies_id_fkey FOREIGN KEY (id) REFERENCES public.job_runs(id) ON DELETE CASCADE;


--
-- Name: job_runs job_runs_pipeline_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_runs
    ADD CONSTRAINT job_runs_pipeline_id_fkey FOREIGN KEY (pipeline_id) REFERENCES public.pipelines(id) ON DELETE SET NULL;


--
-- Name: mapped_variants_clinical_controls mapped_variants_clinical_controls_clinical_control_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapped_variants_clinical_controls
    ADD CONSTRAINT mapped_variants_clinical_controls_clinical_control_id_fkey FOREIGN KEY (clinical_control_id) REFERENCES public.clinvar_controls(id);


--
-- Name: mapped_variants_clinical_controls mapped_variants_clinical_controls_mapped_variant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapped_variants_clinical_controls
    ADD CONSTRAINT mapped_variants_clinical_controls_mapped_variant_id_fkey FOREIGN KEY (mapped_variant_id) REFERENCES public.mapped_variants(id);


--
-- Name: mapped_variants mapped_variants_variant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mapped_variants
    ADD CONSTRAINT mapped_variants_variant_id_fkey FOREIGN KEY (variant_id) REFERENCES public.variants(id);


--
-- Name: pipelines pipelines_created_by_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT pipelines_created_by_user_id_fkey FOREIGN KEY (created_by_user_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: refseq_offsets refseq_offsets_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refseq_offsets
    ADD CONSTRAINT refseq_offsets_identifier_id_fkey FOREIGN KEY (identifier_id) REFERENCES public.refseq_identifiers(id);


--
-- Name: refseq_offsets refseq_offsets_target_gene_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refseq_offsets
    ADD CONSTRAINT refseq_offsets_target_gene_id_fkey FOREIGN KEY (target_gene_id) REFERENCES public.target_genes(id);


--
-- Name: score_calibration_functional_classification_variants score_calibration_functional__functional_classification_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_functional_classification_variants
    ADD CONSTRAINT score_calibration_functional__functional_classification_id_fkey FOREIGN KEY (functional_classification_id) REFERENCES public.score_calibration_functional_classifications(id);


--
-- Name: score_calibration_functional_classifications score_calibration_functional_classi_acmg_classification_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_functional_classifications
    ADD CONSTRAINT score_calibration_functional_classi_acmg_classification_id_fkey FOREIGN KEY (acmg_classification_id) REFERENCES public.acmg_classifications(id);


--
-- Name: score_calibration_functional_classifications score_calibration_functional_classification_calibration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_functional_classifications
    ADD CONSTRAINT score_calibration_functional_classification_calibration_id_fkey FOREIGN KEY (calibration_id) REFERENCES public.score_calibrations(id);


--
-- Name: score_calibration_functional_classification_variants score_calibration_functional_classification_var_variant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_functional_classification_variants
    ADD CONSTRAINT score_calibration_functional_classification_var_variant_id_fkey FOREIGN KEY (variant_id) REFERENCES public.variants(id);


--
-- Name: score_calibration_publication_identifiers score_calibration_publication_id_publication_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_publication_identifiers
    ADD CONSTRAINT score_calibration_publication_id_publication_identifier_id_fkey FOREIGN KEY (publication_identifier_id) REFERENCES public.publication_identifiers(id);


--
-- Name: score_calibration_publication_identifiers score_calibration_publication_identif_score_calibration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibration_publication_identifiers
    ADD CONSTRAINT score_calibration_publication_identif_score_calibration_id_fkey FOREIGN KEY (score_calibration_id) REFERENCES public.score_calibrations(id);


--
-- Name: score_calibrations score_calibrations_created_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibrations
    ADD CONSTRAINT score_calibrations_created_by_id_fkey FOREIGN KEY (created_by_id) REFERENCES public.users(id);


--
-- Name: score_calibrations score_calibrations_modified_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibrations
    ADD CONSTRAINT score_calibrations_modified_by_id_fkey FOREIGN KEY (modified_by_id) REFERENCES public.users(id);


--
-- Name: score_calibrations score_calibrations_score_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.score_calibrations
    ADD CONSTRAINT score_calibrations_score_set_id_fkey FOREIGN KEY (score_set_id) REFERENCES public.scoresets(id);


--
-- Name: scoreset_contributors scoreset_contributors_contributor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_contributors
    ADD CONSTRAINT scoreset_contributors_contributor_id_fkey FOREIGN KEY (contributor_id) REFERENCES public.contributors(id);


--
-- Name: scoreset_contributors scoreset_contributors_scoreset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_contributors
    ADD CONSTRAINT scoreset_contributors_scoreset_id_fkey FOREIGN KEY (scoreset_id) REFERENCES public.scoresets(id);


--
-- Name: scoreset_doi_identifiers scoreset_doi_identifiers_doi_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_doi_identifiers
    ADD CONSTRAINT scoreset_doi_identifiers_doi_identifier_id_fkey FOREIGN KEY (doi_identifier_id) REFERENCES public.doi_identifiers(id);


--
-- Name: scoreset_doi_identifiers scoreset_doi_identifiers_scoreset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_doi_identifiers
    ADD CONSTRAINT scoreset_doi_identifiers_scoreset_id_fkey FOREIGN KEY (scoreset_id) REFERENCES public.scoresets(id);


--
-- Name: scoreset_keywords scoreset_keywords_keyword_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_keywords
    ADD CONSTRAINT scoreset_keywords_keyword_id_fkey FOREIGN KEY (keyword_id) REFERENCES public.keywords(id);


--
-- Name: scoreset_keywords scoreset_keywords_scoreset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_keywords
    ADD CONSTRAINT scoreset_keywords_scoreset_id_fkey FOREIGN KEY (scoreset_id) REFERENCES public.scoresets(id);


--
-- Name: scoreset_meta_analysis_sources scoreset_meta_analysis_sources_meta_analysis_scoreset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_meta_analysis_sources
    ADD CONSTRAINT scoreset_meta_analysis_sources_meta_analysis_scoreset_id_fkey FOREIGN KEY (meta_analysis_scoreset_id) REFERENCES public.scoresets(id);


--
-- Name: scoreset_meta_analysis_sources scoreset_meta_analysis_sources_source_scoreset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_meta_analysis_sources
    ADD CONSTRAINT scoreset_meta_analysis_sources_source_scoreset_id_fkey FOREIGN KEY (source_scoreset_id) REFERENCES public.scoresets(id);


--
-- Name: scoreset_publication_identifiers scoreset_pub_identifiers_publication_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_publication_identifiers
    ADD CONSTRAINT scoreset_pub_identifiers_publication_identifier_id_fkey FOREIGN KEY (publication_identifier_id) REFERENCES public.publication_identifiers(id);


--
-- Name: scoreset_publication_identifiers scoreset_pub_identifiers_scoreset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_publication_identifiers
    ADD CONSTRAINT scoreset_pub_identifiers_scoreset_id_fkey FOREIGN KEY (scoreset_id) REFERENCES public.scoresets(id);


--
-- Name: scoreset_sra_identifiers scoreset_sra_identifiers_scoreset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_sra_identifiers
    ADD CONSTRAINT scoreset_sra_identifiers_scoreset_id_fkey FOREIGN KEY (scoreset_id) REFERENCES public.scoresets(id);


--
-- Name: scoreset_sra_identifiers scoreset_sra_identifiers_sra_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoreset_sra_identifiers
    ADD CONSTRAINT scoreset_sra_identifiers_sra_identifier_id_fkey FOREIGN KEY (sra_identifier_id) REFERENCES public.sra_identifiers(id);


--
-- Name: scoresets scoresets_created_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoresets
    ADD CONSTRAINT scoresets_created_by_id_fkey FOREIGN KEY (created_by_id) REFERENCES public.users(id);


--
-- Name: scoresets scoresets_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoresets
    ADD CONSTRAINT scoresets_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(id);


--
-- Name: scoresets scoresets_licence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoresets
    ADD CONSTRAINT scoresets_licence_id_fkey FOREIGN KEY (licence_id) REFERENCES public.licenses(id);


--
-- Name: scoresets scoresets_modified_by_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoresets
    ADD CONSTRAINT scoresets_modified_by_id_fkey FOREIGN KEY (modified_by_id) REFERENCES public.users(id);


--
-- Name: scoresets scoresets_replaces_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoresets
    ADD CONSTRAINT scoresets_replaces_id_fkey FOREIGN KEY (replaces_id) REFERENCES public.scoresets(id);


--
-- Name: target_gene_mappings target_gene_mappings_target_gene_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_gene_mappings
    ADD CONSTRAINT target_gene_mappings_target_gene_id_fkey FOREIGN KEY (target_gene_id) REFERENCES public.target_genes(id);


--
-- Name: target_genes target_genes_accession_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_genes
    ADD CONSTRAINT target_genes_accession_id_fkey FOREIGN KEY (accession_id) REFERENCES public.target_accessions(id);


--
-- Name: target_genes target_genes_scoreset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_genes
    ADD CONSTRAINT target_genes_scoreset_id_fkey FOREIGN KEY (scoreset_id) REFERENCES public.scoresets(id);


--
-- Name: target_genes target_genes_wt_sequence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_genes
    ADD CONSTRAINT target_genes_wt_sequence_id_fkey FOREIGN KEY (target_sequence_id) REFERENCES public.target_sequences(id);


--
-- Name: target_sequences target_sequence_taxonomy_id_foreign_key_constraint; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_sequences
    ADD CONSTRAINT target_sequence_taxonomy_id_foreign_key_constraint FOREIGN KEY (taxonomy_id) REFERENCES public.taxonomies(id);


--
-- Name: taxonomies taxonomies_genome_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.taxonomies
    ADD CONSTRAINT taxonomies_genome_identifier_id_fkey FOREIGN KEY (genome_identifier_id) REFERENCES public.genome_identifiers(id);


--
-- Name: uniprot_offsets uniprot_offsets_identifier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.uniprot_offsets
    ADD CONSTRAINT uniprot_offsets_identifier_id_fkey FOREIGN KEY (identifier_id) REFERENCES public.uniprot_identifiers(id);


--
-- Name: uniprot_offsets uniprot_offsets_target_gene_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.uniprot_offsets
    ADD CONSTRAINT uniprot_offsets_target_gene_id_fkey FOREIGN KEY (target_gene_id) REFERENCES public.target_genes(id);


--
-- Name: users_roles users_roles_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users_roles
    ADD CONSTRAINT users_roles_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id);


--
-- Name: users_roles users_roles_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users_roles
    ADD CONSTRAINT users_roles_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: variant_annotation_status variant_annotation_status_job_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variant_annotation_status
    ADD CONSTRAINT variant_annotation_status_job_run_id_fkey FOREIGN KEY (job_run_id) REFERENCES public.job_runs(id) ON DELETE SET NULL;


--
-- Name: variant_annotation_status variant_annotation_status_variant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variant_annotation_status
    ADD CONSTRAINT variant_annotation_status_variant_id_fkey FOREIGN KEY (variant_id) REFERENCES public.variants(id) ON DELETE CASCADE;


--
-- Name: variants variants_scoreset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variants
    ADD CONSTRAINT variants_scoreset_id_fkey FOREIGN KEY (scoreset_id) REFERENCES public.scoresets(id);


--
-- PostgreSQL database dump complete
--



--
-- Seed vocabulary
--

INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":1,"key":"Variant Library Creation Method","label":"Endogenous locus library method","system":null,"special":false,"description":"A methodology for generating a variant library at an endogenous locus","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":2,"key":"Variant Library Creation Method","label":"In vitro construct library method","system":null,"special":false,"description":"A methodology for generating a variant library at an endogenous locus","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":3,"key":"Variant Library Creation Method","label":"Other","system":null,"special":false,"description":"A methodology for generating a variant library at an endogenous locus","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":4,"key":"Endogenous Locus Library Method System","label":"SpCas9","system":null,"special":false,"description":"The system used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":5,"key":"Endogenous Locus Library Method System","label":"SaCas9","system":null,"special":false,"description":"The system used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":6,"key":"Endogenous Locus Library Method System","label":"AsCas12a","system":null,"special":false,"description":"The system used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":7,"key":"Endogenous Locus Library Method System","label":"RfsCas13d","system":null,"special":false,"description":"The system used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":8,"key":"Endogenous Locus Library Method System","label":"Other","system":null,"special":false,"description":"The system used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":9,"key":"Endogenous Locus Library Method Mechanism","label":"Nuclease","system":null,"special":false,"description":"The functional mechanism of the library generation method","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":10,"key":"Endogenous Locus Library Method Mechanism","label":"Base editor","system":null,"special":false,"description":"The functional mechanism of the library generation method","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":11,"key":"Endogenous Locus Library Method Mechanism","label":"Prime editor","system":null,"special":false,"description":"The functional mechanism of the library generation method","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":12,"key":"Endogenous Locus Library Method Mechanism","label":"Other","system":null,"special":false,"description":"The functional mechanism of the library generation method","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":13,"key":"In Vitro Construct Library Method System","label":"Oligo-directed mutagenic PCR","system":null,"special":false,"description":"The type of method used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":14,"key":"In Vitro Construct Library Method System","label":"Error-prone PCR","system":null,"special":false,"description":"The type of method used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":15,"key":"In Vitro Construct Library Method System","label":"Nicking mutagenesis","system":null,"special":false,"description":"The type of method used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":16,"key":"In Vitro Construct Library Method System","label":"Microarray synthesis","system":null,"special":false,"description":"The type of method used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":17,"key":"In Vitro Construct Library Method System","label":"Site-directed mutagenesis","system":null,"special":false,"description":"The type of method used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":18,"key":"In Vitro Construct Library Method System","label":"Doped oligo synthesis","system":null,"special":false,"description":"The type of method used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":19,"key":"In Vitro Construct Library Method System","label":"Oligo pool synthesis","system":null,"special":false,"description":"The type of method used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":20,"key":"In Vitro Construct Library Method System","label":"Proprietary method","system":null,"special":false,"description":"The type of method used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":21,"key":"In Vitro Construct Library Method System","label":"Other","system":null,"special":false,"description":"The type of method used to generate the library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":22,"key":"In Vitro Construct Library Method Mechanism","label":"Native locus replacement","system":null,"special":false,"description":"The mechanism for integration or expression of an exogenous construct","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":23,"key":"In Vitro Construct Library Method Mechanism","label":"Extra-local construct insertion","system":null,"special":false,"description":"The mechanism for integration or expression of an exogenous construct","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":24,"key":"In Vitro Construct Library Method Mechanism","label":"Random locus viral integration","system":null,"special":false,"description":"The mechanism for integration or expression of an exogenous construct","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":25,"key":"In Vitro Construct Library Method Mechanism","label":"Episomal delivery","system":null,"special":false,"description":"The mechanism for integration or expression of an exogenous construct","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":26,"key":"In Vitro Construct Library Method Mechanism","label":"Plasmid (not integrated)","system":null,"special":false,"description":"The mechanism for integration or expression of an exogenous construct","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":27,"key":"In Vitro Construct Library Method Mechanism","label":"Transfection of RNA","system":null,"special":false,"description":"The mechanism for integration or expression of an exogenous construct","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":28,"key":"In Vitro Construct Library Method Mechanism","label":"Other","system":null,"special":false,"description":"The mechanism for integration or expression of an exogenous construct","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":29,"key":"Delivery Method","label":"Electroporation","system":null,"special":false,"description":"How the variant library was delivered to the model system for phenotype evaluation.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":30,"key":"Delivery Method","label":"Nucleofection","system":null,"special":false,"description":"How the variant library was delivered to the model system for phenotype evaluation.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":31,"key":"Delivery Method","label":"Chemical-based transfection","system":null,"special":false,"description":"How the variant library was delivered to the model system for phenotype evaluation.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":32,"key":"Delivery Method","label":"Adeno-associated virus transduction","system":null,"special":false,"description":"How the variant library was delivered to the model system for phenotype evaluation.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":33,"key":"Delivery Method","label":"Lentivirus transduction","system":null,"special":false,"description":"How the variant library was delivered to the model system for phenotype evaluation.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":34,"key":"Delivery Method","label":"Chemical or heat shock transformation","system":null,"special":false,"description":"How the variant library was delivered to the model system for phenotype evaluation.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":35,"key":"Delivery Method","label":"Other","system":null,"special":false,"description":"How the variant library was delivered to the model system for phenotype evaluation.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":36,"key":"Phenotypic Assay Dimensionality","label":"Single-dimensional data","system":null,"special":false,"description":"A coding defining the dimensionality of the assay as single or multiple functional readouts.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":37,"key":"Phenotypic Assay Dimensionality","label":"High-dimensional data","system":null,"special":false,"description":"A coding defining the dimensionality of the assay as single or multiple functional readouts.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":38,"key":"Phenotypic Assay Dimensionality","label":"Combined functional data","system":null,"special":false,"description":"A coding defining the dimensionality of the assay as single or multiple functional readouts.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":39,"key":"Phenotypic Assay Dimensionality","label":"Other","system":null,"special":false,"description":"A coding defining the dimensionality of the assay as single or multiple functional readouts.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":40,"key":"Phenotypic Assay Method","label":"Promoter activity detection by reporter gene assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":41,"key":"Phenotypic Assay Method","label":"Bulk RNA-sequencing","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":42,"key":"Phenotypic Assay Method","label":"Single-cell RNA sequencing assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":43,"key":"Phenotypic Assay Method","label":"Fluorescence in-situ hybridization (FISH) assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":44,"key":"Phenotypic Assay Method","label":"Flow cytometry assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":45,"key":"Phenotypic Assay Method","label":"Imaging mass cytometry assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":46,"key":"Phenotypic Assay Method","label":"Systematic evolution of ligands by exponential enrichment assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":47,"key":"Phenotypic Assay Method","label":"Single cell imaging","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":48,"key":"Phenotypic Assay Method","label":"Multiplexed fluorescent antibody imaging","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":49,"key":"Phenotypic Assay Method","label":"Binding assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":50,"key":"Phenotypic Assay Method","label":"Cell proliferation assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":51,"key":"Phenotypic Assay Method","label":"Survival assessment assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":52,"key":"Phenotypic Assay Method","label":"Other","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":53,"key":"Phenotypic Assay Model System","label":"Immortalized human cells","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":54,"key":"Phenotypic Assay Model System","label":"Murine primary cells","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":55,"key":"Phenotypic Assay Model System","label":"Induced pluripotent stem cells from human male","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":56,"key":"Phenotypic Assay Model System","label":"Induced pluripotent stem cells from human female","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":57,"key":"Phenotypic Assay Model System","label":"Patient derived primary cells (e.g. T-cells, adipocytes)","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":58,"key":"Phenotypic Assay Model System","label":"Yeast","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":59,"key":"Phenotypic Assay Model System","label":"Bacteria","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":60,"key":"Phenotypic Assay Model System","label":"Bacteriophage","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":61,"key":"Phenotypic Assay Model System","label":"Molecular display","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":62,"key":"Phenotypic Assay Model System","label":"Other","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":63,"key":"Phenotypic Assay Profiling Strategy","label":"Direct sequencing","system":null,"special":false,"description":"The strategy used to profile the variant library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":64,"key":"Phenotypic Assay Profiling Strategy","label":"Shotgun sequencing","system":null,"special":false,"description":"The strategy used to profile the variant library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":65,"key":"Phenotypic Assay Profiling Strategy","label":"Barcode sequencing","system":null,"special":false,"description":"The strategy used to profile the variant library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":66,"key":"Phenotypic Assay Profiling Strategy","label":"Other","system":null,"special":false,"description":"The strategy used to profile the variant library","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":67,"key":"Phenotypic Assay Sequencing Read Type","label":"Single-segment (short read)","system":null,"special":false,"description":"The sequencing method used","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":68,"key":"Phenotypic Assay Sequencing Read Type","label":"Single-segment (long read)","system":null,"special":false,"description":"The sequencing method used","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":69,"key":"Phenotypic Assay Sequencing Read Type","label":"Multi-segment","system":null,"special":false,"description":"The sequencing method used","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":70,"key":"Phenotypic Assay Sequencing Read Type","label":"Other","system":null,"special":false,"description":"The sequencing method used","creation_date":"2024-08-28","modification_date":"2024-08-28","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":71,"key":"Phenotypic Assay Mechanism","label":"Other","system":null,"special":false,"description":"The Gene Ontology (GO) is a structured, standardized representation of biological knowledge.","creation_date":"2025-09-15","modification_date":"2025-09-15","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":72,"key":"Phenotypic Assay Mechanism","label":"Loss of function","system":null,"special":false,"description":"The Gene Ontology (GO) is a structured, standardized representation of biological knowledge.","creation_date":"2025-09-15","modification_date":"2025-09-15","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":73,"key":"Phenotypic Assay Mechanism","label":"Gain of function","system":null,"special":false,"description":"The Gene Ontology (GO) is a structured, standardized representation of biological knowledge.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":74,"key":"Phenotypic Assay Mechanism","label":"Dominant-negative effect","system":null,"special":false,"description":"The Gene Ontology (GO) is a structured, standardized representation of biological knowledge.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":75,"key":"Phenotypic Assay Method","label":"Cell morphology assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":76,"key":"Phenotypic Assay Method","label":"Genetic complementation","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":77,"key":"Phenotypic Assay Method","label":"Ion channel assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":78,"key":"Phenotypic Assay Method","label":"Electrophysiological method","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":79,"key":"Phenotypic Assay Method","label":"Localization assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":80,"key":"Phenotypic Assay Method","label":"One-hybrid assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":81,"key":"Phenotypic Assay Method","label":"Posttranslation modification assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":82,"key":"Phenotypic Assay Method","label":"Protein stability assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":83,"key":"Phenotypic Assay Method","label":"Reporter gene assay","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":84,"key":"Phenotypic Assay Method","label":"Homology-directed DNA repair frequency measurement","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":85,"key":"Phenotypic Assay Model System","label":"Not applicable","system":null,"special":false,"description":"The model system context that influences expression of the phenotype.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":86,"key":"Phenotypic Assay Method","label":"Ion channel assay with electrophysicological method","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":87,"key":"Phenotypic Assay Method","label":"Cell proliferation assay with genetic complementation","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":88,"key":"Phenotypic Assay Mechanism","label":"Gain or loss of function","system":null,"special":false,"description":"The Gene Ontology (GO) is a structured, standardized representation of biological knowledge.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":89,"key":"Phenotypic Assay Mechanism","label":"Loss of function or dominant-negative effect","system":null,"special":false,"description":"The Gene Ontology (GO) is a structured, standardized representation of biological knowledge.","creation_date":"2025-09-21","modification_date":"2025-09-21","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":90,"key":"Variant Library Creation Method","label":"N/A (meta-analysis)","system":null,"special":false,"description":"A methodology for generating a variant library at an endogenous locus","creation_date":"2025-09-24","modification_date":"2025-09-24","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":91,"key":"Phenotypic Assay Method","label":"Cell fitness","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-25","modification_date":"2025-09-25","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":92,"key":"Phenotypic Assay Method","label":"Direct protein function","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-25","modification_date":"2025-09-25","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":93,"key":"Phenotypic Assay Method","label":"Not applicable (trained predictor)","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-25","modification_date":"2025-09-25","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":94,"key":"Phenotypic Assay Method","label":"Reporter","system":null,"special":false,"description":"The assay method, defining the molecular properties interrogated.","creation_date":"2025-09-25","modification_date":"2025-09-25","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":95,"key":"Molecular Mechanism Assessed","label":"Catalytic and Cysteine synthase activity","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":96,"key":"Molecular Mechanism Assessed","label":"Catalytic and Hydroxymethylbilane synthase activity","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":97,"key":"Molecular Mechanism Assessed","label":"Catalytic and Lipid phosphatase activity","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":98,"key":"Molecular Mechanism Assessed","label":"Catalytic and Thiamine diphosphokinase activity","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":99,"key":"Molecular Mechanism Assessed","label":"Cellular response to cisplatin, DNA repair and Double-strand break repair via homologous recombination","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":100,"key":"Molecular Mechanism Assessed","label":"DNA damage response, signal transduction by p53 class mediator and MDM2/MDM4 family protein binding","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":101,"key":"Molecular Mechanism Assessed","label":"DNA repair and Double-strand break repair via homologous recombination","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":102,"key":"Molecular Mechanism Assessed","label":"Double-strand break repair, damage response, signal transduction by p53 class mediator and Response to etoposide","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":103,"key":"Molecular Mechanism Assessed","label":"Molecular function","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":104,"key":"Molecular Mechanism Assessed","label":"Monoatomic ion transport and Potassium channel activity","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":105,"key":"Molecular Mechanism Assessed","label":"Sodium channel activity","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":106,"key":"Molecular Mechanism Assessed","label":"Calcium-mediated signaling","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":107,"key":"Molecular Mechanism Assessed","label":"Catalytic and Gluconokinase activity","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":108,"key":"Molecular Mechanism Assessed","label":"Catalytic activity and Ornithine carbamoyltransferase activity","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":109,"key":"Molecular Mechanism Assessed","label":"Core promoter sequence-specific DNA binding","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":110,"key":"Molecular Mechanism Assessed","label":"DNA damage checkpoint signaling","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":111,"key":"Molecular Mechanism Assessed","label":"DNA and Mismatch repair","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":112,"key":"Molecular Mechanism Assessed","label":"Molecular condensate scaffold activity","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":113,"key":"Molecular Mechanism Assessed","label":"Monoatomic ion transport and Sodium ion transport","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":114,"key":"Molecular Mechanism Assessed","label":"Oxidative phosphorylation","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":115,"key":"Molecular Mechanism Assessed","label":"Protein carboxylation","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":116,"key":"Molecular Mechanism Assessed","label":"Protein glycosylation","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":117,"key":"Molecular Mechanism Assessed","label":"Protein localization to plasma membrane","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":118,"key":"Molecular Mechanism Assessed","label":"Regulation of protein stability","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
INSERT INTO public.controlled_keywords SELECT * FROM json_populate_record(NULL::public.controlled_keywords, '{"id":119,"key":"Molecular Mechanism Assessed","label":"Response to misfolded protein","system":null,"special":false,"description":"The biological or molecular process investigated by the assay","creation_date":"2025-09-29","modification_date":"2025-09-29","code":null,"version":null}');
SELECT pg_catalog.setval(pg_get_serial_sequence('public.controlled_keywords', 'id'), (SELECT coalesce(max(id), 1) FROM public.controlled_keywords));
INSERT INTO public.licenses SELECT * FROM json_populate_record(NULL::public.licenses, E'{"id":1,"long_name":"CC0 (Public domain)","short_name":"CC0","text":"Creative Commons Legal Code\\n\\nCC0 1.0 Universal\\n\\n    CREATIVE COMMONS CORPORATION IS NOT A LAW FIRM AND DOES NOT PROVIDE\\n    LEGAL SERVICES. DISTRIBUTION OF THIS DOCUMENT DOES NOT CREATE AN\\n    ATTORNEY-CLIENT RELATIONSHIP. CREATIVE COMMONS PROVIDES THIS\\n    INFORMATION ON AN \\"AS-IS\\" BASIS. CREATIVE COMMONS MAKES NO WARRANTIES\\n    REGARDING THE USE OF THIS DOCUMENT OR THE INFORMATION OR WORKS\\n    PROVIDED HEREUNDER, AND DISCLAIMS LIABILITY FOR DAMAGES RESULTING FROM\\n    THE USE OF THIS DOCUMENT OR THE INFORMATION OR WORKS PROVIDED\\n    HEREUNDER.\\n\\nStatement of Purpose\\n\\nThe laws of most jurisdictions throughout the world automatically confer\\nexclusive Copyright and Related Rights (defined below) upon the creator\\nand subsequent owner(s) (each and all, an \\"owner\\") of an original work of\\nauthorship and/or a database (each, a \\"Work\\").\\n\\nCertain owners wish to permanently relinquish those rights to a Work for\\nthe purpose of contributing to a commons of creative, cultural and\\nscientific works (\\"Commons\\") that the public can reliably and without fear\\nof later claims of infringement build upon, modify, incorporate in other\\nworks, reuse and redistribute as freely as possible in any form whatsoever\\nand for any purposes, including without limitation commercial purposes.\\nThese owners may contribute to the Commons to promote the ideal of a free\\nculture and the further production of creative, cultural and scientific\\nworks, or to gain reputation or greater distribution for their Work in\\npart through the use and efforts of others.\\n\\nFor these and/or other purposes and motivations, and without any\\nexpectation of additional consideration or compensation, the person\\nassociating CC0 with a Work (the \\"Affirmer\\"), to the extent that he or she\\nis an owner of Copyright and Related Rights in the Work, voluntarily\\nelects to apply CC0 to the Work and publicly distribute the Work under its\\nterms, with knowledge of his or her Copyright and Related Rights in the\\nWork and the meaning and intended legal effect of CC0 on those rights.\\n\\n1. Copyright and Related Rights. A Work made available under CC0 may be\\nprotected by copyright and related or neighboring rights (\\"Copyright and\\nRelated Rights\\"). Copyright and Related Rights include, but are not\\nlimited to, the following:\\n\\n  i. the right to reproduce, adapt, distribute, perform, display,\\n     communicate, and translate a Work;\\n ii. moral rights retained by the original author(s) and/or performer(s);\\niii. publicity and privacy rights pertaining to a person''s image or\\n     likeness depicted in a Work;\\n iv. rights protecting against unfair competition in regards to a Work,\\n     subject to the limitations in paragraph 4(a), below;\\n  v. rights protecting the extraction, dissemination, use and reuse of data\\n     in a Work;\\n vi. database rights (such as those arising under Directive 96/9/EC of the\\n     European Parliament and of the Council of 11 March 1996 on the legal\\n     protection of databases, and under any national implementation\\n     thereof, including any amended or successor version of such\\n     directive); and\\nvii. other similar, equivalent or corresponding rights throughout the\\n     world based on applicable law or treaty, and any national\\n     implementations thereof.\\n\\n2. Waiver. To the greatest extent permitted by, but not in contravention\\nof, applicable law, Affirmer hereby overtly, fully, permanently,\\nirrevocably and unconditionally waives, abandons, and surrenders all of\\nAffirmer''s Copyright and Related Rights and associated claims and causes\\nof action, whether now known or unknown (including existing as well as\\nfuture claims and causes of action), in the Work (i) in all territories\\nworldwide, (ii) for the maximum duration provided by applicable law or\\ntreaty (including future time extensions), (iii) in any current or future\\nmedium and for any number of copies, and (iv) for any purpose whatsoever,\\nincluding without limitation commercial, advertising or promotional\\npurposes (the \\"Waiver\\"). Affirmer makes the Waiver for the benefit of each\\nmember of the public at large and to the detriment of Affirmer''s heirs and\\nsuccessors, fully intending that such Waiver shall not be subject to\\nrevocation, rescission, cancellation, termination, or any other legal or\\nequitable action to disrupt the quiet enjoyment of the Work by the public\\nas contemplated by Affirmer''s express Statement of Purpose.\\n\\n3. Public License Fallback. Should any part of the Waiver for any reason\\nbe judged legally invalid or ineffective under applicable law, then the\\nWaiver shall be preserved to the maximum extent permitted taking into\\naccount Affirmer''s express Statement of Purpose. In addition, to the\\nextent the Waiver is so judged Affirmer hereby grants to each affected\\nperson a royalty-free, non transferable, non sublicensable, non exclusive,\\nirrevocable and unconditional license to exercise Affirmer''s Copyright and\\nRelated Rights in the Work (i) in all territories worldwide, (ii) for the\\nmaximum duration provided by applicable law or treaty (including future\\ntime extensions), (iii) in any current or future medium and for any number\\nof copies, and (iv) for any purpose whatsoever, including without\\nlimitation commercial, advertising or promotional purposes (the\\n\\"License\\"). The License shall be deemed effective as of the date CC0 was\\napplied by Affirmer to the Work. Should any part of the License for any\\nreason be judged legally invalid or ineffective under applicable law, such\\npartial invalidity or ineffectiveness shall not invalidate the remainder\\nof the License, and in such case Affirmer hereby affirms that he or she\\nwill not (i) exercise any of his or her remaining Copyright and Related\\nRights in the Work or (ii) assert any associated claims and causes of\\naction with respect to the Work, in either case contrary to Affirmer''s\\nexpress Statement of Purpose.\\n\\n4. Limitations and Disclaimers.\\n\\n a. No trademark or patent rights held by Affirmer are waived, abandoned,\\n    surrendered, licensed or otherwise affected by this document.\\n b. Affirmer offers the Work as-is and makes no representations or\\n    warranties of any kind concerning the Work, express, implied,\\n    statutory or otherwise, including without limitation warranties of\\n    title, merchantability, fitness for a particular purpose, non\\n    infringement, or the absence of latent or other defects, accuracy, or\\n    the present or absence of errors, whether or not discoverable, all to\\n    the greatest extent permissible under applicable law.\\n c. Affirmer disclaims responsibility for clearing rights of other persons\\n    that may apply to the Work or any use thereof, including without\\n    limitation any person''s Copyright and Related Rights in the Work.\\n    Further, Affirmer disclaims responsibility for obtaining any necessary\\n    consents, permissions or other rights required for any use of the\\n    Work.\\n d. Affirmer understands and acknowledges that Creative Commons is not a\\n    party to this document and has no duty or obligation with respect to\\n    this CC0 or use of the Work.","link":"https://creativecommons.org/publicdomain/zero/1.0/","version":"1.0","creation_date":"2018-06-22","modification_date":"2023-03-30","active":true}');
INSERT INTO public.licenses SELECT * FROM json_populate_record(NULL::public.licenses, E'{"id":2,"long_name":"CC BY-NC-SA 4.0 (Attribution-NonCommercial-ShareAlike)","short_name":"CC BY-NC-SA 4.0","text":"Attribution-NonCommercial-ShareAlike 4.0 International\\n\\n=======================================================================\\n\\nCreative Commons Corporation (\\"Creative Commons\\") is not a law firm and\\ndoes not provide legal services or legal advice. Distribution of\\nCreative Commons public licenses does not create a lawyer-client or\\nother relationship. Creative Commons makes its licenses and related\\ninformation available on an \\"as-is\\" basis. Creative Commons gives no\\nwarranties regarding its licenses, any material licensed under their\\nterms and conditions, or any related information. Creative Commons\\ndisclaims all liability for damages resulting from their use to the\\nfullest extent possible.\\n\\nUsing Creative Commons Public Licenses\\n\\nCreative Commons public licenses provide a standard set of terms and\\nconditions that creators and other rights holders may use to share\\noriginal works of authorship and other material subject to copyright\\nand certain other rights specified in the public license below. The\\nfollowing considerations are for informational purposes only, are not\\nexhaustive, and do not form part of our licenses.\\n\\n     Considerations for licensors: Our public licenses are\\n     intended for use by those authorized to give the public\\n     permission to use material in ways otherwise restricted by\\n     copyright and certain other rights. Our licenses are\\n     irrevocable. Licensors should read and understand the terms\\n     and conditions of the license they choose before applying it.\\n     Licensors should also secure all rights necessary before\\n     applying our licenses so that the public can reuse the\\n     material as expected. Licensors should clearly mark any\\n     material not subject to the license. This includes other CC-\\n     licensed material, or material used under an exception or\\n     limitation to copyright. More considerations for licensors:\\n\\twiki.creativecommons.org/Considerations_for_licensors\\n\\n     Considerations for the public: By using one of our public\\n     licenses, a licensor grants the public permission to use the\\n     licensed material under specified terms and conditions. If\\n     the licensor''s permission is not necessary for any reason--for\\n     example, because of any applicable exception or limitation to\\n     copyright--then that use is not regulated by the license. Our\\n     licenses grant only permissions under copyright and certain\\n     other rights that a licensor has authority to grant. Use of\\n     the licensed material may still be restricted for other\\n     reasons, including because others have copyright or other\\n     rights in the material. A licensor may make special requests,\\n     such as asking that all changes be marked or described.\\n     Although not required by our licenses, you are encouraged to\\n     respect those requests where reasonable. More considerations\\n     for the public: \\n\\twiki.creativecommons.org/Considerations_for_licensees\\n\\n=======================================================================\\n\\nCreative Commons Attribution-NonCommercial-ShareAlike 4.0 International\\nPublic License\\n\\nBy exercising the Licensed Rights (defined below), You accept and agree\\nto be bound by the terms and conditions of this Creative Commons\\nAttribution-NonCommercial-ShareAlike 4.0 International Public License\\n(\\"Public License\\"). To the extent this Public License may be\\ninterpreted as a contract, You are granted the Licensed Rights in\\nconsideration of Your acceptance of these terms and conditions, and the\\nLicensor grants You such rights in consideration of benefits the\\nLicensor receives from making the Licensed Material available under\\nthese terms and conditions.\\n\\n\\nSection 1 -- Definitions.\\n\\n  a. Adapted Material means material subject to Copyright and Similar\\n     Rights that is derived from or based upon the Licensed Material\\n     and in which the Licensed Material is translated, altered,\\n     arranged, transformed, or otherwise modified in a manner requiring\\n     permission under the Copyright and Similar Rights held by the\\n     Licensor. For purposes of this Public License, where the Licensed\\n     Material is a musical work, performance, or sound recording,\\n     Adapted Material is always produced where the Licensed Material is\\n     synched in timed relation with a moving image.\\n\\n  b. Adapter''s License means the license You apply to Your Copyright\\n     and Similar Rights in Your contributions to Adapted Material in\\n     accordance with the terms and conditions of this Public License.\\n\\n  c. BY-NC-SA Compatible License means a license listed at\\n     creativecommons.org/compatiblelicenses, approved by Creative\\n     Commons as essentially the equivalent of this Public License.\\n\\n  d. Copyright and Similar Rights means copyright and/or similar rights\\n     closely related to copyright including, without limitation,\\n     performance, broadcast, sound recording, and Sui Generis Database\\n     Rights, without regard to how the rights are labeled or\\n     categorized. For purposes of this Public License, the rights\\n     specified in Section 2(b)(1)-(2) are not Copyright and Similar\\n     Rights.\\n\\n  e. Effective Technological Measures means those measures that, in the\\n     absence of proper authority, may not be circumvented under laws\\n     fulfilling obligations under Article 11 of the WIPO Copyright\\n     Treaty adopted on December 20, 1996, and/or similar international\\n     agreements.\\n\\n  f. Exceptions and Limitations means fair use, fair dealing, and/or\\n     any other exception or limitation to Copyright and Similar Rights\\n     that applies to Your use of the Licensed Material.\\n\\n  g. License Elements means the license attributes listed in the name\\n     of a Creative Commons Public License. The License Elements of this\\n     Public License are Attribution, NonCommercial, and ShareAlike.\\n\\n  h. Licensed Material means the artistic or literary work, database,\\n     or other material to which the Licensor applied this Public\\n     License.\\n\\n  i. Licensed Rights means the rights granted to You subject to the\\n     terms and conditions of this Public License, which are limited to\\n     all Copyright and Similar Rights that apply to Your use of the\\n     Licensed Material and that the Licensor has authority to license.\\n\\n  j. Licensor means the individual(s) or entity(ies) granting rights\\n     under this Public License.\\n\\n  k. NonCommercial means not primarily intended for or directed towards\\n     commercial advantage or monetary compensation. For purposes of\\n     this Public License, the exchange of the Licensed Material for\\n     other material subject to Copyright and Similar Rights by digital\\n     file-sharing or similar means is NonCommercial provided there is\\n     no payment of monetary compensation in connection with the\\n     exchange.\\n\\n  l. Share means to provide material to the public by any means or\\n     process that requires permission under the Licensed Rights, such\\n     as reproduction, public display, public performance, distribution,\\n     dissemination, communication, or importation, and to make material\\n     available to the public including in ways that members of the\\n     public may access the material from a place and at a time\\n     individually chosen by them.\\n\\n  m. Sui Generis Database Rights means rights other than copyright\\n     resulting from Directive 96/9/EC of the European Parliament and of\\n     the Council of 11 March 1996 on the legal protection of databases,\\n     as amended and/or succeeded, as well as other essentially\\n     equivalent rights anywhere in the world.\\n\\n  n. You means the individual or entity exercising the Licensed Rights\\n     under this Public License. Your has a corresponding meaning.\\n\\n\\nSection 2 -- Scope.\\n\\n  a. License grant.\\n\\n       1. Subject to the terms and conditions of this Public License,\\n          the Licensor hereby grants You a worldwide, royalty-free,\\n          non-sublicensable, non-exclusive, irrevocable license to\\n          exercise the Licensed Rights in the Licensed Material to:\\n\\n            a. reproduce and Share the Licensed Material, in whole or\\n               in part, for NonCommercial purposes only; and\\n\\n            b. produce, reproduce, and Share Adapted Material for\\n               NonCommercial purposes only.\\n\\n       2. Exceptions and Limitations. For the avoidance of doubt, where\\n          Exceptions and Limitations apply to Your use, this Public\\n          License does not apply, and You do not need to comply with\\n          its terms and conditions.\\n\\n       3. Term. The term of this Public License is specified in Section\\n          6(a).\\n\\n       4. Media and formats; technical modifications allowed. The\\n          Licensor authorizes You to exercise the Licensed Rights in\\n          all media and formats whether now known or hereafter created,\\n          and to make technical modifications necessary to do so. The\\n          Licensor waives and/or agrees not to assert any right or\\n          authority to forbid You from making technical modifications\\n          necessary to exercise the Licensed Rights, including\\n          technical modifications necessary to circumvent Effective\\n          Technological Measures. For purposes of this Public License,\\n          simply making modifications authorized by this Section 2(a)\\n          (4) never produces Adapted Material.\\n\\n       5. Downstream recipients.\\n\\n            a. Offer from the Licensor -- Licensed Material. Every\\n               recipient of the Licensed Material automatically\\n               receives an offer from the Licensor to exercise the\\n               Licensed Rights under the terms and conditions of this\\n               Public License.\\n\\n            b. Additional offer from the Licensor -- Adapted Material.\\n               Every recipient of Adapted Material from You\\n               automatically receives an offer from the Licensor to\\n               exercise the Licensed Rights in the Adapted Material\\n               under the conditions of the Adapter''s License You apply.\\n\\n            c. No downstream restrictions. You may not offer or impose\\n               any additional or different terms or conditions on, or\\n               apply any Effective Technological Measures to, the\\n               Licensed Material if doing so restricts exercise of the\\n               Licensed Rights by any recipient of the Licensed\\n               Material.\\n\\n       6. No endorsement. Nothing in this Public License constitutes or\\n          may be construed as permission to assert or imply that You\\n          are, or that Your use of the Licensed Material is, connected\\n          with, or sponsored, endorsed, or granted official status by,\\n          the Licensor or others designated to receive attribution as\\n          provided in Section 3(a)(1)(A)(i).\\n\\n  b. Other rights.\\n\\n       1. Moral rights, such as the right of integrity, are not\\n          licensed under this Public License, nor are publicity,\\n          privacy, and/or other similar personality rights; however, to\\n          the extent possible, the Licensor waives and/or agrees not to\\n          assert any such rights held by the Licensor to the limited\\n          extent necessary to allow You to exercise the Licensed\\n          Rights, but not otherwise.\\n\\n       2. Patent and trademark rights are not licensed under this\\n          Public License.\\n\\n       3. To the extent possible, the Licensor waives any right to\\n          collect royalties from You for the exercise of the Licensed\\n          Rights, whether directly or through a collecting society\\n          under any voluntary or waivable statutory or compulsory\\n          licensing scheme. In all other cases the Licensor expressly\\n          reserves any right to collect such royalties, including when\\n          the Licensed Material is used other than for NonCommercial\\n          purposes.\\n\\n\\nSection 3 -- License Conditions.\\n\\nYour exercise of the Licensed Rights is expressly made subject to the\\nfollowing conditions.\\n\\n  a. Attribution.\\n\\n       1. If You Share the Licensed Material (including in modified\\n          form), You must:\\n\\n            a. retain the following if it is supplied by the Licensor\\n               with the Licensed Material:\\n\\n                 i. identification of the creator(s) of the Licensed\\n                    Material and any others designated to receive\\n                    attribution, in any reasonable manner requested by\\n                    the Licensor (including by pseudonym if\\n                    designated);\\n\\n                ii. a copyright notice;\\n\\n               iii. a notice that refers to this Public License;\\n\\n                iv. a notice that refers to the disclaimer of\\n                    warranties;\\n\\n                 v. a URI or hyperlink to the Licensed Material to the\\n                    extent reasonably practicable;\\n\\n            b. indicate if You modified the Licensed Material and\\n               retain an indication of any previous modifications; and\\n\\n            c. indicate the Licensed Material is licensed under this\\n               Public License, and include the text of, or the URI or\\n               hyperlink to, this Public License.\\n\\n       2. You may satisfy the conditions in Section 3(a)(1) in any\\n          reasonable manner based on the medium, means, and context in\\n          which You Share the Licensed Material. For example, it may be\\n          reasonable to satisfy the conditions by providing a URI or\\n          hyperlink to a resource that includes the required\\n          information.\\n       3. If requested by the Licensor, You must remove any of the\\n          information required by Section 3(a)(1)(A) to the extent\\n          reasonably practicable.\\n\\n  b. ShareAlike.\\n\\n     In addition to the conditions in Section 3(a), if You Share\\n     Adapted Material You produce, the following conditions also apply.\\n\\n       1. The Adapter''s License You apply must be a Creative Commons\\n          license with the same License Elements, this version or\\n          later, or a BY-NC-SA Compatible License.\\n\\n       2. You must include the text of, or the URI or hyperlink to, the\\n          Adapter''s License You apply. You may satisfy this condition\\n          in any reasonable manner based on the medium, means, and\\n          context in which You Share Adapted Material.\\n\\n       3. You may not offer or impose any additional or different terms\\n          or conditions on, or apply any Effective Technological\\n          Measures to, Adapted Material that restrict exercise of the\\n          rights granted under the Adapter''s License You apply.\\n\\n\\nSection 4 -- Sui Generis Database Rights.\\n\\nWhere the Licensed Rights include Sui Generis Database Rights that\\napply to Your use of the Licensed Material:\\n\\n  a. for the avoidance of doubt, Section 2(a)(1) grants You the right\\n     to extract, reuse, reproduce, and Share all or a substantial\\n     portion of the contents of the database for NonCommercial purposes\\n     only;\\n\\n  b. if You include all or a substantial portion of the database\\n     contents in a database in which You have Sui Generis Database\\n     Rights, then the database in which You have Sui Generis Database\\n     Rights (but not its individual contents) is Adapted Material,\\n     including for purposes of Section 3(b); and\\n\\n  c. You must comply with the conditions in Section 3(a) if You Share\\n     all or a substantial portion of the contents of the database.\\n\\nFor the avoidance of doubt, this Section 4 supplements and does not\\nreplace Your obligations under this Public License where the Licensed\\nRights include other Copyright and Similar Rights.\\n\\n\\nSection 5 -- Disclaimer of Warranties and Limitation of Liability.\\n\\n  a. UNLESS OTHERWISE SEPARATELY UNDERTAKEN BY THE LICENSOR, TO THE\\n     EXTENT POSSIBLE, THE LICENSOR OFFERS THE LICENSED MATERIAL AS-IS\\n     AND AS-AVAILABLE, AND MAKES NO REPRESENTATIONS OR WARRANTIES OF\\n     ANY KIND CONCERNING THE LICENSED MATERIAL, WHETHER EXPRESS,\\n     IMPLIED, STATUTORY, OR OTHER. THIS INCLUDES, WITHOUT LIMITATION,\\n     WARRANTIES OF TITLE, MERCHANTABILITY, FITNESS FOR A PARTICULAR\\n     PURPOSE, NON-INFRINGEMENT, ABSENCE OF LATENT OR OTHER DEFECTS,\\n     ACCURACY, OR THE PRESENCE OR ABSENCE OF ERRORS, WHETHER OR NOT\\n     KNOWN OR DISCOVERABLE. WHERE DISCLAIMERS OF WARRANTIES ARE NOT\\n     ALLOWED IN FULL OR IN PART, THIS DISCLAIMER MAY NOT APPLY TO YOU.\\n\\n  b. TO THE EXTENT POSSIBLE, IN NO EVENT WILL THE LICENSOR BE LIABLE\\n     TO YOU ON ANY LEGAL THEORY (INCLUDING, WITHOUT LIMITATION,\\n     NEGLIGENCE) OR OTHERWISE FOR ANY DIRECT, SPECIAL, INDIRECT,\\n     INCIDENTAL, CONSEQUENTIAL, PUNITIVE, EXEMPLARY, OR OTHER LOSSES,\\n     COSTS, EXPENSES, OR DAMAGES ARISING OUT OF THIS PUBLIC LICENSE OR\\n     USE OF THE LICENSED MATERIAL, EVEN IF THE LICENSOR HAS BEEN\\n     ADVISED OF THE POSSIBILITY OF SUCH LOSSES, COSTS, EXPENSES, OR\\n     DAMAGES. WHERE A LIMITATION OF LIABILITY IS NOT ALLOWED IN FULL OR\\n     IN PART, THIS LIMITATION MAY NOT APPLY TO YOU.\\n\\n  c. The disclaimer of warranties and limitation of liability provided\\n     above shall be interpreted in a manner that, to the extent\\n     possible, most closely approximates an absolute disclaimer and\\n     waiver of all liability.\\n\\n\\nSection 6 -- Term and Termination.\\n\\n  a. This Public License applies for the term of the Copyright and\\n     Similar Rights licensed here. However, if You fail to comply with\\n     this Public License, then Your rights under this Public License\\n     terminate automatically.\\n\\n  b. Where Your right to use the Licensed Material has terminated under\\n     Section 6(a), it reinstates:\\n\\n       1. automatically as of the date the violation is cured, provided\\n          it is cured within 30 days of Your discovery of the\\n          violation; or\\n\\n       2. upon express reinstatement by the Licensor.\\n\\n     For the avoidance of doubt, this Section 6(b) does not affect any\\n     right the Licensor may have to seek remedies for Your violations\\n     of this Public License.\\n\\n  c. For the avoidance of doubt, the Licensor may also offer the\\n     Licensed Material under separate terms or conditions or stop\\n     distributing the Licensed Material at any time; however, doing so\\n     will not terminate this Public License.\\n\\n  d. Sections 1, 5, 6, 7, and 8 survive termination of this Public\\n     License.\\n\\n\\nSection 7 -- Other Terms and Conditions.\\n\\n  a. The Licensor shall not be bound by any additional or different\\n     terms or conditions communicated by You unless expressly agreed.\\n\\n  b. Any arrangements, understandings, or agreements regarding the\\n     Licensed Material not stated herein are separate from and\\n     independent of the terms and conditions of this Public License.\\n\\n\\nSection 8 -- Interpretation.\\n\\n  a. For the avoidance of doubt, this Public License does not, and\\n     shall not be interpreted to, reduce, limit, restrict, or impose\\n     conditions on any use of the Licensed Material that could lawfully\\n     be made without permission under this Public License.\\n\\n  b. To the extent possible, if any provision of this Public License is\\n     deemed unenforceable, it shall be automatically reformed to the\\n     minimum extent necessary to make it enforceable. If the provision\\n     cannot be reformed, it shall be severed from this Public License\\n     without affecting the enforceability of the remaining terms and\\n     conditions.\\n\\n  c. No term or condition of this Public License will be waived and no\\n     failure to comply consented to unless expressly agreed to by the\\n     Licensor.\\n\\n  d. Nothing in this Public License constitutes or may be interpreted\\n     as a limitation upon, or waiver of, any privileges and immunities\\n     that apply to the Licensor or You, including from the legal\\n     processes of any jurisdiction or authority.\\n\\n=======================================================================\\n\\nCreative Commons is not a party to its public\\nlicenses. Notwithstanding, Creative Commons may elect to apply one of\\nits public licenses to material it publishes and in those instances\\nwill be considered the “Licensor.” The text of the Creative Commons\\npublic licenses is dedicated to the public domain under the CC0 Public\\nDomain Dedication. Except for the limited purpose of indicating that\\nmaterial is shared under a Creative Commons public license or as\\notherwise permitted by the Creative Commons policies published at\\ncreativecommons.org/policies, Creative Commons does not authorize the\\nuse of the trademark \\"Creative Commons\\" or any other trademark or logo\\nof Creative Commons without its prior written consent including,\\nwithout limitation, in connection with any unauthorized modifications\\nto any of its public licenses or any other arrangements,\\nunderstandings, or agreements concerning use of licensed material. For\\nthe avoidance of doubt, this paragraph does not form part of the\\npublic licenses.\\n\\nCreative Commons may be contacted at creativecommons.org.","link":"https://creativecommons.org/licenses/by-nc-sa/4.0/","version":"4.0","creation_date":"2018-06-22","modification_date":"2024-11-21","active":false}');
INSERT INTO public.licenses SELECT * FROM json_populate_record(NULL::public.licenses, E'{"id":3,"long_name":"CC BY 4.0 (Attribution)","short_name":"CC BY 4.0","text":"Attribution 4.0 International\\n\\n=======================================================================\\n\\nCreative Commons Corporation (\\"Creative Commons\\") is not a law firm and\\ndoes not provide legal services or legal advice. Distribution of\\nCreative Commons public licenses does not create a lawyer-client or\\nother relationship. Creative Commons makes its licenses and related\\ninformation available on an \\"as-is\\" basis. Creative Commons gives no\\nwarranties regarding its licenses, any material licensed under their\\nterms and conditions, or any related information. Creative Commons\\ndisclaims all liability for damages resulting from their use to the\\nfullest extent possible.\\n\\nUsing Creative Commons Public Licenses\\n\\nCreative Commons public licenses provide a standard set of terms and\\nconditions that creators and other rights holders may use to share\\noriginal works of authorship and other material subject to copyright\\nand certain other rights specified in the public license below. The\\nfollowing considerations are for informational purposes only, are not\\nexhaustive, and do not form part of our licenses.\\n\\n     Considerations for licensors: Our public licenses are\\n     intended for use by those authorized to give the public\\n     permission to use material in ways otherwise restricted by\\n     copyright and certain other rights. Our licenses are\\n     irrevocable. Licensors should read and understand the terms\\n     and conditions of the license they choose before applying it.\\n     Licensors should also secure all rights necessary before\\n     applying our licenses so that the public can reuse the\\n     material as expected. Licensors should clearly mark any\\n     material not subject to the license. This includes other CC-\\n     licensed material, or material used under an exception or\\n     limitation to copyright. More considerations for licensors:\\n\\twiki.creativecommons.org/Considerations_for_licensors\\n\\n     Considerations for the public: By using one of our public\\n     licenses, a licensor grants the public permission to use the\\n     licensed material under specified terms and conditions. If\\n     the licensor''s permission is not necessary for any reason--for\\n     example, because of any applicable exception or limitation to\\n     copyright--then that use is not regulated by the license. Our\\n     licenses grant only permissions under copyright and certain\\n     other rights that a licensor has authority to grant. Use of\\n     the licensed material may still be restricted for other\\n     reasons, including because others have copyright or other\\n     rights in the material. A licensor may make special requests,\\n     such as asking that all changes be marked or described.\\n     Although not required by our licenses, you are encouraged to\\n     respect those requests where reasonable. More considerations\\n     for the public: \\n\\twiki.creativecommons.org/Considerations_for_licensees\\n\\n=======================================================================\\n\\nCreative Commons Attribution 4.0 International Public License\\n\\nBy exercising the Licensed Rights (defined below), You accept and agree\\nto be bound by the terms and conditions of this Creative Commons\\nAttribution 4.0 International Public License (\\"Public License\\"). To the\\nextent this Public License may be interpreted as a contract, You are\\ngranted the Licensed Rights in consideration of Your acceptance of\\nthese terms and conditions, and the Licensor grants You such rights in\\nconsideration of benefits the Licensor receives from making the\\nLicensed Material available under these terms and conditions.\\n\\n\\nSection 1 -- Definitions.\\n\\n  a. Adapted Material means material subject to Copyright and Similar\\n     Rights that is derived from or based upon the Licensed Material\\n     and in which the Licensed Material is translated, altered,\\n     arranged, transformed, or otherwise modified in a manner requiring\\n     permission under the Copyright and Similar Rights held by the\\n     Licensor. For purposes of this Public License, where the Licensed\\n     Material is a musical work, performance, or sound recording,\\n     Adapted Material is always produced where the Licensed Material is\\n     synched in timed relation with a moving image.\\n\\n  b. Adapter''s License means the license You apply to Your Copyright\\n     and Similar Rights in Your contributions to Adapted Material in\\n     accordance with the terms and conditions of this Public License.\\n\\n  c. Copyright and Similar Rights means copyright and/or similar rights\\n     closely related to copyright including, without limitation,\\n     performance, broadcast, sound recording, and Sui Generis Database\\n     Rights, without regard to how the rights are labeled or\\n     categorized. For purposes of this Public License, the rights\\n     specified in Section 2(b)(1)-(2) are not Copyright and Similar\\n     Rights.\\n\\n  d. Effective Technological Measures means those measures that, in the\\n     absence of proper authority, may not be circumvented under laws\\n     fulfilling obligations under Article 11 of the WIPO Copyright\\n     Treaty adopted on December 20, 1996, and/or similar international\\n     agreements.\\n\\n  e. Exceptions and Limitations means fair use, fair dealing, and/or\\n     any other exception or limitation to Copyright and Similar Rights\\n     that applies to Your use of the Licensed Material.\\n\\n  f. Licensed Material means the artistic or literary work, database,\\n     or other material to which the Licensor applied this Public\\n     License.\\n\\n  g. Licensed Rights means the rights granted to You subject to the\\n     terms and conditions of this Public License, which are limited to\\n     all Copyright and Similar Rights that apply to Your use of the\\n     Licensed Material and that the Licensor has authority to license.\\n\\n  h. Licensor means the individual(s) or entity(ies) granting rights\\n     under this Public License.\\n\\n  i. Share means to provide material to the public by any means or\\n     process that requires permission under the Licensed Rights, such\\n     as reproduction, public display, public performance, distribution,\\n     dissemination, communication, or importation, and to make material\\n     available to the public including in ways that members of the\\n     public may access the material from a place and at a time\\n     individually chosen by them.\\n\\n  j. Sui Generis Database Rights means rights other than copyright\\n     resulting from Directive 96/9/EC of the European Parliament and of\\n     the Council of 11 March 1996 on the legal protection of databases,\\n     as amended and/or succeeded, as well as other essentially\\n     equivalent rights anywhere in the world.\\n\\n  k. You means the individual or entity exercising the Licensed Rights\\n     under this Public License. Your has a corresponding meaning.\\n\\n\\nSection 2 -- Scope.\\n\\n  a. License grant.\\n\\n       1. Subject to the terms and conditions of this Public License,\\n          the Licensor hereby grants You a worldwide, royalty-free,\\n          non-sublicensable, non-exclusive, irrevocable license to\\n          exercise the Licensed Rights in the Licensed Material to:\\n\\n            a. reproduce and Share the Licensed Material, in whole or\\n               in part; and\\n\\n            b. produce, reproduce, and Share Adapted Material.\\n\\n       2. Exceptions and Limitations. For the avoidance of doubt, where\\n          Exceptions and Limitations apply to Your use, this Public\\n          License does not apply, and You do not need to comply with\\n          its terms and conditions.\\n\\n       3. Term. The term of this Public License is specified in Section\\n          6(a).\\n\\n       4. Media and formats; technical modifications allowed. The\\n          Licensor authorizes You to exercise the Licensed Rights in\\n          all media and formats whether now known or hereafter created,\\n          and to make technical modifications necessary to do so. The\\n          Licensor waives and/or agrees not to assert any right or\\n          authority to forbid You from making technical modifications\\n          necessary to exercise the Licensed Rights, including\\n          technical modifications necessary to circumvent Effective\\n          Technological Measures. For purposes of this Public License,\\n          simply making modifications authorized by this Section 2(a)\\n          (4) never produces Adapted Material.\\n\\n       5. Downstream recipients.\\n\\n            a. Offer from the Licensor -- Licensed Material. Every\\n               recipient of the Licensed Material automatically\\n               receives an offer from the Licensor to exercise the\\n               Licensed Rights under the terms and conditions of this\\n               Public License.\\n\\n            b. No downstream restrictions. You may not offer or impose\\n               any additional or different terms or conditions on, or\\n               apply any Effective Technological Measures to, the\\n               Licensed Material if doing so restricts exercise of the\\n               Licensed Rights by any recipient of the Licensed\\n               Material.\\n\\n       6. No endorsement. Nothing in this Public License constitutes or\\n          may be construed as permission to assert or imply that You\\n          are, or that Your use of the Licensed Material is, connected\\n          with, or sponsored, endorsed, or granted official status by,\\n          the Licensor or others designated to receive attribution as\\n          provided in Section 3(a)(1)(A)(i).\\n\\n  b. Other rights.\\n\\n       1. Moral rights, such as the right of integrity, are not\\n          licensed under this Public License, nor are publicity,\\n          privacy, and/or other similar personality rights; however, to\\n          the extent possible, the Licensor waives and/or agrees not to\\n          assert any such rights held by the Licensor to the limited\\n          extent necessary to allow You to exercise the Licensed\\n          Rights, but not otherwise.\\n\\n       2. Patent and trademark rights are not licensed under this\\n          Public License.\\n\\n       3. To the extent possible, the Licensor waives any right to\\n          collect royalties from You for the exercise of the Licensed\\n          Rights, whether directly or through a collecting society\\n          under any voluntary or waivable statutory or compulsory\\n          licensing scheme. In all other cases the Licensor expressly\\n          reserves any right to collect such royalties.\\n\\n\\nSection 3 -- License Conditions.\\n\\nYour exercise of the Licensed Rights is expressly made subject to the\\nfollowing conditions.\\n\\n  a. Attribution.\\n\\n       1. If You Share the Licensed Material (including in modified\\n          form), You must:\\n\\n            a. retain the following if it is supplied by the Licensor\\n               with the Licensed Material:\\n\\n                 i. identification of the creator(s) of the Licensed\\n                    Material and any others designated to receive\\n                    attribution, in any reasonable manner requested by\\n                    the Licensor (including by pseudonym if\\n                    designated);\\n\\n                ii. a copyright notice;\\n\\n               iii. a notice that refers to this Public License;\\n\\n                iv. a notice that refers to the disclaimer of\\n                    warranties;\\n\\n                 v. a URI or hyperlink to the Licensed Material to the\\n                    extent reasonably practicable;\\n\\n            b. indicate if You modified the Licensed Material and\\n               retain an indication of any previous modifications; and\\n\\n            c. indicate the Licensed Material is licensed under this\\n               Public License, and include the text of, or the URI or\\n               hyperlink to, this Public License.\\n\\n       2. You may satisfy the conditions in Section 3(a)(1) in any\\n          reasonable manner based on the medium, means, and context in\\n          which You Share the Licensed Material. For example, it may be\\n          reasonable to satisfy the conditions by providing a URI or\\n          hyperlink to a resource that includes the required\\n          information.\\n\\n       3. If requested by the Licensor, You must remove any of the\\n          information required by Section 3(a)(1)(A) to the extent\\n          reasonably practicable.\\n\\n       4. If You Share Adapted Material You produce, the Adapter''s\\n          License You apply must not prevent recipients of the Adapted\\n          Material from complying with this Public License.\\n\\n\\nSection 4 -- Sui Generis Database Rights.\\n\\nWhere the Licensed Rights include Sui Generis Database Rights that\\napply to Your use of the Licensed Material:\\n\\n  a. for the avoidance of doubt, Section 2(a)(1) grants You the right\\n     to extract, reuse, reproduce, and Share all or a substantial\\n     portion of the contents of the database;\\n\\n  b. if You include all or a substantial portion of the database\\n     contents in a database in which You have Sui Generis Database\\n     Rights, then the database in which You have Sui Generis Database\\n     Rights (but not its individual contents) is Adapted Material; and\\n\\n  c. You must comply with the conditions in Section 3(a) if You Share\\n     all or a substantial portion of the contents of the database.\\n\\nFor the avoidance of doubt, this Section 4 supplements and does not\\nreplace Your obligations under this Public License where the Licensed\\nRights include other Copyright and Similar Rights.\\n\\n\\nSection 5 -- Disclaimer of Warranties and Limitation of Liability.\\n\\n  a. UNLESS OTHERWISE SEPARATELY UNDERTAKEN BY THE LICENSOR, TO THE\\n     EXTENT POSSIBLE, THE LICENSOR OFFERS THE LICENSED MATERIAL AS-IS\\n     AND AS-AVAILABLE, AND MAKES NO REPRESENTATIONS OR WARRANTIES OF\\n     ANY KIND CONCERNING THE LICENSED MATERIAL, WHETHER EXPRESS,\\n     IMPLIED, STATUTORY, OR OTHER. THIS INCLUDES, WITHOUT LIMITATION,\\n     WARRANTIES OF TITLE, MERCHANTABILITY, FITNESS FOR A PARTICULAR\\n     PURPOSE, NON-INFRINGEMENT, ABSENCE OF LATENT OR OTHER DEFECTS,\\n     ACCURACY, OR THE PRESENCE OR ABSENCE OF ERRORS, WHETHER OR NOT\\n     KNOWN OR DISCOVERABLE. WHERE DISCLAIMERS OF WARRANTIES ARE NOT\\n     ALLOWED IN FULL OR IN PART, THIS DISCLAIMER MAY NOT APPLY TO YOU.\\n\\n  b. TO THE EXTENT POSSIBLE, IN NO EVENT WILL THE LICENSOR BE LIABLE\\n     TO YOU ON ANY LEGAL THEORY (INCLUDING, WITHOUT LIMITATION,\\n     NEGLIGENCE) OR OTHERWISE FOR ANY DIRECT, SPECIAL, INDIRECT,\\n     INCIDENTAL, CONSEQUENTIAL, PUNITIVE, EXEMPLARY, OR OTHER LOSSES,\\n     COSTS, EXPENSES, OR DAMAGES ARISING OUT OF THIS PUBLIC LICENSE OR\\n     USE OF THE LICENSED MATERIAL, EVEN IF THE LICENSOR HAS BEEN\\n     ADVISED OF THE POSSIBILITY OF SUCH LOSSES, COSTS, EXPENSES, OR\\n     DAMAGES. WHERE A LIMITATION OF LIABILITY IS NOT ALLOWED IN FULL OR\\n     IN PART, THIS LIMITATION MAY NOT APPLY TO YOU.\\n\\n  c. The disclaimer of warranties and limitation of liability provided\\n     above shall be interpreted in a manner that, to the extent\\n     possible, most closely approximates an absolute disclaimer and\\n     waiver of all liability.\\n\\n\\nSection 6 -- Term and Termination.\\n\\n  a. This Public License applies for the term of the Copyright and\\n     Similar Rights licensed here. However, if You fail to comply with\\n     this Public License, then Your rights under this Public License\\n     terminate automatically.\\n\\n  b. Where Your right to use the Licensed Material has terminated under\\n     Section 6(a), it reinstates:\\n\\n       1. automatically as of the date the violation is cured, provided\\n          it is cured within 30 days of Your discovery of the\\n          violation; or\\n\\n       2. upon express reinstatement by the Licensor.\\n\\n     For the avoidance of doubt, this Section 6(b) does not affect any\\n     right the Licensor may have to seek remedies for Your violations\\n     of this Public License.\\n\\n  c. For the avoidance of doubt, the Licensor may also offer the\\n     Licensed Material under separate terms or conditions or stop\\n     distributing the Licensed Material at any time; however, doing so\\n     will not terminate this Public License.\\n\\n  d. Sections 1, 5, 6, 7, and 8 survive termination of this Public\\n     License.\\n\\n\\nSection 7 -- Other Terms and Conditions.\\n\\n  a. The Licensor shall not be bound by any additional or different\\n     terms or conditions communicated by You unless expressly agreed.\\n\\n  b. Any arrangements, understandings, or agreements regarding the\\n     Licensed Material not stated herein are separate from and\\n     independent of the terms and conditions of this Public License.\\n\\n\\nSection 8 -- Interpretation.\\n\\n  a. For the avoidance of doubt, this Public License does not, and\\n     shall not be interpreted to, reduce, limit, restrict, or impose\\n     conditions on any use of the Licensed Material that could lawfully\\n     be made without permission under this Public License.\\n\\n  b. To the extent possible, if any provision of this Public License is\\n     deemed unenforceable, it shall be automatically reformed to the\\n     minimum extent necessary to make it enforceable. If the provision\\n     cannot be reformed, it shall be severed from this Public License\\n     without affecting the enforceability of the remaining terms and\\n     conditions.\\n\\n  c. No term or condition of this Public License will be waived and no\\n     failure to comply consented to unless expressly agreed to by the\\n     Licensor.\\n\\n  d. Nothing in this Public License constitutes or may be interpreted\\n     as a limitation upon, or waiver of, any privileges and immunities\\n     that apply to the Licensor or You, including from the legal\\n     processes of any jurisdiction or authority.\\n\\n\\n=======================================================================\\n\\nCreative Commons is not a party to its public\\nlicenses. Notwithstanding, Creative Commons may elect to apply one of\\nits public licenses to material it publishes and in those instances\\nwill be considered the “Licensor.” The text of the Creative Commons\\npublic licenses is dedicated to the public domain under the CC0 Public\\nDomain Dedication. Except for the limited purpose of indicating that\\nmaterial is shared under a Creative Commons public license or as\\notherwise permitted by the Creative Commons policies published at\\ncreativecommons.org/policies, Creative Commons does not authorize the\\nuse of the trademark \\"Creative Commons\\" or any other trademark or logo\\nof Creative Commons without its prior written consent including,\\nwithout limitation, in connection with any unauthorized modifications\\nto any of its public licenses or any other arrangements,\\nunderstandings, or agreements concerning use of licensed material. For\\nthe avoidance of doubt, this paragraph does not form part of the\\npublic licenses.\\n\\nCreative Commons may be contacted at creativecommons.org.","link":"https://creativecommons.org/licenses/by/4.0/","version":"4.0","creation_date":"2018-07-05","modification_date":"2023-03-30","active":true}');
INSERT INTO public.licenses SELECT * FROM json_populate_record(NULL::public.licenses, '{"id":4,"long_name":"Other - See Data Usage Guidelines","short_name":"Other - See Data Usage Guidelines","text":"Other - See Data Usage Guidelines","link":"","version":"1.0","creation_date":"2019-08-06","modification_date":"2024-11-21","active":false}');
INSERT INTO public.licenses SELECT * FROM json_populate_record(NULL::public.licenses, '{"id":6,"long_name":"CC BY-SA 4.0 (Attribution-ShareAlike)","short_name":"CC BY-SA 4.0","text":"Attribution-ShareAlike 4.0 International","link":"https://creativecommons.org/licenses/by-sa/4.0/","version":"4.0","creation_date":"2024-11-21","modification_date":"2024-11-21","active":true}');
SELECT pg_catalog.setval(pg_get_serial_sequence('public.licenses', 'id'), (SELECT coalesce(max(id), 1) FROM public.licenses));
INSERT INTO public.roles SELECT * FROM json_populate_record(NULL::public.roles, '{"id":1,"name":"admin"}');
INSERT INTO public.roles SELECT * FROM json_populate_record(NULL::public.roles, '{"id":2,"name":"mapper"}');
SELECT pg_catalog.setval(pg_get_serial_sequence('public.roles', 'id'), (SELECT coalesce(max(id), 1) FROM public.roles));
INSERT INTO public.mondo_terms SELECT * FROM json_populate_record(NULL::public.mondo_terms, '{"id":1,"code":"MONDO:0000001","system":"https://purl.obolibrary.org/obo/mondo.owl","system_version":null,"label":"disease or disorder","creation_date":"2026-10-15","modification_date":"2026-10-15"}');
SELECT pg_catalog.setval(pg_get_serial_sequence('public.mondo_terms', 'id'), (SELECT coalesce(max(id), 1) FROM public.mondo_terms));

REFRESH MATERIALIZED VIEW public.published_variants_materialized_view;
