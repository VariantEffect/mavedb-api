"""Database roles, and what each role may do to every relation and function in the schema (#808, #827).

The bootstrap (``mavedb.db.bootstrap``) makes the database's grants match these registries exactly, and the CI
and startup checks (#824) read them. Anything missing from a registry gets no grants, so a new table or
function is unreachable until it's listed here. Listing one is a reviewed change: say beside it why its access
is safe, and for a table the api can read, whether row-level security decides which rows.
"""

from dataclasses import dataclass
from enum import Enum

OWNER_ROLE = "mavedb_owner"
"""Owns every table and function. No role logs in as it. Migrations run through it via ``MIGRATE_ROLE``."""

MIGRATE_ROLE = "mavedb_migrate"
"""The login for migrations and deploy steps. Its only membership is ``OWNER_ROLE``, which it runs as."""

WORKER_ROLE = "mavedb_worker"
"""The worker's login. It has only the privileges granted to it here, and belongs to no role."""

API_ROLE = "mavedb_api"
"""The api's login, in every environment. It has only the privileges granted to it here, and belongs to no role."""


class ApiAccess(Enum):
    GUARDED = "guarded"
    """The api is granted the relation, and an RLS policy decides which rows it sees (#809).

    A view reads its tables as its owner, who skips their policies, so a guarded view must be
    ``security_invoker``; the api isn't granted one that isn't. A materialized view can't be guarded: its rows
    were read as the owner at refresh, and Postgres has no RLS on it.
    """

    CARVE_OUT = "carve_out"
    """The api is granted the relation with no policy, so every caller sees every row. Say why that's safe."""

    NONE = "none"
    """The api gets no privileges on the relation."""


@dataclass(frozen=True)
class Function:
    executors: frozenset[str] = frozenset()
    security_definer: bool = False


@dataclass(frozen=True)
class Relation:
    api: ApiAccess
    worker: bool = True


GUARDED = Relation(ApiAccess.GUARDED)
CARVE_OUT = Relation(ApiAccess.CARVE_OUT)
WORKER_ONLY = Relation(ApiAccess.NONE)
UNGRANTED = Relation(ApiAccess.NONE, worker=False)

# Every table, view and materialized view in the schema. Tables, including foreign tables, get SELECT, INSERT,
# UPDATE and DELETE; views and materialized views get SELECT. A table's sequences go to the roles that can write the table. A name
# that isn't in the database is skipped, so production's leftover tables can be listed here.
RELATIONS: dict[str, Relation] = {
    # Records with their own rule (#809).
    "scoresets": GUARDED,
    "experiments": GUARDED,
    "experiment_sets": GUARDED,
    "collections": GUARDED,
    "score_calibrations": GUARDED,
    # Owned rows, visible when the record that owns them is (#809).
    "variants": GUARDED,
    "target_genes": GUARDED,
    "target_sequences": GUARDED,
    "target_accessions": GUARDED,
    "target_gene_external_identifiers": GUARDED,
    "target_gene_mappings": GUARDED,
    "mapping_records": GUARDED,
    "mapping_record_alleles": GUARDED,
    "projection_groups": GUARDED,
    "score_calibration_functional_classifications": GUARDED,
    "score_calibration_functional_classification_variants": GUARDED,
    "score_calibration_publication_identifiers": GUARDED,
    "calibration_controls": GUARDED,
    "scoreset_publication_identifiers": GUARDED,
    "experiment_publication_identifiers": GUARDED,
    "experiment_set_publication_identifiers": GUARDED,
    "scoreset_doi_identifiers": GUARDED,
    "experiment_doi_identifiers": GUARDED,
    "experiment_set_doi_identifiers": GUARDED,
    "scoreset_sra_identifiers": GUARDED,
    "experiment_sra_identifiers": GUARDED,
    "experiment_set_sra_identifiers": GUARDED,
    "experiment_controlled_keywords": GUARDED,
    "scoreset_meta_analysis_sources": GUARDED,
    "collection_score_sets": GUARDED,
    "collection_experiments": GUARDED,
    "urn_redirects": GUARDED,
    "pipelines": GUARDED,
    "job_runs": GUARDED,
    "job_dependencies": GUARDED,
    # Shared rows, visible when a record the caller can see links to them (#809).
    "alleles": GUARDED,
    "vep_allele_consequences": GUARDED,
    "clinvar_allele_links": GUARDED,
    "gnomad_allele_links": GUARDED,
    "clinvar_controls": GUARDED,
    "gnomad_variants": GUARDED,
    "variant_translations": GUARDED,
    "publication_identifiers": GUARDED,
    # Fixed vocabulary, not produced by anyone's work.
    "licenses": CARVE_OUT,
    "controlled_keywords": CARVE_OUT,
    "acmg_classifications": CARVE_OUT,
    "mondo_terms": CARVE_OUT,
    "genome_identifiers": CARVE_OUT,
    "taxonomies": CARVE_OUT,
    # Routes look up strangers by ORCID and list users, and the policies read it for the caller's ORCID. The
    # sensitive column, email, is returned only to the user and to admins.
    "users": CARVE_OUT,
    # Read by the parent records' policies; a policy here would loop back through the parent.
    "contributors": CARVE_OUT,
    "scoreset_contributors": CARVE_OUT,
    "experiment_contributors": CARVE_OUT,
    "experiment_set_contributors": CARVE_OUT,
    # Read by role checks.
    "users_roles": CARVE_OUT,
    "roles": CARVE_OUT,
    # Read by the collections policy.
    "collection_user_associations": CARVE_OUT,
    # Looked up before the caller is known; the listing route filters by user. The worker resolves no keys.
    "access_keys": Relation(ApiAccess.CARVE_OUT, worker=False),
    # Holds only published variants, and no record is both private and published (#893).
    "published_variants_materialized_view": CARVE_OUT,
    # Legacy, and no route reads them, but deleting a score set, experiment or experiment set loads them
    # through ORM relationships. The mapped variant tables go with #775; the keyword tables when their
    # relationships are removed.
    "mapped_variants": CARVE_OUT,
    "gnomad_variants_mapped_variants": CARVE_OUT,
    "mapped_variants_clinical_controls": CARVE_OUT,
    "keywords": CARVE_OUT,
    "scoreset_keywords": CARVE_OUT,
    "experiment_keywords": CARVE_OUT,
    "experiment_set_keywords": CARVE_OUT,
    # Only the worker and operator scripts read these. A route that needs one adds a policy and moves it.
    "annotation_event": WORKER_ONLY,
    "variant_annotation_status": WORKER_ONLY,
    "v_current_annotation_events": WORKER_ONLY,
    # No route reads it yet. It can become GUARDED once #809 makes it security_invoker; until then it would
    # read its tables as the owner, past their policies.
    "v_variant_annotations": WORKER_ONLY,
    # Alembic's record of the schema's revision. Only migrations, running as the owner, read or write it.
    "alembic_version": UNGRANTED,
    # Unmodelled tables left over in production, which nothing reads. Dropped by #829.
    "mavedb_ldh_records": UNGRANTED,
    "mavedb_ldh_records_audit": UNGRANTED,
    "mavedb_staging_mapped_clinvar_allele_ids_1": UNGRANTED,
    "mavedb_staging_mapped_variants_clinical_controls": UNGRANTED,
    "variants_to_sync": UNGRANTED,
    "vep_functional_consequence_archive": UNGRANTED,
}


# Every function the owner owns in the schema, by name, covering all its overloads. Only the listed roles may
# execute one. A SECURITY DEFINER function runs as the owner, so it gets one executor and a reason, and pins its
# search_path; one that doesn't pin it gets no executors.
FUNCTIONS: dict[str, Function] = {
    # Refreshing a materialized view requires owning it, and the worker owns nothing.
    "mavedb_refresh_mat_views": Function(frozenset({WORKER_ROLE}), security_definer=True),
    # From the 2026.3.0 baseline. Nothing calls it.
    "reset_sequence": Function(),
}
