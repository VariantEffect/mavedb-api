"""add RLS policies to the tables whose rows belong to one record

An owned row is visible exactly when the record that owns it is. Each policy reads its parent table, whose own
policy filters the subquery, so the rule for a record lives in one place. Each table also gets a worker policy
showing ``mavedb_worker`` every row. Nothing is enforced until row-level security is enabled (#825, #878).

``variants`` and ``target_genes`` use ``IN (SELECT id FROM scoresets)``, where Postgres hashes the visible score
set ids once. Every other table uses a correlated ``EXISTS``: below ``variants`` the planner can't hash the
visible ids or misprices the hashed subplan per lookup, and scans whole tables instead (#832).

Revision ID: a4c7d2e5f863
Revises: 9a3b6c1d4e52
Create Date: 2026-11-12

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a4c7d2e5f863"
down_revision = "9a3b6c1d4e52"
branch_labels = None
depends_on = None


def _exists(parent: str, key: str, table: str, column: str) -> str:
    return f"EXISTS (SELECT 1 FROM {parent} p WHERE p.{key} = {table}.{column})"


def _owned_by(parent: str, table: str, column: str) -> str:
    return _exists(parent, "id", table, column)


RULES = {
    "variants": "scoreset_id IN (SELECT id FROM scoresets)",
    "target_genes": "scoreset_id IN (SELECT id FROM scoresets)",
    "mapping_records": _owned_by("scoresets", "mapping_records", "score_set_id"),
    "mapping_record_alleles": _owned_by("scoresets", "mapping_record_alleles", "score_set_id"),
    "projection_groups": _owned_by("scoresets", "projection_groups", "score_set_id"),
    "target_gene_external_identifiers": _owned_by("target_genes", "target_gene_external_identifiers", "target_gene_id"),
    "target_gene_mappings": _owned_by("target_genes", "target_gene_mappings", "target_gene_id"),
    "score_calibration_functional_classifications": _owned_by(
        "score_calibrations", "score_calibration_functional_classifications", "calibration_id"
    ),
    "score_calibration_publication_identifiers": _owned_by(
        "score_calibrations", "score_calibration_publication_identifiers", "score_calibration_id"
    ),
    "calibration_controls": _owned_by("score_calibrations", "calibration_controls", "calibration_id"),
    # Measured against IN (SELECT id FROM score_calibration_functional_classifications) before #809 merges.
    "score_calibration_functional_classification_variants": _owned_by(
        "score_calibration_functional_classifications",
        "score_calibration_functional_classification_variants",
        "functional_classification_id",
    ),
    "scoreset_publication_identifiers": _owned_by("scoresets", "scoreset_publication_identifiers", "scoreset_id"),
    "experiment_publication_identifiers": _owned_by(
        "experiments", "experiment_publication_identifiers", "experiment_id"
    ),
    "scoreset_doi_identifiers": _owned_by("scoresets", "scoreset_doi_identifiers", "scoreset_id"),
    "experiment_doi_identifiers": _owned_by("experiments", "experiment_doi_identifiers", "experiment_id"),
    "experiment_set_doi_identifiers": _owned_by(
        "experiment_sets", "experiment_set_doi_identifiers", "experiment_set_id"
    ),
    "scoreset_sra_identifiers": _owned_by("scoresets", "scoreset_sra_identifiers", "scoreset_id"),
    "experiment_sra_identifiers": _owned_by("experiments", "experiment_sra_identifiers", "experiment_id"),
    "experiment_set_sra_identifiers": _owned_by(
        "experiment_sets", "experiment_set_sra_identifiers", "experiment_set_id"
    ),
    "experiment_controlled_keywords": _owned_by("experiments", "experiment_controlled_keywords", "experiment_id"),
    # Both sides, so neither record discloses the other.
    "scoreset_meta_analysis_sources": (
        _owned_by("scoresets", "scoreset_meta_analysis_sources", "source_scoreset_id")
        + " AND "
        + _owned_by("scoresets", "scoreset_meta_analysis_sources", "meta_analysis_scoreset_id")
    ),
    # Both sides, so a caller can't learn that a private collection holds a public record.
    "collection_score_sets": (
        _owned_by("collections", "collection_score_sets", "collection_id")
        + " AND "
        + _owned_by("scoresets", "collection_score_sets", "score_set_id")
    ),
    "collection_experiments": (
        _owned_by("collections", "collection_experiments", "collection_id")
        + " AND "
        + _owned_by("experiments", "collection_experiments", "experiment_id")
    ),
    # Publication writes the row onto the record it's making public, and requests are forwarded before
    # authentication, so a redirect is visible when the record it names is.
    "urn_redirects": (
        _exists("scoresets", "urn", "urn_redirects", "new_urn")
        + " OR "
        + _exists("experiments", "urn", "urn_redirects", "new_urn")
        + " OR "
        + _exists("experiment_sets", "urn", "urn_redirects", "new_urn")
    ),
    "pipelines": "created_by_user_id = mavedb_user_id() OR mavedb_roles() && '{admin}'",
    # A run with no pipeline, an ad hoc run, has no creator to show it to, so only admins see it.
    "job_runs": _owned_by("pipelines", "job_runs", "pipeline_id") + " OR mavedb_roles() && '{admin}'",
    "job_dependencies": _owned_by("job_runs", "job_dependencies", "id"),
}


def upgrade():
    for table, predicate in RULES.items():
        op.execute(f"CREATE POLICY {table}_rule ON {table} FOR ALL USING ({predicate}) WITH CHECK ({predicate})")
        op.execute(f"CREATE POLICY {table}_worker ON {table} FOR ALL TO mavedb_worker USING (true) WITH CHECK (true)")


def downgrade():
    for table in reversed(RULES):
        op.execute(f"DROP POLICY {table}_worker ON {table}")
        op.execute(f"DROP POLICY {table}_rule ON {table}")
