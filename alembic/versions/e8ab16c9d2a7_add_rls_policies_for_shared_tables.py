"""add RLS policies to the tables whose rows any record can link to

A shared row belongs to no one record. It's visible when a record the caller can see links to it: its presence
can say what someone is working on, so these aren't public reference data. Each policy reads its link table,
whose own policy filters the subquery. Each table also gets a worker policy. Nothing is enforced until row-level
security is enabled (#825, #878).

- ``alleles``: a link from a visible score set, retired links included, or ``first_published_at``, which a
  public score set's link has already set.
- The rows fetched for alleles (VEP consequences, ClinVar and gnomAD links) follow their allele; ClinVar controls
  and gnomAD variants follow their links; a variant translation needs both of its alleles.
- ``publication_identifiers``: a citation from a visible record, by natural key (#945). Its ``WITH CHECK`` is
  ``true``: the api stores a publication before anything cites it (``INSERT ... ON CONFLICT DO NOTHING``, no
  ``RETURNING``), so the new row couldn't pass the read rule. Accepting any written row discloses nothing, and an
  update still reaches only the rows the caller can see.

Every policy is a correlated ``EXISTS``: ``IN`` over alleles can't be hashed in memory (#832).

Revision ID: e8ab16c9d2a7
Revises: d7fa05b8c196
Create Date: 2026-11-12

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "e8ab16c9d2a7"
down_revision = "d7fa05b8c196"
branch_labels = None
depends_on = None


def _cited_by(table: str) -> str:
    return (
        f"EXISTS (SELECT 1 FROM {table} c WHERE c.db_name = publication_identifiers.db_name "
        "AND c.identifier = publication_identifiers.identifier)"
    )


RULES = {
    "alleles": (
        "first_published_at IS NOT NULL "
        "OR EXISTS (SELECT 1 FROM mapping_record_alleles m WHERE m.allele_id = alleles.id)"
    ),
    "vep_allele_consequences": "EXISTS (SELECT 1 FROM alleles a WHERE a.id = vep_allele_consequences.allele_id)",
    "clinvar_allele_links": "EXISTS (SELECT 1 FROM alleles a WHERE a.id = clinvar_allele_links.allele_id)",
    "gnomad_allele_links": "EXISTS (SELECT 1 FROM alleles a WHERE a.id = gnomad_allele_links.allele_id)",
    "clinvar_controls": (
        "EXISTS (SELECT 1 FROM clinvar_allele_links l WHERE l.clinvar_control_id = clinvar_controls.id)"
    ),
    "gnomad_variants": "EXISTS (SELECT 1 FROM gnomad_allele_links l WHERE l.gnomad_variant_id = gnomad_variants.id)",
    "variant_translations": (
        "EXISTS (SELECT 1 FROM alleles a WHERE a.clingen_allele_id = variant_translations.aa_clingen_id) "
        "AND EXISTS (SELECT 1 FROM alleles a WHERE a.clingen_allele_id = variant_translations.nt_clingen_id)"
    ),
    "publication_identifiers": " OR ".join(
        _cited_by(table)
        for table in (
            "scoreset_publication_identifiers",
            "experiment_publication_identifiers",
            "score_calibration_publication_identifiers",
        )
    ),
}


# Tables whose written rows needn't pass the read rule.
CHECKS = {"publication_identifiers": "true"}


def upgrade():
    for table, predicate in RULES.items():
        check = CHECKS.get(table, predicate)
        op.execute(f"CREATE POLICY {table}_rule ON {table} FOR ALL USING ({predicate}) WITH CHECK ({check})")
        op.execute(f"CREATE POLICY {table}_worker ON {table} FOR ALL TO mavedb_worker USING (true) WITH CHECK (true)")


def downgrade():
    for table in reversed(RULES):
        op.execute(f"DROP POLICY {table}_worker ON {table}")
        op.execute(f"DROP POLICY {table}_rule ON {table}")
