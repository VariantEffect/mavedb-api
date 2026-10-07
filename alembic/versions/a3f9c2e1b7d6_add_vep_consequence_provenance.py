"""add resolution provenance to vep_allele_consequences

Revision ID: a3f9c2e1b7d6
Revises: b2e5a8d3f0c1
Create Date: 2026-08-26

Adds four nullable columns to ``vep_allele_consequences`` so a stored consequence records *how* it was
resolved, not just the headline term (#772):

- ``consequence_terms`` (JSONB) — every term from the matched transcript entry, severity-ordered.
- ``consequence_source`` (varchar check-constraint enum) — ``transcript`` | ``most_severe`` |
  ``reference_identical``.
- ``matched_transcript`` (varchar) — the versioned transcript VEP used; NULL unless the source is
  ``transcript``.
- ``resolver_version`` (varchar) — ``variant_annotation.lib.vep.RESOLVER_VERSION``, the version of our
  resolution *rule* (severity ranking, transcript-matching, Recoder combination). Pairs with
  ``source_version`` (the Ensembl release) as the second version axis the current-release skip keys on,
  so a rule fix shipped in the shared kernel re-queries every allele instead of looking current forever.

All nullable, no backfill: rows written before this migration keep NULL provenance / resolver_version.
A NULL ``resolver_version`` never matches the current one, so a legacy row is re-queried on the next VEP
run and then filled in place (the linker supersedes on a value change, and a NULL→populated column for
an unchanged headline term advances in place rather than churning history).
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "a3f9c2e1b7d6"
down_revision = "b2e5a8d3f0c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "vep_allele_consequences",
        sa.Column("consequence_terms", postgresql.JSONB(none_as_null=True), nullable=True),
    )
    op.add_column(
        "vep_allele_consequences",
        sa.Column(
            "consequence_source",
            sa.Enum(
                "transcript",
                "most_severe",
                "reference_identical",
                name="vepconsequencesource",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=True,
        ),
    )
    op.add_column(
        "vep_allele_consequences",
        sa.Column("matched_transcript", sa.String(), nullable=True),
    )
    op.add_column(
        "vep_allele_consequences",
        sa.Column("resolver_version", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("vep_allele_consequences", "resolver_version")
    op.drop_column("vep_allele_consequences", "matched_transcript")
    op.drop_column("vep_allele_consequences", "consequence_source")
    op.drop_column("vep_allele_consequences", "consequence_terms")
