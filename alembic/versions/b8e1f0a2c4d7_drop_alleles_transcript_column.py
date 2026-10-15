"""drop alleles.transcript column

Revision ID: b8e1f0a2c4d7
Revises: f4d2a9c1b7e3
Create Date: 2026-06-05

The `transcript` column duplicated data already present in the HGVS columns — it was
always extract_accession(hgvs_g/hgvs_c/hgvs_p). It is now a derived hybrid_property on
the Allele model (split_part(coalesce(hgvs_g, hgvs_c, hgvs_p), ':', 1)), so the stored
column is removed to keep a single source of truth and avoid drift.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b8e1f0a2c4d7"
down_revision = "f4d2a9c1b7e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("alleles", "transcript")


def downgrade() -> None:
    # Re-add the column and backfill it from the HGVS columns (the same derivation the
    # hybrid_property uses) so the restored NOT NULL column is consistent.
    op.add_column("alleles", sa.Column("transcript", sa.String(), nullable=True))
    op.execute("UPDATE alleles SET transcript = split_part(coalesce(hgvs_g, hgvs_c, hgvs_p), ':', 1)")
    op.alter_column("alleles", "transcript", nullable=False)
