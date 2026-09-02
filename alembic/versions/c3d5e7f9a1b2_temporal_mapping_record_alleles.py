"""add valid-time versioning to mapping_record_alleles

Revision ID: c3d5e7f9a1b2
Revises: b8e1f0a2c4d7
Create Date: 2026-06-05

Make the link table valid-time versioned (TemporalLink): a link is live while valid_to is
NULL, and superseding it closes valid_to instead of deleting, so reverse translation can be
re-run independently while prior derivations remain queryable point-in-time. The partial
unique index enforces a single live link per (mapping_record, allele).

Assumes no pre-existing duplicate live links — true for these parallel tables, which are
new-only writes and not yet serving. If this ever runs against data with duplicates, the
unique index creation will fail and the duplicates must be retired first.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c3d5e7f9a1b2"
down_revision = "b8e1f0a2c4d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "mapping_record_alleles",
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.add_column(
        "mapping_record_alleles",
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "uq_mapping_record_alleles_live",
        "mapping_record_alleles",
        ["mapping_record_id", "allele_id"],
        unique=True,
        postgresql_where=sa.text("valid_to IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_mapping_record_alleles_live", table_name="mapping_record_alleles")
    op.drop_column("mapping_record_alleles", "valid_to")
    op.drop_column("mapping_record_alleles", "valid_from")
