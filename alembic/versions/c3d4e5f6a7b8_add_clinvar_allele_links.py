"""add clinvar_allele_links table

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-06-22

New valid-time link table connecting deduplicated alleles to ClinvarControl rows, replacing
the frozen mapped_variants_clinical_controls association for new-model writes.

ClinVar's link shape is deliberately multi-live: the partial unique index is
(allele_id, clinvar_control_id) WHERE valid_to IS NULL, so an allele accumulates one live
link per ClinVar release rather than superseding as in gnomAD/VEP. Each ClinVar release is a
distinct ClinvarControl row, so different releases stack as independent live links. A link is
retired (valid_to closed) only if ClinVar later removes the variant from a release, which
would surface as a re-run finding no data for that release and retiring the corresponding
link — archival data never changes, so this path is theoretical.

The existing mapped_variants_clinical_controls association table is left untouched (frozen
for serving existing data).
"""

import sqlalchemy as sa

from alembic import op

revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "clinvar_allele_links",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("allele_id", sa.Integer(), nullable=False),
        sa.Column("clinvar_control_id", sa.Integer(), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["allele_id"],
            ["alleles.id"],
            name="fk_clinvar_allele_links_allele_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["clinvar_control_id"],
            ["clinvar_controls.id"],
            name="fk_clinvar_allele_links_clinvar_control_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_clinvar_allele_links_allele_id", "clinvar_allele_links", ["allele_id"])
    op.create_index("ix_clinvar_allele_links_clinvar_control_id", "clinvar_allele_links", ["clinvar_control_id"])
    # Multi-live: one live link per (allele, release). An allele accumulates one live link per
    # ClinVar release rather than superseding — unlike gnomAD/VEP which enforce one live link
    # per allele across all versions. Superseded rows (valid_to IS NOT NULL) are preserved for
    # point-in-time queries.
    op.create_index(
        "uq_clinvar_allele_links_live",
        "clinvar_allele_links",
        ["allele_id", "clinvar_control_id"],
        unique=True,
        postgresql_where=sa.text("valid_to IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_clinvar_allele_links_live", table_name="clinvar_allele_links")
    op.drop_index("ix_clinvar_allele_links_clinvar_control_id", table_name="clinvar_allele_links")
    op.drop_index("ix_clinvar_allele_links_allele_id", table_name="clinvar_allele_links")
    op.drop_table("clinvar_allele_links")
