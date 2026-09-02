"""add gnomad_allele_links table

Revision ID: e5f7a9c1b3d4
Revises: d4e6f8a0b2c3
Create Date: 2026-06-18

New valid-time link table connecting deduplicated alleles to gnomAD variants, replacing the frozen
gnomad_variants_mapped_variants association for new-model writes (Step 1 of the annotation
infrastructure migration, docs/design/annotation-infrastructure-migration.md). A link is live while
valid_to is NULL; the partial unique index enforces a single live link per (allele, gnomad variant)
pair. The existing gnomad_variants_mapped_variants table is left untouched (frozen serving).
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e5f7a9c1b3d4"
down_revision = "d4e6f8a0b2c3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "gnomad_allele_links",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("allele_id", sa.Integer(), nullable=False),
        sa.Column("gnomad_variant_id", sa.Integer(), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["allele_id"],
            ["alleles.id"],
            name="fk_gnomad_allele_links_allele_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["gnomad_variant_id"],
            ["gnomad_variants.id"],
            name="fk_gnomad_allele_links_gnomad_variant_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_gnomad_allele_links_allele_id",
        "gnomad_allele_links",
        ["allele_id"],
    )
    op.create_index(
        "ix_gnomad_allele_links_gnomad_variant_id",
        "gnomad_allele_links",
        ["gnomad_variant_id"],
    )
    # One live link per allele: gnomAD frequency is a single current value, so a version bump
    # supersedes the prior link rather than accumulating one live link per version.
    op.create_index(
        "uq_gnomad_allele_links_live",
        "gnomad_allele_links",
        ["allele_id"],
        unique=True,
        postgresql_where=sa.text("valid_to IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_gnomad_allele_links_live", table_name="gnomad_allele_links")
    op.drop_index("ix_gnomad_allele_links_gnomad_variant_id", table_name="gnomad_allele_links")
    op.drop_index("ix_gnomad_allele_links_allele_id", table_name="gnomad_allele_links")
    op.drop_table("gnomad_allele_links")
