"""Add mapping_records, alleles, and mapping_record_alleles tables

Revision ID: a1b2c3d4e5f6
Revises: a7f3c2e9b104
Create Date: 2026-05-29

New parallel tables for the Better Reverse Translation epic (#746).
The existing mapped_variants table is left untouched (frozen serving).
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "a7f3c2e9b104"
branch_labels = None
depends_on = None

VALID_ASSAY_LEVELS = "('genomic', 'cdna', 'protein')"
VALID_ALIGNMENT_LEVELS = "('protein', 'cdna', 'genomic')"


def upgrade() -> None:
    op.create_table(
        "alleles",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("vrs_digest", sa.String(), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("transcript", sa.String(), nullable=False),
        sa.Column("hgvs_g", sa.String(), nullable=True),
        sa.Column("hgvs_c", sa.String(), nullable=True),
        sa.Column("hgvs_p", sa.String(), nullable=True),
        sa.Column("clingen_allele_id", sa.String(), nullable=True),
        sa.Column("post_mapped", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.Date(), nullable=False, server_default=sa.text("CURRENT_DATE")),
        sa.Column(
            "updated_at",
            sa.Date(),
            nullable=False,
            server_default=sa.text("CURRENT_DATE"),
            onupdate=sa.text("CURRENT_DATE"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("vrs_digest", name="uq_alleles_vrs_digest"),
    )
    op.create_index("ix_alleles_vrs_digest", "alleles", ["vrs_digest"])
    op.create_index("ix_alleles_level", "alleles", ["level"])
    op.create_index("ix_alleles_clingen_allele_id", "alleles", ["clingen_allele_id"])

    op.create_table(
        "mapping_records",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("variant_id", sa.Integer(), nullable=False),
        sa.Column("vrs_digest", sa.String(), nullable=True),
        sa.Column("pre_mapped", postgresql.JSONB(), nullable=True),
        sa.Column("assay_level", sa.String(length=16), nullable=False),
        sa.Column("hgvs_assay_level", sa.String(), nullable=True),
        sa.Column("mapping_api_version", sa.String(), nullable=False),
        sa.Column("mapped_date", sa.Date(), nullable=False),
        sa.Column("vrs_version", sa.String(), nullable=True),
        sa.Column("current", sa.Boolean(), nullable=False),
        sa.Column("alignment_level", sa.String(length=16), nullable=True),
        sa.Column("at_mismatched_locus", sa.Boolean(), nullable=True),
        sa.Column("near_gap", sa.Boolean(), nullable=True),
        sa.Column("target_gene_mapping_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.Date(), nullable=False, server_default=sa.text("CURRENT_DATE")),
        sa.Column(
            "updated_at",
            sa.Date(),
            nullable=False,
            server_default=sa.text("CURRENT_DATE"),
            onupdate=sa.text("CURRENT_DATE"),
        ),
        sa.ForeignKeyConstraint(
            ["variant_id"],
            ["variants.id"],
            name="fk_mapping_records_variant_id",
        ),
        sa.ForeignKeyConstraint(
            ["target_gene_mapping_id"],
            ["target_gene_mappings.id"],
            name="fk_mapping_records_target_gene_mapping_id",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            f"assay_level IN {VALID_ASSAY_LEVELS}",
            name="ck_mapping_records_assay_level_valid",
        ),
    )
    op.create_index("ix_mapping_records_variant_id", "mapping_records", ["variant_id"])
    op.create_index("ix_mapping_records_vrs_digest", "mapping_records", ["vrs_digest"])
    op.create_index(
        "ix_mapping_records_target_gene_mapping_id",
        "mapping_records",
        ["target_gene_mapping_id"],
    )

    op.create_table(
        "mapping_record_alleles",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("mapping_record_id", sa.Integer(), nullable=False),
        sa.Column("allele_id", sa.Integer(), nullable=False),
        sa.Column(
            "is_authoritative",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.ForeignKeyConstraint(
            ["mapping_record_id"],
            ["mapping_records.id"],
            name="fk_mapping_record_alleles_mapping_record_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["allele_id"],
            ["alleles.id"],
            name="fk_mapping_record_alleles_allele_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_mapping_record_alleles_mapping_record_id",
        "mapping_record_alleles",
        ["mapping_record_id"],
    )
    op.create_index(
        "ix_mapping_record_alleles_allele_id",
        "mapping_record_alleles",
        ["allele_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_mapping_record_alleles_allele_id", table_name="mapping_record_alleles")
    op.drop_index("ix_mapping_record_alleles_mapping_record_id", table_name="mapping_record_alleles")
    op.drop_table("mapping_record_alleles")

    op.drop_index("ix_mapping_records_target_gene_mapping_id", table_name="mapping_records")
    op.drop_index("ix_mapping_records_vrs_digest", table_name="mapping_records")
    op.drop_index("ix_mapping_records_variant_id", table_name="mapping_records")
    op.drop_table("mapping_records")

    op.drop_index("ix_alleles_clingen_allele_id", table_name="alleles")
    op.drop_index("ix_alleles_level", table_name="alleles")
    op.drop_index("ix_alleles_vrs_digest", table_name="alleles")
    op.drop_table("alleles")
