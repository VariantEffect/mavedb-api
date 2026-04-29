"""target_gene_mappings table and ScoreAnnotation columns on mapped_variants

Revision ID: 8c4a2f1d9e6b
Revises: e3a7b9f1d2c5
Create Date: 2026-04-28

Adds the ``target_gene_mappings`` table (per-(target gene, alignment level)
provenance and QC produced by the dcd-mapping QC API) and extends
``mapped_variants`` with the new ``ScoreAnnotation`` columns:

* ``target_gene_mapping_id`` -- FK to the QC record for this mapping (the
  resolved target gene is reachable via this relationship)
* ``alignment_level``        -- ``p`` / ``c`` / ``g``
* ``at_mismatched_locus`` / ``near_gap`` -- per-variant QC flags

The new columns are nullable because variants whose mapping fails before an
alignment is selected cannot be attributed to a specific target/layer.

Backfill of existing rows is performed by the standalone manual migration
``alembic/manual_migrations/migrate_target_gene_mapping_qc.py`` (it walks all
mapped variants and infers the (target_gene, alignment_level) tuple from the
HGVS strings).
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "8c4a2f1d9e6b"
down_revision = "e3a7b9f1d2c5"
branch_labels = None
depends_on = None


VALID_ALIGNMENT_LEVELS = "('protein', 'cdna', 'genomic')"


def upgrade() -> None:
    op.create_table(
        "target_gene_mappings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("target_gene_id", sa.Integer(), nullable=False),
        sa.Column("alignment_level", sa.String(length=16), nullable=False),
        sa.Column("preferred", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("reference_assembly", sa.String(), nullable=True),
        sa.Column("reference_accession", sa.String(), nullable=True),
        sa.Column("reference_sequence_id", sa.String(), nullable=True),
        sa.Column("alignment_score", sa.Float(), nullable=True),
        sa.Column("next_best_alignment_score", sa.Float(), nullable=True),
        sa.Column("alignment_length", sa.Integer(), nullable=True),
        sa.Column("alignment_string", sa.String(), nullable=True),
        sa.Column("mismatch_count", sa.Integer(), nullable=True),
        sa.Column("gap_count", sa.Integer(), nullable=True),
        sa.Column("percent_identity", sa.Float(), nullable=True),
        sa.Column("total_variants", sa.Integer(), nullable=True),
        sa.Column("variants_failed", sa.Integer(), nullable=True),
        sa.Column("variants_with_alignment_warnings", sa.Integer(), nullable=True),
        sa.Column("variants_mapped_cleanly", sa.Integer(), nullable=True),
        sa.Column("tool_name", sa.String(), nullable=False, server_default="dcd-mapping"),
        sa.Column("tool_version", sa.String(), nullable=False),
        sa.Column("tool_parameters", sa.dialects.postgresql.JSONB(), nullable=True),
        sa.Column("alignment_metadata", sa.dialects.postgresql.JSONB(), nullable=True),
        sa.Column("vrs_version", sa.String(), nullable=True),
        sa.Column("mapped_date", sa.Date(), nullable=True),
        sa.Column("creation_date", sa.Date(), nullable=False),
        sa.Column("modification_date", sa.Date(), nullable=False),
        sa.ForeignKeyConstraint(["target_gene_id"], ["target_genes.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            f"alignment_level IN {VALID_ALIGNMENT_LEVELS}",
            name="ck_target_gene_mappings_alignment_level_valid",
        ),
    )
    op.create_index(
        "ix_target_gene_mappings_target_gene_id",
        "target_gene_mappings",
        ["target_gene_id"],
    )
    op.create_index(
        "ix_target_gene_mappings_target_alignment",
        "target_gene_mappings",
        ["target_gene_id", "alignment_level"],
    )

    op.add_column(
        "mapped_variants",
        sa.Column("target_gene_mapping_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "mapped_variants",
        sa.Column("alignment_level", sa.String(length=16), nullable=True),
    )
    op.add_column("mapped_variants", sa.Column("at_mismatched_locus", sa.Boolean(), nullable=True))
    op.add_column("mapped_variants", sa.Column("near_gap", sa.Boolean(), nullable=True))

    op.create_foreign_key(
        "fk_mapped_variants_target_gene_mapping_id",
        "mapped_variants",
        "target_gene_mappings",
        ["target_gene_mapping_id"],
        ["id"],
    )
    op.create_index(
        "ix_mapped_variants_target_gene_mapping_id",
        "mapped_variants",
        ["target_gene_mapping_id"],
    )
    op.create_check_constraint(
        "ck_mapped_variants_alignment_level_valid",
        "mapped_variants",
        f"alignment_level IS NULL OR alignment_level IN {VALID_ALIGNMENT_LEVELS}",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_mapped_variants_alignment_level_valid",
        "mapped_variants",
        type_="check",
    )
    op.drop_index("ix_mapped_variants_target_gene_mapping_id", table_name="mapped_variants")
    op.drop_constraint("fk_mapped_variants_target_gene_mapping_id", "mapped_variants", type_="foreignkey")
    op.drop_column("mapped_variants", "near_gap")
    op.drop_column("mapped_variants", "at_mismatched_locus")
    op.drop_column("mapped_variants", "alignment_level")
    op.drop_column("mapped_variants", "target_gene_mapping_id")

    op.drop_index("ix_target_gene_mappings_target_alignment", table_name="target_gene_mappings")
    op.drop_index("ix_target_gene_mappings_target_gene_id", table_name="target_gene_mappings")
    op.drop_table("target_gene_mappings")
