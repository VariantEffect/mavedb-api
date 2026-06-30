"""add annotation_event log

Revision ID: a7e1c4f9b3d2
Revises: d4e5f6a7b8c9
Create Date: 2026-06-25 16:00:00.000000

"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "a7e1c4f9b3d2"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None

_VARIANT_SUBJECT_TYPES = "'vrs_mapping', 'cross_level_translation', 'variant_translation', 'ldh_submission'"
_ALLELE_SUBJECT_TYPES = (
    "'clingen_allele_id', 'gnomad_allele_frequency', 'vep_functional_consequence', 'clinvar_control', 'mapped_hgvs'"
)


def upgrade():
    op.create_table(
        "annotation_event",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("annotation_type", sa.String(length=50), nullable=False),
        sa.Column("variant_id", sa.Integer(), nullable=True),
        sa.Column("allele_id", sa.Integer(), nullable=True),
        sa.Column("disposition", sa.String(length=50), nullable=False),
        sa.Column("reason", sa.String(length=50), nullable=False),
        sa.Column("source_version", sa.String(length=50), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("job_run_id", sa.Integer(), nullable=True),
        sa.Column("score_set_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            f"(annotation_type IN ({_VARIANT_SUBJECT_TYPES}) "
            "AND variant_id IS NOT NULL AND allele_id IS NULL) "
            f"OR (annotation_type IN ({_ALLELE_SUBJECT_TYPES}) "
            "AND allele_id IS NOT NULL AND variant_id IS NULL)",
            name="ck_annotation_event_subject",
        ),
        sa.ForeignKeyConstraint(["variant_id"], ["variants.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["allele_id"], ["alleles.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["job_run_id"], ["job_runs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["score_set_id"], ["scoresets.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_annotation_event_allele_type_id",
        "annotation_event",
        ["allele_id", "annotation_type", sa.text("id DESC")],
        unique=False,
    )
    op.create_index(
        "ix_annotation_event_variant_type_id",
        "annotation_event",
        ["variant_id", "annotation_type", sa.text("id DESC")],
        unique=False,
    )
    op.create_index(
        "ix_annotation_event_allele_type_version",
        "annotation_event",
        ["allele_id", "annotation_type", "source_version"],
        unique=False,
    )
    op.create_index("ix_annotation_event_job_run_id", "annotation_event", ["job_run_id"], unique=False)


def downgrade():
    op.drop_index("ix_annotation_event_job_run_id", table_name="annotation_event")
    op.drop_index("ix_annotation_event_allele_type_version", table_name="annotation_event")
    op.drop_index("ix_annotation_event_variant_type_id", table_name="annotation_event")
    op.drop_index("ix_annotation_event_allele_type_id", table_name="annotation_event")
    op.drop_table("annotation_event")
