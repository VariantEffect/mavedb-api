"""add annotation_status and v_annotation_status_current

annotation_status replaces the append-only annotation_event log with a ValidTime table whose rows change only
when an outcome does (#973). This revision only adds it; annotation_event stays until the cutover revision.
annotation_event_backfill holds how far the backfill from annotation_event has got, which the cutover checks.

Revision ID: 9a73f37ec1bc
Revises: 020b7e003efd
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "9a73f37ec1bc"
down_revision = "020b7e003efd"
branch_labels = None
depends_on = None

VARIANT_SUBJECT_TYPES = "'vrs_mapping', 'cross_level_translation', 'ldh_submission'"
"""annotation_type values whose subject is the variant, as an SQL list."""

ALLELE_SUBJECT_TYPES = (
    "'clingen_allele_id', 'gnomad_allele_frequency', 'vep_functional_consequence', 'clinvar_allele_id', "
    "'clinvar_control'"
)
"""annotation_type values whose subject is the allele, as an SQL list."""


def upgrade():
    op.create_table(
        "annotation_status",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("annotation_type", sa.String(length=50), nullable=False),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("variant_id", sa.Integer(), nullable=True),
        sa.Column("allele_id", sa.Integer(), nullable=True),
        sa.Column("release", sa.String(length=50), nullable=True),
        sa.Column("disposition", sa.String(length=50), nullable=False),
        sa.Column("reason", sa.String(length=50), nullable=False),
        sa.Column("source_version", sa.String(length=50), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("failing_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("job_run_id", sa.Integer(), nullable=True),
        sa.Column("valid_from", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            f"(annotation_type IN ({VARIANT_SUBJECT_TYPES}) AND variant_id IS NOT NULL AND allele_id IS NULL) "
            f"OR (annotation_type IN ({ALLELE_SUBJECT_TYPES}) AND allele_id IS NOT NULL AND variant_id IS NULL)",
            name="ck_annotation_status_subject",
        ),
        sa.CheckConstraint(
            "(annotation_type IN ('clinvar_control')) = (release IS NOT NULL)",
            name="ck_annotation_status_release",
        ),
        sa.CheckConstraint(
            "(kind = 'outcome' AND disposition <> 'failed') OR (kind = 'failure' AND disposition = 'failed')",
            name="ck_annotation_status_kind",
        ),
        sa.CheckConstraint(
            "(kind = 'failure') = (failing_since IS NOT NULL)",
            name="ck_annotation_status_failing_since",
        ),
        sa.ForeignKeyConstraint(["allele_id"], ["alleles.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["job_run_id"], ["job_runs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["variant_id"], ["variants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_annotation_status_live",
        "annotation_status",
        ["allele_id", "variant_id", "annotation_type", "release", "kind"],
        unique=True,
        postgresql_where=sa.text("valid_to IS NULL"),
        postgresql_nulls_not_distinct=True,
    )
    op.create_index("ix_annotation_status_allele_type", "annotation_status", ["allele_id", "annotation_type"])
    op.create_index("ix_annotation_status_variant_type", "annotation_status", ["variant_id", "annotation_type"])
    op.create_index("ix_annotation_status_job_run_id", "annotation_status", ["job_run_id"])

    op.execute(
        """
        CREATE VIEW v_annotation_status_current WITH (security_invoker = true) AS
        SELECT
            allele_id,
            variant_id,
            annotation_type,
            release,
            max(disposition) FILTER (WHERE kind = 'outcome') AS disposition,
            max(reason) FILTER (WHERE kind = 'outcome') AS reason,
            max(source_version) FILTER (WHERE kind = 'outcome') AS source_version,
            (array_agg(metadata) FILTER (WHERE kind = 'outcome'))[1] AS status_metadata,
            max(valid_from) FILTER (WHERE kind = 'outcome') AS established_at,
            max(job_run_id) FILTER (WHERE kind = 'outcome') AS established_job_run_id,
            max(failing_since) FILTER (WHERE kind = 'failure') AS failing_since,
            max(reason) FILTER (WHERE kind = 'failure') AS failure_reason,
            (array_agg(metadata) FILTER (WHERE kind = 'failure'))[1] AS failure_metadata
        FROM annotation_status
        WHERE valid_to IS NULL
        GROUP BY allele_id, variant_id, annotation_type, release
        """
    )

    op.create_table(
        "annotation_event_backfill",
        sa.Column("id", sa.SmallInteger(), nullable=False),
        sa.Column("through_event_id", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("id = 1", name="ck_annotation_event_backfill_single_row"),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("annotation_event_backfill")
    op.execute("DROP VIEW v_annotation_status_current")
    op.drop_index("ix_annotation_status_job_run_id", table_name="annotation_status")
    op.drop_index("ix_annotation_status_variant_type", table_name="annotation_status")
    op.drop_index("ix_annotation_status_allele_type", table_name="annotation_status")
    op.drop_index("uq_annotation_status_live", table_name="annotation_status")
    op.drop_table("annotation_status")
