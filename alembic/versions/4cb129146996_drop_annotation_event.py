"""drop annotation_event, v_current_annotation_events and annotation_event_backfill

annotation_status replaced the event log (#973). This revision refuses to run until
``scripts/backfill_annotation_status.py`` has replayed every event into annotation_status, so nothing the log
holds is lost. Archive the table first (``pg_dump -t annotation_event``) if its raw history should be kept. The
downgrade recreates the tables empty.

Revision ID: 4cb129146996
Revises: 9a73f37ec1bc
Create Date: 2026-11-24
"""

import sqlalchemy as sa

from alembic import op

revision = "4cb129146996"
down_revision = "9a73f37ec1bc"
branch_labels = None
depends_on = None


def upgrade():
    latest, backfilled = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT (SELECT coalesce(max(id), 0) FROM annotation_event), "
                "(SELECT coalesce(max(through_event_id), 0) FROM annotation_event_backfill)"
            )
        )
        .one()
    )
    if backfilled < latest:
        raise RuntimeError(
            f"annotation_event has events up to {latest}, but the backfill has only reached {backfilled}. Run "
            "`python -m mavedb.scripts.backfill_annotation_status run --commit` with annotation jobs paused, then "
            "retry."
        )

    op.execute("DROP VIEW v_current_annotation_events")
    op.drop_table("annotation_event")
    op.drop_table("annotation_event_backfill")


def downgrade():
    op.create_table(
        "annotation_event_backfill",
        sa.Column("id", sa.SmallInteger(), nullable=False),
        sa.Column("through_event_id", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("id = 1", name="ck_annotation_event_backfill_single_row"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "annotation_event",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("annotation_type", sa.String(length=50), nullable=False),
        sa.Column("variant_id", sa.Integer(), nullable=True),
        sa.Column("allele_id", sa.Integer(), nullable=True),
        sa.Column("disposition", sa.String(length=50), nullable=False),
        sa.Column("reason", sa.String(length=50), nullable=False),
        sa.Column("source_version", sa.String(length=50), nullable=True),
        sa.Column("metadata", sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("job_run_id", sa.Integer(), nullable=True),
        sa.Column("score_set_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "(annotation_type IN ('vrs_mapping', 'cross_level_translation', 'variant_translation', 'ldh_submission') "
            "AND variant_id IS NOT NULL AND allele_id IS NULL) "
            "OR (annotation_type IN ('clingen_allele_id', 'gnomad_allele_frequency', 'vep_functional_consequence', "
            "'clinvar_control', 'mapped_hgvs') AND allele_id IS NOT NULL AND variant_id IS NULL)",
            name="ck_annotation_event_subject",
        ),
        sa.ForeignKeyConstraint(["allele_id"], ["alleles.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["job_run_id"], ["job_runs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["score_set_id"], ["scoresets.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["variant_id"], ["variants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(
        "CREATE INDEX ix_annotation_event_allele_type_id ON annotation_event (allele_id, annotation_type, id DESC)"
    )
    op.execute(
        "CREATE INDEX ix_annotation_event_variant_type_id ON annotation_event (variant_id, annotation_type, id DESC)"
    )
    op.create_index(
        "ix_annotation_event_allele_type_version",
        "annotation_event",
        ["allele_id", "annotation_type", "source_version"],
    )
    op.create_index("ix_annotation_event_job_run_id", "annotation_event", ["job_run_id"])
    op.create_index("ix_annotation_event_score_set_id", "annotation_event", ["score_set_id"])
    op.execute(
        """
        CREATE VIEW v_current_annotation_events AS
        SELECT id, annotation_type, variant_id, allele_id, disposition, reason, source_version, event_metadata,
               job_run_id, created_at
        FROM (
            SELECT id, annotation_type, variant_id, allele_id, disposition, reason, source_version,
                   metadata AS event_metadata, job_run_id, created_at,
                   row_number() OVER (
                       PARTITION BY allele_id, variant_id, annotation_type,
                           CASE WHEN annotation_type = 'clinvar_control' THEN source_version END
                       ORDER BY id DESC
                   ) AS row_number
            FROM annotation_event
        ) ranked_annotation_events
        WHERE row_number = 1
        """
    )
