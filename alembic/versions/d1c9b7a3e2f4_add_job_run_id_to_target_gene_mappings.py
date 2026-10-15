"""add job_run_id to target_gene_mappings

Revision ID: d1c9b7a3e2f4
Revises: a3f9c2e1b7d6
Create Date: 2026-09-02

Adds a nullable ``job_run_id`` (FK ``job_runs``, ``ON DELETE SET NULL``) recording the mapping job that
produced each row. Reverse translation uses it to bind a variant's cross-level transcript to its *own* run's
cdna alignment rather than to any same-day run's (#763): the day-granular ``mapped_date`` key it previously
used cannot tell two runs on one calendar day apart, so a same-day remap that emits no cdna row would bind the
earlier run's stale transcript instead of correctly skipping.

Nullable, no backfill: rows written before this migration keep NULL and fall back to the coarser
``(target_gene_id, mapped_date)`` match. ``SET NULL`` so a pruned job_run degrades to that fallback rather
than dangling.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "d1c9b7a3e2f4"
down_revision = "a3f9c2e1b7d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("target_gene_mappings", sa.Column("job_run_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_target_gene_mappings_job_run_id",
        "target_gene_mappings",
        "job_runs",
        ["job_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_target_gene_mappings_job_run_id", "target_gene_mappings", ["job_run_id"])


def downgrade() -> None:
    op.drop_index("ix_target_gene_mappings_job_run_id", table_name="target_gene_mappings")
    op.drop_constraint("fk_target_gene_mappings_job_run_id", "target_gene_mappings", type_="foreignkey")
    op.drop_column("target_gene_mappings", "job_run_id")
