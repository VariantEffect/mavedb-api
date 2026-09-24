"""add v_current_annotation_events view

Revision ID: b8f2c5a1d3e4
Revises: a7e1c4f9b3d2
Create Date: 2026-06-26

Creates the v_current_annotation_events view: the latest AnnotationEvent per (subject, annotation_type),
with ClinVar partitioned additionally by source_version (multi-live, one current row per release).
Replaces the per-variant v_variant_annotations as the current-state projection over the new
allele-model annotation log. Intended for operator queries, BI, and the annotation CLI scripts.
"""

from alembic import op

from mavedb.db.view import CreateView, DropView
from mavedb.models.annotation_event_view import definition, signature

# revision identifiers, used by Alembic.
revision = "b8f2c5a1d3e4"
down_revision = "a7e1c4f9b3d2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(CreateView(signature, definition, materialized=False))


def downgrade() -> None:
    op.execute(DropView(signature, materialized=False))
