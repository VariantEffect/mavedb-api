"""add v_variant_annotations view

Revision ID: f2a1b3c4d5e6
Revises: e3a7b9f1d2c5
Create Date: 2026-05-03

Creates the v_variant_annotations convenience view, which joins variants, mapped_variants,
variant_annotation_status, and scoresets into a single flat row per (variant, annotation_type).
Intended for operator queries and the variant_annotations CLI script.
"""

from alembic import op

from mavedb.db.view import CreateView, DropView
from mavedb.models.variant_annotation_view import definition, signature

# revision identifiers, used by Alembic.
revision = "f2a1b3c4d5e6"
down_revision = "e3a7b9f1d2c5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(CreateView(signature, definition, materialized=False))


def downgrade() -> None:
    op.execute(DropView(signature, materialized=False))
