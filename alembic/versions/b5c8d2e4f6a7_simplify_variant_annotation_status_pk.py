"""simplify variant_annotation_status pk to id only

Revision ID: b5c8d2e4f6a7
Revises: a3b7c9d1e2f4
Create Date: 2026-04-20

The composite PK (id, variant_id, annotation_type) is unnecessary because `id`
is already unique (autoincrement serial).  Keeping variant_id and annotation_type
in the PK just widens the B-tree on every INSERT with no benefit — no FK
references this composite key.

This migration drops the composite PK and recreates it on `id` alone.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b5c8d2e4f6a7"
down_revision = "a3b7c9d1e2f4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("variant_annotation_status_pkey", "variant_annotation_status", type_="primary")
    op.create_primary_key("variant_annotation_status_pkey", "variant_annotation_status", ["id"])


def downgrade() -> None:
    op.drop_constraint("variant_annotation_status_pkey", "variant_annotation_status", type_="primary")
    op.create_primary_key(
        "variant_annotation_status_pkey",
        "variant_annotation_status",
        ["id", "variant_id", "annotation_type"],
    )
