"""cascade annotation events when their variant is deleted

Revision ID: 393764b9e833
Revises: 45e8ec1db15e
Create Date: 2026-09-29

The mapping job records a variant-subject ``annotation_event`` for every variant it maps. With the foreign key
on ``RESTRICT``, deleting a mapped score set or re-uploading its variants failed on that key. Variant-subject
events now go with their variant, as ``variant_annotation_status`` rows do. Allele-subject events keep
``RESTRICT``: alleles are shared across score sets and are never deleted with one.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "393764b9e833"
down_revision = "45e8ec1db15e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("annotation_event_variant_id_fkey", "annotation_event", type_="foreignkey")
    op.create_foreign_key(
        "annotation_event_variant_id_fkey",
        "annotation_event",
        "variants",
        ["variant_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint("annotation_event_variant_id_fkey", "annotation_event", type_="foreignkey")
    op.create_foreign_key(
        "annotation_event_variant_id_fkey",
        "annotation_event",
        "variants",
        ["variant_id"],
        ["id"],
        ondelete="RESTRICT",
    )
