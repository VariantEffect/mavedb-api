"""add ix for mapped variant pre/post id

Revision ID: aa3933cf0cb3
Revises: f69b4049bc3b
Create Date: 2025-06-02 11:42:34.479411

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "aa3933cf0cb3"
down_revision = "f69b4049bc3b"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ix_mapped_variants_pre_mapped_id", "mapped_variants", [sa.text("(pre_mapped->>'id')")], unique=False
    )
    op.create_index(
        "ix_mapped_variants_post_mapped_id", "mapped_variants", [sa.text("(post_mapped->>'id')")], unique=False
    )


def downgrade():
    op.drop_index("ix_mapped_variants_pre_mapped_id", table_name="mapped_variants")
    op.drop_index("ix_mapped_variants_post_mapped_id", table_name="mapped_variants")
