"""Add ClinGen allele IDs

Revision ID: e8a3b5d8f885
Revises: b85bc7b1bec7
Create Date: 2025-01-27 18:55:09.283855

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e8a3b5d8f885"
down_revision = "b85bc7b1bec7"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("variants", sa.Column("clingen_allele_id", sa.String(), nullable=True))
    op.create_index(op.f("ix_variants_clingen_allele_id"), "variants", ["clingen_allele_id"], unique=False)


def downgrade():
    op.drop_index(op.f("ix_variants_clingen_allele_id"), table_name="variants")
    op.drop_column("variants", "clingen_allele_id")
