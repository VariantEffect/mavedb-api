"""move clingen allele id to mapped variants table

Revision ID: d6e5a9fde3c9
Revises: 695b73abe581
Create Date: 2025-02-19 10:51:07.319962

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d6e5a9fde3c9"
down_revision = "695b73abe581"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_variants_clingen_allele_id", table_name="variants")
    op.add_column("mapped_variants", sa.Column("clingen_allele_id", sa.String(), nullable=True))
    op.execute(
        """
        UPDATE mapped_variants
        SET clingen_allele_id=variants.clingen_allele_id
        FROM variants
        WHERE variants.id=mapped_variants.variant_id
        """
    )
    op.drop_column("variants", "clingen_allele_id")
    op.create_index(
        op.f("ix_mapped_variants_clingen_allele_id"), "mapped_variants", ["clingen_allele_id"], unique=False
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_mapped_variants_clingen_allele_id"), table_name="mapped_variants")
    op.add_column("variants", sa.Column("clingen_allele_id", sa.String(), nullable=True))
    op.execute(
        """
        UPDATE variants
        SET clingen_allele_id=mapped_variants.clingen_allele_id
        FROM mapped_variants
        WHERE variants.id=mapped_variants.variant_id
        """
    )
    op.drop_column("mapped_variants", "clingen_allele_id")
    op.create_index("ix_variants_clingen_allele_id", "variants", ["clingen_allele_id"], unique=False)
    # ### end Alembic commands ###
