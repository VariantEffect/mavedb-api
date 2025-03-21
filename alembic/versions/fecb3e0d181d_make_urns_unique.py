"""empty message

Revision ID: fecb3e0d181d
Revises: c6154dd7d9b9
Create Date: 2023-11-15 19:45:42.769529

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "fecb3e0d181d"
down_revision = "c6154dd7d9b9"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(op.f("ix_experiment_sets_urn"), "experiment_sets", ["urn"], unique=True)
    op.create_index(op.f("ix_experiments_urn"), "experiments", ["urn"], unique=True)
    op.create_index(op.f("ix_scoresets_urn"), "scoresets", ["urn"], unique=True)
    op.create_index(op.f("ix_variants_urn"), "variants", ["urn"], unique=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_variants_urn"), table_name="variants")
    op.drop_index(op.f("ix_scoresets_urn"), table_name="scoresets")
    op.drop_index(op.f("ix_experiments_urn"), table_name="experiments")
    op.drop_index(op.f("ix_experiment_sets_urn"), table_name="experiment_sets")
    # ### end Alembic commands ###
