"""Add contributors

Revision ID: 76e1e55bc5c1
Revises: 9702d32bacb3
Create Date: 2024-08-22 06:17:03.265438

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '76e1e55bc5c1'
down_revision = '9702d32bacb3'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('contributors',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('orcid_id', sa.String(), nullable=False),
    sa.Column('given_name', sa.String(), nullable=True),
    sa.Column('family_name', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_contributors_orcid_id'), 'contributors', ['orcid_id'], unique=False)
    op.create_table('experiment_set_contributors',
    sa.Column('experiment_set_id', sa.Integer(), nullable=False),
    sa.Column('contributor_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['contributor_id'], ['contributors.id'], ),
    sa.ForeignKeyConstraint(['experiment_set_id'], ['experiment_sets.id'], ),
    sa.PrimaryKeyConstraint('experiment_set_id', 'contributor_id')
    )
    op.create_table('experiment_contributors',
    sa.Column('experiment_id', sa.Integer(), nullable=False),
    sa.Column('contributor_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['contributor_id'], ['contributors.id'], ),
    sa.ForeignKeyConstraint(['experiment_id'], ['experiments.id'], ),
    sa.PrimaryKeyConstraint('experiment_id', 'contributor_id')
    )
    op.create_table('scoreset_contributors',
    sa.Column('scoreset_id', sa.Integer(), nullable=False),
    sa.Column('contributor_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['contributor_id'], ['contributors.id'], ),
    sa.ForeignKeyConstraint(['scoreset_id'], ['scoresets.id'], ),
    sa.PrimaryKeyConstraint('scoreset_id', 'contributor_id')
    )


def downgrade():
    op.drop_table('scoreset_contributors')
    op.drop_table('experiment_contributors')
    op.drop_table('experiment_set_contributors')
    op.drop_index(op.f('ix_contributors_orcid_id'), table_name='contributors')
    op.drop_table('contributors')
