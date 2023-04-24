"""empty message

Revision ID: 988ca84c701b
Revises: 9d566d915a2c
Create Date: 2023-04-10 05:19:28.099693

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '988ca84c701b'
down_revision = '9d566d915a2c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('licenses',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('long_name', sa.String(), nullable=False),
    sa.Column('short_name', sa.String(), nullable=False),
    sa.Column('text', sa.String(), nullable=False),
    sa.Column('link', sa.String(), nullable=True),
    sa.Column('version', sa.String(), nullable=True),
    sa.Column('creation_date', sa.Date(), nullable=False),
    sa.Column('modification_date', sa.Date(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('long_name'),
    sa.UniqueConstraint('short_name')
    )
    op.create_index(op.f('ix_licenses_id'), 'licenses', ['id'], unique=False)
    op.alter_column('scoresets', 'licence_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.create_foreign_key(None, 'scoresets', 'licenses', ['licence_id'], ['id'])
    op.create_foreign_key(None, 'scoresets', 'scoresets', ['replaces_id'], ['id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'scoresets', type_='foreignkey')
    op.drop_constraint(None, 'scoresets', type_='foreignkey')
    op.alter_column('scoresets', 'licence_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.drop_index(op.f('ix_licenses_id'), table_name='licenses')
    op.drop_table('licenses')
    # ### end Alembic commands ###
