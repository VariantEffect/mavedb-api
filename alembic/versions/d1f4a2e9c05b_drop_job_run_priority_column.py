"""drop job_run priority column

Revision ID: d1f4a2e9c05b
Revises: c6d9e3f7a8b2
Create Date: 2026-04-21 00:00:00.000000

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "d1f4a2e9c05b"
down_revision = "c6d9e3f7a8b2"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("ck_job_runs_priority_positive", "job_runs", type_="check")
    op.drop_column("job_runs", "priority")


def downgrade():
    op.add_column("job_runs", sa.Column("priority", sa.Integer(), nullable=False, server_default="0"))
    op.create_check_constraint("ck_job_runs_priority_positive", "job_runs", "priority >= 0")
    op.alter_column("job_runs", "priority", server_default=None)
