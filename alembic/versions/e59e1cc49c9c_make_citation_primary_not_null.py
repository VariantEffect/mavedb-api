"""make citation primary not null

``primary`` on score set and experiment citations was nullable, and legacy rows hold ``NULL``. Reads already treat
``NULL`` as secondary, so this stores it as ``false`` and makes the column ``NOT NULL`` with a ``false`` default, so
the column says what reads assume.

The downgrade can't tell backfilled ``false`` from submitted ``false``, so it leaves them as ``false``.

Revision ID: e59e1cc49c9c
Revises: d0c23896b0c5
Create Date: 2026-10-26

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "e59e1cc49c9c"
down_revision = "d0c23896b0c5"
branch_labels = None
depends_on = None

CITATIONS = ["scoreset_publication_identifiers", "experiment_publication_identifiers"]


def upgrade() -> None:
    for citation in CITATIONS:
        op.execute(f'UPDATE {citation} SET "primary" = false WHERE "primary" IS NULL')
        op.execute(
            f'ALTER TABLE {citation} ALTER COLUMN "primary" SET DEFAULT false, ALTER COLUMN "primary" SET NOT NULL'
        )


def downgrade() -> None:
    for citation in CITATIONS:
        op.execute(f'ALTER TABLE {citation} ALTER COLUMN "primary" DROP NOT NULL, ALTER COLUMN "primary" DROP DEFAULT')
