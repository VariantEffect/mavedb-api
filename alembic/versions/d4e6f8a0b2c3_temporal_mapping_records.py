"""move mapping_records onto valid-time versioning

Revision ID: d4e6f8a0b2c3
Revises: c3d5e7f9a1b2
Create Date: 2026-06-05

Replace the stored `current` flag and the `created_at`/`updated_at` audit dates with valid-time
columns (ValidTime mixin): a mapping record is live while valid_to is NULL, and a re-map retires
the prior version (closing valid_to) instead of flipping a boolean. `current` becomes derived
(valid_to IS NULL). `mapped_date` (the date the mapping was performed) is domain data and stays.

The partial unique index promotes to the database the "one live mapping record per variant"
invariant the mapping job previously enforced only in app code.

Backfills from the columns being dropped, so existing rows keep their validity. Assumes no
duplicate live records per variant (true for these pre-cutover parallel tables; otherwise the
unique index creation fails and the duplicates must be retired first).
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e6f8a0b2c3"
down_revision = "c3d5e7f9a1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "mapping_records",
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=True, server_default=sa.func.now()),
    )
    op.add_column(
        "mapping_records",
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
    )

    # Backfill validity from the columns being replaced: a record's life began at created_at, and
    # a non-current record was retired at updated_at (the only in-place update it ever took).
    op.execute("UPDATE mapping_records SET valid_from = created_at::timestamptz")
    op.execute("UPDATE mapping_records SET valid_to = updated_at::timestamptz WHERE current = false")

    op.alter_column("mapping_records", "valid_from", nullable=False)

    op.drop_column("mapping_records", "current")
    op.drop_column("mapping_records", "created_at")
    op.drop_column("mapping_records", "updated_at")

    op.create_index(
        "uq_mapping_records_current",
        "mapping_records",
        ["variant_id"],
        unique=True,
        postgresql_where=sa.text("valid_to IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_mapping_records_current", table_name="mapping_records")

    op.add_column("mapping_records", sa.Column("current", sa.Boolean(), nullable=True))
    op.add_column("mapping_records", sa.Column("created_at", sa.Date(), nullable=True))
    op.add_column("mapping_records", sa.Column("updated_at", sa.Date(), nullable=True))

    op.execute("UPDATE mapping_records SET current = (valid_to IS NULL)")
    op.execute("UPDATE mapping_records SET created_at = valid_from::date")
    op.execute("UPDATE mapping_records SET updated_at = coalesce(valid_to, valid_from)::date")

    op.alter_column("mapping_records", "current", nullable=False)
    op.alter_column("mapping_records", "created_at", nullable=False)
    op.alter_column("mapping_records", "updated_at", nullable=False)

    op.drop_column("mapping_records", "valid_to")
    op.drop_column("mapping_records", "valid_from")
