"""add mavedb_user_id and mavedb_roles, which read the request's principal for RLS policies

The api sends ``app.user_id`` and ``app.active_roles`` into every transaction (``mavedb.db.principal``). Policies
read them only through these two functions. ``nullif`` turns the empty value a pooled connection keeps after a
transaction-local setting ends into "nobody", instead of a cast error. Both are plain SQL functions, not
``SECURITY DEFINER``, so Postgres inlines them into each policy and they bypass nothing.

Revision ID: 7c1d4e9a2b30
Revises: 4b8e2c7d1f3a
Create Date: 2026-11-12

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "7c1d4e9a2b30"
down_revision = "4b8e2c7d1f3a"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """CREATE FUNCTION mavedb_user_id() RETURNS integer LANGUAGE sql STABLE PARALLEL SAFE
AS $$ SELECT nullif(current_setting('app.user_id', true), '')::integer $$"""
    )
    op.execute(
        """CREATE FUNCTION mavedb_roles() RETURNS text[] LANGUAGE sql STABLE PARALLEL SAFE
AS $$ SELECT coalesce(nullif(current_setting('app.active_roles', true), '')::text[], '{}') $$"""
    )


def downgrade():
    op.execute("DROP FUNCTION mavedb_roles()")
    op.execute("DROP FUNCTION mavedb_user_id()")
