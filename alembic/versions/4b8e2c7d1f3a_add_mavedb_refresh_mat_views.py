"""add mavedb_refresh_mat_views, which refreshes materialized views as their owner

Refreshing a materialized view requires owning it. The worker logs in as ``mavedb_worker``, which owns nothing
(#827), so it calls this ``SECURITY DEFINER`` function instead. Run as ``mavedb_migrate``, the function belongs
to ``mavedb_owner``, whose default privileges keep ``PUBLIC`` from executing it. The role bootstrap's grants
step, which runs in the same transaction, lets ``mavedb_worker`` execute it. Keep the body identical to
``REFRESH_MAT_VIEWS_FUNCTION`` in ``mavedb.db.view``.

Revision ID: 4b8e2c7d1f3a
Revises: e59e1cc49c9c
Create Date: 2026-11-06

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "4b8e2c7d1f3a"
down_revision = "e59e1cc49c9c"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE OR REPLACE FUNCTION mavedb_refresh_mat_views(view_name regclass DEFAULT NULL, concurrent boolean DEFAULT true)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    mv regclass;
    refreshed integer := 0;
BEGIN
    FOR mv IN
        SELECT c.oid::regclass
          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'm' AND n.nspname = 'public' AND (view_name IS NULL OR c.oid = view_name)
         ORDER BY c.relname
    LOOP
        IF concurrent THEN
            EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY ' || mv::text;
        ELSE
            EXECUTE 'REFRESH MATERIALIZED VIEW ' || mv::text;
        END IF;
        refreshed := refreshed + 1;
    END LOOP;

    IF view_name IS NOT NULL AND refreshed = 0 THEN
        RAISE EXCEPTION USING MESSAGE = view_name::text || ' is not a materialized view in the public schema';
    END IF;
END
$$;""")


def downgrade():
    op.execute("DROP FUNCTION mavedb_refresh_mat_views(regclass, boolean)")
