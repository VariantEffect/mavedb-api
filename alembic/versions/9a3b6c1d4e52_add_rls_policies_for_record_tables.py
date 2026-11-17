"""add RLS policies to scoresets, experiments, experiment_sets, collections and score_calibrations

Each table gets a business policy (``<table>_rule``) deciding which rows the api sees and writes, and a worker
policy (``<table>_worker``) showing ``mavedb_worker`` every row. Nothing is enforced until a table's row-level
security is enabled (#825, #878), so this changes no behavior.

A contributor is matched by ORCID: contributor rows hold an ORCID, not a user id, so each rule looks up the
caller's ORCID in ``users``. An experiment is also visible when one of its score sets is, and an experiment set
when one of its experiments is (#564). Those subqueries are filtered by the child table's own policy, and no child
policy reads its parent, so the policies don't recurse.

A calibration carries its own rule instead of delegating, because a public score set can hold a private
calibration, and it's visible only if its score set is.

Revision ID: 9a3b6c1d4e52
Revises: 8e2f5a0b3c41
Create Date: 2026-11-12

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "9a3b6c1d4e52"
down_revision = "8e2f5a0b3c41"
branch_labels = None
depends_on = None

SCORESETS = """
     private = false
  OR created_by_id = mavedb_user_id()
  OR mavedb_roles() && '{admin,mapper}'
  OR id IN (SELECT sc.scoreset_id FROM scoreset_contributors sc
              JOIN contributors c ON c.id = sc.contributor_id
              JOIN users u ON u.username = c.orcid_id
             WHERE u.id = mavedb_user_id())
"""

EXPERIMENTS = """
     private = false
  OR created_by_id = mavedb_user_id()
  OR mavedb_roles() && '{admin,mapper}'
  OR id IN (SELECT ec.experiment_id FROM experiment_contributors ec
              JOIN contributors c ON c.id = ec.contributor_id
              JOIN users u ON u.username = c.orcid_id
             WHERE u.id = mavedb_user_id())
  OR id IN (SELECT experiment_id FROM scoresets)
"""

EXPERIMENT_SETS = """
     private = false
  OR created_by_id = mavedb_user_id()
  OR mavedb_roles() && '{admin,mapper}'
  OR id IN (SELECT esc.experiment_set_id FROM experiment_set_contributors esc
              JOIN contributors c ON c.id = esc.contributor_id
              JOIN users u ON u.username = c.orcid_id
             WHERE u.id = mavedb_user_id())
  OR id IN (SELECT experiment_set_id FROM experiments)
"""

COLLECTIONS = """
     private = false
  OR created_by_id = mavedb_user_id()
  OR mavedb_roles() && '{admin}'
  OR id IN (SELECT cua.collection_id FROM collection_user_associations cua
             WHERE cua.user_id = mavedb_user_id()
               AND cua.contribution_role IN ('admin', 'editor', 'viewer'))
"""

SCORE_CALIBRATIONS = """
  EXISTS (SELECT 1 FROM scoresets s WHERE s.id = score_calibrations.score_set_id)
  AND (
       private = false
    OR created_by_id = mavedb_user_id()
    OR mavedb_roles() && '{admin}'
    OR (investigator_provided AND EXISTS (
          SELECT 1 FROM scoresets ss
           WHERE ss.id = score_calibrations.score_set_id
             AND (ss.created_by_id = mavedb_user_id()
                  OR ss.id IN (SELECT sc.scoreset_id FROM scoreset_contributors sc
                                 JOIN contributors c ON c.id = sc.contributor_id
                                 JOIN users u ON u.username = c.orcid_id
                                WHERE u.id = mavedb_user_id()))))
  )
"""

RULES = {
    "scoresets": SCORESETS,
    "experiments": EXPERIMENTS,
    "experiment_sets": EXPERIMENT_SETS,
    "collections": COLLECTIONS,
    "score_calibrations": SCORE_CALIBRATIONS,
}


def upgrade():
    for table, predicate in RULES.items():
        op.execute(f"CREATE POLICY {table}_rule ON {table} FOR ALL USING ({predicate}) WITH CHECK ({predicate})")
        op.execute(f"CREATE POLICY {table}_worker ON {table} FOR ALL TO mavedb_worker USING (true) WITH CHECK (true)")


def downgrade():
    for table in reversed(RULES):
        op.execute(f"DROP POLICY {table}_worker ON {table}")
        op.execute(f"DROP POLICY {table}_rule ON {table}")
