"""make v_variant_annotations and v_current_annotation_events read their tables as the caller

A view reads its tables as its owner unless it's ``security_invoker``, and the owner skips every policy, so a
caller granted the view would see rows the tables' policies hide. Only the worker and scripts read these two
today (``rls.py``), so nothing changes, but a route granted one later reads through the policies.

Revision ID: f9bc27dae3b8
Revises: e8ab16c9d2a7
Create Date: 2026-11-12

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f9bc27dae3b8"
down_revision = "e8ab16c9d2a7"
branch_labels = None
depends_on = None

VIEWS = ("v_variant_annotations", "v_current_annotation_events")


def upgrade():
    for view in VIEWS:
        op.execute(f"ALTER VIEW {view} SET (security_invoker = true)")


def downgrade():
    for view in VIEWS:
        op.execute(f"ALTER VIEW {view} RESET (security_invoker)")
