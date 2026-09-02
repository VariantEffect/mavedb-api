"""add clinvar_variation_id to clinvar_controls

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-06-22

Additive, non-breaking column for ClinVar's canonical public identifier (VariationID), captured forward
from the variant_summary TSV. db_identifier continues to hold the AlleleID (the allele-level handle used
for gnomAD cross-references); this carries the VariationID beside it for eventual external ClinVar links
(clinvar/variation/{id}). Nullable and not yet served — the dedicated clinvar_variants remodel (explicit
fields replacing the generic db_* shape, the serving/UI cutover, and backfill of existing rows) is
deferred.
"""

import sqlalchemy as sa
from alembic import op

revision = "d4e5f6a7b8c9"
down_revision = "c3d4e5f6a7b8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("clinvar_controls", sa.Column("clinvar_variation_id", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("clinvar_controls", "clinvar_variation_id")
