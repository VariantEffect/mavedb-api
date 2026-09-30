"""rename clinical_controls to clinvar_controls

Revision ID: b2c3d4e5f6a7
Revises: a7c4e9d2f1b8
Create Date: 2026-06-22

Renames the clinical_controls entity table to clinvar_controls, and renames the unique
constraint to match. The frozen association table (mapped_variants_clinical_controls)
and its FK to the renamed table are left structurally intact — PostgreSQL updates the FK
target automatically on table rename. The Python model is renamed ClinicalControl →
ClinvarControl in the same changeset (no data migration).
"""

from alembic import op

revision = "b2c3d4e5f6a7"
down_revision = "a7c4e9d2f1b8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("clinical_controls", "clinvar_controls")
    # PostgreSQL does not auto-rename constraints on table rename; rename explicitly so the
    # on_conflict_do_update(constraint=...) in the job references the correct name.
    op.execute(
        "ALTER TABLE clinvar_controls RENAME CONSTRAINT "
        "uq_clinical_controls_db_name_identifier_version "
        "TO uq_clinvar_controls_db_name_identifier_version"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE clinvar_controls RENAME CONSTRAINT "
        "uq_clinvar_controls_db_name_identifier_version "
        "TO uq_clinical_controls_db_name_identifier_version"
    )
    op.rename_table("clinvar_controls", "clinical_controls")
