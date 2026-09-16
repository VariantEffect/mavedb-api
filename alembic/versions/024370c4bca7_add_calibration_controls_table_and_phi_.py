"""add calibration controls table and phi/disease columns

Revision ID: 024370c4bca7
Revises: a7f3c2e9b104
Create Date: 2026-09-15 16:01:06.013964

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "024370c4bca7"
down_revision = "a7f3c2e9b104"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "calibration_controls",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("calibration_id", sa.Integer(), nullable=False),
        sa.Column("variant_id", sa.Integer(), nullable=False),
        sa.Column(
            "clinical_status",
            sa.Enum("pathogenic", "benign", name="calibrationcontrolstatus", native_enum=False, length=32),
            nullable=False,
        ),
        sa.Column("created_by_id", sa.Integer(), nullable=False),
        sa.Column("modified_by_id", sa.Integer(), nullable=False),
        sa.Column("creation_date", sa.Date(), nullable=False),
        sa.Column("modification_date", sa.Date(), nullable=False),
        sa.ForeignKeyConstraint(["calibration_id"], ["score_calibrations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["variant_id"], ["variants.id"]),
        sa.ForeignKeyConstraint(["created_by_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["modified_by_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("calibration_id", "variant_id", name="uq_calibration_controls_calibration_id_variant_id"),
    )
    op.create_index(
        op.f("ix_calibration_controls_calibration_id"), "calibration_controls", ["calibration_id"], unique=False
    )
    op.create_index(op.f("ix_calibration_controls_variant_id"), "calibration_controls", ["variant_id"], unique=False)
    op.create_index(
        op.f("ix_calibration_controls_created_by_id"), "calibration_controls", ["created_by_id"], unique=False
    )
    op.create_index(
        op.f("ix_calibration_controls_modified_by_id"), "calibration_controls", ["modified_by_id"], unique=False
    )

    op.add_column("score_calibrations", sa.Column("disease", sa.String(), nullable=True))
    op.add_column("score_calibrations", sa.Column("controls_not_phi", sa.Boolean(), nullable=True))


def downgrade():
    op.drop_column("score_calibrations", "controls_not_phi")
    op.drop_column("score_calibrations", "disease")

    op.drop_index(op.f("ix_calibration_controls_modified_by_id"), table_name="calibration_controls")
    op.drop_index(op.f("ix_calibration_controls_created_by_id"), table_name="calibration_controls")
    op.drop_index(op.f("ix_calibration_controls_variant_id"), table_name="calibration_controls")
    op.drop_index(op.f("ix_calibration_controls_calibration_id"), table_name="calibration_controls")
    op.drop_table("calibration_controls")
