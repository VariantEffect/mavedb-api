"""add calibration controls, mondo terms, and phi/disease columns

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

# The generic "disease or disorder" MONDO term seeded so the non-nullable calibration FK always resolves.
MONDO_SYSTEM = "https://purl.obolibrary.org/obo/mondo.owl"
MONDO_GENERIC_CODE = "MONDO:0000001"
MONDO_GENERIC_LABEL = "disease or disorder"


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

    # MONDO disease terms, with the generic root seeded as the default for calibrations naming no disease.
    op.create_table(
        "mondo_terms",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(), nullable=False),
        sa.Column("system", sa.String(), nullable=False),
        sa.Column("system_version", sa.String(), nullable=True),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("creation_date", sa.Date(), nullable=False),
        sa.Column("modification_date", sa.Date(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("system", "code", name="uq_mondo_terms_system_code"),
    )
    op.execute(
        sa.text(
            "INSERT INTO mondo_terms (code, system, label, creation_date, modification_date) "
            "VALUES (:code, :system, :label, CURRENT_DATE, CURRENT_DATE)"
        ).bindparams(code=MONDO_GENERIC_CODE, system=MONDO_SYSTEM, label=MONDO_GENERIC_LABEL)
    )

    # Disease FK: add nullable, backfill existing calibrations to the generic term, then enforce NOT NULL.
    op.add_column("score_calibrations", sa.Column("disease_term_id", sa.Integer(), nullable=True))
    op.execute(
        sa.text(
            "UPDATE score_calibrations SET disease_term_id = "
            "(SELECT id FROM mondo_terms WHERE system = :system AND code = :code)"
        ).bindparams(system=MONDO_SYSTEM, code=MONDO_GENERIC_CODE)
    )
    op.alter_column("score_calibrations", "disease_term_id", nullable=False)
    op.create_index(
        op.f("ix_score_calibrations_disease_term_id"), "score_calibrations", ["disease_term_id"], unique=False
    )
    op.create_foreign_key(
        "fk_score_calibrations_disease_term_id_mondo_terms",
        "score_calibrations",
        "mondo_terms",
        ["disease_term_id"],
        ["id"],
    )

    op.add_column("score_calibrations", sa.Column("controls_not_phi", sa.Boolean(), nullable=True))


def downgrade():
    op.drop_column("score_calibrations", "controls_not_phi")

    op.drop_constraint("fk_score_calibrations_disease_term_id_mondo_terms", "score_calibrations", type_="foreignkey")
    op.drop_index(op.f("ix_score_calibrations_disease_term_id"), table_name="score_calibrations")
    op.drop_column("score_calibrations", "disease_term_id")
    op.drop_table("mondo_terms")

    op.drop_index(op.f("ix_calibration_controls_modified_by_id"), table_name="calibration_controls")
    op.drop_index(op.f("ix_calibration_controls_created_by_id"), table_name="calibration_controls")
    op.drop_index(op.f("ix_calibration_controls_variant_id"), table_name="calibration_controls")
    op.drop_index(op.f("ix_calibration_controls_calibration_id"), table_name="calibration_controls")
    op.drop_table("calibration_controls")
