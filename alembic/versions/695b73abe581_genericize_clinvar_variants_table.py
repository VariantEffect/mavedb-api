"""genericize clinvar variants table

Revision ID: 695b73abe581
Revises: 34026092c7f8
Create Date: 2025-02-18 11:54:15.243078

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "695b73abe581"
down_revision = "34026092c7f8"
branch_labels = None
depends_on = None


def upgrade():
    op.rename_table("clinvar_variants", "clinical_controls")
    op.execute("ALTER SEQUENCE clinvar_variants_id_seq RENAME TO clinical_controls_id_seq")
    op.execute("ALTER INDEX clinvar_variants_pkey RENAME TO clinical_controls_pkey")

    op.alter_column("clinical_controls", "clinvar_db_version", nullable=False, new_column_name="db_version")
    op.alter_column("clinical_controls", "allele_id", nullable=False, new_column_name="db_identifier")
    op.add_column("clinical_controls", sa.Column("db_name", sa.String(), nullable=True))

    op.create_index("ix_clinical_controls_gene_symbol", "clinical_controls", ["gene_symbol"])
    op.create_index("ix_clinical_controls_db_name", "clinical_controls", ["db_name"])
    op.create_index("ix_clinical_controls_db_identifier", "clinical_controls", ["db_identifier"])
    op.create_index("ix_clinical_controls_db_version", "clinical_controls", ["db_version"])

    op.create_table(
        "mapped_variants_clinical_controls",
        sa.Column("mapped_variant_id", sa.Integer(), nullable=False),
        sa.Column("clinical_control_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["mapped_variant_id"],
            ["mapped_variants.id"],
        ),
        sa.ForeignKeyConstraint(
            ["clinical_control_id"],
            ["clinical_controls.id"],
        ),
        sa.PrimaryKeyConstraint("mapped_variant_id", "clinical_control_id"),
    )

    # Convert any existing ClinVar variants into clinical control variants. Since
    # this table is being update from a clinvar specific table, we assume all existing
    # controls are from ClinVar.
    op.execute(
        """
        INSERT INTO mapped_variants_clinical_controls (
            mapped_variant_id,
            clinical_control_id
        )
        SELECT id, clinvar_variant_id
        FROM mapped_variants
        WHERE clinvar_variant_id IS NOT NULL
        """
    )

    op.execute("UPDATE clinical_controls SET db_name='ClinVar'")
    op.alter_column("clinical_controls", "db_name", nullable=False)

    op.drop_index("ix_mapped_variants_clinvar_variant_id", "mapped_variants")
    op.drop_column("mapped_variants", "clinvar_variant_id")


def downgrade():
    op.rename_table("clinical_controls", "clinvar_variants")
    op.execute("ALTER SEQUENCE clinical_controls_id_seq RENAME TO clinvar_variants_id_seq")
    op.execute("ALTER INDEX clinical_controls_pkey RENAME TO clinvar_variants_pkey")

    op.drop_index("ix_clinical_controls_gene_symbol", "clinical_controls")
    op.drop_index("ix_clinical_controls_db_name", "clinical_controls")
    op.drop_index("ix_clinical_controls_db_identifier", "clinical_controls")
    op.drop_index("ix_clinical_controls_db_version", "clinical_controls")

    op.alter_column("clinvar_variants", "db_version", nullable=False, new_column_name="clinvar_db_version")
    op.alter_column("clinvar_variants", "db_identifier", nullable=False, new_column_name="allele_id")
    op.drop_column("clinvar_variants", "db_name")

    op.add_column(
        "mapped_variants",
        sa.Column("clinvar_variant_id", sa.Integer(), sa.ForeignKey("clinvar_variants.id"), nullable=True),
    )

    # Downgrades a many-to-many relationship to a one to many. This will result in data loss.
    op.execute(
        """
        UPDATE mapped_variants
        SET clinvar_variant_id=mapped_variants_clinical_controls.clinical_control_id
        FROM mapped_variants_clinical_controls
        WHERE mapped_variants_clinical_controls.mapped_variant_id=mapped_variants.id
        """
    )

    op.create_index("ix_mapped_variants_clinvar_variant_id", "mapped_variants", ["clinvar_variant_id"])
    op.drop_table("mapped_variants_clinical_controls")
