"""add cross_level_translation annotation type

Revision ID: f4d2a9c1b7e3
Revises: a1b2c3d4e5f6
Create Date: 2026-06-02

Extends ck_variant_annotation_type_valid to allow the 'cross_level_translation'
annotation type. The VRS mapping worker writes one such row per variant to record
whether cross-level translation (filling the levels the assay did not map)
succeeded, was skipped (multivariant / no transcript), or failed.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f4d2a9c1b7e3"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None

_TYPES_OLD = (
    "'vrs_mapping', 'clingen_allele_id', 'mapped_hgvs', 'variant_translation', "
    "'gnomad_allele_frequency', 'clinvar_control', 'vep_functional_consequence', "
    "'ldh_submission'"
)
_TYPES_NEW = "'vrs_mapping', 'cross_level_translation', " + (
    "'clingen_allele_id', 'mapped_hgvs', 'variant_translation', "
    "'gnomad_allele_frequency', 'clinvar_control', 'vep_functional_consequence', "
    "'ldh_submission'"
)


def upgrade() -> None:
    op.drop_constraint("ck_variant_annotation_type_valid", "variant_annotation_status", type_="check")
    op.create_check_constraint(
        "ck_variant_annotation_type_valid",
        "variant_annotation_status",
        f"annotation_type IN ({_TYPES_NEW})",
    )


def downgrade() -> None:
    op.execute("DELETE FROM variant_annotation_status WHERE annotation_type = 'cross_level_translation'")
    op.drop_constraint("ck_variant_annotation_type_valid", "variant_annotation_status", type_="check")
    op.create_check_constraint(
        "ck_variant_annotation_type_valid",
        "variant_annotation_status",
        f"annotation_type IN ({_TYPES_OLD})",
    )
