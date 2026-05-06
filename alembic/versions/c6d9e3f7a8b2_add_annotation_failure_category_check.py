"""add annotation failure category check constraint

Revision ID: c6d9e3f7a8b2
Revises: b5c8d2e4f6a7
Create Date: 2026-04-20

Adds a CHECK constraint on variant_annotation_status.failure_category to enforce
the AnnotationFailureCategory enum values. Also migrates existing free-text
failure_category values to their corresponding enum values.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "c6d9e3f7a8b2"
down_revision = "b5c8d2e4f6a7"
branch_labels = None
depends_on = None

# Mapping from old free-text values to new enum values
OLD_TO_NEW = {
    "missing_clingen_allele_id": "missing_identifier",
    "multi_variant_clingen_allele_id": "unsupported_identifier",
    "invalid_allele_format": "unsupported_identifier",
    "clingen_api_error": "external_api_error",
    "not_found": "external_reference_not_found",
    "clingen_allele_not_found": "external_reference_not_found",
    "no_associated_clinvar_allele_id": "no_linked_allele",
    "no_canonical_pa_ids": "no_linked_allele",
    "no_registered_ca_ids": "no_linked_allele",
}


def upgrade() -> None:
    # Migrate existing free-text values to enum values
    for old_value, new_value in OLD_TO_NEW.items():
        op.execute(
            f"UPDATE variant_annotation_status SET failure_category = '{new_value}' "
            f"WHERE failure_category = '{old_value}'"
        )

    # Set any remaining non-null values that don't match known enum values to 'unknown'
    valid_values = "', '".join(
        [
            "missing_identifier",
            "unsupported_identifier",
            "external_api_error",
            "external_reference_not_found",
            "no_linked_allele",
            "unknown",
        ]
    )
    op.execute(
        f"UPDATE variant_annotation_status SET failure_category = 'unknown' "
        f"WHERE failure_category IS NOT NULL AND failure_category NOT IN ('{valid_values}')"
    )

    # Add the check constraint
    op.create_check_constraint(
        "ck_variant_annotation_failure_category_valid",
        "variant_annotation_status",
        "failure_category IS NULL OR failure_category IN "
        "('missing_identifier', 'unsupported_identifier', 'external_api_error', "
        "'external_reference_not_found', 'no_linked_allele', 'unknown')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_variant_annotation_failure_category_valid", "variant_annotation_status", type_="check")
