"""add external_service_rejected annotation failure category

Revision ID: e3a7b9f1d2c5
Revises: d1f4a2e9c05b
Create Date: 2026-04-24

Extends the failure_category CHECK constraint on variant_annotation_status to include
'external_service_rejected', which distinguishes explicit rejections by an external
service (e.g. CAR returning InvalidHGVS) from generic API errors (network failures,
timeouts, etc.).
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "e3a7b9f1d2c5"
down_revision = "d1f4a2e9c05b"
branch_labels = None
depends_on = None

OLD_VALID_VALUES = (
    "('missing_identifier', 'unsupported_identifier', 'external_api_error', "
    "'external_reference_not_found', 'no_linked_allele', 'unknown')"
)
NEW_VALID_VALUES = (
    "('missing_identifier', 'unsupported_identifier', 'external_api_error', "
    "'external_service_rejected', 'external_reference_not_found', 'no_linked_allele', 'unknown')"
)


def upgrade() -> None:
    op.drop_constraint("ck_variant_annotation_failure_category_valid", "variant_annotation_status", type_="check")
    op.create_check_constraint(
        "ck_variant_annotation_failure_category_valid",
        "variant_annotation_status",
        f"failure_category IS NULL OR failure_category IN {NEW_VALID_VALUES}",
    )


def downgrade() -> None:
    # Reclassify any 'external_service_rejected' rows back to 'external_api_error' before
    # dropping the new value from the constraint.
    op.execute(
        "UPDATE variant_annotation_status SET failure_category = 'external_api_error' "
        "WHERE failure_category = 'external_service_rejected'"
    )
    op.drop_constraint("ck_variant_annotation_failure_category_valid", "variant_annotation_status", type_="check")
    op.create_check_constraint(
        "ck_variant_annotation_failure_category_valid",
        "variant_annotation_status",
        f"failure_category IS NULL OR failure_category IN {OLD_VALID_VALUES}",
    )
