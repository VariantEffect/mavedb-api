"""add v_variant_annotations view

Revision ID: f2a1b3c4d5e6
Revises: e3a7b9f1d2c5
Create Date: 2026-05-03

Creates the v_variant_annotations convenience view, which joins variants, mapped_variants,
variant_annotation_status, and scoresets into a single flat row per (variant, annotation_type).
Intended for operator queries and the variant_annotations CLI script.

The view SQL is inlined as a frozen literal rather than imported from the model. At this point in
history the view joins the (now legacy) ``mapped_variants`` table; a later migration rewrites it onto
the ``mapping_records`` / ``alleles`` substrate. Importing the live model definition here would make a
fresh replay build *that* later shape at this early revision — before the substrate tables exist —
and fail. Freezing the historical SQL keeps replay green; the rewrite happens in its own migration.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f2a1b3c4d5e6"
down_revision = "e3a7b9f1d2c5"
branch_labels = None
depends_on = None

SIGNATURE = "v_variant_annotations"

# Historical definition, frozen at this revision. Do not edit to track the model.
DEFINITION = """
SELECT variants.urn AS variant_urn, scoresets.urn AS score_set_urn, variants.hgvs_nt, variants.hgvs_pro,
    variants.hgvs_splice, mapped_variants.id AS mapped_variant_id, mapped_variants.clingen_allele_id,
    mapped_variants.hgvs_assay_level, mapped_variants.hgvs_g, mapped_variants.hgvs_c, mapped_variants.hgvs_p,
    mapped_variants.vep_functional_consequence, mapped_variants.vep_access_date, mapped_variants.mapped_date,
    mapped_variants.mapping_api_version, mapped_variants.vrs_version, mapped_variants.error_message AS mapping_error,
    variant_annotation_status.annotation_type, variant_annotation_status.status AS annotation_status,
    variant_annotation_status.failure_category, variant_annotation_status.error_message AS annotation_error,
    variant_annotation_status.version AS annotation_version, variant_annotation_status.annotation_metadata,
    variant_annotation_status.current, variant_annotation_status.created_at AS annotation_created_at,
    variant_annotation_status.updated_at AS annotation_updated_at
FROM variants JOIN scoresets ON scoresets.id = variants.scoreset_id
    LEFT OUTER JOIN mapped_variants ON mapped_variants.variant_id = variants.id AND mapped_variants.current = true
    LEFT OUTER JOIN variant_annotation_status
        ON variant_annotation_status.variant_id = variants.id AND variant_annotation_status.current = true
"""


def upgrade() -> None:
    op.execute(f"CREATE VIEW {SIGNATURE} AS {DEFINITION}")


def downgrade() -> None:
    op.execute(f"DROP VIEW {SIGNATURE}")
