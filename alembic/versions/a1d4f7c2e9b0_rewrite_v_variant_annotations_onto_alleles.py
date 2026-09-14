"""rewrite v_variant_annotations onto the mapping_records/alleles substrate

Revision ID: a1d4f7c2e9b0
Revises: e7b2c9a1f4d3
Create Date: 2026-07-18

Drops the legacy ``mapped_variants``-based ``v_variant_annotations`` view and recreates it over the
``MappingRecord`` / ``Allele`` / ``MappingRecordAllele`` substrate (part of the MappedVariant
read-path teardown). Column changes vs. the legacy shape:
  * ``mapped_variant_id`` -> ``mapping_record_id`` (the mapping identity is now the mapping record).
  * ``mapping_error`` dropped — there is no per-mapping-record error column; per-subject annotation
    dispositions live in ``v_current_annotation_events`` and score-set-level mapping errors on
    ``scoresets.mapping_errors``.

Both directions are frozen inline as literal SQL — the migration imports nothing from the model layer.
A migration is an immutable historical record: importing ``variant_annotation_view.definition`` would
couple this fixed revision to mutable code, so the next model edit (the following view rewrite) would
make a replay of *this* revision build that later shape — against tables that may not exist yet at this
point in history — and fail. ``_UPGRADE_DEFINITION`` below was compiled once from the model at
authoring time (``CreateView(signature, definition).compile(postgresql.dialect())``) and must not be
edited to track the model; the next shape change gets its own migration.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a1d4f7c2e9b0"
down_revision = "e7b2c9a1f4d3"
branch_labels = None
depends_on = None

SIGNATURE = "v_variant_annotations"

# New (mapping_records/alleles) definition, frozen at this revision. Compiled from the model once at
# authoring time; do not edit to track the model. Allele-derived columns are correlated scalar
# subqueries so the grain stays one row per (variant, annotation_type), matching the legacy view.
_UPGRADE_DEFINITION = """
SELECT variants.urn AS variant_urn, scoresets.urn AS score_set_urn, variants.hgvs_nt, variants.hgvs_pro,
    variants.hgvs_splice, mapping_records.id AS mapping_record_id, (SELECT alleles.clingen_allele_id
FROM mapping_record_alleles JOIN alleles ON alleles.id = mapping_record_alleles.allele_id
WHERE mapping_record_alleles.mapping_record_id = mapping_records.id AND mapping_record_alleles.is_authoritative IS true AND mapping_record_alleles.valid_to IS NULL
 LIMIT 1) AS clingen_allele_id, mapping_records.hgvs_assay_level, (SELECT alleles.hgvs_g
FROM mapping_record_alleles JOIN alleles ON alleles.id = mapping_record_alleles.allele_id
WHERE mapping_record_alleles.mapping_record_id = mapping_records.id AND mapping_record_alleles.valid_to IS NULL AND alleles.level = 'genomic'
 LIMIT 1) AS hgvs_g, (SELECT alleles.hgvs_c
FROM mapping_record_alleles JOIN alleles ON alleles.id = mapping_record_alleles.allele_id
WHERE mapping_record_alleles.mapping_record_id = mapping_records.id AND mapping_record_alleles.valid_to IS NULL AND alleles.level = 'cdna'
 LIMIT 1) AS hgvs_c, (SELECT alleles.hgvs_p
FROM mapping_record_alleles JOIN alleles ON alleles.id = mapping_record_alleles.allele_id
WHERE mapping_record_alleles.mapping_record_id = mapping_records.id AND mapping_record_alleles.valid_to IS NULL AND alleles.level = 'protein'
 LIMIT 1) AS hgvs_p, (SELECT vep_allele_consequences.functional_consequence
FROM mapping_record_alleles JOIN alleles ON alleles.id = mapping_record_alleles.allele_id JOIN vep_allele_consequences ON vep_allele_consequences.allele_id = alleles.id AND vep_allele_consequences.valid_to IS NULL
WHERE mapping_record_alleles.mapping_record_id = mapping_records.id AND mapping_record_alleles.is_authoritative IS true AND mapping_record_alleles.valid_to IS NULL
 LIMIT 1) AS vep_functional_consequence, (SELECT vep_allele_consequences.access_date
FROM mapping_record_alleles JOIN alleles ON alleles.id = mapping_record_alleles.allele_id JOIN vep_allele_consequences ON vep_allele_consequences.allele_id = alleles.id AND vep_allele_consequences.valid_to IS NULL
WHERE mapping_record_alleles.mapping_record_id = mapping_records.id AND mapping_record_alleles.is_authoritative IS true AND mapping_record_alleles.valid_to IS NULL
 LIMIT 1) AS vep_access_date, mapping_records.mapped_date, mapping_records.mapping_api_version, mapping_records.vrs_version, variant_annotation_status.annotation_type, variant_annotation_status.status AS annotation_status, variant_annotation_status.failure_category, variant_annotation_status.error_message AS annotation_error, variant_annotation_status.version AS annotation_version, variant_annotation_status.annotation_metadata, variant_annotation_status.current, variant_annotation_status.created_at AS annotation_created_at, variant_annotation_status.updated_at AS annotation_updated_at
FROM variants JOIN scoresets ON scoresets.id = variants.scoreset_id
    LEFT OUTER JOIN mapping_records ON mapping_records.variant_id = variants.id AND mapping_records.valid_to IS NULL
    LEFT OUTER JOIN variant_annotation_status ON variant_annotation_status.variant_id = variants.id AND variant_annotation_status.current = true
"""

# Legacy (mapped_variants-based) definition, inlined so the downgrade can restore the prior shape.
# ``mapped_variants`` still exists at this revision (dropped in a later teardown migration), so this
# remains runnable on downgrade.
_LEGACY_DEFINITION = """
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
    op.execute(f"DROP VIEW {SIGNATURE}")
    op.execute(f"CREATE VIEW {SIGNATURE} AS {_UPGRADE_DEFINITION}")


def downgrade() -> None:
    op.execute(f"DROP VIEW {SIGNATURE}")
    op.execute(f"CREATE VIEW {SIGNATURE} AS {_LEGACY_DEFINITION}")
