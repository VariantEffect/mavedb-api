from typing import Union
from ga4gh.va_spec.base import Statement, ExperimentalVariantFunctionalImpactProposition
from ga4gh.va_spec.base.core import (
    MappableConcept,
    StudyResult,
    EvidenceLineType,
    StatementType,
    iriReference,
)
from ga4gh.core.models import Coding
from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.annotation.method import variant_interpretation_functional_guideline_method
from mavedb.lib.annotation.classification import functional_classification_of_variant
from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_vrs_contribution,
    mavedb_creator_contribution,
    mavedb_modifier_contribution,
)


def mapped_variant_to_functional_statement(
    mapped_variant: MappedVariant,
    proposition: ExperimentalVariantFunctionalImpactProposition,
    evidence: list[Union[StudyResult, EvidenceLineType, StatementType, iriReference]],
) -> Statement:
    classification = functional_classification_of_variant(mapped_variant)

    return Statement(
        description=f"Variant functional impact statement for {mapped_variant.variant.urn}.",
        specifiedBy=variant_interpretation_functional_guideline_method(),
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
            mavedb_creator_contribution(mapped_variant.variant, mapped_variant.variant.score_set.created_by),
            mavedb_modifier_contribution(mapped_variant.variant, mapped_variant.variant.score_set.modified_by),
        ],
        proposition=proposition,
        direction="supports",
        classification=MappableConcept(
            primaryCoding=Coding(
                code=classification, system="ga4gh-gks-term:experimental-var-func-impact-classification"
            ),
        ),
        hasEvidenceLines=[evidence_item.model_dump(exclude_none=True) for evidence_item in evidence],
    )
