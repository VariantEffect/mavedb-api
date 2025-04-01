from typing import Optional, Union

from ga4gh.core.models import Coding
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityFunctionalImpactEvidenceLine
from ga4gh.va_spec.base import VariantPathogenicityProposition
from ga4gh.va_spec.base.core import (
    MappableConcept,
    EvidenceLine,
    StudyResult,
    EvidenceLineType,
    StatementType,
    iriReference,
)

from mavedb.lib.annotation.classification import pillar_project_clinical_classification_of_variant
from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_vrs_contribution,
    pillar_project_calibration_contribution,
    mavedb_creator_contribution,
    mavedb_modifier_contribution,
)
from mavedb.lib.annotation.document import score_set_to_document
from mavedb.lib.annotation.method import (
    pillar_project_calibration_method,
    publication_identifiers_to_method,
)
from mavedb.models.mapped_variant import MappedVariant


def acmg_evidence_line(
    mapped_variant: MappedVariant,
    proposition: VariantPathogenicityProposition,
    evidence: list[Union[StudyResult, EvidenceLineType, StatementType, iriReference]],
) -> Optional[VariantPathogenicityFunctionalImpactEvidenceLine]:
    evidence_outcome, evidence_strength = pillar_project_clinical_classification_of_variant(mapped_variant)

    if not evidence_outcome and not evidence_strength:
        return None

    return VariantPathogenicityFunctionalImpactEvidenceLine(
        description=f"Pathogenicity evidence line {mapped_variant.variant.urn}.",
        specifiedBy=pillar_project_calibration_method(),
        evidenceOutcome={
            "primaryCoding": Coding(
                code=evidence_outcome,
                system="ACMG Guidelines, 2015",
            ),
            "name": f"ACMG 2015 {evidence_outcome} Criterion Met",
        },
        strengthOfEvidenceProvided=MappableConcept(
            primaryCoding=Coding(
                code=evidence_strength,
                system="ACMG Guidelines, 2015",
            ),
        ),
        directionOfEvidenceProvided="supports",
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
            pillar_project_calibration_contribution(),
            mavedb_creator_contribution(mapped_variant.variant, mapped_variant.variant.score_set.created_by),
            mavedb_modifier_contribution(mapped_variant.variant, mapped_variant.variant.score_set.modified_by),
        ],
        targetProposition=proposition,
        hasEvidenceItems=[evidence_item.model_dump(exclude_none=True) for evidence_item in evidence],
    )


def functional_evidence_line(
    mapped_variant: MappedVariant, evidence: list[Union[StudyResult, EvidenceLineType, StatementType, iriReference]]
) -> EvidenceLine:
    return EvidenceLine(
        description=f"Functional evidence line for {mapped_variant.variant.urn}",
        hasEvidenceItems=[evidence_item.model_dump(exclude_none=True) for evidence_item in evidence],
        directionOfEvidenceProvided="supports",
        specifiedBy=publication_identifiers_to_method(
            mapped_variant.variant.score_set.publication_identifier_associations
        ),
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
            mavedb_creator_contribution(mapped_variant.variant, mapped_variant.variant.score_set.created_by),
            mavedb_modifier_contribution(mapped_variant.variant, mapped_variant.variant.score_set.modified_by),
        ],
        reportedIn=[score_set_to_document(mapped_variant.variant.score_set)],
    )
