from typing import Optional, Union

from ga4gh.core.models import Coding, MappableConcept, iriReference
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.core import (
    Direction,
    EvidenceLine,
    EvidenceLineType,
    StatementType,
    StudyResult,
    VariantPathogenicityProposition,
)
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

from mavedb.lib.annotation.classification import pathogenicity_classification_of_variant
from mavedb.lib.annotation.contribution import (
    excalibr_calibration_contribution,
    mavedb_api_contribution,
    mavedb_creator_contribution,
    mavedb_modifier_contribution,
    mavedb_vrs_contribution,
)
from mavedb.lib.annotation.document import score_set_to_document
from mavedb.lib.annotation.method import (
    excalibr_calibration_method,
    publication_identifiers_to_method,
)
from mavedb.models.mapped_variant import MappedVariant


def acmg_evidence_line(
    mapped_variant: MappedVariant,
    proposition: VariantPathogenicityProposition,
    evidence: list[Union[StudyResult, EvidenceLineType, StatementType, iriReference]],
) -> Optional[VariantPathogenicityEvidenceLine]:
    evidence_outcome, evidence_strength = pathogenicity_classification_of_variant(mapped_variant)

    if not evidence_strength:
        evidence_outcome_code = f"{evidence_outcome.value}_not_met"
        direction_of_evidence = Direction.NEUTRAL
        strength_of_evidence = None
    else:
        evidence_outcome_code = (
            f"{evidence_outcome.value}_{evidence_strength.name.lower()}"
            if evidence_strength != StrengthOfEvidenceProvided.STRONG
            else evidence_outcome.value
        )
        direction_of_evidence = (
            Direction.SUPPORTS
            if evidence_outcome == VariantPathogenicityEvidenceLine.Criterion.PS3
            else Direction.DISPUTES
        )
        strength_of_evidence = MappableConcept(
            primaryCoding=Coding(
                code=evidence_strength,
                system="ACMG Guidelines, 2015",
            ),
        )

    return VariantPathogenicityEvidenceLine(
        description=f"Pathogenicity evidence line {mapped_variant.variant.urn}.",
        specifiedBy=excalibr_calibration_method(evidence_outcome),
        evidenceOutcome={
            "primaryCoding": Coding(
                code=evidence_outcome_code,
                system="ACMG Guidelines, 2015",
            ),
            "name": f"ACMG 2015 {evidence_outcome.name} Criterion {'Met' if strength_of_evidence else 'Not Met'}",
        },
        strengthOfEvidenceProvided=strength_of_evidence,
        directionOfEvidenceProvided=direction_of_evidence,
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
            excalibr_calibration_contribution(),
            mavedb_creator_contribution(mapped_variant.variant, mapped_variant.variant.score_set.created_by),
            mavedb_modifier_contribution(mapped_variant.variant, mapped_variant.variant.score_set.modified_by),
        ],
        targetProposition=proposition,
        hasEvidenceItems=[evidence_item for evidence_item in evidence],
    )


def functional_evidence_line(
    mapped_variant: MappedVariant, evidence: list[Union[StudyResult, EvidenceLineType, StatementType, iriReference]]
) -> EvidenceLine:
    return EvidenceLine(
        description=f"Functional evidence line for {mapped_variant.variant.urn}",
        # Pydantic validates the provided dictionary meets the expected structure of possible models, but
        # chokes if you provide the model directly. It probably isn't surprising MyPy doesn't love this method
        # of validation, so just ignore the error.
        hasEvidenceItems=[evidence_item.model_dump(exclude_none=True) for evidence_item in evidence],  # type: ignore
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
