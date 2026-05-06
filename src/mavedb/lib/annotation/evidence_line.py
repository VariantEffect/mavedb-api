from typing import Union

from ga4gh.core.models import Coding, Extension, MappableConcept, iriReference
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

from mavedb.lib.annotation.classification import (
    functional_classification_of_variant,
    pathogenicity_classification_of_variant,
)
from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_score_calibration_contribution,
    mavedb_vrs_contribution,
)
from mavedb.lib.annotation.direction import (
    direction_of_support_for_functional_classification,
    direction_of_support_for_pathogenicity_classification,
)
from mavedb.lib.annotation.document import score_calibration_as_document
from mavedb.lib.annotation.method import (
    functional_score_calibration_as_method,
    pathogenicity_score_calibration_as_method,
)
from mavedb.lib.annotation.util import serialize_evidence_items
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration


def acmg_evidence_line(
    mapped_variant: MappedVariant,
    score_calibration: ScoreCalibration,
    proposition: VariantPathogenicityProposition,
    evidence: list[Union[StudyResult, EvidenceLineType, StatementType, iriReference]],
) -> VariantPathogenicityEvidenceLine:
    containing_evidence_range, evidence_outcome, evidence_strength = pathogenicity_classification_of_variant(
        mapped_variant, score_calibration
    )

    if not evidence_strength:
        evidence_outcome_code = f"{evidence_outcome.value}_not_met"
        strength_of_evidence = None
        direction_of_evidence = Direction.NEUTRAL
    else:
        evidence_outcome_code = (
            f"{evidence_outcome.value}_{evidence_strength.name.lower()}"
            if evidence_strength != StrengthOfEvidenceProvided.STRONG
            else evidence_outcome.value
        )
        strength_of_evidence = MappableConcept(
            primaryCoding=Coding(
                code=evidence_strength,
                system="ACMG Guidelines, 2015",
            ),
        )
        direction_of_evidence = direction_of_support_for_pathogenicity_classification(evidence_outcome)

    return VariantPathogenicityEvidenceLine(
        description=f"Pathogenicity evidence line for {mapped_variant.variant.urn}.",
        hasEvidenceItems=serialize_evidence_items(evidence),
        specifiedBy=pathogenicity_score_calibration_as_method(score_calibration, evidence_outcome),
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
            mavedb_score_calibration_contribution(score_calibration),
        ],
        targetProposition=proposition,
        reportedIn=[score_calibration_as_document(score_calibration)],
        extensions=[
            Extension(
                name="Containing classification name",
                value=containing_evidence_range.label if containing_evidence_range else "Unclassified",
                description="The name of the classification which contains this variant.",
            )
        ],
    )


def functional_evidence_line(
    mapped_variant: MappedVariant,
    score_calibration: ScoreCalibration,
    evidence: list[Union[StudyResult, EvidenceLineType, StatementType, iriReference]],
) -> EvidenceLine:
    containing_evidence_range, classification = functional_classification_of_variant(mapped_variant, score_calibration)

    return EvidenceLine(
        description=f"Functional evidence line for {mapped_variant.variant.urn}",
        hasEvidenceItems=serialize_evidence_items(evidence),
        directionOfEvidenceProvided=direction_of_support_for_functional_classification(classification),
        evidenceOutcome=MappableConcept(
            primaryCoding=Coding(
                code=classification.value,
                system="ga4gh-gks-term:experimental-var-func-impact-classification",
            ),
        ),
        specifiedBy=functional_score_calibration_as_method(score_calibration),
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
            mavedb_score_calibration_contribution(score_calibration),
        ],
        reportedIn=[score_calibration_as_document(score_calibration)],
        extensions=[
            Extension(
                name="Containing functional classification",
                value=containing_evidence_range.label if containing_evidence_range else "Unclassified",
                description="The functional classification which contains this variant.",
            ),
        ],
    )
