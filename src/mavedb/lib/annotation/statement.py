from typing import Optional

from ga4gh.core.models import Coding, MappableConcept
from ga4gh.va_spec.acmg_2015 import AcmgClassification, VariantPathogenicityEvidenceLine, VariantPathogenicityStatement
from ga4gh.va_spec.base.core import (
    EvidenceLine,
    ExperimentalVariantFunctionalImpactProposition,
    Statement,
    VariantPathogenicityProposition,
)

from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification
from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_score_calibration_contribution,
    mavedb_vrs_contribution,
)
from mavedb.lib.annotation.direction import aggregate_direction_of_evidence
from mavedb.lib.annotation.method import (
    variant_interpretation_functional_guideline_method,
    variant_interpretation_pathogenicity_guideline_method,
)
from mavedb.lib.annotation.util import serialize_evidence_items
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification


def mapped_variant_to_functional_statement(
    mapped_variant: MappedVariant,
    proposition: ExperimentalVariantFunctionalImpactProposition,
    evidence: list[EvidenceLine],
    score_calibration: ScoreCalibration,
    functional_classification: ExperimentalVariantFunctionalImpactClassification,
) -> Statement:
    """
    Create a functional impact statement for a mapped variant.

    Args:
        mapped_variant: The variant being classified
        proposition: The functional impact proposition
        evidence: List of evidence lines supporting the statement
        score_calibration: The score calibration with the strongest evidence
        functional_classification: The functional classification from the strongest evidence range

    Returns:
        A Statement containing the functional impact classification
    """
    direction = aggregate_direction_of_evidence(evidence)

    return Statement(
        description=f"Variant functional impact statement for {mapped_variant.variant.urn}.",
        specifiedBy=variant_interpretation_functional_guideline_method(),
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
            mavedb_score_calibration_contribution(score_calibration),
        ],
        proposition=proposition,
        direction=direction,
        classification=MappableConcept(
            primaryCoding=Coding(
                code=functional_classification,
                system="ga4gh-gks-term:experimental-var-func-impact-classification",
            ),
        ),
        hasEvidenceLines=[evidence_item for evidence_item in evidence],
    )


def mapped_variant_to_pathogenicity_statement(
    mapped_variant: MappedVariant,
    proposition: VariantPathogenicityProposition,
    evidence: list[VariantPathogenicityEvidenceLine],
    score_calibration: ScoreCalibration,
    functional_range: Optional[ScoreCalibrationFunctionalClassification],
) -> VariantPathogenicityStatement:
    """
    Create a pathogenicity statement for a mapped variant.

    Args:
        mapped_variant: The variant being classified
        proposition: The pathogenicity proposition
        evidence: List of evidence lines supporting the statement
        score_calibration: The score calibration with the strongest evidence
        functional_range: The functional classification range containing the ACMG classification,
                         or None if variant is not in any range (will use UNCERTAIN_SIGNIFICANCE)

    Returns:
        A VariantPathogenicityStatement containing the pathogenicity classification
    """
    direction = aggregate_direction_of_evidence(evidence)

    # Determine ACMG classification from the functional range's ACMG classification
    # If functional_range is None, the variant is not in any range, so use UNCERTAIN_SIGNIFICANCE
    if functional_range and functional_range.acmg_classification and functional_range.acmg_classification.criterion:
        criterion = functional_range.acmg_classification.criterion
        # Map criterion to ACMG classification based on whether it's pathogenic or benign
        if criterion.is_pathogenic:
            acmg_classification = AcmgClassification.PATHOGENIC
        elif criterion.is_benign:
            acmg_classification = AcmgClassification.BENIGN
        else:
            acmg_classification = AcmgClassification.UNCERTAIN_SIGNIFICANCE
    else:
        acmg_classification = AcmgClassification.UNCERTAIN_SIGNIFICANCE

    return VariantPathogenicityStatement(
        description=f"Variant pathogenicity statement for {mapped_variant.variant.urn}.",
        specifiedBy=variant_interpretation_pathogenicity_guideline_method(),
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
            mavedb_score_calibration_contribution(score_calibration),
        ],
        proposition=proposition,
        direction=direction,
        classification=MappableConcept(
            primaryCoding=Coding(
                code=acmg_classification,
                system="ACMG Guidelines, 2015",
            ),
        ),
        hasEvidenceLines=serialize_evidence_items(evidence),
    )
