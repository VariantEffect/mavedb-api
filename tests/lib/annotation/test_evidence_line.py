import pytest
from unittest.mock import patch
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided
from ga4gh.va_spec.base.core import Direction, EvidenceLine
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine

from mavedb.lib.annotation.annotate import variant_functional_impact_statement, variant_study_result
from mavedb.lib.annotation.evidence_line import acmg_evidence_line, functional_evidence_line
from mavedb.lib.annotation.proposition import mapped_variant_to_experimental_variant_clinical_impact_proposition


@pytest.mark.parametrize(
    "expected_outcome, expected_direction",
    [
        (VariantPathogenicityEvidenceLine.Criterion.BS3, Direction.DISPUTES),
        (VariantPathogenicityEvidenceLine.Criterion.PS3, Direction.SUPPORTS),
    ],
)
@pytest.mark.parametrize(
    "expected_strength",
    [
        StrengthOfEvidenceProvided.SUPPORTING,
        StrengthOfEvidenceProvided.MODERATE,
        StrengthOfEvidenceProvided.STRONG,
        StrengthOfEvidenceProvided.VERY_STRONG,
    ],
)
def test_acmg_evidence_line_with_met_valid_clinical_classification(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
    expected_outcome,
    expected_strength,
    expected_direction,
):
    with patch(
        "mavedb.lib.annotation.evidence_line.pathogenicity_classification_of_variant",
        return_value=(expected_outcome, expected_strength),
    ):
        proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(
            mock_mapped_variant_with_pathogenicity_calibration_score_set
        )
        evidence = variant_functional_impact_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)
        result = acmg_evidence_line(
            mock_mapped_variant_with_pathogenicity_calibration_score_set, proposition, [evidence]
        )

    if expected_strength == StrengthOfEvidenceProvided.STRONG:
        expected_evidence_outcome = expected_outcome.value
    else:
        expected_evidence_outcome = f"{expected_outcome.value}_{expected_strength.name.lower()}"

    assert isinstance(result, VariantPathogenicityEvidenceLine)
    assert (
        result.description
        == f"Pathogenicity evidence line {mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.urn}."
    )
    assert result.evidenceOutcome.primaryCoding.code.root == expected_evidence_outcome
    assert result.evidenceOutcome.primaryCoding.system == "ACMG Guidelines, 2015"
    assert result.evidenceOutcome.name == f"ACMG 2015 {expected_outcome.name} Criterion Met"
    assert result.strengthOfEvidenceProvided.primaryCoding.code.root == expected_strength
    assert result.strengthOfEvidenceProvided.primaryCoding.system == "ACMG Guidelines, 2015"
    assert result.directionOfEvidenceProvided == expected_direction if expected_strength else None
    assert result.contributions
    assert result.specifiedBy
    assert result.targetProposition == proposition
    assert len(result.hasEvidenceItems) == 1
    assert result.hasEvidenceItems[0] == evidence


def test_acmg_evidence_line_with_not_met_clinical_classification(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
):
    expected_outcome = VariantPathogenicityEvidenceLine.Criterion.PS3
    expected_strength = None
    expected_evidence_outcome = f"{expected_outcome.value}_not_met"

    with patch(
        "mavedb.lib.annotation.evidence_line.pathogenicity_classification_of_variant",
        return_value=(expected_outcome, expected_strength),
    ):
        proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(
            mock_mapped_variant_with_pathogenicity_calibration_score_set
        )
        evidence = variant_functional_impact_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)
        result = acmg_evidence_line(
            mock_mapped_variant_with_pathogenicity_calibration_score_set, proposition, [evidence]
        )

    assert isinstance(result, VariantPathogenicityEvidenceLine)
    assert (
        result.description
        == f"Pathogenicity evidence line {mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.urn}."
    )
    assert result.evidenceOutcome.primaryCoding.code.root == expected_evidence_outcome
    assert result.evidenceOutcome.primaryCoding.system == "ACMG Guidelines, 2015"
    assert result.evidenceOutcome.name == f"ACMG 2015 {expected_outcome.name} Criterion Not Met"
    assert result.strengthOfEvidenceProvided is None
    assert result.directionOfEvidenceProvided == Direction.NEUTRAL
    assert result.contributions
    assert result.specifiedBy
    assert result.targetProposition == proposition
    assert len(result.hasEvidenceItems) == 1
    assert result.hasEvidenceItems[0] == evidence


def test_acmg_evidence_line_with_no_calibrations(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_calibrations = None

    with pytest.raises(ValueError) as exc:
        proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant)
        evidence = variant_functional_impact_statement(mock_mapped_variant)
        acmg_evidence_line(mock_mapped_variant, proposition, [evidence])

    assert f"Variant {mock_mapped_variant.variant.urn} does not have a score set with score calibrations" in str(
        exc.value
    )


def test_functional_evidence_line_with_valid_functional_evidence(mock_mapped_variant):
    evidence = variant_study_result(mock_mapped_variant)
    result = functional_evidence_line(mock_mapped_variant, [evidence])

    assert isinstance(result, EvidenceLine)
    assert result.description == f"Functional evidence line for {mock_mapped_variant.variant.urn}"
    assert result.directionOfEvidenceProvided == "supports"
    assert result.specifiedBy
    assert result.contributions
    assert result.reportedIn
    assert len(result.hasEvidenceItems) == 1
    assert result.hasEvidenceItems[0].root == evidence
