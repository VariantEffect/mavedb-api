import pytest
from unittest.mock import patch
from ga4gh.va_spec.base import StrengthOfEvidenceProvided, EvidenceLine
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityFunctionalImpactEvidenceLine, EvidenceOutcome

from mavedb.lib.annotation.annotate import variant_functional_impact_statement, variant_study_result
from mavedb.lib.annotation.evidence_line import acmg_evidence_line, functional_evidence_line
from mavedb.lib.annotation.proposition import mapped_variant_to_experimental_variant_clinical_impact_proposition


def test_acmg_evidence_line_with_valid_clinical_classification(mock_mapped_variant):
    with patch(
        "mavedb.lib.annotation.evidence_line.pillar_project_clinical_classification_of_variant",
        return_value=(EvidenceOutcome.BS3_SUPPORTING, StrengthOfEvidenceProvided.SUPPORTING),
    ):
        proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant)
        evidence = variant_functional_impact_statement(mock_mapped_variant)
        result = acmg_evidence_line(mock_mapped_variant, proposition, [evidence])

    assert isinstance(result, VariantPathogenicityFunctionalImpactEvidenceLine)
    assert result.description == f"Pathogenicity evidence line {mock_mapped_variant.variant.urn}."
    assert result.evidenceOutcome.primaryCoding.code.root == EvidenceOutcome.BS3_SUPPORTING
    assert result.evidenceOutcome.primaryCoding.system == "ACMG Guidelines, 2015"
    assert result.evidenceOutcome.name == f"ACMG 2015 {EvidenceOutcome.BS3_SUPPORTING.name} Criterion Met"
    assert result.strengthOfEvidenceProvided.primaryCoding.code.root == StrengthOfEvidenceProvided.SUPPORTING
    assert result.strengthOfEvidenceProvided.primaryCoding.system == "ACMG Guidelines, 2015"
    assert result.directionOfEvidenceProvided == "supports"
    assert result.contributions
    assert result.specifiedBy
    assert result.targetProposition == proposition
    assert len(result.hasEvidenceItems) == 1
    assert result.hasEvidenceItems[0] == evidence


def test_acmg_evidence_line_with_no_clinical_classification(mock_mapped_variant):
    with patch(
        "mavedb.lib.annotation.evidence_line.pillar_project_clinical_classification_of_variant",
        return_value=(None, None),
    ):
        proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant)
        evidence = variant_functional_impact_statement(mock_mapped_variant)
        result = acmg_evidence_line(mock_mapped_variant, proposition, [evidence])

    assert result is None


def test_acmg_evidence_line_with_no_score_thresholds(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_calibrations = None

    with pytest.raises(ValueError) as exc:
        proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant)
        evidence = variant_functional_impact_statement(mock_mapped_variant)
        acmg_evidence_line(mock_mapped_variant, proposition, [evidence])

    assert f"Variant {mock_mapped_variant.variant.urn} does not have a score set with score thresholds" in str(
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
