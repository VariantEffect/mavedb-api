import pytest
from unittest.mock import patch
from ga4gh.va_spec.base.core import Statement, Direction

from mavedb.lib.annotation.annotate import variant_study_result
from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification
from mavedb.lib.annotation.evidence_line import functional_evidence_line
from mavedb.lib.annotation.proposition import mapped_variant_to_experimental_variant_functional_impact_proposition
from mavedb.lib.annotation.statement import mapped_variant_to_functional_statement


@pytest.mark.parametrize(
    "classification, expected_direction",
    [
        (ExperimentalVariantFunctionalImpactClassification.NORMAL, Direction.DISPUTES),
        (ExperimentalVariantFunctionalImpactClassification.ABNORMAL, Direction.SUPPORTS),
        (ExperimentalVariantFunctionalImpactClassification.INDETERMINATE, Direction.NEUTRAL),
    ],
)
def test_mapped_variant_to_functional_statement(mock_mapped_variant, classification, expected_direction):
    with patch(
        "mavedb.lib.annotation.statement.functional_classification_of_variant",
        return_value=classification,
    ):
        proposition = mapped_variant_to_experimental_variant_functional_impact_proposition(mock_mapped_variant)
        evidence = functional_evidence_line(mock_mapped_variant, [variant_study_result(mock_mapped_variant)])
        result = mapped_variant_to_functional_statement(mock_mapped_variant, proposition, [evidence])

    assert isinstance(result, Statement)
    assert result.description == f"Variant functional impact statement for {mock_mapped_variant.variant.urn}."
    assert result.specifiedBy
    assert result.contributions
    assert result.proposition == proposition
    assert result.direction == expected_direction
    assert result.classification.primaryCoding.code.root == classification.value
    assert result.classification.primaryCoding.system == "ga4gh-gks-term:experimental-var-func-impact-classification"
    assert result.hasEvidenceLines
    assert len(result.hasEvidenceLines) == 1
    assert result.hasEvidenceLines[0] == evidence


def test_mapped_variant_to_functional_statement_no_score_ranges(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = None

    proposition = mapped_variant_to_experimental_variant_functional_impact_proposition(mock_mapped_variant)
    evidence = functional_evidence_line(mock_mapped_variant, [variant_study_result(mock_mapped_variant)])

    with pytest.raises(ValueError) as exc:
        mapped_variant_to_functional_statement(mock_mapped_variant, proposition, [evidence])

    assert f"Variant {mock_mapped_variant.variant.urn} does not have a score set with score ranges" in str(exc.value)
