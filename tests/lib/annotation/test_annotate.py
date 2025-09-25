from mavedb.lib.annotation.annotate import variant_study_result
from mavedb.lib.annotation.annotate import variant_functional_impact_statement
from mavedb.lib.annotation.annotate import variant_pathogenicity_evidence

# The contents of these results are tested elsewhere. These tests focus on object structure.


def test_variant_study_result(mock_mapped_variant):
    result = variant_study_result(mock_mapped_variant)

    assert result is not None
    assert result.type == "ExperimentalVariantFunctionalImpactStudyResult"


def test_variant_functional_impact_statement_no_score_ranges(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = None
    result = variant_functional_impact_statement(mock_mapped_variant)

    assert result is None


def test_variant_functional_impact_statement_no_score(mock_mapped_variant):
    mock_mapped_variant.variant.data = {"score_data": {"score": None}}
    result = variant_functional_impact_statement(mock_mapped_variant)

    assert result is None


def test_variant_functional_impact_statement_with_score_ranges(mock_mapped_variant):
    result = variant_functional_impact_statement(mock_mapped_variant)

    assert result is not None
    assert result.type == "Statement"
    assert all(evidence_item.type == "EvidenceLine" for evidence_item in result.hasEvidenceLines)
    assert all(
        study_result.root.type == "ExperimentalVariantFunctionalImpactStudyResult"
        for evidence_line in [evidence_line for evidence_line in result.hasEvidenceLines]
        for study_result in evidence_line.hasEvidenceItems
    )


def test_variant_pathogenicity_evidence_no_score_ranges_no_thresholds(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = None
    mock_mapped_variant.variant.score_set.score_calibrations = None
    result = variant_pathogenicity_evidence(mock_mapped_variant)

    assert result is None


def test_variant_pathogenicity_evidence_no_score(mock_mapped_variant):
    mock_mapped_variant.variant.data = {"score_data": {"score": None}}
    result = variant_pathogenicity_evidence(mock_mapped_variant)

    assert result is None


def test_variant_pathogenicity_evidence_no_score_ranges_with_thresholds(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges.pop("investigator_provided")
    result = variant_pathogenicity_evidence(mock_mapped_variant)

    assert result is not None
    assert result.targetProposition.type == "VariantPathogenicityProposition"
    assert all(
        evidence_item.root.type == "ExperimentalVariantFunctionalImpactStudyResult"
        for evidence_item in result.hasEvidenceItems
    )


def test_variant_pathogenicity_evidence_with_score_ranges_no_thresholds(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges.pop("zeiberg_calibration")
    result = variant_pathogenicity_evidence(mock_mapped_variant)

    assert result is None


def test_variant_pathogenicity_evidence_with_score_ranges_with_thresholds(mock_mapped_variant):
    result = variant_pathogenicity_evidence(mock_mapped_variant)

    assert result is not None
    assert result.targetProposition.type == "VariantPathogenicityProposition"

    statements = [statement for statement in result.hasEvidenceItems]
    evidence_lines = [evidence_item for statement in statements for evidence_item in statement.hasEvidenceLines]

    assert all(evidence_item.type == "Statement" for evidence_item in result.hasEvidenceItems)
    assert all(evidence_item.type == "EvidenceLine" for evidence_item in evidence_lines)
    assert all(
        study_result.root.type == "ExperimentalVariantFunctionalImpactStudyResult"
        for evidence_line in evidence_lines
        for study_result in evidence_line.hasEvidenceItems
    )
