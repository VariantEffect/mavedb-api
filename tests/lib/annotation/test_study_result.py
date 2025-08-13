import pytest  # noqa: F401

from ga4gh.va_spec.base import ExperimentalVariantFunctionalImpactStudyResult, Method
from ga4gh.vrs.models import MolecularVariation

from mavedb.lib.annotation.document import mapped_variant_as_iri, variant_as_iri
from mavedb.lib.annotation.study_result import (
    mapped_variant_to_experimental_variant_impact_study_result,
)


def test_mapped_variant_to_experimental_variant_impact_study_result(mock_mapped_variant):
    result = mapped_variant_to_experimental_variant_impact_study_result(mock_mapped_variant)

    assert isinstance(result, ExperimentalVariantFunctionalImpactStudyResult)
    assert result.description == f"Variant effect study result for {mock_mapped_variant.variant.urn}."
    assert isinstance(result.focusVariant, MolecularVariation)
    assert result.functionalImpactScore == mock_mapped_variant.variant.data["score_data"]["score"]
    assert len(result.contributions) == 2
    assert result.specifiedBy is not None and isinstance(result.specifiedBy, Method)
    assert result.sourceDataSet is not None
    assert result.reportedIn is not None
    assert mapped_variant_as_iri(mock_mapped_variant) in result.reportedIn
    assert variant_as_iri(mock_mapped_variant.variant) in result.reportedIn


def test_mapped_variant_to_experimental_variant_impact_study_result_no_mapped_variant_is_filtered(mock_mapped_variant):
    mock_mapped_variant.clingen_allele_id = None
    result = mapped_variant_to_experimental_variant_impact_study_result(mock_mapped_variant)

    assert isinstance(result, ExperimentalVariantFunctionalImpactStudyResult)
    assert result.description == f"Variant effect study result for {mock_mapped_variant.variant.urn}."
    assert isinstance(result.focusVariant, MolecularVariation)
    assert result.functionalImpactScore == mock_mapped_variant.variant.data["score_data"]["score"]
    assert len(result.contributions) == 2
    assert result.specifiedBy is not None and isinstance(result.specifiedBy, Method)
    assert result.sourceDataSet is not None
    assert result.reportedIn is not None
    assert variant_as_iri(mock_mapped_variant.variant) in result.reportedIn
    assert len(result.reportedIn) == 1
