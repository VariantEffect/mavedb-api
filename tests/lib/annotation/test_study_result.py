import pytest

from ga4gh.core.models import iriReference
from ga4gh.va_spec.base import ExperimentalVariantFunctionalImpactStudyResult, Method
from ga4gh.vrs.models import Allele

from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.annotation.study_result import (
    _variation_from_mapped_variant,
    mapped_variant_to_experimental_variant_impact_study_result,
)

from tests.helpers.constants import TEST_VALID_POST_MAPPED_VRS_ALLELE, TEST_SEQUENCE_LOCATION_ACCESSION


@pytest.mark.parametrize(
    "variation_version", [{"variation": TEST_VALID_POST_MAPPED_VRS_ALLELE}, TEST_VALID_POST_MAPPED_VRS_ALLELE]
)
def test_variation_from_mapped_variant_post_mapped_variation(mock_mapped_variant, variation_version):
    mock_mapped_variant.post_mapped = variation_version

    result = _variation_from_mapped_variant(mock_mapped_variant)

    assert result["location"]["sequenceReference"] == TEST_SEQUENCE_LOCATION_ACCESSION
    assert result["location"]["start"] == 5
    assert result["location"]["end"] == 6


def test_variation_from_mapped_variant_no_post_mapped(mock_mapped_variant):
    mock_mapped_variant.post_mapped = None

    with pytest.raises(MappingDataDoesntExistException):
        _variation_from_mapped_variant(mock_mapped_variant)


def test_mapped_variant_to_experimental_variant_impact_study_result(mock_mapped_variant):
    result = mapped_variant_to_experimental_variant_impact_study_result(mock_mapped_variant)

    assert isinstance(result, ExperimentalVariantFunctionalImpactStudyResult)
    assert result.description == f"Variant effect study result for {mock_mapped_variant.variant.urn}."
    assert isinstance(result.focusVariant, Allele)
    assert result.functionalImpactScore == mock_mapped_variant.variant.data["score_data"]["score"]
    assert len(result.contributions) == 2
    assert result.specifiedBy is not None and isinstance(result.specifiedBy, Method)
    assert result.sourceDataSet is not None
    assert result.reportedIn is not None and isinstance(result.reportedIn, iriReference)
