import pytest

from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.annotation.util import variation_from_mapped_variant

from tests.helpers.constants import TEST_VALID_POST_MAPPED_VRS_ALLELE, TEST_SEQUENCE_LOCATION_ACCESSION


@pytest.mark.parametrize(
    "variation_version", [{"variation": TEST_VALID_POST_MAPPED_VRS_ALLELE}, TEST_VALID_POST_MAPPED_VRS_ALLELE]
)
def test_variation_from_mapped_variant_post_mapped_variation(mock_mapped_variant, variation_version):
    mock_mapped_variant.post_mapped = variation_version

    result = variation_from_mapped_variant(mock_mapped_variant).model_dump()

    assert result["location"]["id"] == TEST_SEQUENCE_LOCATION_ACCESSION
    assert result["location"]["start"] == 5
    assert result["location"]["end"] == 6


def test_variation_from_mapped_variant_no_post_mapped(mock_mapped_variant):
    mock_mapped_variant.post_mapped = None

    with pytest.raises(MappingDataDoesntExistException):
        variation_from_mapped_variant(mock_mapped_variant)
