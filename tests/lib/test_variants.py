import pytest
from unittest.mock import MagicMock

from mavedb.lib.variants import hgvs_from_vrs_allele
from mavedb.lib.variants import hgvs_from_mapped_variant

from tests.helpers.constants import (
    TEST_HGVS_IDENTIFIER,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_POST_MAPPED_VRS_HAPLOTYPE,
    TEST_VALID_POST_MAPPED_VRS_CIS_PHASED_BLOCK,
)


@pytest.mark.parametrize("allele", [TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X, TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X])
def test_hgvs_from_vrs_allele(allele):
    result = hgvs_from_vrs_allele(allele)
    assert result == TEST_HGVS_IDENTIFIER


def test_hgvs_from_vrs_allele_invalid():
    allele = {"invalid_key": "invalid_value"}
    with pytest.raises(KeyError):
        hgvs_from_vrs_allele(allele)


def test_hgvs_from_mapped_variant_haplotype():
    mapped_variant = MagicMock()
    mapped_variant.post_mapped = TEST_VALID_POST_MAPPED_VRS_HAPLOTYPE
    result = hgvs_from_mapped_variant(mapped_variant)
    assert result == [TEST_HGVS_IDENTIFIER, TEST_HGVS_IDENTIFIER]


def test_hgvs_from_mapped_variant_cis_phased_block():
    mapped_variant = MagicMock()
    mapped_variant.post_mapped = TEST_VALID_POST_MAPPED_VRS_CIS_PHASED_BLOCK
    result = hgvs_from_mapped_variant(mapped_variant)
    assert result == [TEST_HGVS_IDENTIFIER, TEST_HGVS_IDENTIFIER]


@pytest.mark.parametrize("allele", [TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X, TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X])
def test_hgvs_from_mapped_variant_single_allele(allele):
    mapped_variant = MagicMock()
    mapped_variant.post_mapped = allele
    result = hgvs_from_mapped_variant(mapped_variant)
    assert result == [TEST_HGVS_IDENTIFIER]


def test_hgvs_from_mapped_variant_empty_post_mapped():
    mapped_variant = MagicMock()
    mapped_variant.post_mapped = None
    result = hgvs_from_mapped_variant(mapped_variant)
    assert result == []


def test_hgvs_from_mapped_variant_invalid_type():
    mapped_variant = MagicMock()
    mapped_variant.post_mapped = {"type": "InvalidType"}
    with pytest.raises(ValueError):
        hgvs_from_mapped_variant(mapped_variant)


def test_hgvs_from_mapped_variant_invalid_structure():
    mapped_variant = MagicMock()
    mapped_variant.post_mapped = {"invalid_key": "InvalidType"}
    with pytest.raises(ValueError):
        hgvs_from_mapped_variant(mapped_variant)
