import pytest

from mavedb.lib.variants import (
    get_digest_from_post_mapped,
    get_hgvs_from_post_mapped,
    hgvs_from_vrs_allele,
    is_hgvs_g,
    is_hgvs_p,
)
from tests.helpers.constants import (
    TEST_HGVS_IDENTIFIER,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_POST_MAPPED_VRS_CIS_PHASED_BLOCK,
    TEST_VALID_POST_MAPPED_VRS_HAPLOTYPE,
)

### Tests for hgvs_from_vrs_allele function ###


def test_hgvs_from_vrs_allele_vrs_1():
    with pytest.raises(ValueError):
        hgvs_from_vrs_allele(TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X)


def test_hgvs_from_vrs_allele_vrs_2():
    hgvs_string = hgvs_from_vrs_allele(TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X)
    assert hgvs_string == TEST_HGVS_IDENTIFIER


def test_hgvs_from_vrs_allele_invalid():
    with pytest.raises(KeyError):
        hgvs_from_vrs_allele({"invalid_key": "invalid_value"})


### Tests for get_hgvs_from_post_mapped function ###


def test_get_hgvs_from_post_mapped_haplotype():
    with pytest.raises(ValueError):
        get_hgvs_from_post_mapped(TEST_VALID_POST_MAPPED_VRS_HAPLOTYPE)


def test_get_hgvs_from_post_mapped_cis_phased_block():
    result = get_hgvs_from_post_mapped(TEST_VALID_POST_MAPPED_VRS_CIS_PHASED_BLOCK)
    assert result is None


def test_get_hgvs_from_post_mapped_single_allele_vrs_1():
    with pytest.raises(ValueError):
        get_hgvs_from_post_mapped(TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X)


def test_get_hgvs_from_post_mapped_single_allele_vrs_2():
    result = get_hgvs_from_post_mapped(TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X)
    assert result == TEST_HGVS_IDENTIFIER


def test_get_hgvs_from_post_mapped_empty_post_mapped():
    result = get_hgvs_from_post_mapped(None)
    assert result is None


def test_get_hgvs_from_post_mapped_invalid_type():
    result = get_hgvs_from_post_mapped({"type": "InvalidType"})
    assert result is None


def test_get_hgvs_from_post_mapped_invalid_structure():
    with pytest.raises(KeyError):
        get_hgvs_from_post_mapped({"invalid_key": "InvalidType"})


### Tests for get_digest_from_post_mapped function ###


def test_get_digest_from_post_mapped_with_digest():
    post_mapped_vrs = {"digest": "test_digest_value", "type": "Allele"}
    result = get_digest_from_post_mapped(post_mapped_vrs)
    assert result == "test_digest_value"


def test_get_digest_from_post_mapped_without_digest():
    post_mapped_vrs = {"type": "Allele", "other_field": "value"}

    result = get_digest_from_post_mapped(post_mapped_vrs)

    assert result is None


def test_get_digest_from_post_mapped_none_input():
    result = get_digest_from_post_mapped(None)
    assert result is None


def test_get_digest_from_post_mapped_empty_dict():
    result = get_digest_from_post_mapped({})
    assert result is None


### Tests for is_hgvs_g and is_hgvs_p functions ###


@pytest.mark.parametrize(
    "hgvs,expected",
    [
        ("NC_000001.11:g.123456A>T", True),
        ("chr1:g.123456A>T", True),
        ("NM_000546.5:c.215C>G", False),
        ("NP_000537.3:p.Arg72Pro", False),
        ("g.123456A>T", True),
        ("p.Arg72Pro", False),
        ("", False),
    ],
)
def test_is_hgvs_g(hgvs, expected):
    assert is_hgvs_g(hgvs) == expected


@pytest.mark.parametrize(
    "hgvs,expected",
    [
        ("NP_000537.3:p.Arg72Pro", True),
        ("p.Arg72Pro", True),
        ("NC_000001.11:g.123456A>T", False),
        ("chr1:g.123456A>T", False),
        ("c.215C>G", False),
        ("", False),
    ],
)
def test_is_hgvs_p(hgvs, expected):
    assert is_hgvs_p(hgvs) == expected
