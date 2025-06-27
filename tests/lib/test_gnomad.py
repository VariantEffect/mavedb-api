# ruff: noqa: E402

import pytest
import importlib
from unittest.mock import patch

pyathena = pytest.importorskip("pyathena")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.gnomad import (
    gnomad_identifier,
    allele_list_from_list_like_string,
    link_gnomad_variants_to_mapped_variants,
)
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.gnomad_variant import GnomADVariant

from tests.helpers.constants import (
    TEST_GNOMAD_ALLELE_NUMBER,
    TEST_GNOMAD_VARIANT,
    TEST_MINIMAL_MAPPED_VARIANT,
    TEST_GNOMAD_DATA_VERSION,
)

### Tests for gnomad_identifier function ###


def test_gnomad_identifier_basic():
    result = gnomad_identifier("chr1", "12345", ["A", "T"])
    assert result == "1-12345-A-T"


def test_gnomad_identifier_integer_position():
    result = gnomad_identifier("chr1", 12345, ["A", "T"])
    assert result == "1-12345-A-T"


def test_gnomad_identifier_handles_no_chr_prefix():
    result = gnomad_identifier("2", "111", ["C", "A"])
    assert result == "2-111-C-A"


def test_gnomad_identifier_position_as_string():
    result = gnomad_identifier("chr4", "333", ["T", "C"])
    assert result == "4-333-T-C"


def test_gnomad_identifier_multiple_alleles():
    with pytest.raises(ValueError, match="The allele list may only contain two alleles."):
        gnomad_identifier("chr2", 123, ["A", "T", "G"])


def test_gnomad_identifier_raises_with_one_allele():
    with pytest.raises(ValueError, match="The allele list may only contain two alleles."):
        gnomad_identifier("chr5", 444, ["A"])


def test_gnomad_identifier_raises_with_no_alleles():
    with pytest.raises(ValueError, match="The allele list may only contain two alleles."):
        gnomad_identifier("chr6", 555, [])


### Tests for gnomad_table_name function ###


def test_gnomad_table_name_returns_expected(monkeypatch):
    monkeypatch.setenv("GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION)
    # Reload the module to update GNOMAD_DATA_VERSION global
    import mavedb.lib.gnomad as gnomad_mod

    importlib.reload(gnomad_mod)
    assert gnomad_mod.gnomad_table_name() == TEST_GNOMAD_DATA_VERSION.replace(".", "_")


def test_gnomad_table_name_raises_if_env_not_set(monkeypatch):
    monkeypatch.delenv("GNOMAD_DATA_VERSION", raising=False)
    import mavedb.lib.gnomad as gnomad_mod

    importlib.reload(gnomad_mod)
    with pytest.raises(ValueError, match="GNOMAD_DATA_VERSION environment variable is not set."):
        gnomad_mod.gnomad_table_name()


### Tests for allele_list_from_list_like_string function ###


def test_allele_list_from_list_like_string_empty():
    assert allele_list_from_list_like_string("") == []


def test_allele_list_from_list_like_string_valid_two_alleles():
    assert allele_list_from_list_like_string('"[A, T]"') == ["A", "T"]


def test_allele_list_from_list_like_string_valid_with_whitespace():
    assert allele_list_from_list_like_string('"[A,  TG]"') == ["A", "TG"]


def test_allele_list_from_list_like_string_invalid_format_missing_quotes():
    with pytest.raises(ValueError, match="Invalid format for alleles string."):
        allele_list_from_list_like_string("[A, T]")


def test_allele_list_from_list_like_string_invalid_format_single_allele():
    with pytest.raises(ValueError, match="Invalid format for alleles string."):
        allele_list_from_list_like_string('"[G]"')


def test_allele_list_from_list_like_string_invalid_format_extra_allele():
    with pytest.raises(ValueError, match="Invalid format for alleles string."):
        allele_list_from_list_like_string('"[A, T, C]"')


def test_allele_list_from_list_like_string_invalid_format_non_AGTC():
    with pytest.raises(ValueError, match="Invalid format for alleles string."):
        allele_list_from_list_like_string('"[A, X]"')


def test_allele_list_from_list_like_string_invalid_format_not_list():
    with pytest.raises(ValueError, match="Invalid format for alleles string."):
        allele_list_from_list_like_string('"A, T"')


### Tests for gnomad_variant_data_for_caids function ###
# This function is intentionally omitted from testing.
# It's a simple wrapper around an athena query that's more trouble than it's worth to mock.
# If the package is working correctly, this function should work as expected.


### Tests for link_gnomad_variants_to_mapped_variants function ###


def test_links_new_gnomad_variant_to_mapped_variant(
    session, mocked_gnomad_variant_row, setup_lib_db_with_mapped_variant
):
    mapped_variant = setup_lib_db_with_mapped_variant
    mapped_variant.clingen_allele_id = mocked_gnomad_variant_row.caid
    session.add(mapped_variant)
    session.commit()

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_mapped_variants(session, [mocked_gnomad_variant_row])
        assert result == 1
        session.commit()

    session.refresh(mapped_variant)

    edited_saved_gnomad_variant = TEST_GNOMAD_VARIANT.copy()
    edited_saved_gnomad_variant.pop("creation_date")
    edited_saved_gnomad_variant.pop("modification_date")

    assert len(mapped_variant.gnomad_variants) == 1
    for attr in edited_saved_gnomad_variant:
        assert getattr(mapped_variant.gnomad_variants[0], attr) == edited_saved_gnomad_variant[attr]


def test_links_existing_gnomad_variant(session, mocked_gnomad_variant_row, setup_lib_db_with_mapped_variant):
    gnomad_variant = GnomADVariant(**TEST_GNOMAD_VARIANT)
    mapped_variant = setup_lib_db_with_mapped_variant
    mapped_variant.clingen_allele_id = mocked_gnomad_variant_row.caid
    session.add(mapped_variant)
    session.add(gnomad_variant)
    session.commit()

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_mapped_variants(session, [mocked_gnomad_variant_row])
        assert result == 1
        session.commit()

    session.refresh(mapped_variant)

    edited_saved_gnomad_variant = TEST_GNOMAD_VARIANT.copy()
    edited_saved_gnomad_variant.pop("creation_date")
    edited_saved_gnomad_variant.pop("modification_date")

    assert len(mapped_variant.gnomad_variants) == 1
    for attr in edited_saved_gnomad_variant:
        assert getattr(mapped_variant.gnomad_variants[0], attr) == edited_saved_gnomad_variant[attr]


def test_removes_existing_gnomad_variant_with_same_version(
    session, mocked_gnomad_variant_row, setup_lib_db_with_mapped_variant
):
    mapped_variant = setup_lib_db_with_mapped_variant
    mapped_variant.clingen_allele_id = mocked_gnomad_variant_row.caid
    session.add(mapped_variant)
    session.commit()

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_mapped_variants(session, [mocked_gnomad_variant_row])
        assert result == 1

    setattr(mocked_gnomad_variant_row, "joint.freq.all.ac", "1234")
    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_mapped_variants(session, [mocked_gnomad_variant_row])
        assert result == 1
        session.commit()

    session.refresh(mapped_variant)

    edited_saved_gnomad_variant = TEST_GNOMAD_VARIANT.copy()
    edited_saved_gnomad_variant["allele_count"] = 1234
    edited_saved_gnomad_variant["allele_frequency"] = float(1234 / int(TEST_GNOMAD_ALLELE_NUMBER))
    edited_saved_gnomad_variant.pop("creation_date")
    edited_saved_gnomad_variant.pop("modification_date")

    assert len(mapped_variant.gnomad_variants) == 1
    for attr in edited_saved_gnomad_variant:
        assert getattr(mapped_variant.gnomad_variants[0], attr) == edited_saved_gnomad_variant[attr]


def test_links_multiple_rows_and_variants(session, mocked_gnomad_variant_row, setup_lib_db_with_mapped_variant):
    mapped_variant1 = setup_lib_db_with_mapped_variant
    mapped_variant2 = MappedVariant(**TEST_MINIMAL_MAPPED_VARIANT, variant_id=mapped_variant1.variant_id)

    mapped_variant1.clingen_allele_id = mocked_gnomad_variant_row.caid
    mapped_variant2.clingen_allele_id = mocked_gnomad_variant_row.caid
    session.add(mapped_variant1)
    session.add(mapped_variant2)
    session.commit()

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_mapped_variants(session, [mocked_gnomad_variant_row])
        assert result == 2
        session.commit()

    gnomad_variant_comparator = TEST_GNOMAD_VARIANT.copy()
    gnomad_variant_comparator.pop("creation_date")
    gnomad_variant_comparator.pop("modification_date")

    assert len(mapped_variant1.gnomad_variants) == 1
    assert len(mapped_variant2.gnomad_variants) == 1
    for mv in [mapped_variant1, mapped_variant2]:
        for attr in gnomad_variant_comparator:
            assert getattr(mv.gnomad_variants[0], attr) == gnomad_variant_comparator[attr]


def test_returns_zero_when_no_mapped_variants(session, mocked_gnomad_variant_row):
    result = link_gnomad_variants_to_mapped_variants(session, [mocked_gnomad_variant_row])
    assert result == 0


def test_only_current_flag_filters_variants(session, mocked_gnomad_variant_row, setup_lib_db_with_mapped_variant):
    mapped_variant1 = setup_lib_db_with_mapped_variant
    mapped_variant2 = MappedVariant(**TEST_MINIMAL_MAPPED_VARIANT, variant_id=mapped_variant1.variant_id)

    mapped_variant1.current = False
    mapped_variant1.clingen_allele_id = mocked_gnomad_variant_row.caid
    mapped_variant2.clingen_allele_id = mocked_gnomad_variant_row.caid
    session.add(mapped_variant1)
    session.add(mapped_variant2)
    session.commit()

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_mapped_variants(session, [mocked_gnomad_variant_row])
        assert result == 1
        session.commit()

    gnomad_variant_comparator = TEST_GNOMAD_VARIANT.copy()
    gnomad_variant_comparator.pop("creation_date")
    gnomad_variant_comparator.pop("modification_date")

    assert len(mapped_variant1.gnomad_variants) == 0
    assert len(mapped_variant2.gnomad_variants) == 1
    for attr in gnomad_variant_comparator:
        assert getattr(mapped_variant2.gnomad_variants[0], attr) == gnomad_variant_comparator[attr]


def test_only_current_flag_is_false_operates_on_all_variants(
    session, mocked_gnomad_variant_row, setup_lib_db_with_mapped_variant
):
    mapped_variant1 = setup_lib_db_with_mapped_variant
    mapped_variant2 = MappedVariant(**TEST_MINIMAL_MAPPED_VARIANT, variant_id=mapped_variant1.variant_id)

    mapped_variant1.current = False
    mapped_variant1.clingen_allele_id = mocked_gnomad_variant_row.caid
    mapped_variant2.clingen_allele_id = mocked_gnomad_variant_row.caid
    session.add(mapped_variant1)
    session.add(mapped_variant2)
    session.commit()

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_mapped_variants(session, [mocked_gnomad_variant_row], False)
        assert result == 2
        session.commit()

    gnomad_variant_comparator = TEST_GNOMAD_VARIANT.copy()
    gnomad_variant_comparator.pop("creation_date")
    gnomad_variant_comparator.pop("modification_date")
    for mv in [mapped_variant1, mapped_variant2]:
        assert len(mv.gnomad_variants) == 1
        for attr in gnomad_variant_comparator:
            assert getattr(mv.gnomad_variants[0], attr) == gnomad_variant_comparator[attr]
