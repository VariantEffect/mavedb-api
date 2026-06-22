# ruff: noqa: E402

from unittest.mock import patch

import pytest
from sqlalchemy import select

pyathena = pytest.importorskip("pyathena")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.gnomad import (
    allele_list_from_list_like_string,
    gnomad_identifier,
    gnomad_table_name,
    link_gnomad_variants_to_alleles,
    normalize_caid,
)
from mavedb.models.allele import Allele
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant
from tests.helpers.constants import (
    TEST_GNOMAD_DATA_VERSION,
    TEST_GNOMAD_VARIANT,
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


def test_gnomad_table_name_returns_expected():
    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        assert gnomad_table_name() == TEST_GNOMAD_DATA_VERSION.replace(".", "_")


def test_gnomad_table_name_raises_if_env_not_set():
    with (
        pytest.raises(ValueError, match="GNOMAD_DATA_VERSION environment variable is not set."),
        patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", None),
    ):
        gnomad_table_name()


### Tests for normalize_caid function ###


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("CA025094", "CA25094"),  # the #722 example: gnomAD dump drops the leading zero
        ("CA000123", "CA123"),  # multiple leading zeros collapse
        ("CA341478553", "CA341478553"),  # already unpadded — unchanged
        ("CA0", "CA0"),  # keep the final digit even if it is a zero
        ("not-a-caid", "not-a-caid"),  # unrecognized input is passed through
    ],
)
def test_normalize_caid(raw, expected):
    assert normalize_caid(raw) == expected


### Tests for allele_list_from_list_like_string function ###


def test_allele_list_from_list_like_string_empty():
    assert allele_list_from_list_like_string("") == []


def test_allele_list_from_list_like_string_valid_two_alleles():
    assert allele_list_from_list_like_string("[A, T]") == ["A", "T"]


def test_allele_list_from_list_like_string_valid_with_whitespace():
    assert allele_list_from_list_like_string("[A,  TG]") == ["A", "TG"]


def test_allele_list_from_list_like_string_invalid_format_single_allele():
    with pytest.raises(ValueError, match="Invalid format for alleles string."):
        allele_list_from_list_like_string("[G]")


def test_allele_list_from_list_like_string_invalid_format_extra_allele():
    with pytest.raises(ValueError, match="Invalid format for alleles string."):
        allele_list_from_list_like_string("[A, T, C]")


def test_allele_list_from_list_like_string_invalid_format_non_AGTC():
    with pytest.raises(ValueError, match="Invalid format for alleles string."):
        allele_list_from_list_like_string("[A, X]")


def test_allele_list_from_list_like_string_invalid_format_not_list():
    with pytest.raises(ValueError, match="Invalid format for alleles string."):
        allele_list_from_list_like_string("A, T")


### Tests for gnomad_variant_data_for_caids function ###
# This function is intentionally omitted from testing.
# It's a simple wrapper around an athena query that's more trouble than it's worth to mock.
# If the package is working correctly, this function should work as expected.


### Tests for link_gnomad_variants_to_alleles function ###


def _make_allele(session, caid, *, vrs_digest, level="genomic"):
    """Create and persist a deduplicated Allele carrying a CAID."""
    allele = Allele(vrs_digest=vrs_digest, level=level, clingen_allele_id=caid)
    session.add(allele)
    session.commit()
    session.refresh(allele)
    return allele


def _live_links_for(session, allele_id):
    return session.scalars(
        select(GnomadAlleleLink).where(
            GnomadAlleleLink.allele_id == allele_id,
            GnomadAlleleLink.current,
        )
    ).all()


def _assert_gnomad_variant_matches(gnomad_variant, **overrides):
    expected = TEST_GNOMAD_VARIANT.copy()
    expected.pop("creation_date")
    expected.pop("modification_date")
    expected.update(overrides)
    for attr, value in expected.items():
        assert getattr(gnomad_variant, attr) == value


def test_links_new_gnomad_variant_to_allele(session, mocked_gnomad_variant_row):
    allele = _make_allele(session, mocked_gnomad_variant_row.caid, vrs_digest="vrs-1")

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row])
        assert result == {allele.id}
        session.commit()

    live_links = _live_links_for(session, allele.id)
    assert len(live_links) == 1
    _assert_gnomad_variant_matches(live_links[0].gnomad_variant)


def test_can_link_gnomad_variants_with_none_type_faf_fields(session, mocked_gnomad_variant_row):
    allele = _make_allele(session, mocked_gnomad_variant_row.caid, vrs_digest="vrs-1")

    mocked_gnomad_variant_row.__setattr__("joint.fafmax.faf95_max_gen_anc", None)
    mocked_gnomad_variant_row.__setattr__("joint.fafmax.faf95_max", None)

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row])
        assert result == {allele.id}
        session.commit()

    live_links = _live_links_for(session, allele.id)
    assert len(live_links) == 1
    _assert_gnomad_variant_matches(live_links[0].gnomad_variant, faf95_max=None, faf95_max_ancestry=None)


def test_links_existing_gnomad_variant(session, mocked_gnomad_variant_row):
    gnomad_variant = GnomADVariant(**TEST_GNOMAD_VARIANT)
    session.add(gnomad_variant)
    session.commit()
    allele = _make_allele(session, mocked_gnomad_variant_row.caid, vrs_digest="vrs-1")

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row])
        assert result == {allele.id}
        session.commit()

    # Reused the existing gnomAD variant rather than creating a second.
    assert len(session.scalars(select(GnomADVariant)).all()) == 1
    live_links = _live_links_for(session, allele.id)
    assert len(live_links) == 1
    assert live_links[0].gnomad_variant_id == gnomad_variant.id


def test_re_running_unchanged_data_is_idempotent(session, mocked_gnomad_variant_row):
    """Supersede only on change: a second run with identical data writes nothing — one live link,
    no retired rows, so the valid-time history records no spurious boundary."""
    allele = _make_allele(session, mocked_gnomad_variant_row.caid, vrs_digest="vrs-1")

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        assert link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row]) == {allele.id}
        session.commit()
        # Second run sees the live link already points to this gnomAD variant → no change reported.
        assert link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row]) == set()
        session.commit()

    # One link, still live, never retired — the re-run did not churn the history.
    all_links = session.scalars(select(GnomadAlleleLink).where(GnomadAlleleLink.allele_id == allele.id)).all()
    assert len(all_links) == 1
    assert all_links[0].valid_to is None
    assert len(session.scalars(select(GnomADVariant)).all()) == 1


def test_version_bump_supersedes_to_single_live_link(session, mocked_gnomad_variant_row):
    """A new gnomAD version retires the prior link and installs the new one — exactly one live link
    per allele (not one per version), with the old version preserved as a retired row."""
    allele = _make_allele(session, mocked_gnomad_variant_row.caid, vrs_digest="vrs-1")

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", "v1.old"):
        assert link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row]) == {allele.id}
        session.commit()

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", "v2.new"):
        assert link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row]) == {allele.id}
        session.commit()

    live_links = _live_links_for(session, allele.id)
    assert len(live_links) == 1
    assert live_links[0].gnomad_variant.db_version == "v2.new"
    # Old-version link retired, not deleted; both gnomAD variant rows persist.
    all_links = session.scalars(select(GnomadAlleleLink).where(GnomadAlleleLink.allele_id == allele.id)).all()
    assert len(all_links) == 2
    assert len([link for link in all_links if link.valid_to is not None]) == 1
    assert len(session.scalars(select(GnomADVariant)).all()) == 2


def test_same_version_different_identifier_supersedes_newest_wins(session, mocked_gnomad_variant_row):
    """A CAID re-resolving to a different identifier within the same version is an anomaly: log and
    supersede newest-wins rather than raise — one odd allele must not abort the batch."""
    allele = _make_allele(session, mocked_gnomad_variant_row.caid, vrs_digest="vrs-1")
    # Prior live link at the current version, but to a different identifier than the row resolves to.
    stale = GnomADVariant(
        db_name="gnomAD",
        db_identifier="9-99999-C-T",
        db_version=TEST_GNOMAD_DATA_VERSION,
        allele_count=1,
        allele_number=2,
        allele_frequency=0.5,
    )
    session.add(stale)
    session.commit()
    session.add(GnomadAlleleLink(allele_id=allele.id, gnomad_variant_id=stale.id))
    session.commit()

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        assert link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row]) == {allele.id}
        session.commit()

    live_links = _live_links_for(session, allele.id)
    assert len(live_links) == 1
    assert live_links[0].gnomad_variant.db_identifier != "9-99999-C-T"  # newest wins


def test_links_one_gnomad_variant_to_multiple_alleles_sharing_a_caid(session, mocked_gnomad_variant_row):
    """A CAID shared by multiple alleles (cross-score-set dedup) fans the gnomAD variant to each."""
    allele1 = _make_allele(session, mocked_gnomad_variant_row.caid, vrs_digest="vrs-1")
    allele2 = _make_allele(session, mocked_gnomad_variant_row.caid, vrs_digest="vrs-2", level="cdna")

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row])
        assert result == {allele1.id, allele2.id}
        session.commit()

    for allele in (allele1, allele2):
        assert len(_live_links_for(session, allele.id)) == 1
    # Both links point at the single get-or-created gnomAD variant.
    assert len(session.scalars(select(GnomADVariant)).all()) == 1


def test_links_allele_when_dump_strips_leading_zero_from_caid(session, mocked_gnomad_variant_row):
    """The gnomAD dump records CAIDs without leading zeros (#722). An allele stored with the
    zero-padded CAID must still match the dump's stripped form across the join."""
    allele = _make_allele(session, "CA025094", vrs_digest="vrs-1")
    mocked_gnomad_variant_row.caid = "CA25094"  # dump form: leading zero stripped

    with patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
        result = link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row])
        assert result == {allele.id}
        session.commit()

    assert len(_live_links_for(session, allele.id)) == 1


def test_returns_empty_set_when_no_alleles_match(session, mocked_gnomad_variant_row):
    result = link_gnomad_variants_to_alleles(session, [mocked_gnomad_variant_row])
    assert result == set()
    assert len(session.scalars(select(GnomadAlleleLink)).all()) == 0
    # No gnomAD variant is created when nothing matches the CAID.
    assert len(session.scalars(select(GnomADVariant)).all()) == 0
