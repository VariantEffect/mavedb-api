# ruff: noqa: E402
"""Integration tests for the record-scoped allele-link query backing Cat-VRS transit.

``get_live_record_allele_links`` stays within one variant's own live ``MappingRecord`` — unlike
``get_allele_translations``, which takes the cross-record union an anchor allele can belong to. These
tests pin that scoping, the ValidTime live/retired filtering, and the ``as_of`` reconstruction.
"""

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.alleles import get_live_record_allele_links
from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant
from tests.helpers.constants import TEST_MINIMAL_VARIANT

# Deterministic windows far from the transaction clock, so an accidental func.now() stamp is visibly
# wrong rather than coincidentally equal (mirrors tests/db/test_mixins.py).
T0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2021, 1, 1, tzinfo=timezone.utc)
T2 = datetime(2022, 1, 1, tzinfo=timezone.utc)


def _allele(session, digest, *, level="genomic"):
    allele = Allele(vrs_digest=digest, level=level, post_mapped={"type": "Allele"})
    session.add(allele)
    session.commit()
    return allele


def _variant(session, score_set, suffix):
    variant = Variant(**TEST_MINIMAL_VARIANT, urn=f"{score_set.urn}#{suffix}", score_set_id=score_set.id)
    session.add(variant)
    session.commit()
    return variant


def _record(session, variant, *, assay_level="genomic", valid_from=None):
    record = MappingRecord(
        variant_id=variant.id, assay_level=assay_level, mapping_api_version="test.0.0", valid_from=valid_from
    )
    session.add(record)
    session.commit()
    return record


def _link(session, record, allele, *, is_authoritative=False, valid_from=None):
    link = MappingRecordAllele(
        mapping_record_id=record.id, allele_id=allele.id, is_authoritative=is_authoritative, valid_from=valid_from
    )
    session.add(link)
    session.commit()
    return link


def _digests(links):
    return {link.allele.vrs_digest for link in links}


@pytest.mark.integration
def test_returns_live_links_with_alleles(session, setup_lib_db_with_score_set):
    """The variant's live record yields its authoritative + derived links, each allele eagerly loaded."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1)
    record = _record(session, variant)
    authoritative = _allele(session, "auth", level="genomic")
    derived = _allele(session, "deriv", level="protein")
    _link(session, record, authoritative, is_authoritative=True)
    _link(session, record, derived, is_authoritative=False)

    links = get_live_record_allele_links(session, variant.id)

    assert _digests(links) == {"auth", "deriv"}
    # Exactly one authoritative (the defining allele); the eager-loaded allele is reachable.
    assert [link.allele.vrs_digest for link in links if link.is_authoritative] == ["auth"]


@pytest.mark.integration
def test_no_mapping_record_returns_empty(session, setup_lib_db_with_score_set):
    assert get_live_record_allele_links(session, 1) == []


@pytest.mark.integration
def test_excludes_a_retired_link(session, setup_lib_db_with_score_set):
    """A link retired within a still-live record is not part of the current set."""
    variant = _variant(session, setup_lib_db_with_score_set, 1)
    record = _record(session, variant)
    live = _allele(session, "live")
    stale = _allele(session, "stale")
    _link(session, record, live, is_authoritative=True)
    retired = _link(session, record, stale)
    retired.retire(at=T1)
    session.commit()

    assert _digests(get_live_record_allele_links(session, variant.id)) == {"live"}


@pytest.mark.integration
def test_excludes_a_superseded_record(session, setup_lib_db_with_score_set):
    """A re-map retires the prior record (cascading to its links); only the new record's links surface."""
    variant = _variant(session, setup_lib_db_with_score_set, 1)
    old_allele = _allele(session, "old")
    new_allele = _allele(session, "new")

    old_record = _record(session, variant, valid_from=T0)
    _link(session, old_record, old_allele, is_authoritative=True, valid_from=T0)
    old_record.retire(session, at=T1)  # cascades to the link via __retire_cascade__
    session.commit()

    new_record = _record(session, variant, valid_from=T1)
    _link(session, new_record, new_allele, is_authoritative=True, valid_from=T1)

    assert _digests(get_live_record_allele_links(session, variant.id)) == {"new"}


@pytest.mark.integration
def test_record_scoped_not_cross_record_union(session, setup_lib_db_with_score_set):
    """A shared allele linked by two variants' records does NOT pull the other record's members in —
    this is the deliberate difference from get_allele_translations' cross-record union."""
    score_set = setup_lib_db_with_score_set
    shared = _allele(session, "shared")
    only_a = _allele(session, "only_a")
    only_b = _allele(session, "only_b")

    variant_a = _variant(session, score_set, 1)
    record_a = _record(session, variant_a)
    _link(session, record_a, shared, is_authoritative=True)
    _link(session, record_a, only_a)

    variant_b = _variant(session, score_set, 2)
    record_b = _record(session, variant_b)
    _link(session, record_b, shared, is_authoritative=True)
    _link(session, record_b, only_b)

    # Variant A's record sees the shared allele and its own member, never variant B's.
    assert _digests(get_live_record_allele_links(session, variant_a.id)) == {"shared", "only_a"}


@pytest.mark.integration
def test_as_of_returns_the_historical_link_set(session, setup_lib_db_with_score_set):
    """as_of reconstructs the record + links live at a past instant, not the current re-mapped set."""
    variant = _variant(session, setup_lib_db_with_score_set, 1)
    past_allele = _allele(session, "past")
    current_allele = _allele(session, "current")

    old_record = _record(session, variant, valid_from=T0)
    _link(session, old_record, past_allele, is_authoritative=True, valid_from=T0)
    old_record.retire(session, at=T1)
    session.commit()

    new_record = _record(session, variant, valid_from=T1)
    _link(session, new_record, current_allele, is_authoritative=True, valid_from=T1)

    # Inside the old window: the historical set.
    assert _digests(get_live_record_allele_links(session, variant.id, as_of=T0)) == {"past"}
    assert _digests(get_live_record_allele_links(session, variant.id, as_of=T1 - timedelta(days=1))) == {"past"}
    # At/after the handoff and current: the new set.
    assert _digests(get_live_record_allele_links(session, variant.id, as_of=T2)) == {"current"}
    assert _digests(get_live_record_allele_links(session, variant.id)) == {"current"}
