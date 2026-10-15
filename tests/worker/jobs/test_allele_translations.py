# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from datetime import datetime, timezone

from mavedb.lib.alleles import get_allele_translations


@pytest.mark.unit
class TestGetAlleleTranslations:
    """The cross-layer equivalence query traverses the MappingRecordAllele link graph.

    Uses the RT-derived fixture, which links an authoritative allele and a derived (cross-layer)
    allele to one MappingRecord — exactly the co-membership the query resolves.
    """

    def test_returns_full_equivalence_set_from_any_member(self, session, setup_rt_derived_allele_with_caid):
        _variant, authoritative_allele, rt_allele = setup_rt_derived_allele_with_caid

        from_authoritative = {a.id for a in get_allele_translations(session, authoritative_allele.id)}
        from_rt = {a.id for a in get_allele_translations(session, rt_allele.id)}

        # Reachable from either member, and includes the anchor itself — the full equivalence set.
        assert from_authoritative == {authoritative_allele.id, rt_allele.id}
        assert from_rt == {authoritative_allele.id, rt_allele.id}

    def test_as_of_before_links_existed_is_empty(self, session, setup_rt_derived_allele_with_caid):
        _variant, authoritative_allele, _rt = setup_rt_derived_allele_with_caid

        past = datetime(2000, 1, 1, tzinfo=timezone.utc)
        assert get_allele_translations(session, authoritative_allele.id, as_of=past) == []

    def test_as_of_after_links_existed_returns_set(self, session, setup_rt_derived_allele_with_caid):
        _variant, authoritative_allele, rt_allele = setup_rt_derived_allele_with_caid

        future = datetime(2999, 1, 1, tzinfo=timezone.utc)
        ids = {a.id for a in get_allele_translations(session, authoritative_allele.id, as_of=future)}
        assert ids == {authoritative_allele.id, rt_allele.id}
