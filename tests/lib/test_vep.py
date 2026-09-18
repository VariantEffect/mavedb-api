"""Unit tests for mavedb.lib.vep — the Ensembl release lookup, the resolution-delegation adapter, and
the allele linker.

The resolution flow itself — VEP querying, transcript matching, the Recoder fallback, how forms combine
— lives in ``variant_annotation.lib.vep`` and is covered by that library's own tests. This module tests
only what the api owns: that ``resolve_consequences`` delegates to the library over an Ensembl client
and re-keys the result by input HGVS, the release lookup, and the DB linker (run against the DB).
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from variant_annotation.lib.vep import (
    RESOLVER_VERSION,
    ConsequenceOutcome,
    ConsequenceResolution,
    ConsequenceSource,
    VepInput,
)

from mavedb.lib.vep import (
    VepLinkVerdict,
    get_ensembl_release,
    link_vep_consequences_to_alleles,
    resolve_consequences,
)
from mavedb.models.allele import Allele
from mavedb.models.enums.vep import VepConsequenceSource
from mavedb.models.vep_allele_consequence import VepAlleleConsequence


def _resolved(
    hgvs,
    term,
    *,
    source=ConsequenceSource.TRANSCRIPT,
    terms=None,
    matched=None,
) -> ConsequenceResolution:
    """A RESOLVED resolution the linker can persist, defaulting to a transcript-matched single term."""
    return ConsequenceResolution(
        input=VepInput(hgvs=hgvs),
        outcome=ConsequenceOutcome.RESOLVED,
        consequence_terms=list(terms) if terms is not None else [term],
        most_severe_consequence=term,
        source=source,
        matched_transcript=matched,
    )


def _absent(hgvs) -> ConsequenceResolution:
    """A resolution for an input VEP answered with no consequence — a genuine empty."""
    return ConsequenceResolution(input=VepInput(hgvs=hgvs), outcome=ConsequenceOutcome.ABSENT)


### Tests for get_ensembl_release function ###


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_ensembl_release_returns_the_clients_release():
    """The release lookup returns whatever the Ensembl client's software_release reports (it version-keys
    the skip). The /info/software call itself is the client's, covered by the library."""
    with patch("mavedb.lib.vep.EnsemblRestClient.software_release", return_value="116"):
        assert await get_ensembl_release() == "116"


### Tests for resolve_consequences (the delegation adapter) ###


@pytest.mark.asyncio
@pytest.mark.unit
class TestResolveConsequences:
    """resolve_consequences is a thin adapter over the shared library orchestration: it runs the
    library's resolve_consequences over an Ensembl client and re-keys the result by input HGVS. The
    resolution flow itself is the library's, covered by its own tests."""

    async def test_delegates_and_keys_results_by_hgvs(self):
        """The library's list-in-input-order result comes back re-keyed by each input's HGVS."""
        a = VepInput(hgvs="NM_1:c.5G>A")
        b = VepInput(hgvs="NM_1:c.10C>T")
        library_result = [_resolved(a.hgvs, "missense_variant"), _absent(b.hgvs)]
        with patch("mavedb.lib.vep.resolve_consequences_kernel", MagicMock(return_value=library_result)) as kernel:
            result = await resolve_consequences([a, b])

        kernel.assert_called_once()  # delegated to the library, not re-implemented here
        assert set(result) == {a.hgvs, b.hgvs}
        assert result[a.hgvs].outcome is ConsequenceOutcome.RESOLVED
        assert result[b.hgvs].outcome is ConsequenceOutcome.ABSENT

    async def test_empty_input_short_circuits_without_calling_the_library(self):
        """No inputs -> no client built and no library call, just an empty result."""
        with patch("mavedb.lib.vep.resolve_consequences_kernel", MagicMock()) as kernel:
            result = await resolve_consequences([])

        assert result == {}
        kernel.assert_not_called()


### Tests for link_vep_consequences_to_alleles function ###


def _make_allele(session, *, vrs_digest, level="genomic"):
    """Create and persist a deduplicated Allele."""
    allele = Allele(vrs_digest=vrs_digest, level=level)
    session.add(allele)
    session.commit()
    session.refresh(allele)
    return allele


def _live_rows_for(session, allele_id):
    return session.scalars(
        select(VepAlleleConsequence).where(
            VepAlleleConsequence.allele_id == allele_id,
            VepAlleleConsequence.current,
        )
    ).all()


def _all_rows_for(session, allele_id):
    return session.scalars(select(VepAlleleConsequence).where(VepAlleleConsequence.allele_id == allele_id)).all()


def test_link_vep_creates_new_consequence(session):
    """A consequence for an allele with no live row creates a single live row carrying the headline term
    and its full resolution provenance, and is reported changed."""
    allele = _make_allele(session, vrs_digest="vrs-1")

    verdicts = link_vep_consequences_to_alleles(
        session,
        {
            allele.id: _resolved(
                "NM_007294.4:c.5G>A",
                "missense_variant",
                terms=["missense_variant", "splice_region_variant"],
                matched="NM_007294.4",
            )
        },
        source_version="116",
        access_date=date.today(),
    )
    session.commit()

    assert verdicts == {allele.id: VepLinkVerdict.CREATED}
    live = _live_rows_for(session, allele.id)
    assert len(live) == 1
    assert live[0].functional_consequence == "missense_variant"
    assert live[0].source_version == "116"
    assert live[0].access_date == date.today()
    # Full resolution provenance is persisted, not just the headline term (#772).
    assert live[0].consequence_terms == ["missense_variant", "splice_region_variant"]
    assert live[0].consequence_source == VepConsequenceSource.transcript
    assert live[0].matched_transcript == "NM_007294.4"
    # The resolution-rule version is stamped so the skip can tell a rule-fixed row from a stale one.
    assert live[0].resolver_version == RESOLVER_VERSION


def test_link_vep_unchanged_bumps_version_and_fills_provenance_in_place(session):
    """Re-confirming an unchanged headline term at a new release advances source_version/access_date in
    place — no supersede — and fills in the resolution provenance a pre-#772 row was missing. The allele
    is reported UNCHANGED (status preexisting) so the caller need not re-query consequence state."""
    allele = _make_allele(session, vrs_digest="vrs-1")
    session.add(
        VepAlleleConsequence(
            allele_id=allele.id,
            functional_consequence="missense_variant",
            source_version="115",
            access_date=date.today() - timedelta(days=90),
        )
    )
    session.commit()

    verdicts = link_vep_consequences_to_alleles(
        session,
        {
            allele.id: _resolved(
                "NM_1:c.5G>A", "missense_variant", source=ConsequenceSource.TRANSCRIPT, matched="NM_1.4"
            )
        },
        source_version="116",
        access_date=date.today(),
    )
    session.commit()

    assert verdicts == {allele.id: VepLinkVerdict.UNCHANGED}
    # One row, still live, never retired — version, date, and provenance advanced in place.
    all_rows = _all_rows_for(session, allele.id)
    assert len(all_rows) == 1
    assert all_rows[0].valid_to is None
    assert all_rows[0].source_version == "116"
    assert all_rows[0].access_date == date.today()
    assert all_rows[0].consequence_source == VepConsequenceSource.transcript
    assert all_rows[0].matched_transcript == "NM_1.4"
    # A pre-column row (NULL resolver_version) is filled in place, not superseded.
    assert all_rows[0].resolver_version == RESOLVER_VERSION


def test_link_vep_source_change_same_term_advances_in_place(session):
    """A better provenance for the *same* headline term (most_severe -> transcript) is not a value
    change: it advances in place rather than churning a new history row. History records term changes."""
    allele = _make_allele(session, vrs_digest="vrs-1")
    session.add(
        VepAlleleConsequence(
            allele_id=allele.id,
            functional_consequence="missense_variant",
            consequence_source=VepConsequenceSource.most_severe,
            source_version="116",
            access_date=date.today() - timedelta(days=1),
        )
    )
    session.commit()

    verdicts = link_vep_consequences_to_alleles(
        session,
        {
            allele.id: _resolved(
                "NM_1:c.5G>A", "missense_variant", source=ConsequenceSource.TRANSCRIPT, matched="NM_1.4"
            )
        },
        source_version="116",
        access_date=date.today(),
    )
    session.commit()

    assert verdicts == {allele.id: VepLinkVerdict.UNCHANGED}
    rows = _all_rows_for(session, allele.id)
    assert len(rows) == 1  # no supersede
    assert rows[0].consequence_source == VepConsequenceSource.transcript
    assert rows[0].matched_transcript == "NM_1.4"


def test_link_vep_changed_consequence_supersedes(session):
    """A changed headline term retires the live row and inserts the successor — exactly one live row,
    keyed on allele_id, with the old one preserved as retired history."""
    allele = _make_allele(session, vrs_digest="vrs-1")
    session.add(
        VepAlleleConsequence(
            allele_id=allele.id,
            functional_consequence="synonymous_variant",
            source_version="115",
            access_date=date.today() - timedelta(days=90),
        )
    )
    session.commit()

    verdicts = link_vep_consequences_to_alleles(
        session,
        {allele.id: _resolved("NM_1:c.5G>A", "missense_variant")},
        source_version="116",
        access_date=date.today(),
    )
    session.commit()

    assert verdicts == {allele.id: VepLinkVerdict.CREATED}
    live = _live_rows_for(session, allele.id)
    assert len(live) == 1
    assert live[0].functional_consequence == "missense_variant"
    assert live[0].source_version == "116"

    all_rows = _all_rows_for(session, allele.id)
    assert len(all_rows) == 2
    assert len([r for r in all_rows if r.valid_to is not None]) == 1


def test_link_vep_absent_leaves_live_row_untouched(session):
    """A transient absent result must not overwrite a held consequence: the live row is left intact
    (value, version, and date). The held consequence is reported RETAINED_ON_ABSENCE (status
    preexisting) — the allele still has a live consequence, it just was not re-confirmed this run."""
    allele = _make_allele(session, vrs_digest="vrs-1")
    session.add(
        VepAlleleConsequence(
            allele_id=allele.id,
            functional_consequence="missense_variant",
            source_version="115",
            access_date=date.today() - timedelta(days=90),
        )
    )
    session.commit()

    verdicts = link_vep_consequences_to_alleles(
        session, {allele.id: _absent("NM_1:c.5G>A")}, source_version="116", access_date=date.today()
    )
    session.commit()

    # RETAINED_ON_ABSENCE, not UNCHANGED: the prior value was held because VEP found nothing this run,
    # a case the caller surfaces distinctly (job metadata + a loud warning).
    assert verdicts == {allele.id: VepLinkVerdict.RETAINED_ON_ABSENCE}
    live = _live_rows_for(session, allele.id)
    assert len(live) == 1
    assert live[0].functional_consequence == "missense_variant"
    # Not re-confirmed -> neither version nor access_date advanced.
    assert live[0].source_version == "115"
    assert live[0].access_date == date.today() - timedelta(days=90)


def test_link_vep_absent_with_no_live_row_writes_nothing(session):
    """An absent result for an allele with no live row writes nothing and leaves the allele out of the
    verdict map (the caller reads that as a no-result and re-queries next run), mirroring gnomAD's
    no-match handling."""
    allele = _make_allele(session, vrs_digest="vrs-1")

    verdicts = link_vep_consequences_to_alleles(
        session, {allele.id: _absent("NM_1:c.5G>A")}, source_version="116", access_date=date.today()
    )
    session.commit()

    assert verdicts == {}
    assert len(_all_rows_for(session, allele.id)) == 0


def test_link_vep_errored_is_never_linked(session):
    """An ERRORED resolution must never reach the value table (that would overwrite a held consequence
    with a failure). It is ignored defensively and leaves no verdict — failures live in the event stream."""
    allele = _make_allele(session, vrs_digest="vrs-1")

    verdicts = link_vep_consequences_to_alleles(
        session,
        {
            allele.id: ConsequenceResolution(
                input=VepInput(hgvs="NM_1:c.5G>A"), outcome=ConsequenceOutcome.ERRORED, error="boom"
            )
        },
        source_version="116",
        access_date=date.today(),
    )
    session.commit()

    assert verdicts == {}
    assert len(_all_rows_for(session, allele.id)) == 0
