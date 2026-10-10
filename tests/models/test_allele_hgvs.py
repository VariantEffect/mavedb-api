# ruff: noqa: E402
"""``Allele.hgvs`` must mean the same thing on an instance and in SQL."""

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import select

from mavedb.models.allele import Allele

G, C, P = "NC_000017.11:g.1A>G", "NM_000001.1:c.2A>G", "NP_000001.1:p.Ala3Gly"


def test_instance_value_prefers_genomic_then_coding_then_protein():
    assert Allele(hgvs_g=G, hgvs_c=C, hgvs_p=P).hgvs == G
    assert Allele(hgvs_c=C, hgvs_p=P).hgvs == C
    assert Allele(hgvs_p=P).hgvs == P


def test_instance_value_is_none_when_nothing_is_set():
    assert Allele().hgvs is None


@pytest.mark.integration
def test_the_sql_expression_matches_the_allele_at_every_level(session):
    """Regression: a hybrid with no SQL expression compiled ``Allele.hgvs`` to ``hgvs_g`` alone, so a query
    for a coding or protein allele silently matched nothing.
    """
    alleles = {
        G: Allele(vrs_digest="g", level="genomic", hgvs_g=G),
        C: Allele(vrs_digest="c", level="cdna", hgvs_c=C),
        P: Allele(vrs_digest="p", level="protein", hgvs_p=P),
    }
    session.add_all(alleles.values())
    session.commit()

    for hgvs, allele in alleles.items():
        assert session.scalars(select(Allele.id).where(Allele.hgvs == hgvs)).all() == [allele.id]


@pytest.mark.integration
def test_transcript_still_derives_from_the_same_expression(session):
    session.add(Allele(vrs_digest="c", level="cdna", hgvs_c=C))
    session.commit()

    assert session.scalars(select(Allele.vrs_digest).where(Allele.transcript == "NM_000001.1")).all() == ["c"]
