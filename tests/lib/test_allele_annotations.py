# ruff: noqa: E402
"""Integration tests for the digest-keyed annotation assembler (``lib/allele_annotations.py``).

These pin: the map is keyed by ``vrs_digest`` and covers every allele handed in (an allele with no
annotations still gets an empty entry); VEP/gnomAD resolve to one value each while ClinVar is a list
(multi-live, one per release); absence is ``None``/empty; and ``as_of`` reconstructs each source at a
past instant over the immutable, content-addressed alleles.
"""

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.allele_annotations import get_allele_annotations
from mavedb.models.allele import Allele
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence

T0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2021, 1, 1, tzinfo=timezone.utc)


def _allele(session, digest, *, level="cdna"):
    allele = Allele(vrs_digest=digest, level=level, post_mapped={"type": "Allele"})
    session.add(allele)
    session.commit()
    return allele


def _vep(session, allele, value, *, source_version="116", valid_from=None):
    row = VepAlleleConsequence(
        allele_id=allele.id,
        functional_consequence=value,
        source_version=source_version,
        access_date="2026-01-01",
        valid_from=valid_from,
    )
    session.add(row)
    session.commit()
    return row


def _gnomad(session, allele, *, allele_frequency=0.0123, db_version="4.1.0"):
    gv = GnomADVariant(
        db_name="gnomAD",
        db_identifier=f"gnomad-{allele.vrs_digest}",
        db_version=db_version,
        allele_count=42,
        allele_number=10000,
        allele_frequency=allele_frequency,
        faf95_max=0.05,
    )
    session.add(gv)
    session.commit()
    link = GnomadAlleleLink(allele_id=allele.id, gnomad_variant_id=gv.id)
    session.add(link)
    session.commit()
    return gv


def _clinvar(session, allele, *, significance="Likely benign", db_version="11_2024", variation_id="12345"):
    control = ClinvarControl(
        db_identifier=f"cv-{db_version}-{allele.vrs_digest}",
        gene_symbol="PTEN",
        clinical_significance=significance,
        clinical_review_status="criteria provided, multiple submitters, no conflicts",
        db_name="ClinVar",
        db_version=db_version,
        clinvar_variation_id=variation_id,
    )
    session.add(control)
    session.commit()
    link = ClinvarAlleleLink(allele_id=allele.id, clinvar_control_id=control.id)
    session.add(link)
    session.commit()
    return control


@pytest.mark.integration
def test_empty_allele_list_returns_empty_map(session, setup_lib_db):
    assert get_allele_annotations(session, []) == {}


@pytest.mark.integration
def test_allele_without_annotations_gets_an_empty_entry(session, setup_lib_db):
    """The map's keys mirror the alleles handed in — a fully un-annotated allele still gets an
    (empty) block, so the envelope can join every Cat-VRS member to something."""
    allele = _allele(session, "bare-digest")

    annotations = get_allele_annotations(session, [allele])

    assert set(annotations) == {"bare-digest"}
    block = annotations["bare-digest"]
    assert block.vep is None
    assert block.gnomad is None
    assert block.clinvar == []


@pytest.mark.integration
def test_all_three_sources_present(session, setup_lib_db):
    allele = _allele(session, "rich-digest")
    _vep(session, allele, "missense_variant")
    _gnomad(session, allele, allele_frequency=0.0123)
    _clinvar(session, allele, significance="Likely benign")

    block = get_allele_annotations(session, [allele])["rich-digest"]

    assert block.vep is not None and block.vep.consequence == "missense_variant"
    assert block.vep.source_version == "116"
    assert block.gnomad is not None and block.gnomad.allele_frequency == 0.0123
    assert block.gnomad.faf95_max == 0.05
    assert [c.clinical_significance for c in block.clinvar] == ["Likely benign"]


@pytest.mark.integration
def test_clinvar_is_multi_live_across_releases(session, setup_lib_db):
    """ClinVar stacks one live link per release, so an allele can carry several assertions at once."""
    allele = _allele(session, "cv-digest")
    _clinvar(session, allele, significance="Likely benign", db_version="11_2024")
    _clinvar(session, allele, significance="Pathogenic", db_version="05_2025")

    block = get_allele_annotations(session, [allele])["cv-digest"]

    assert {(c.db_version, c.clinical_significance) for c in block.clinvar} == {
        ("11_2024", "Likely benign"),
        ("05_2025", "Pathogenic"),
    }


@pytest.mark.integration
def test_map_is_keyed_by_digest_across_multiple_alleles(session, setup_lib_db):
    a1 = _allele(session, "digest-1", level="cdna")
    a2 = _allele(session, "digest-2", level="protein")
    _vep(session, a1, "missense_variant")
    _gnomad(session, a2)

    annotations = get_allele_annotations(session, [a1, a2])

    assert set(annotations) == {"digest-1", "digest-2"}
    assert annotations["digest-1"].vep is not None and annotations["digest-1"].gnomad is None
    assert annotations["digest-2"].gnomad is not None and annotations["digest-2"].vep is None


@pytest.mark.integration
def test_as_of_reconstructs_the_historical_annotation(session, setup_lib_db):
    """A changed VEP consequence supersedes the old row; as_of selects the value live at the instant."""
    allele = _allele(session, "vep-digest")
    old = _vep(session, allele, "missense_variant", source_version="110", valid_from=T0)
    old.retire(session, at=T1)
    session.commit()
    _vep(session, allele, "stop_gained", source_version="116", valid_from=T1)

    current = get_allele_annotations(session, [allele])["vep-digest"]
    historical = get_allele_annotations(session, [allele], as_of=T1 - timedelta(days=1))["vep-digest"]

    assert current.vep is not None and current.vep.consequence == "stop_gained"
    assert historical.vep is not None and historical.vep.consequence == "missense_variant"
