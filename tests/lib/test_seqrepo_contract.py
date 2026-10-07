"""Golden tests for the accession -> refget contract shared with dcd_mapping.

A VRS allele digest covers the ``refgetAccession`` of the sequence its location sits on, and the allele
table deduplicates on that digest. The mapper and reverse translation each build alleles, so if they
resolve an accession to different sequences they mint different digests for the same variant and the
copies never merge. That happened: the mapper's fetcher chain lacked hgvs's SeqRepo-backed ``SeqFetcher``,
so cdot returned a transcript assembled from the genome (7207 nt) rather than NCBI's NM_007294.3 record
(7224 nt), and the mapper minted ``SQ.bh0R…`` where reverse translation minted ``SQ.jj1R…``. Nothing failed,
because both digests were internally consistent.

The two repos run different VRS versions and cannot import each other, so the contract is pinned by a
fixture. ``tests/fixtures/canonical_accession_sequences.json`` is a byte-identical copy of the one in
dcd_mapping; it holds NCBI's records and the refget each must have. If either repo drifts, its copy of
these tests fails. Expected refgets are ``sha512t24u(sequence)``, so they are correct by construction.
"""

# ruff: noqa: E402
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("biocommons.seqrepo")

from biocommons.seqrepo import SeqRepo
from ga4gh.core import sha512t24u
from hgvs.dataproviders.seqfetcher import SeqFetcher
from hgvs.exceptions import HGVSDataNotAvailableError

from mavedb.data_providers import services
from mavedb.lib.seqrepo import AmbiguousSequenceError, SequenceNotFoundError, resolve_refget
from mavedb.lib.vrs_utils import AlleleRefgetMismatchError, verify_allele_refget

FIXTURE = Path(__file__).parents[1] / "fixtures" / "canonical_accession_sequences.json"
ACCESSIONS = json.loads(FIXTURE.read_text())["accessions"]


@pytest.fixture
def seqrepo_dir(tmp_path: Path) -> Path:
    """Build a small SeqRepo of only the canonical fixture sequences, stored under ``refseq:<accession>``."""
    root = tmp_path / "seqrepo"
    root.mkdir()
    sr = SeqRepo(str(root), writeable=True)
    for accession, record in ACCESSIONS.items():
        sr.store(record["sequence"], [{"namespace": "refseq", "alias": accession}])
    sr.commit()
    return root


@pytest.fixture
def sr(seqrepo_dir: Path) -> SeqRepo:
    return SeqRepo(str(seqrepo_dir))


@pytest.mark.parametrize("accession", ACCESSIONS)
def test_fixture_refget_is_digest_of_sequence(accession):
    """Guards the fixture itself against a hand-edit that desynchronises sequence and refget."""
    record = ACCESSIONS[accession]
    assert record["expected_refget"] == f"SQ.{sha512t24u(record['sequence'].encode())}"
    assert record["expected_refget"] not in record["non_canonical_refgets"]


@pytest.mark.parametrize("accession", ACCESSIONS)
def test_resolve_refget_is_the_canonical_refget(accession, sr):
    record = ACCESSIONS[accession]
    refget = resolve_refget(sr, accession)
    assert refget == record["expected_refget"]
    assert refget not in record["non_canonical_refgets"]


def test_missing_accession_is_reported(sr):
    with pytest.raises(SequenceNotFoundError):
        resolve_refget(sr, "NM_000000.1")


def test_more_than_one_sequence_is_an_error_not_a_pick():
    sr = MagicMock()
    sr.aliases.find_aliases.return_value = [{"seq_id": "aaa"}, {"seq_id": "bbb"}]
    with pytest.raises(AmbiguousSequenceError):
        resolve_refget(sr, "NM_007294.3")


@pytest.mark.parametrize("accession", ACCESSIONS)
def test_seqfetcher_serves_ncbi_records_ahead_of_the_fasta_fallback(accession, seqrepo_dir, monkeypatch):
    """The chain feeding hgvs validation must resolve accessions from SeqRepo before the genome-assembled
    FASTA fallback. The FASTA fetcher is stubbed to refuse, so a chain without the SeqRepo-backed fetcher in
    front (the original defect in dcd_mapping) cannot serve the sequence.
    """
    monkeypatch.setenv("HGVS_SEQREPO_DIR", str(seqrepo_dir))
    fasta = MagicMock(source="stub fasta")
    fasta.fetch_seq.side_effect = HGVSDataNotAvailableError("no fasta in test")
    with (
        patch.object(services, "FastaSeqFetcher", return_value=fasta),
        patch.object(services, "GENOMIC_FASTA_FILES", ["stub.fna"]),
    ):
        chain = services.seqfetcher()

    assert isinstance(chain.seq_fetchers[0], SeqFetcher)
    sequence = chain.fetch_seq(accession)
    record = ACCESSIONS[accession]
    assert sequence == record["sequence"]
    assert f"SQ.{sha512t24u(sequence.encode())}" == record["expected_refget"]


def _allele(refget: str) -> dict:
    return {
        "type": "Allele",
        "location": {"type": "SequenceLocation", "sequenceReference": {"refgetAccession": refget}},
    }


@pytest.mark.parametrize("accession", ACCESSIONS)
def test_insert_check_accepts_the_canonical_refget(accession, sr):
    refget = ACCESSIONS[accession]["expected_refget"]
    verify_allele_refget(_allele(refget), f"{accession}:c.1A>G", sr, subject="test")


@pytest.mark.parametrize("accession", ACCESSIONS)
def test_insert_check_rejects_a_non_canonical_refget(accession, sr):
    """The original failure: an allele built on a sequence that is not SeqRepo's for its accession."""
    bad = ACCESSIONS[accession]["non_canonical_refgets"][0]
    with pytest.raises(AlleleRefgetMismatchError):
        verify_allele_refget(_allele(bad), f"{accession}:c.1A>G", sr, subject="test")


def test_insert_check_inspects_every_member_of_a_block(sr):
    good = ACCESSIONS["NM_007294.3"]["expected_refget"]
    bad = ACCESSIONS["NM_007294.3"]["non_canonical_refgets"][0]
    block = {"type": "CisPhasedBlock", "members": [_allele(good), _allele(bad)]}
    with pytest.raises(AlleleRefgetMismatchError):
        verify_allele_refget(block, "NM_007294.3:c.[1A>G;5C>T]", sr, subject="test")


def test_insert_check_ignores_accessions_seqrepo_is_not_the_authority_for(sr):
    verify_allele_refget(_allele("SQ.anything"), "target_label:c.1A>G", sr, subject="test")
    verify_allele_refget(_allele("SQ.anything"), "c.1A>G", sr, subject="test")


def test_insert_check_alerts_but_does_not_raise_for_an_unresolvable_accession(sr, caplog):
    with caplog.at_level("ERROR"):
        verify_allele_refget(_allele("SQ.anything"), "NM_000000.1:c.1A>G", sr, subject="test")
    assert "Cannot verify the refget" in caplog.text
