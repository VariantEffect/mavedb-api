# ruff: noqa: E402
import pytest
from unittest.mock import patch

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")
biocommons = pytest.importorskip("biocommons")
bioutils = pytest.importorskip("bioutils")

from tests.helpers.constants import TEST_SEQREPO_INITIAL_STATE, VALID_ENSEMBL_IDENTIFIER


@pytest.mark.parametrize("entry", TEST_SEQREPO_INITIAL_STATE)
def test_get_sequence_success(client, entry):
    alias = list(entry.keys())[0]
    metadata = list(entry.values())[0]
    resp = client.get(f"/api/v1/seqrepo/sequence/{alias}")
    assert resp.status_code == 200
    assert resp.text == metadata["seq"]


@pytest.mark.parametrize("entry", TEST_SEQREPO_INITIAL_STATE)
def test_get_sequence_with_range(client, entry):
    alias = list(entry.keys())[0]
    metadata = list(entry.values())[0]
    start, end = 1, 3
    resp = client.get(f"/api/v1/seqrepo/sequence/{alias}?start={start}&end={end}")
    assert resp.status_code == 200
    assert resp.text == metadata["seq"][start:end]


def test_get_sequence_not_found(client):
    resp = client.get("/api/v1/seqrepo/sequence/notfound")
    assert resp.status_code == 404
    assert "Sequence not found" in resp.text


def test_get_sequence_multiple_ids(client):
    # Patch SeqRepo to return multiple IDs for the same alias
    # This simulates a scenario where the alias resolves to multiple sequences
    with patch("mavedb.routers.seqrepo.get_sequence_ids", return_value=["seq1", "seq2"]):
        resp = client.get(f"/api/v1/seqrepo/sequence/{VALID_ENSEMBL_IDENTIFIER}")
        assert resp.status_code == 422
        assert "Multiple sequences exist" in resp.text


def test_get_sequence_invalid_coords(client):
    resp = client.get(f"/api/v1/seqrepo/sequence/{VALID_ENSEMBL_IDENTIFIER}?start=10&end=5")
    assert resp.status_code == 422
    assert "Invalid coordinates" in resp.text


@pytest.mark.parametrize("entry", TEST_SEQREPO_INITIAL_STATE)
def test_get_metadata_success(client, entry):
    alias = list(entry.keys())[0]
    metadata = list(entry.values())[0]
    resp = client.get(f"/api/v1/seqrepo/metadata/{alias}")
    assert resp.status_code == 200
    data = resp.json()
    assert "added" in data
    # The alphabet is the unique characters in the sequence
    assert data["alphabet"] == "".join(sorted(set(metadata["seq"])))
    assert data["length"] == len(metadata["seq"])
    assert alias in data["aliases"]


def test_get_metadata_not_found(client):
    resp = client.get("/api/v1/seqrepo/metadata/notfound")
    assert resp.status_code == 404
    assert "Sequence not found" in resp.text


def test_get_metadata_multiple_ids(client):
    # Patch SeqRepo to return multiple IDs for the same alias
    # This simulates a scenario where the alias resolves to multiple sequences
    with patch("mavedb.routers.seqrepo.get_sequence_ids", return_value=["seq1", "seq2"]):
        resp = client.get(f"/api/v1/seqrepo/metadata/{VALID_ENSEMBL_IDENTIFIER}")
        assert resp.status_code == 422
        assert "Multiple sequences exist" in resp.text


def test_get_versions(client):
    resp = client.get("/api/v1/seqrepo/version")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)
