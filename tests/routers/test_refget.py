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
def test_get_metadata_success(client, entry):
    alias = list(entry.keys())[0]
    metadata = list(entry.values())[0]
    namespace, identifier = alias.split(":", 1)
    expected_md5 = identifier if "MD5" in alias else None
    expected_ga4gh = identifier if "ga4gh" in alias else None

    resp = client.get(f"/api/v1/refget/sequence/{alias}/metadata")
    assert resp.status_code == 200
    data = resp.json()["metadata"]
    assert data["id"] == metadata["seq_id"]
    assert data["MD5"] == expected_md5
    assert data["ga4gh"] == expected_ga4gh
    assert data["length"] == len(metadata["seq"])
    assert any([a["naming_authority"] == namespace and a["alias"] == identifier for a in data["aliases"]])


def test_get_metadata_multiple_ids(client):
    # Patch SeqRepo to return multiple IDs for the same alias
    # This simulates a scenario where the alias resolves to multiple sequences
    with patch("mavedb.routers.refget.get_sequence_ids", return_value=["seq1", "seq2"]):
        resp = client.get(f"/api/v1/refget/sequence/{VALID_ENSEMBL_IDENTIFIER}/metadata")
        assert resp.status_code == 422
        assert "Multiple sequences exist" in resp.text


def test_get_metadata_not_found(client):
    resp = client.get("/api/v1/refget/sequence/notfound/metadata")
    assert resp.status_code == 404
    assert "Sequence not found" in resp.text


@pytest.mark.parametrize("entry", TEST_SEQREPO_INITIAL_STATE)
def test_get_sequence_success(client, entry):
    alias = list(entry.keys())[0]
    metadata = list(entry.values())[0]
    resp = client.get(f"/api/v1/refget/sequence/{alias}")
    assert resp.status_code == 200
    assert resp.text == metadata["seq"]


@pytest.mark.parametrize("entry", TEST_SEQREPO_INITIAL_STATE)
def test_get_sequence_with_range_header(client, entry):
    alias = list(entry.keys())[0]
    metadata = list(entry.values())[0]
    start, end = 1, 3
    resp = client.get(f"/api/v1/refget/sequence/{alias}", headers={"Range": f"bytes={start}-{end}"})
    assert resp.status_code == 206
    # Adjusted to take into account that range is inclusive of the end byte in HTTP Range headers.
    assert resp.text == metadata["seq"][start : end + 1]


@pytest.mark.parametrize("entry", TEST_SEQREPO_INITIAL_STATE)
def test_get_sequence_with_range_query(client, entry):
    alias = list(entry.keys())[0]
    metadata = list(entry.values())[0]
    start, end = 1, 3
    resp = client.get(f"/api/v1/refget/sequence/{alias}", params={"start": start, "end": end})
    assert resp.status_code == 200
    # Left-unadjusted to take into account that range is exclusive of the end byte in param requests.
    assert resp.text == metadata["seq"][start:end]


def test_get_sequence_not_found(client):
    resp = client.get("/api/v1/refget/sequence/notfound")
    assert resp.status_code == 404
    assert "Sequence not found" in resp.text


def test_get_sequence_multiple_ids(client):
    # Patch SeqRepo to return multiple IDs for the same alias
    # This simulates a scenario where the alias resolves to multiple sequences
    with patch("mavedb.routers.refget.get_sequence_ids", return_value=["seq1", "seq2"]):
        resp = client.get(f"/api/v1/refget/sequence/{VALID_ENSEMBL_IDENTIFIER}")
        assert resp.status_code == 422
        assert "Multiple sequences exist" in resp.text


def test_get_sequence_invalid_header_range_coords_start_larger_than_end(client):
    resp = client.get(f"/api/v1/refget/sequence/{VALID_ENSEMBL_IDENTIFIER}", headers={"Range": "bytes=12-10"})
    assert resp.status_code == 501
    assert "Invalid coordinates" in resp.text


def test_get_sequence_invalid_header_range_coords_start_too_large(client):
    resp = client.get(f"/api/v1/refget/sequence/{VALID_ENSEMBL_IDENTIFIER}", headers={"Range": "bytes=7-10"})
    assert resp.status_code == 416
    assert "Invalid coordinates" in resp.text
    assert "Content-Range" in resp.headers


def test_get_sequence_invalid_header_range_coords_end_too_large(client):
    resp = client.get(f"/api/v1/refget/sequence/{VALID_ENSEMBL_IDENTIFIER}", headers={"Range": "bytes=1-10"})
    assert resp.status_code == 416
    assert "Invalid coordinates" in resp.text
    assert "Content-Range" in resp.headers


def test_get_sequence_invalid_query_range_coords_start_larger_than_end(client):
    resp = client.get(f"/api/v1/refget/sequence/{VALID_ENSEMBL_IDENTIFIER}", params={"start": 12, "end": 10})
    assert resp.status_code == 501
    assert "Invalid coordinates" in resp.text


def test_get_sequence_invalid_query_range_coords_start_too_large(client):
    resp = client.get(f"/api/v1/refget/sequence/{VALID_ENSEMBL_IDENTIFIER}", params={"start": 7, "end": 10})
    assert resp.status_code == 416
    assert "Invalid coordinates" in resp.text
    assert "Content-Range" in resp.headers


def test_get_sequence_invalid_query_range_coords_end_too_large(client):
    resp = client.get(f"/api/v1/refget/sequence/{VALID_ENSEMBL_IDENTIFIER}", params={"start": 1, "end": 10})
    assert resp.status_code == 416
    assert "Invalid coordinates" in resp.text
    assert "Content-Range" in resp.headers


def test_get_sequence_range_header_invalid(client):
    headers = {"Range": "invalid"}
    resp = client.get(f"/api/v1/refget/sequence/{VALID_ENSEMBL_IDENTIFIER}", headers=headers)
    assert resp.status_code == 400
    assert "Invalid range header format" in resp.text
