from unittest.mock import patch

import cdot.hgvs.dataproviders
import requests_mock
from hgvs.exceptions import HGVSDataNotAvailableError

from tests.helpers.constants import TEST_CDOT_TRANSCRIPT, VALID_ACCESSION, VALID_GENE

VALID_MAJOR_ASSEMBLY = "GRCh38"
VALID_MINOR_ASSEMBLY = "GRCh38.p3"
INVALID_ASSEMBLY = "undefined"
INVALID_ACCESSION = "NC_999999.99"
SMALL_ACCESSION = "NM_002977.4"
INVALID_GENE = "fnord"
VALID_TRANSCRIPT = "NM_001408458.1"
INVALID_TRANSCRIPT = "NX_99999.1"
VALID_VARIANT = VALID_ACCESSION + ":c.1G>A"
INVALID_VARIANT = VALID_ACCESSION + ":c.1delA"
HAS_PROTEIN_ACCESSION = "NM_000014.4"
PROTEIN_ACCESSION = "NP_000005.2"


def test_hgvs_fetch_valid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/fetch/{VALID_ACCESSION}")

    assert response.status_code == 200
    assert response.text == '"GATTACAGATTACAGATTACAGATTACAGATTACAGATTACAGATTACA"'


def test_hgvs_fetch_invalid(client, setup_router_db):
    with patch.object(
        cdot.hgvs.dataproviders.ChainedSeqFetcher, "fetch_seq", side_effect=HGVSDataNotAvailableError()
    ) as p:
        response = client.get(f"/api/v1/hgvs/fetch/{SMALL_ACCESSION}")
        p.assert_called_once()
        assert response.status_code == 404


def test_hgvs_validate_valid(client, setup_router_db):
    with patch.object(cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT):
        payload = {"variant": VALID_VARIANT}
        response = client.post("/api/v1/hgvs/validate", json=payload)
        assert response.status_code == 200


def test_hgvs_validate_invalid(client, setup_router_db):
    with patch.object(cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT):
        payload = {"variant": INVALID_VARIANT}
        response = client.post("/api/v1/hgvs/validate", json=payload)

        assert response.status_code == 400
        assert "does not agree" in response.json()["detail"]


def test_hgvs_list_assemblies(client, setup_router_db):
    response = client.get("/api/v1/hgvs/assemblies")
    assert response.status_code == 200
    assert VALID_MAJOR_ASSEMBLY in response.json()


def test_hgvs_accessions_major(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/{VALID_MAJOR_ASSEMBLY}/accessions")
    assert response.status_code == 200
    assert "NC_000001.11" in response.json()


def test_hgvs_accessions_minor(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/{VALID_MINOR_ASSEMBLY}/accessions")
    assert response.status_code == 404


def test_hgvs_accessions_invalid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/{INVALID_ASSEMBLY}/accessions")
    assert response.status_code == 404


def test_hgvs_genes(client, setup_router_db):
    response = client.get("/api/v1/hgvs/genes")
    assert response.status_code == 200
    assert VALID_GENE in response.json()


def test_hgvs_gene_info_valid(client, setup_router_db):
    with requests_mock.mock() as m:
        m.get(
            f"https://cdot.cc/gene/{VALID_GENE}",
            headers={"Content-Type": "application/json"},
            json={
                "gene_symbol": "BRCA1",
                "aliases": "BRCAI, BRCC1",
                "map_location": "17q21.31",
                "description": "BRCA1 DNA repair associated",
                "summary": "This gene, etc",
            },
        )
        response = client.get(f"/api/v1/hgvs/genes/{VALID_GENE}")

        assert m.called

        assert response.status_code == 200
        assert response.json()["hgnc"] == VALID_GENE
        assert response.json()["descr"] is not None


def test_hgvs_gene_info_invalid(client, setup_router_db):
    with requests_mock.mock() as m:
        m.get(
            "https://cdot.cc/gene/fnord",
            status_code=404,
        )
        response = client.get(f"/api/v1/hgvs/genes/{INVALID_GENE}")

        assert m.called
        assert response.status_code == 404


def test_hgvs_gene_transcript_valid(client, setup_router_db):
    with requests_mock.mock() as m:
        m.get(
            f"https://cdot.cc/transcripts/gene/{VALID_GENE}",
            headers={"Content-Type": "application/json"},
            json={"results": [{"hgnc": f"{VALID_GENE}", "tx_ac": VALID_TRANSCRIPT}]},
        )

        response = client.get(f"/api/v1/hgvs/gene/{VALID_GENE}")
        assert response.status_code == 200
        assert VALID_TRANSCRIPT in response.json()


def test_hgvs_gene_transcript_invalid(client, setup_router_db):
    with requests_mock.mock() as m:
        m.get(f"https://cdot.cc/transcripts/gene/{INVALID_GENE}", status_code=404)

        response = client.get(f"/api/v1/hgvs/gene/{INVALID_GENE}")

        assert m.called
        assert response.status_code == 404


def test_hgvs_transcript_valid(client, setup_router_db):
    with patch.object(cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT):
        response = client.get(f"/api/v1/hgvs/{VALID_TRANSCRIPT}")

    assert response.status_code == 200
    assert response.json()["hgnc"] == VALID_GENE


def test_hgvs_transcript_invalid(client, setup_router_db):
    with requests_mock.mock() as m:
        m.get(f"https://cdot.cc/transcript/{INVALID_TRANSCRIPT}", status_code=404)

        response = client.get(f"/api/v1/hgvs/{INVALID_TRANSCRIPT}")

        assert m.called
        assert response.status_code == 404


def test_hgvs_transcript_protein_valid(client, setup_router_db):
    with requests_mock.mock() as m:
        m.get(
            f"https://cdot.cc/transcript/{HAS_PROTEIN_ACCESSION}",
            headers={"Content-Type": "application/json"},
            json={"biotype": ["protein_coding"], "gene_name": "A2M", "gene_vesion": "2", "protein": "NP_000005.2"},
        )

        response = client.get(f"/api/v1/hgvs/protein/{HAS_PROTEIN_ACCESSION}")

        assert m.called

        assert response.status_code == 200
        assert response.json() == PROTEIN_ACCESSION


def test_hgvs_transcript_protein_no_protein(client, setup_router_db):
    with requests_mock.mock() as m:
        m.get(f"https://cdot.cc/transcript/{SMALL_ACCESSION}", status_code=404)

        response = client.get(f"/api/v1/hgvs/protein/{SMALL_ACCESSION}")

        assert m.called
        assert response.status_code == 404


def test_hgvs_transcript_protein_invalid(client, setup_router_db):
    with requests_mock.mock() as m:
        m.get(f"https://cdot.cc/transcript/{INVALID_ACCESSION}", status_code=404)

        response = client.get(f"/api/v1/hgvs/protein/{INVALID_ACCESSION}")

        assert m.called
        assert response.status_code == 404
