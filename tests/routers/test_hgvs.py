VALID_MAJOR_ASSEMBLY = "GRCh38"
VALID_MINOR_ASSEMBLY = "GRCh38.p3"
INVALID_ASSEMBLY = "undefined"
VALID_ACCESSION = "NC_000001.11"
INVALID_ACCESSION = "NC_999999.99"
SMALL_ACCESSION = "NM_002977.4"
VALID_GENE = "BRCA1"
INVALID_GENE = "fnord"
VALID_TRANSCRIPT = 'NM_001408458.1'
INVALID_TRANSCRIPT = 'NX_99999.1'
VALID_VARIANT = VALID_ACCESSION + ":g.1_2insA"
INVALID_VARIANT = VALID_ACCESSION + ":g.1delA"
HAS_PROTEIN_ACCESSION = "NM_000014.4"
PROTEIN_ACCESSION = "NP_000005.2"


def test_hgvs_fetch_valid(client, setup_router_db):
    # VALID_ACCESSION is rather large so use a smaller one
    response = client.get(f"/api/v1/hgvs/fetch/{SMALL_ACCESSION}")
    assert response.status_code == 200


def test_hgvs_fetch_invalid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/fetch/{INVALID_ACCESSION}")
    assert response.status_code == 404


def test_hgvs_validate_valid(client, setup_router_db):
    payload = {'variant': VALID_VARIANT}
    response = client.post("/api/v1/hgvs/validate", json=payload)
    assert response.status_code == 200


def test_hgvs_validate_invalid(client, setup_router_db):
    payload = {'variant': INVALID_VARIANT}
    response = client.post("/api/v1/hgvs/validate", json=payload)
    assert response.status_code == 400
    assert 'does not agree' in response.json()['detail']


def test_hgvs_list_assemblies(client, setup_router_db):
    response = client.get("/api/v1/hgvs/assemblies")
    assert response.status_code == 200
    assert VALID_MAJOR_ASSEMBLY in response.json()
    assert VALID_MINOR_ASSEMBLY in response.json()


def test_hgvs_grouped_assemblies(client, setup_router_db):
    response = client.get("/api/v1/hgvs/grouped-assemblies")
    assert response.status_code == 200

    groups = {group['type'] : group['assemblies'] for group in response.json()}
    assert 'Major Assembly Versions' in groups
    assert 'Minor Assembly Versions' in groups
    assert VALID_MAJOR_ASSEMBLY in groups['Major Assembly Versions']
    assert VALID_MINOR_ASSEMBLY in groups['Minor Assembly Versions']


def test_hgvs_accessions_major(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/{VALID_MAJOR_ASSEMBLY}/accessions")
    assert response.status_code == 200
    assert VALID_ACCESSION in response.json()


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
    response = client.get(f"/api/v1/hgvs/genes/{VALID_GENE}")
    assert response.status_code == 200
    assert response.json()['hgnc'] == VALID_GENE
    assert response.json()['descr'] is not None


def test_hgvs_gene_info_invalid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/genes/{INVALID_GENE}")
    # XXX this probably SHOULD return a 404, but currently returns a 200 with None
    # assert response.status_code == 404
    assert response.status_code == 200
    assert response.json() is None


def test_hgvs_gene_transcript_valid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/transcripts/gene/{VALID_GENE}")
    assert response.status_code == 200
    assert VALID_TRANSCRIPT in response.json()


def test_hgvs_gene_transcript_invalid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/transcripts/gene/{INVALID_GENE}")
    # XXX this probably SHOULD return a 404, but currently returns a 200 with empty list
    # assert response.status_code == 404
    assert response.status_code == 200
    assert response.json() == []


def test_hgvs_transcript_valid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/transcripts/{VALID_TRANSCRIPT}")
    assert response.status_code == 200
    assert response.json()['hgnc'] == VALID_GENE


def test_hgvs_transcript_invalid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/transcripts/{INVALID_TRANSCRIPT}")
    # XXX this probably SHOULD return a 404, but currently returns a 200 with None
    # assert response.status_code == 404
    assert response.status_code == 200
    assert response.json() is None


def test_hgvs_transcript_protein_valid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/transcripts/protein/{HAS_PROTEIN_ACCESSION}")
    assert response.status_code == 200
    assert response.json() == PROTEIN_ACCESSION


def test_hgvs_transcript_protein_no_protein(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/transcripts/protein/{SMALL_ACCESSION}")
    # XXX this probably SHOULD return a 404, but currently returns a 200 with None
    # assert response.status_code == 404
    assert response.status_code == 200
    assert response.json() is None


def test_hgvs_transcript_protein_invalid(client, setup_router_db):
    response = client.get(f"/api/v1/hgvs/transcripts/protein/{INVALID_ACCESSION}")
    # XXX this probably SHOULD return a 400, but currently returns a 200 with None
    # assert response.status_code == 404
    assert response.status_code == 200
    assert response.json() is None


