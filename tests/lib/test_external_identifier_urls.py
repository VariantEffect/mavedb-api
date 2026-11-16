import pytest

from mavedb.lib.external_identifier_urls import doi_url, external_gene_identifier_url, raw_read_url


@pytest.mark.unit
def test_doi_url_links_to_the_doi_resolver():
    assert doi_url("10.7554/ELIFE.58026") == "https://doi.org/10.7554/ELIFE.58026"


@pytest.mark.unit
@pytest.mark.parametrize(
    "identifier, expected_url",
    [
        ("PRJNA941815", "https://www.ncbi.nlm.nih.gov/bioproject/PRJNA941815"),
        ("GSE226732", "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE226732"),
        ("E-MTAB-1234", "https://www.ebi.ac.uk/biostudies/arrayexpress/studies/E-MTAB-1234"),
        ("A-AFFY-44", "https://www.ebi.ac.uk/biostudies/arrayexpress/arrays/A-AFFY-44"),
        ("SRP002725", "https://www.ebi.ac.uk/ena/browser/view/SRP002725"),
        ("not-an-accession", "https://www.ebi.ac.uk/ena/browser/view/not-an-accession"),
    ],
    ids=["bioproject", "geo", "arrayexpress-experiment", "arrayexpress-array", "sra", "unrecognized"],
)
def test_raw_read_url_links_by_accession_type(identifier, expected_url):
    assert raw_read_url(identifier) == expected_url


@pytest.mark.unit
@pytest.mark.parametrize(
    "db_name, identifier, expected_url",
    [
        ("Ensembl", "ENSG00000103275", "https://www.ensembl.org/id/ENSG00000103275"),
        ("UniProt", "Q9Y617", "https://www.uniprot.org/uniprotkb/Q9Y617"),
        ("RefSeq", "NP_478059.1", "https://www.ncbi.nlm.nih.gov/protein/NP_478059.1"),
        ("RefSeq", "NM_003352.8", "https://www.ncbi.nlm.nih.gov/nuccore/NM_003352.8"),
    ],
    ids=["ensembl", "uniprot", "refseq-protein", "refseq-nucleotide"],
)
def test_external_gene_identifier_url_links_to_the_database_page(db_name, identifier, expected_url):
    assert external_gene_identifier_url(db_name, identifier) == expected_url
