import metapub
from eutils import EutilsNCBIError

from app.models.doi_identifier import DoiIdentifier
from app.models.pubmed_identifier import PubmedIdentifier


async def find_or_create_doi_identifier(db, identifier):
    doi_identifier = db.query(DoiIdentifier).filter(DoiIdentifier.identifier == identifier).one_or_none()
    if not doi_identifier:
        doi_identifier = DoiIdentifier(
            identifier=identifier,
            db_name='DOI',
            url=f'https://doi.org/{identifier}'
        )
    return doi_identifier


def fetch_pubmed_citation_html(identifier):
    fetch = metapub.PubMedFetcher()
    try:
        article = fetch.article_by_pmid(identifier)
    except EutilsNCBIError:
        return f'Unable to retrieve PubMed ID {identifier}'
    else:
        return article.citation_html


async def find_or_create_pubmed_identifier(db, identifier):
    pubmed_identifier = db.query(PubmedIdentifier).filter(PubmedIdentifier.identifier == identifier).one_or_none()
    if not pubmed_identifier:
        pubmed_identifier = PubmedIdentifier(
            identifier=identifier,
            db_name='PubMed',
            url=f'http://www.ncbi.nlm.nih.gov/pubmed/{identifier}',
            reference_html=fetch_pubmed_citation_html(identifier)
        )
    return pubmed_identifier
