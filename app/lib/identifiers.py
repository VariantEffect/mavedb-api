from typing import Union

import metapub
from eutils import EutilsNCBIError
from sqlalchemy.orm import Session

from app.models.doi_identifier import DoiIdentifier
from app.models.ensembl_identifier import EnsemblIdentifier
from app.models.ensembl_offset import EnsemblOffset
from app.models.pubmed_identifier import PubmedIdentifier
from app.models.refseq_identifier import RefseqIdentifier
from app.models.refseq_offset import RefseqOffset
from app.models.target_gene import TargetGene
from app.models.uniprot_identifier import UniprotIdentifier
from app.models.uniprot_offset import UniprotOffset

EXTERNAL_GENE_IDENTIFIER_CLASSES = {
    'Ensembl': EnsemblIdentifier,
    'RefSeq': RefseqIdentifier,
    'UniProt': UniprotIdentifier
}

EXTERNAL_GENE_IDENTIFIER_OFFSET_CLASSES = {
    'Ensembl': EnsemblOffset,
    'RefSeq': RefseqOffset,
    'UniProt': UniprotOffset
}

EXTERNAL_GENE_IDENTIFIER_OFFSET_ATTRIBUTES = {
    'Ensembl': 'ensembl_offset',
    'RefSeq': 'refseq_offset',
    'UniProt': 'uniprot_offset'
}


async def find_or_create_doi_identifier(db: Session, identifier: str):
    doi_identifier = db.query(DoiIdentifier).filter(DoiIdentifier.identifier == identifier).one_or_none()
    if not doi_identifier:
        doi_identifier = DoiIdentifier(
            identifier=identifier,
            db_name='DOI',
            url=f'https://doi.org/{identifier}'
        )
    return doi_identifier


def fetch_pubmed_citation_html(identifier: str):
    fetch = metapub.PubMedFetcher()
    try:
        article = fetch.article_by_pmid(identifier)
    except EutilsNCBIError:
        return f'Unable to retrieve PubMed ID {identifier}'
    else:
        return article.citation_html


async def find_or_create_pubmed_identifier(db: Session, identifier: str):
    pubmed_identifier = db.query(PubmedIdentifier).filter(PubmedIdentifier.identifier == identifier).one_or_none()
    if not pubmed_identifier:
        pubmed_identifier = PubmedIdentifier(
            identifier=identifier,
            db_name='PubMed',
            url=f'http://www.ncbi.nlm.nih.gov/pubmed/{identifier}',
            reference_html=fetch_pubmed_citation_html(identifier)
        )
    return pubmed_identifier


async def find_or_create_external_gene_identifier(db: Session, db_name: str, identifier: str):
    # TODO Handle key errors.
    identifier_class: Union[EnsemblIdentifier, RefseqIdentifier, UniprotIdentifier] = EXTERNAL_GENE_IDENTIFIER_CLASSES[db_name]

    external_gene_identifier = db.query(identifier_class) \
        .filter(identifier_class.identifier == identifier) \
        .one_or_none()

    if not external_gene_identifier:
        external_gene_identifier = identifier_class(
            identifier=identifier,
            db_name=db_name
            # TODO Set URL from identifier
            # url=f'https://doi.org/{identifier}'
        )
    return external_gene_identifier


async def create_external_gene_identifier_offset(db: Session, target_gene: TargetGene, db_name: str, identifier: str,
                                                 offset: int):
    external_gene_identifier = await find_or_create_external_gene_identifier(db, db_name, identifier)

    # TODO Handle key errors.
    offset_class = EXTERNAL_GENE_IDENTIFIER_OFFSET_CLASSES[external_gene_identifier.db_name]
    external_gene_identifier_offset = offset_class(
        identifier=external_gene_identifier,
        offset=offset
    )

    identifier_offset_attribute = EXTERNAL_GENE_IDENTIFIER_OFFSET_ATTRIBUTES[external_gene_identifier.db_name]
    setattr(target_gene, identifier_offset_attribute, external_gene_identifier_offset)
