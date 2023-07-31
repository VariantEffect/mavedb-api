from typing import Union

import httpx
import metapub
from eutils import EutilsNCBIError
from fastapi.exceptions import HTTPException
from sqlalchemy.orm import Session

from mavedb.models.doi_identifier import DoiIdentifier
from mavedb.models.ensembl_identifier import EnsemblIdentifier
from mavedb.models.ensembl_offset import EnsemblOffset
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.refseq_identifier import RefseqIdentifier
from mavedb.models.refseq_offset import RefseqOffset
from mavedb.models.target_gene import TargetGene
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.uniprot_identifier import UniprotIdentifier
from mavedb.models.uniprot_offset import UniprotOffset
from mavedb.models.raw_read_identifier import RawReadIdentifier

EXTERNAL_GENE_IDENTIFIER_CLASSES = {
    "Ensembl": EnsemblIdentifier,
    "RefSeq": RefseqIdentifier,
    "UniProt": UniprotIdentifier,
}

EXTERNAL_GENE_IDENTIFIER_OFFSET_CLASSES = {"Ensembl": EnsemblOffset, "RefSeq": RefseqOffset, "UniProt": UniprotOffset}

EXTERNAL_GENE_IDENTIFIER_OFFSET_ATTRIBUTES = {
    "Ensembl": "ensembl_offset",
    "RefSeq": "refseq_offset",
    "UniProt": "uniprot_offset",
}


async def find_or_create_doi_identifier(db: Session, identifier: str):
    """
    Find an existing DOI identifier record with the specified identifier string, or create a new one.

    :param db: An active database session
    :param identifier: A valid DOI identifier
    :return: An existing DoiIdentifier containing the specified identifier string, or a new, unsaved DoiIdentifier
    """
    doi_identifier = db.query(DoiIdentifier).filter(DoiIdentifier.identifier == identifier).one_or_none()
    if not doi_identifier:
        doi_identifier = DoiIdentifier(identifier=identifier, db_name="DOI", url=f"https://doi.org/{identifier}")
    return doi_identifier


def fetch_pubmed_citation_html(identifier: str):
    fetch = metapub.PubMedFetcher()
    try:
        article = fetch.article_by_pmid(identifier)
    except EutilsNCBIError:
        return f"Unable to retrieve PubMed ID {identifier}"
    else:
        return article.citation_html


async def find_or_create_publication_identifier(db: Session, identifier: str):
    """
    Find an existing PubMed identifier record with the specified identifier string, or create a new one.

    :param db: An active database session
    :param identifier: A valid PubMed identifier
    :return: An existing PublicationIdentifier containing the specified identifier string, or a new, unsaved PublicationIdentifier
    """
    publication_identifier = (
        db.query(PublicationIdentifier).filter(PublicationIdentifier.identifier == identifier).one_or_none()
    )
    if not publication_identifier:
        publication_identifier = PublicationIdentifier(
            identifier=identifier,
            db_name="PubMed",
            url=f"http://www.ncbi.nlm.nih.gov/pubmed/{identifier}",
            reference_html=fetch_pubmed_citation_html(identifier),
        )
    return publication_identifier


async def find_or_create_raw_read_identifier(db: Session, identifier: str):
    raw_read_identifier = db.query(RawReadIdentifier).filter(RawReadIdentifier.identifier == identifier).one_or_none()
    if not raw_read_identifier:
        raw_read_identifier = RawReadIdentifier(
            identifier=identifier, db_name="SRA", url=f"http://www.ebi.ac.uk/ena/data/view/{identifier}"
        )
    return raw_read_identifier


async def find_or_create_external_gene_identifier(db: Session, db_name: str, identifier: str):
    """
    Find an existing gene identifier record with the specified gene database name and identifier string, or create a new
    one.

    :param db: An active database session
    :param identifier: A valid identifier
    :return: An existing EnsemblIdentifier, RefseqIdentifier, or UniprotIdentifier containing the specified identifier
      string, or a new, unsaved instance of one of these classes
    """

    # TODO Handle key errors.
    identifier_class: Union[EnsemblIdentifier, RefseqIdentifier, UniprotIdentifier] = EXTERNAL_GENE_IDENTIFIER_CLASSES[
        db_name
    ]

    external_gene_identifier = (
        db.query(identifier_class).filter(identifier_class.identifier == identifier).one_or_none()
    )

    if not external_gene_identifier:
        external_gene_identifier = identifier_class(
            identifier=identifier,
            db_name=db_name
            # TODO Set URL from identifier
            # url=f'https://doi.org/{identifier}'
        )
    return external_gene_identifier


async def create_external_gene_identifier_offset(
    db: Session, target_gene: TargetGene, db_name: str, identifier: str, offset: int
):
    external_gene_identifier = await find_or_create_external_gene_identifier(db, db_name, identifier)

    # TODO Handle key errors.
    offset_class = EXTERNAL_GENE_IDENTIFIER_OFFSET_CLASSES[external_gene_identifier.db_name]
    external_gene_identifier_offset = offset_class(identifier=external_gene_identifier, offset=offset)

    identifier_offset_attribute = EXTERNAL_GENE_IDENTIFIER_OFFSET_ATTRIBUTES[external_gene_identifier.db_name]
    setattr(target_gene, identifier_offset_attribute, external_gene_identifier_offset)

async def find_or_create_taxonomy(db: Session, taxonomy: Taxonomy):
    """
    Find an existing taxonomy ID record with the specified tax_id int, or create a new one.

    :param db: An active database session
    :param tax_id: A valid taxonomy ID
    :return: An existing Taxonomy containing the specified taxonomy ID, or a new, unsaved Taxonomy
    """
    taxonomy_record = db.query(Taxonomy).filter(Taxonomy.tax_id == taxonomy.tax_id).one_or_none()
    if not taxonomy_record:
        url = f"https://api.ncbi.nlm.nih.gov/datasets/v2alpha/taxonomy/taxon/{taxonomy.tax_id}"
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()
                # Process the retrieved data as needed
                ncbi_taxonomy = data['taxonomy_nodes'][0]['taxonomy']
                ncbi_taxonomy.setdefault('organism_name', 'NULL')
                ncbi_taxonomy.setdefault('common_name', 'NULL')
                ncbi_taxonomy.setdefault('rank', 'NULL')
                ncbi_taxonomy.setdefault('has_described_species_name', 'NULL')
                taxonomy_record = Taxonomy(tax_id=ncbi_taxonomy['tax_id'],
                                           organism_name=ncbi_taxonomy['organism_name'],
                                           common_name=ncbi_taxonomy['common_name'],
                                           rank=ncbi_taxonomy['rank'],
                                           has_described_species_name=ncbi_taxonomy['has_described_species_name'],
                                           url="https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id="+str(ncbi_taxonomy['tax_id']),
                                           article_reference="NCBI:txid"+str(ncbi_taxonomy['tax_id']))
                db.add(taxonomy_record)
                db.commit()
                db.refresh(taxonomy_record)
            else:
                raise HTTPException(status_code=404, detail=f"Taxonomy with taxID {taxonomy.tax_id} not found")
    return taxonomy_record