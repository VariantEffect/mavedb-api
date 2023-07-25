import os
from datetime import date
from typing import Optional, Union

import eutils
from eutils import EutilsNCBIError
from eutils._internal.xmlfacades.pubmedarticle import PubmedArticle
from eutils._internal.xmlfacades.pubmedarticleset import PubmedArticleSet
from sqlalchemy.orm import Session

from mavedb.lib.exceptions import AmbiguousIdentifierError, NonexistentIdentifierError
from mavedb.lib.rxiv import Rxiv, RxivContentDetail
from mavedb.lib.validation.publication import identifier_valid_for, validate_db_name
from mavedb.models.doi_identifier import DoiIdentifier
from mavedb.models.ensembl_identifier import EnsemblIdentifier
from mavedb.models.ensembl_offset import EnsemblOffset
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.raw_read_identifier import RawReadIdentifier
from mavedb.models.refseq_identifier import RefseqIdentifier
from mavedb.models.refseq_offset import RefseqOffset
from mavedb.models.target_gene import TargetGene
from mavedb.models.uniprot_identifier import UniprotIdentifier
from mavedb.models.uniprot_offset import UniprotOffset

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


class ExternalPublication:
    """
    Class for a generic External Publication object, which may be a response
    from the public APIs of any accepted publication db.
    """

    identifier: str
    title: str
    abstract: str
    authors: list[dict[str, Union[str, bool]]]
    publication_year: int
    publication_volume: Optional[str]
    publication_pages: Optional[str]
    publication_doi: Optional[str]
    publication_journal: Optional[str]
    preprint_doi: Optional[str]
    preprint_date: Optional[date]
    db_name: str

    _article_cit_fmt = "{author}. {title}. {journal}. {year}; {volume}:{pages}. {doi}"

    def __init__(
        self,
        identifier: str,
        db_name: str,
        external_publication: Union[RxivContentDetail, PubmedArticle],
    ) -> None:
        """
        NOTE: We assume here that the first author in each of these author lists is the primary author
              of a publication. From what I have seen so far from the metapub and biorxiv APIs, this
              is a fine assumption to make, but it doesn't come with future guarantees and this may
              not be the case for certain publications.
        """
        validate_db_name(db_name)

        # Shared fields
        self.identifier = identifier
        self.db_name = db_name
        self.title = str(external_publication.title)
        self.abstract = str(external_publication.abstract)
        self.authors = self._generate_author_list(external_publication.authors)

        # Non-shared fields
        if isinstance(external_publication, PubmedArticle):
            self.publication_year = int(external_publication.year)
            self.publication_journal = external_publication.jrnl
            self.publication_doi = external_publication.doi
            self.publication_volume = external_publication.volume
            self.publication_pages = external_publication.pages
        elif isinstance(external_publication, RxivContentDetail):
            self.preprint_doi = external_publication.doi
            self.preprint_date = external_publication.date

    def _generate_author_list(self, authors: list[str]) -> list[dict[str, Union[str, bool]]]:
        """
        Generates a list of author names and thier authorship level associated with this publication.
        """
        return [{"name": author, "primary": idx == 0} for idx, author in enumerate(authors)]

    def _format_authors(self) -> str:
        """Helper function for returning a well formatted HTML author list"""
        if self.authors and len(self.authors) > 2:
            author = str(self.authors[0]["name"]) + ", <i>et al</i>"
        elif self.authors and len(self.authors) == 2:
            author = " and ".join([str(author["name"]) for author in self.authors])
        elif self.authors and len(self.authors) < 2:
            author = str(self.authors[0]["name"])
        else:
            author = ""

        return author

    @property
    def first_author(self) -> str:
        return str(self.authors[0]["name"])

    @property
    def secondary_authors(self) -> list[str]:
        if len(self.authors) > 1:
            return [str(author["name"]) for author in self.authors[1:]]
        else:
            return []

    @property
    def url(self) -> str:
        if self.db_name == "PubMed":
            return f"http://www.ncbi.nlm.nih.gov/pubmed/{self.identifier}"
        elif self.db_name == "bioRxiv":
            return f"https://www.biorxiv.org/content/10.1101/{self.identifier}"
        elif self.db_name == "medRxiv":
            return f"https://www.medrxiv.org/content/10.1101/{self.identifier}"
        else:
            return ""

    @property
    def reference_html(self) -> str:
        """
        Return a well formatted citation HTML string based on article data.
        Intends to return an identical citation html string to metapub.PubMedArticle.
        """
        author = self._format_authors()

        if self.db_name in ["PubMed"]:
            doi_str = "" if not self.publication_doi else self.publication_doi
            title = "(None)" if not self.title else self.title.strip(".")
            journal = "(None)" if not self.publication_journal else self.publication_journal.strip(".")
            year = "(Unknown year)" if not self.publication_year else self.publication_year
            volume = "(Unknown volume)" if not self.publication_volume else self.publication_volume
            pages = "(Unknown pages)" if not self.publication_pages else self.publication_pages
        else:
            doi_str = "" if not self.preprint_doi else self.preprint_doi
            title = "(None)" if not self.title else self.title.strip(".")
            journal = "(None)" if not self.publication_journal else self.publication_journal.strip(".")
            year = "(Unknown year)" if not self.preprint_date else self.preprint_date.year

            # We don't receive these fields from rxiv platforms
            volume = "(Unknown volume)"
            pages = "(Unknown pages)"

        return self._article_cit_fmt.format(
            author=author, volume=volume, pages=pages, year=year, title=title, journal=journal, doi=doi_str
        )


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


async def fetch_pubmed_article(identifier: str) -> Optional[ExternalPublication]:
    """
    Fetch an existing PubMed article from NCBI
    """
    fetch = eutils.QueryService(api_key=os.getenv("NCBI_API_KEY"))
    try:
        fetched_articles = list(PubmedArticleSet(fetch.efetch({"db": "pubmed", "id": identifier})))
        assert len(fetched_articles) < 2
        article = ExternalPublication(identifier=identifier, db_name="PubMed", external_publication=fetched_articles[0])

    except AssertionError as exc:
        raise AmbiguousIdentifierError(f"Fetched more than 1 PubMed article associated with PMID {identifier}") from exc
    except EutilsNCBIError:
        return None
    except IndexError:
        return None
    else:
        return article


# TODO: Could search on article_detail -> content_detail to try and get the richer
#       metadata that exists on already published articles. leaving for now since
#       the main purpose of these changes is to support preprints, which we now do,
#       and adding any additional API calls would slow this fetch process down.
#       - capodb 2023.06.06
#
# NOTE: The most up to date version of a preprint will be the last element in the
#       content detail list.
async def fetch_biorxiv_article(identifier: str) -> Optional[ExternalPublication]:
    """
    Fetch an existing bioRxiv article from Rxiv
    """
    fetch = Rxiv("https://api.biorxiv.org", "biorxiv")
    try:
        article = fetch.content_detail(identifier=identifier)
        article = ExternalPublication(identifier=identifier, db_name="bioRxiv", external_publication=article[-1])
    except IndexError:
        return None
    else:
        return article


async def fetch_medrxiv_article(identifier: str) -> Optional[ExternalPublication]:
    """
    Fetch an existing medRxiv article from Rxiv
    """
    fetch = Rxiv("https://api.biorxiv.org", "medrxiv")
    try:
        article = fetch.content_detail(identifier=identifier)
        article = ExternalPublication(identifier=identifier, db_name="medRxiv", external_publication=article[-1])
    except IndexError:
        return None
    else:
        return article


async def find_generic_article(
    db: Session, identifier: str
) -> dict[str, Union[ExternalPublication, PublicationIdentifier]]:
    """
    Check if a provided publication identifier ambiguously identifies a publication,
    ie the same identifier is identifies publications in multiple publication databases
    that we accept.

    :param db: An active database session
    :param identifier: A valid publication identifier
    :return: A list of databases where this identifier exists.
    """
    valid_databases = identifier_valid_for(identifier)
    matching_articles = {}

    if valid_databases["PubMed"]:
        pubmed_pub = (
            db.query(PublicationIdentifier)
            .filter(PublicationIdentifier.identifier == identifier, PublicationIdentifier.db_name == "PubMed")
            .one_or_none()
        )
        if not pubmed_pub:
            pubmed_pub = await fetch_pubmed_article(identifier)

        if pubmed_pub:
            matching_articles["PubMed"] = pubmed_pub

    if valid_databases["bioRxiv"]:
        biorxiv_pub = (
            db.query(PublicationIdentifier)
            .filter(PublicationIdentifier.identifier == identifier, PublicationIdentifier.db_name == "bioRxiv")
            .one_or_none()
        )

        if not biorxiv_pub:
            biorxiv_pub = await fetch_biorxiv_article(identifier)

        if biorxiv_pub:
            matching_articles["bioRxiv"] = biorxiv_pub

    if valid_databases["medRxiv"]:
        medrxiv_pub = (
            db.query(PublicationIdentifier)
            .filter(PublicationIdentifier.identifier == identifier, PublicationIdentifier.db_name == "medRxiv")
            .one_or_none()
        )
        if not medrxiv_pub:
            medrxiv_pub = await fetch_medrxiv_article(identifier)

        if medrxiv_pub:
            matching_articles["medRxiv"] = medrxiv_pub

    return matching_articles


def create_generic_article(article: ExternalPublication) -> PublicationIdentifier:
    """
    Create a new publication identifier object based on the provided identifier, article metadata,
    and publication database name.
    """
    if article.db_name in ["bioRxiv", "medRxiv"]:
        return PublicationIdentifier(
            identifier=article.identifier,
            db_name=article.db_name,
            url=article.url,
            title=article.title,
            abstract=article.abstract,
            authors=article.authors,
            preprint_date=article.preprint_date,
            preprint_doi=article.preprint_doi,
            publication_journal="Preprint",  # blanket `Preprint` journal for preprint articles
            reference_html=article.reference_html,
        )
    else:
        return PublicationIdentifier(
            identifier=article.identifier,
            db_name=article.db_name,
            url=article.url,
            title=article.title,
            abstract=article.abstract,
            authors=article.authors,
            publication_doi=article.publication_doi,
            publication_year=article.publication_year,
            publication_journal=article.publication_journal,
            reference_html=article.reference_html,
        )


async def find_or_create_publication_identifier(
    db: Session, identifier: str, db_name: Optional[str] = None
) -> PublicationIdentifier:
    """
    Find an existing publication identifier record with the specified identifier string, or create a new one.

    :param db: An active database session
    :param identifier: A valid publication identifier
    :return: An existing PublicationIdentifier containing the specified identifier string, or a new, unsaved PublicationIdentifier
    """
    matching_articles = await find_generic_article(db, identifier)

    if not matching_articles:
        raise NonexistentIdentifierError(
            f"No matching articles found for identifier {identifier} across all accepted publication databases."
        )

    # If we aren't provided with a specific DB name, infer the desired article based on those that match
    if not db_name:
        if len(matching_articles.keys()) > 1:
            raise AmbiguousIdentifierError(
                f"Found multiple articles associated with identifier {identifier}. Specify a `db_name` along with this identifier to avoid ambiguity."
            )

        # Return the article directly if it is an existing publication in our db
        db_name, article = list(matching_articles.items())[0]
        if isinstance(article, PublicationIdentifier):
            return article
        else:
            return create_generic_article(article)

    article = matching_articles.get(db_name)
    if article:
        if isinstance(article, PublicationIdentifier):
            return article
        else:
            return create_generic_article(article)
    else:
        raise NonexistentIdentifierError(
            f"Could not find any articles matching identifier {identifier} in database {db_name}."
        )


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
