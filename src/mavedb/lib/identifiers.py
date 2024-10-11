import os
from typing import Mapping, Optional, Union

import eutils  # type: ignore
from eutils import EutilsNCBIError  # type: ignore
from eutils._internal.xmlfacades.pubmedarticle import PubmedArticle  # type: ignore
from eutils._internal.xmlfacades.pubmedarticleset import PubmedArticleSet  # type: ignore
from idutils import is_doi, normalize_doi
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.exceptions import AmbiguousIdentifierError, NonexistentIdentifierError
from mavedb.lib.external_publications import Crossref, CrossrefWork, PublicationAuthors, Rxiv, RxivContentDetail
from mavedb.lib.validation.publication import identifier_valid_for, infer_identifier_from_url, validate_db_name
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

# XXX these classes all have an "identifier" attribute but there's no superclass
# to unify them ...

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
    db_name: str

    title: str
    authors: list[PublicationAuthors]

    abstract: Optional[str]
    doi: Optional[str]
    publication_year: Optional[int]
    publication_volume: Optional[str]
    publication_pages: Optional[str]
    publication_journal: Optional[str]

    supplied_url: Optional[str] = None

    _article_cit_fmt = "{author}. {title}. {journal}. {year}; {volume}:{pages}. {doi}"

    def __init__(
        self,
        identifier: str,
        db_name: str,
        external_publication: Union[RxivContentDetail, PubmedArticle, CrossrefWork],
    ) -> None:
        validate_db_name(db_name)

        # Required identification fields
        self.identifier = identifier
        self.db_name = db_name

        # Shared fields
        self.title = str(external_publication.title)
        self.abstract = str(external_publication.abstract) if external_publication.abstract else None
        self.doi = str(external_publication.doi) if external_publication.doi else None

        # Non-shared fields
        if isinstance(external_publication, PubmedArticle):
            self.authors = self._infer_author_list(external_publication.authors)
            self.publication_year = int(external_publication.year) if external_publication.year else None
            self.publication_journal = str(external_publication.jrnl) if external_publication.jrnl else None
            self.publication_volume = str(external_publication.volume) if external_publication.volume else None
            self.publication_pages = str(external_publication.pages) if external_publication.pages else None

        elif isinstance(external_publication, RxivContentDetail):
            self.authors = external_publication.authors
            self.publication_year = external_publication.date.year if external_publication.date else None
            self.publication_journal = "Preprint"  # blanket `Preprint` journal for preprint articles
            self.publication_volume = None
            self.publication_pages = None

        elif isinstance(external_publication, CrossrefWork):
            self.authors = external_publication.authors
            self.supplied_url = external_publication.url
            self.publication_year = external_publication.publication_year
            self.publication_journal = external_publication.publication_journal
            self.publication_volume = external_publication.volume
            self.publication_pages = None

    def _infer_author_list(self, authors: list[str]) -> list[PublicationAuthors]:
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
    def primary_author(self) -> str:
        (primary_author,) = (author["name"] for author in self.authors if author["primary"])
        return primary_author

    @property
    def secondary_authors(self) -> list[str]:
        return [author["name"] for author in self.authors if not author["primary"]]

    @property
    def url(self) -> Optional[str]:
        if self.db_name == "PubMed":
            return f"http://www.ncbi.nlm.nih.gov/pubmed/{self.identifier}"
        elif self.db_name == "bioRxiv":
            return f"https://www.biorxiv.org/content/10.1101/{self.identifier}"
        elif self.db_name == "medRxiv":
            return f"https://www.medrxiv.org/content/10.1101/{self.identifier}"
        elif self.db_name == "Crossref":
            return self.supplied_url
        else:
            return None

    @property
    def reference_html(self) -> str:
        """
        Return a well formatted citation HTML string based on article data.
        Intends to return an identical citation html string to metapub.PubMedArticle.
        """
        author = self._format_authors()

        doi_str = "" if not self.doi else self.doi
        title = "(None)" if not self.title else self.title.strip(".")
        journal = (
            "(None)"
            if not self.publication_journal or self.publication_journal == "Preprint"
            else self.publication_journal.strip(".")
        )
        year = "(Unknown year)" if not self.publication_year else self.publication_year
        volume = "(Unknown volume)" if not self.publication_volume else self.publication_volume
        pages = "(Unknown pages)" if not self.publication_pages else self.publication_pages

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


async def fetch_pubmed_article(identifier: str) -> Optional[PubmedArticle]:
    """
    Fetch an existing PubMed article from NCBI
    """
    fetch = eutils.QueryService(api_key=os.getenv("NCBI_API_KEY"))
    try:
        fetched_articles = list(PubmedArticleSet(fetch.efetch({"db": "pubmed", "id": identifier})))
        assert len(fetched_articles) < 2

    except AssertionError as exc:
        raise AmbiguousIdentifierError(f"Fetched more than 1 PubMed article associated with PMID {identifier}") from exc
    except EutilsNCBIError:
        return None

    if fetched_articles:
        return fetched_articles[0]
    else:
        return None


# TODO: Could search on article_detail -> content_detail to try and get the richer
#       metadata that exists on already published articles. leaving for now since
#       the main purpose of these changes is to support preprints, which we now do,
#       and adding any additional API calls would slow this fetch process down.
#       - capodb 2023.06.06
#
# NOTE: The most up to date version of a preprint will be the last element in the
#       content detail list.
async def fetch_biorxiv_article(identifier: str) -> Optional[RxivContentDetail]:
    """
    Fetch an existing bioRxiv article from Rxiv
    """
    fetch = Rxiv("https://api.biorxiv.org", "biorxiv")
    articles = fetch.content_detail(identifier=identifier)
    try:
        return articles[-1]
    except IndexError:
        return None


async def fetch_medrxiv_article(identifier: str) -> Optional[RxivContentDetail]:
    """
    Fetch an existing medRxiv article from Rxiv
    """
    fetch = Rxiv("https://api.biorxiv.org", "medrxiv")
    articles = fetch.content_detail(identifier=identifier)
    try:
        return articles[-1]
    except IndexError:
        return None


async def fetch_crossref_work(identifier: str) -> Optional[CrossrefWork]:
    fetch = Crossref(endpoint="works")
    return fetch.doi(identifier)


async def find_generic_article(
    db: Session, identifier: str, db_name: Optional[str] = None
) -> Mapping[str, Union[PublicationIdentifier, ExternalPublication, None]]:
    """
    Check if a provided publication identifier ambiguously identifies a publication,
    ie the same identifier is identifies publications in multiple publication databases
    that we accept.

    :param db: An active database session
    :param identifier: A valid publication identifier
    :return: A list of databases where this identifier exists.
    """
    db_specific_fetches = {
        "Crossref": fetch_crossref_work,
        "PubMed": fetch_pubmed_article,
        "bioRxiv": fetch_biorxiv_article,
        "medRxiv": fetch_medrxiv_article,
    }

    # We also accept URLs from our accepted publications. Attempt to convert a potential URL to an identifier.
    identifier = infer_identifier_from_url(identifier)

    # Only check entries with the appropriate `db_name` if one is provided.
    db_specific_match: dict[str, Union[PublicationIdentifier, ExternalPublication, None]] = {}
    if db_name:
        if db_name == "Crossref":
            internal_publication_query = select(PublicationIdentifier).filter(PublicationIdentifier.doi == identifier)
        else:
            internal_publication_query = select(PublicationIdentifier).filter(
                PublicationIdentifier.identifier == identifier
            )

        existing_publication = db.execute(
            internal_publication_query.filter(PublicationIdentifier.db_name == db_name)
        ).scalar_one_or_none()

        if not existing_publication:
            external_publication = await db_specific_fetches[db_name](identifier)
            db_specific_match = {
                db_name: (
                    ExternalPublication(identifier, db_name, external_publication) if external_publication else None
                )
            }
        else:
            db_specific_match = {db_name: existing_publication}

        return db_specific_match

    # If the identifier is a DOI, it will necessarily have a unique match in Crossref (if such a match exists).
    # Return this match directly where possible.
    found_articles: dict[str, Union[PublicationIdentifier, ExternalPublication, None]] = {}
    if is_doi(identifier):
        identifier = normalize_doi(identifier)

        existing_publication = db.execute(
            select(PublicationIdentifier).filter(PublicationIdentifier.doi == identifier)
        ).scalar_one_or_none()

        if not existing_publication:
            external_publication = await fetch_crossref_work(identifier)
            found_articles["Crossref"] = (
                ExternalPublication(identifier, "Crossref", external_publication) if external_publication else None
            )
        else:
            # When we find an existing publication via DOI, it is not always the case that it came from Crossref originally.
            # Use the existing publication db as the article key if it exists, otherwise default to Crossref since this is a DOI.
            existing_db_name = existing_publication.db_name if existing_publication.db_name else "Crossref"
            found_articles[existing_db_name] = existing_publication

        return found_articles

    # When we are not provided a db name, we must try to match the provided identifier to a
    # publication from each one of our accepted databases.
    for publication_db, identifier_valid in identifier_valid_for(identifier).items():
        if identifier_valid:
            existing_publication = db.execute(
                select(PublicationIdentifier)
                .filter(PublicationIdentifier.identifier == identifier)
                .filter(PublicationIdentifier.db_name == publication_db)
            ).scalar_one_or_none()

            if not existing_publication:
                external_publication = await db_specific_fetches[publication_db](identifier)
                found_articles[publication_db] = (
                    ExternalPublication(identifier, publication_db, external_publication)
                    if external_publication
                    else None
                )
            else:
                found_articles[publication_db] = existing_publication

    return found_articles


def create_generic_article(article: ExternalPublication) -> PublicationIdentifier:
    """
    Create a new (unsaved) publication identifier object based on the provided identifier, article metadata,
    and publication database name.
    """
    return PublicationIdentifier(
        identifier=article.identifier,
        db_name=article.db_name,
        url=article.url,
        title=article.title,
        abstract=article.abstract,
        authors=article.authors,
        publication_year=article.publication_year,
        doi=article.doi,
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
    article_matches = await find_generic_article(db, identifier, db_name)

    if not any(article_matches.values()):
        raise NonexistentIdentifierError(
            f"No matching articles found for identifier {identifier} across all accepted publication databases."
        )

    if sum(article is not None for article in article_matches.values()) > 1:
        raise AmbiguousIdentifierError(
            f"Found multiple articles associated with identifier {identifier}. Specify a `db_name` along with this identifier to avoid ambiguity."
        )

    matched_article = next(article for article in article_matches.values() if article is not None)

    # If the article already exists, return it directly.
    if isinstance(matched_article, PublicationIdentifier):
        return matched_article

    # TODO(#214): It may be useful for internal consistency to use the Crossref record fetched via DOI if it exists. If a publication did not
    #             have a DOI, we would need to use the record as returned by PubMed/bioRxiv/medRxiv.

    return create_generic_article(matched_article)


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
    identifier_class = EXTERNAL_GENE_IDENTIFIER_CLASSES[db_name]
    assert hasattr(identifier_class, "identifier")

    external_gene_identifier = (
        db.query(identifier_class).filter(identifier_class.identifier == identifier).one_or_none()
    )

    if not external_gene_identifier:
        external_gene_identifier = identifier_class(
            identifier=identifier,
            db_name=db_name,
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
