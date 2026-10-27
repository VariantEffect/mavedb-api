"""
(Extremely) Minimal library for fetching articles from the publication databases MaveDB accepts: PubMed, bioRxiv,
medRxiv and Crossref.
"""

import datetime
import json
import logging
import os
import re
from typing import Any, Optional, Union

import eutils  # type: ignore
import requests
from eutils import EutilsNCBIError, EutilsRequestError  # type: ignore
from eutils._internal.xmlfacades.pubmedarticle import PubmedArticle  # type: ignore
from eutils._internal.xmlfacades.pubmedarticleset import PubmedArticleSet  # type: ignore
from idutils import is_doi
from typing_extensions import TypedDict

from mavedb.lib.exceptions import AmbiguousIdentifierError, PublicationServiceError, PublicationServiceTimeoutError
from mavedb.lib.logging.context import format_raised_exception_info_as_dict, logging_context, save_to_logging_context
from mavedb.lib.validation.publication import validate_db_name

logger = logging.getLogger(__name__)


def _publication_service_error(database: str, exc: Exception) -> PublicationServiceError:
    """The service error to raise for a failed request to a publication database, logged."""
    save_to_logging_context(format_raised_exception_info_as_dict(exc))
    if isinstance(exc, requests.exceptions.Timeout):
        logger.error(msg=f"{database} request timed out.", exc_info=exc, extra=logging_context())
        return PublicationServiceTimeoutError(f"{database} did not respond in time")

    logger.error(msg=f"{database} request failed.", exc_info=exc, extra=logging_context())
    return PublicationServiceError(f"{database} could not provide the publication")


class PublicationAuthors(TypedDict):
    name: str
    primary: bool


class CrossrefObject:
    doi: str
    identifier: str

    def __init__(self, identifier: str) -> None:
        assert is_doi(identifier)
        self.doi = self.identifier = identifier


class CrossrefWork(CrossrefObject):
    title: str
    abstract: Optional[str]
    url: Optional[str]
    authors: list[PublicationAuthors]
    publication_year: Optional[int]
    volume: Optional[str]
    publication_journal: Optional[str]

    def __init__(self, resource: dict[str, Any]) -> None:
        super().__init__(resource["DOI"])

        # title is within a list.
        self.title = resource["title"][0]
        self.url = resource.get("URL")
        self.volume = resource.get("volume")

        # Strip HTML tags from returned abstract text. Strip pre-pended `Abstract` from resulting string.
        self.abstract = resource.get("abstract")
        if self.abstract:
            self.abstract = re.compile(r"(<!--.*?-->|<[^>]*>)").sub("", self.abstract).strip("Abstract")

        # Publication journal is contained within a list field.
        container_title: Optional[list[str]] = resource.get("container-title")
        if container_title:
            # Empty lists are falsy, so we are safe from IndexErrors here.
            self.publication_journal = container_title[0]
        else:
            self.publication_journal = None

        # Publication date is contained within a list of date parts.
        publication_date: Optional[dict[str, list[list[int]]]] = resource.get("published")
        if publication_date:
            try:
                # publication_date = {"date-parts": [[Y, M, D]]}
                self.publication_year = publication_date["date-parts"][0][0]
            # Some publications may only have year and month information.
            except IndexError:
                self.publication_year = None
        else:
            self.publication_year = None

        # Construct internally styled author list from the resources' list of authors.
        authors: list[dict[str, str]] = resource.get("author", [])
        self.authors = [
            {
                "name": f"{author.get('given', '').strip()} {author.get('family', '').strip()}",
                "primary": True if author.get("sequence") == "first" else False,
            }
            for author in authors
        ]


class CrossrefAgency(CrossrefObject):
    id: Optional[str]
    label: Optional[str]

    def __init__(self, resource: dict[str, Any]) -> None:
        super().__init__(resource["DOI"])

        agency: dict = resource.get("agency", {})
        self.id = agency.get("id")
        self.label = agency.get("label")


class RxivPublication:
    """
    Class for generic returned Rxiv publication metadata
    """

    title: str
    preprint_doi: Optional[str]
    published_doi: Optional[str]
    category: Optional[str]
    preprint_date: Optional[datetime.date]
    published_date: Optional[datetime.date]

    def __init__(self, metadata: dict[str, str]) -> None:
        self.title = metadata.get("title", metadata.get("preprint_title", ""))
        self.preprint_doi = metadata.get("preprint_doi")
        self.published_doi = metadata.get("published_doi")
        self.preprint_category = metadata.get("preprint_category")

        preprint_date = metadata.get("preprint_date")
        published_date = metadata.get("published_date")
        self.preprint_date = datetime.datetime.strptime(preprint_date, "%Y-%m-%d") if preprint_date else None
        self.published_date = datetime.datetime.strptime(published_date, "%Y-%m-%d") if published_date else None

    def generate_author_list(self, metadata: dict[str, str]) -> list[PublicationAuthors]:
        authors = [s.strip() for s in metadata.get("authors", "").split(";")]
        return [{"name": author, "primary": idx == 0} for idx, author in enumerate(authors)]


class RxivContentDetail(RxivPublication):
    """
    Class for generic returned Rxiv publication content (non-published)
    """

    authors: list[PublicationAuthors]

    doi: Optional[str]
    author_corresponding: Optional[str]
    author_corresponding_institution: Optional[str]
    date: Optional[datetime.date]
    version: Optional[str]
    type: Optional[str]
    license: Optional[str]
    jatsxml: Optional[str]
    abstract: Optional[str]
    published: Optional[str]
    server: Optional[str]

    def __init__(self, metadata: dict[str, str]) -> None:
        super().__init__(metadata)

        if metadata.get("doi"):
            self.doi = metadata["doi"]
        else:
            self.doi = self.published_doi if self.published_doi else self.preprint_doi

        self.author_corresponding = metadata.get("author_corresponding")
        self.author_corresponding_institution = metadata.get("author_corresponding_institution")
        self.version = metadata.get("version")
        self.type = metadata.get("type")
        self.license = metadata.get("license")
        self.jatsxml = metadata.get("jatsxml")
        self.abstract = metadata.get("abstract")
        self.published = metadata.get("published")
        self.server = metadata.get("server")

        if metadata.get("date"):
            self.date = datetime.datetime.strptime(metadata["date"], "%Y-%m-%d")
        else:
            self.date = self.published_date if self.published_date else self.preprint_date

        self.authors = self.generate_author_list(metadata)


class RxivPublisherDetail(RxivPublication):
    """
    Class for generic returned Rxiv publisher metadata
    """

    published_citation_count: Optional[str]

    def __init__(self, metadata: dict[str, str]) -> None:
        super().__init__(metadata)
        self.published_citation_count = metadata.get("published_citation_count")


class RxivPublicationDetail(RxivPublication):
    """
    Class for generic returned Rxiv article metadata (published)
    """

    authors: list[PublicationAuthors]
    author_corresponding: Optional[str]
    author_corresponding_institution: Optional[str]
    platform: Optional[str]
    abstract: Optional[str]
    published_journal: Optional[str]

    _article_cit_fmt = "{author}. {title}. {journal}. {year}; {volume}:{pages}.{doi}"

    def __init__(self, metadata: dict[str, str]) -> None:
        """
        NOTE: We assume here that the first author in each of these author lists is the primary author
              of a publication. From what I have seen so far from the metapub and biorxiv APIs, this
              is a fine assumption to make, but it doesn't come with future guarantees and this may
              not be the case for certain publications.
        """
        super().__init__(metadata)
        self.author_corresponding = metadata.get("author_corresponding")
        self.author_corresponding_institution = metadata.get("author_corresponding_institution")
        self.platform = metadata.get("platform")
        self.abstract = metadata.get("abstract")
        self.published_journal = metadata.get("published_journal")

        self.authors = self.generate_author_list(metadata)


class RxivStatistics:
    interval: str

    def __init__(self, metadata: dict[str, Union[str, int]]) -> None:
        self.interval = str(metadata.get("month", metadata["year"]))  # interval will be one of these fields


class RxivContentStatistics(RxivStatistics):
    new_papers: int
    new_papers_cumulative: int
    revised_papers: int
    revised_papers_cumulative: int

    def __init__(self, metadata: dict[str, Union[str, int]]) -> None:
        super().__init__(metadata)
        self.new_papers = int(metadata["new_papers"])
        self.new_papers_cumulative = int(metadata["new_papers_cumulative"])
        self.revised_papers = int(metadata["revised_papers_cumulative"])
        self.revised_papers_cumulative = int(metadata["revised_papers_cumulative"])


class RxivUsageStatistics(RxivStatistics):
    abstract_views: int
    full_text_views: int
    pdf_downloads: int
    abstract_cumulative: int
    full_text_cumulative: int
    pdf_cumulative: int

    def __init__(self, metadata: dict[str, Union[str, int]]) -> None:
        super().__init__(metadata)
        self.abstract_views = int(metadata["abstract_views"])
        self.full_text_views = int(metadata["full_text_views"])
        self.pdf_downloads = int(metadata["pdf_downloads"])
        self.abstract_cumulative = int(metadata["abstract_cumulative"])
        self.full_text_cumulative = int(metadata["full_text_cumulative"])
        self.pdf_cumulative = int(metadata["pdf_cumulative"])


class Crossref:
    url = "https://api.crossref.org"
    endpoint: str

    def __init__(self, endpoint) -> None:
        super().__init__()
        self.endpoint = endpoint

    def _fetch(self, url) -> Optional[dict]:
        try:
            result = requests.get(url)

            if result.status_code == 404:
                return None

            result.raise_for_status()
            return result.json()["message"]

        # ValueError covers a body that isn't JSON.
        except (requests.exceptions.RequestException, ValueError) as exc:
            raise _publication_service_error("Crossref", exc) from exc

    def doi(self, identifier: str) -> Optional[CrossrefWork]:
        result = self._fetch(f"{self.url}/{self.endpoint}/{identifier}")
        return CrossrefWork(result) if result else None

    def agency(self, identifier: str) -> Optional[CrossrefAgency]:
        result = self._fetch(f"{self.url}/{self.endpoint}/{identifier}/agency")
        return CrossrefAgency(result) if result else None


class Rxiv:
    """
    Interact with the bioRxiv and medRxiv pre-print servers via their web APIs.

    For more detailed API documentation, see:
    - https://api.biorxiv.org/
    - https://api.medrxiv.org/
    """

    api_url: str
    server: Optional[str]

    def __init__(self, api_url: str, server: Optional[str]) -> None:
        self.api_url = api_url
        self.server = server

    def content_detail(
        self,
        server: Optional[str] = None,
        identifier: Optional[str] = None,
        interval: Optional[tuple[str, str]] = None,
        cursor: int = 0,
        format: str = "json",
    ) -> list[RxivContentDetail]:
        """
        Fetch content detail for an article matching the passed `identifier` or for
        articles that match the provided `interval`. This content has generally
        not yet been published.

        The optional `server` parameter can be used to specify the pre-print
        server on which to search on. If this parameter is not provided, the
        server given in `self.server` will be used. One of these values must
        be set.

        The optional `identifier` parameter can be used to filter content that matches
        the provided identifier.

        The optional `interval` parameter can be used to filter content so that
        it matches the provided interval. Valid intervals are:
        - A tuple containing two YYYY-MM-DD dates. Note that the earlier of the
          two dates should appear first.

        The `cursor` parameter can be used to move the start cursor for the search.
        For instance, passing the number '5' will show matching results starting
        from the 5th piece of content. Default: 0

        The `format` parameter can be used to specify the return format of this
        method. Accepted values are `json` and `xml`.

        NOTE: This API endpoint only supports the searching via either `interval`
              or `identifier`, not both.
        """
        search_server = server if server else self.server
        if not search_server:
            raise ValueError("No server provided with default set as `None`.")

        if (interval) and (identifier):
            raise ValueError(
                "Searching by `interval` or `cursor` and `identifier` is not supported. Please provide one or the other."
            )

        if (not interval) and (not identifier):
            raise ValueError("To search for article details, you must supply either an `identifier` or an `interval`.")

        if identifier:
            return [
                RxivContentDetail(article_detail)
                for article_detail in self._fetch_by_identifier(
                    server=search_server, content="details", identifier=identifier, format=format
                ).get("collection", [])
            ]

        if interval:
            return [
                RxivContentDetail(article_detail)
                for article_detail in self._fetch_by_interval(
                    server=search_server, content="details", interval=interval, cursor=cursor, format=format
                ).get("collection", [])
            ]

        return []

    def article_detail(
        self,
        server: Optional[str] = None,
        identifier: Optional[str] = None,
        interval: Optional[tuple[str, str]] = None,
        cursor: int = 0,
        format: str = "json",
    ) -> list[RxivPublicationDetail]:
        """
        Fetch article detail for an article matching the passed `identifier` or for
        articles that match the provided `interval`. This content has generally
        already been published.

        The optional `server` parameter can be used to specify the pre-print
        server on which to search on. If this parameter is not provided, the
        server given in `self.server` will be used. One of these values must
        be set.

        The optional `identifier` parameter can be used to filter content that matches
        the provided identifier.

        The optional `interval` parameter can be used to filter content so that
        it matches the provided interval. Valid intervals are:
        - A tuple containing two YYYY-MM-DD dates. Note that the earlier of the
          two dates should appear first.

        The `cursor` parameter can be used to move the start cursor for the search.
        For instance, passing the number '5' will show matching results starting
        from the 5th piece of content. Default: 0

        The `format` parameter can be used to specify the return format of this
        method. Accepted values are `json` and `xml`.

        NOTE: This API endpoint only supports the searching via either `interval`
              or `identifier`, not both.
        """
        search_server = server if server else self.server
        if not search_server:
            raise ValueError("No server provided with default set as `None`.")

        if (interval) and (identifier):
            raise ValueError(
                "Searching by `interval` or `cursor` and `identifier` is not supported. Please provide one or the other."
            )

        if (not interval) and (not identifier):
            raise ValueError("To search for article details, you must supply either an`identifier` or an `interval`.")

        if identifier:
            return [
                RxivPublicationDetail(article)
                for article in self._fetch_by_identifier(
                    server=search_server, content="pubs", identifier=identifier, format=format
                ).get("collection", [])
            ]

        if interval:
            return [
                RxivPublicationDetail(article)
                for article in self._fetch_by_interval(
                    server=search_server, content="pubs", interval=interval, cursor=cursor, format=format
                ).get("collection", [])
            ]

        return []

    def article_metadata(
        self,
        interval: tuple[str, str],
        cursor: int = 0,
        format: str = "json",
    ) -> list[RxivPublication]:
        """
        Fetch article detail for a published article matching the passed
        `interval`. This is generally for already published articles.

        The `interval` parameter can be used to filter content so that
        it matches the provided interval. Valid intervals are:
        - A tuple containing two YYYY-MM-DD dates. Note that the earlier of the
          two dates should appear first.

        The `cursor` parameter can be used to move the start cursor for the search.
        For instance, passing the number '5' will show matching results starting
        from the 5th piece of content. Default: 0

        The `format` parameter can be used to specify the return format of this
        method. Accepted values are `json` and `csv`.
        """
        return [
            RxivPublication(article)
            for article in self._fetch_by_interval(
                server=None, content="pub", interval=interval, cursor=cursor, format=format
            ).get("collection", [])
        ]

    def publisher_detail(self, prefix: str, interval: tuple[str, str], cursor: int = 0) -> list[RxivPublisherDetail]:
        """
        Fetch article details for all article matching the passed `prefix` for
        a given publisher. This is for already published articles.

        The `prefix` parameter can be used to specify the publisher prefix, which
        is the publisher prefix string prior to any slash. eg '10.15252'.

        The optional `interval` parameter can be used to filter content so that
        it matches the provided interval. Valid intervals are:
        - A tuple containing two YYYY-MM-DD dates. Note that the earlier of the
          two dates should appear first.

        The `cursor` parameter can be used to move the start cursor for the search.
        For instance, passing the number '5' will show matching results starting
        from the 5th piece of content. Default: 0
        """
        return [
            RxivPublisherDetail(publisher)
            for publisher in self._fetch_by_publisher(
                content="publisher", prefix=prefix, interval=interval, cursor=cursor
            ).get("collection", [])
        ]

    def content_statistics(self, interval: str = "m", format: str = "json") -> list[RxivContentStatistics]:
        """
        Fetch summary statistics for monthly or yearly new and revised paper
        interval and cumulative counts.

        The `interval` parameter can be used to filter content so that
        it matches the provided interval. Valid intervals are:
        - A string matching either 'm' (monthly) or 'y' (yearly).

        The `format` parameter can be used to specify the return format of this
        method. Accepted values are `json` and `csv`.
        """
        return [
            RxivContentStatistics(stats)
            for stats in self._fetch_summary_stats(content="sum", interval=interval, format=format).get(
                "collection", []
            )
        ]

    def usage_statistics(self, interval: str = "m", format: str = "json") -> list[RxivUsageStatistics]:
        """
        Fetch summary statistics for monthly or yearly abstract views, full
        text views, and PDF downloads.

        The `interval` parameter can be used to filter content so that
        it matches the provided interval. Valid intervals are:
        - A string matching either 'm' (monthly) or 'y' (yearly).

        The `format` parameter can be used to specify the return format of this
        method. Accepted values are `json` and `csv`.
        """
        return [
            RxivUsageStatistics(stats)
            for stats in self._fetch_summary_stats(content="usage", interval=interval, format=format).get(
                "collection", []
            )
        ]

    # TODO: support different interval types.
    def _fetch_by_interval(
        self,
        content: str,
        server: Optional[str],
        interval: tuple[str, str],
        cursor: int,
        format: str,
    ) -> Any:
        """
        Fetch the passed content from the provided rxiv server via the provided interval.
        """
        if server:
            return self._fetch(
                f"{self.api_url}/{content}/{server}/{interval[0]}/{interval[1]}/{cursor}/{format}", return_format=format
            )
        else:
            return self._fetch(
                f"{self.api_url}/{content}/{interval[0]}/{interval[1]}/{cursor}/{format}", return_format=format
            )

    def _fetch_by_identifier(self, server: str, content: str, identifier: str, format: str) -> Any:
        """
        Fetch the passed content from the provided Rxiv server via its identifier.
        """
        return self._fetch(f"{self.api_url}/{content}/{server}/10.1101/{identifier}/na/{format}", return_format=format)

    def _fetch_by_publisher(self, content: str, prefix: str, interval: tuple[str, str], cursor: int) -> Any:
        """
        Fetch article details from the base rxiv server via its publisher prefix.
        """
        return self._fetch(f"{self.api_url}/{content}/{prefix}/{interval[0]}/{interval[1]}/{cursor}")

    def _fetch_summary_stats(self, content: str, interval: str, format: str) -> Any:
        """
        Fetch summary statistics from the base rxiv server based on a passed interval.
        """
        return self._fetch(f"{self.api_url}/{content}/{interval}/{format}")

    def _fetch(self, url: str, return_format: str = "json") -> Any:
        """
        Fetch content from the provided url, verify the request was successful,
        and load as JSON if desired.
        """
        try:
            response = requests.get(url)

            if response.status_code == 404:
                return []

            response.raise_for_status()

            return json.loads(response.text) if return_format == "json" else response.text

        # ValueError covers a body that isn't JSON.
        except (requests.exceptions.RequestException, ValueError) as exc:
            raise _publication_service_error(self.server or "Rxiv", exc) from exc


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


def fetch_pubmed_article(identifier: str) -> Optional[PubmedArticle]:
    """
    Fetch an existing PubMed article from NCBI

    :raises PublicationServiceError: PubMed couldn't be reached, or answered with an error.
    """
    fetch = eutils.QueryService(api_key=os.getenv("NCBI_API_KEY"))
    try:
        fetched_articles = list(PubmedArticleSet(fetch.efetch({"db": "pubmed", "id": identifier})))
        assert len(fetched_articles) < 2

    except AssertionError as exc:
        raise AmbiguousIdentifierError(f"Fetched more than 1 PubMed article associated with PMID {identifier}") from exc
    # NCBI answers an unknown PMID with an empty article set, not an error, so any error here means PubMed failed:
    # eutils raises EutilsRequestError for a JSON error body such as a rate limit, and EutilsNCBIError for one it
    # can't parse, such as an outage page.
    except (EutilsRequestError, EutilsNCBIError, requests.exceptions.RequestException) as exc:
        raise _publication_service_error("PubMed", exc) from exc

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
def fetch_biorxiv_article(identifier: str) -> Optional[RxivContentDetail]:
    """
    Fetch an existing bioRxiv article from Rxiv

    :raises PublicationServiceError: bioRxiv couldn't be reached, or answered with an error.
    """
    fetch = Rxiv("https://api.biorxiv.org", "biorxiv")
    articles = fetch.content_detail(identifier=identifier)
    try:
        return articles[-1]
    except IndexError:
        return None


def fetch_medrxiv_article(identifier: str) -> Optional[RxivContentDetail]:
    """
    Fetch an existing medRxiv article from Rxiv

    :raises PublicationServiceError: medRxiv couldn't be reached, or answered with an error.
    """
    fetch = Rxiv("https://api.biorxiv.org", "medrxiv")
    articles = fetch.content_detail(identifier=identifier)
    try:
        return articles[-1]
    except IndexError:
        return None


def fetch_crossref_work(identifier: str) -> Optional[CrossrefWork]:
    """
    Fetch an existing work from Crossref by its DOI

    :raises PublicationServiceError: Crossref couldn't be reached, or answered with an error.
    """
    fetch = Crossref(endpoint="works")
    return fetch.doi(identifier)
