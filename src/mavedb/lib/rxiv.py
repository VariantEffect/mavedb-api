"""
Minimal library for interacting with bioRxiv and medRxiv pre-print servers
"""

import datetime
import json
import logging
from typing import Any, Optional, Union

import requests

logger = logging.getLogger(__name__)


class RxivContentDetail:
    """
    Class for generic returned Rxiv publication content (non-published)
    """

    title: str
    doi: str
    category: str
    authors: list[str]
    author_corresponding: str
    author_corresponding_institution: str
    date: datetime.date
    version: str
    type: str
    license: str
    jatsxml: str
    abstract: str
    published: str
    server: Optional[str]  # not guaranteed

    def __init__(self, metadata: dict[str, str]) -> None:
        self.title = metadata["title"]
        self.doi = metadata["doi"]
        self.category = metadata["category"]
        self.authors = [s.strip() for s in metadata.get("authors", "").split(";")]
        self.author_corresponding = metadata["author_corresponding"]
        self.author_corresponding_institution = metadata["author_corresponding_institution"]
        self.date = datetime.datetime.strptime(metadata["date"], "%Y-%m-%d")
        self.version = metadata["version"]
        self.type = metadata["type"]
        self.license = metadata["license"]
        self.jatsxml = metadata["jatsxml"]
        self.abstract = metadata["abstract"]
        self.published = metadata["published"]
        self.server = metadata.get("server")

    @property
    def first_author(self) -> Optional[str]:
        if len(self.authors) > 0:
            return self.authors[0]
        else:
            return None


class RxivPublication:
    """
    Class for generic returned Rxiv publication metadata
    """

    preprint_title: str
    preprint_doi: str
    published_doi: str
    preprint_category: str
    preprint_date: datetime.date
    published_date: datetime.date

    def __init__(self, metadata: dict[str, str]) -> None:
        self.preprint_title = metadata["preprint_title"]
        self.preprint_doi = metadata["preprint_doi"]
        self.published_doi = metadata["published_doi"]
        self.preprint_category = metadata["preprint_category"]
        self.preprint_date = datetime.datetime.strptime(metadata["preprint_date"], "%Y-%m-%d")
        self.published_date = datetime.datetime.strptime(metadata["published_date"], "%Y-%m-%d")


class RxivPublisherDetail(RxivPublication):
    """
    Class for generic returned Rxiv publisher metadata
    """

    published_citation_count: str

    def __init__(self, metadata: dict[str, str]) -> None:
        super().__init__(metadata)
        self.published_citation_count = metadata["published_citation_count"]


class RxivPublicationDetail(RxivPublication):
    """
    Class for generic returned Rxiv article metadata (published)
    """

    preprint_authors: list[str]
    preprint_author_corresponding: str
    preprint_author_corresponding_institution: str
    preprint_platform: str
    preprint_abstract: str
    published_journal: str

    _article_cit_fmt = "{author}. {title}. {journal}. {year}; {volume}:{pages}.{doi}"

    def __init__(self, metadata: dict[str, str]) -> None:
        super().__init__(metadata)
        self.preprint_authors = [s.strip() for s in metadata.get("preprint_authors", "").split(";")]
        self.preprint_author_corresponding = metadata["preprint_author_corresponding"]
        self.preprint_author_corresponding_institution = metadata["preprint_author_corresponding_institution"]
        self.preprint_platform = metadata["preprint_platform"]
        self.preprint_abstract = metadata["preprint_abstract"]
        self.published_journal = metadata["published_journal"]

    def _format_authors(self) -> str:
        """Helper function for returning a well formatted HTML author list"""
        if self.preprint_authors and len(self.preprint_authors) > 2:
            author = self.preprint_authors[0] + ", <i>et al</i>"
        elif self.preprint_authors and len(self.preprint_authors) == 2:
            author = " and ".join([author for author in self.preprint_authors])
        elif self.preprint_authors and len(self.preprint_authors) < 2:
            author = self.preprint_authors[0]
        else:
            author = ""

        return author

    @property
    def first_author(self) -> Optional[str]:
        if len(self.preprint_authors) > 0:
            return self.preprint_authors[0]
        else:
            return None


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


class Rxiv:
    """
    Interact with the bioRxiv and medRxiv pre-print servers via thier web APIs.

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
        response = requests.get(url)
        response.raise_for_status()
        return json.loads(response.text) if return_format == "json" else response.text
