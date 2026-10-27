# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from contextlib import contextmanager

import requests
from sqlalchemy import event, func, select

from mavedb.lib.exceptions import PublicationServiceError, PublicationServiceTimeoutError
from mavedb.lib.external_publications import (
    PUBLICATION_FETCH_TIMEOUT,
    ExternalPublication,
    fetch_biorxiv_article,
    fetch_crossref_work,
    fetch_pubmed_article,
)
from mavedb.lib.publications import (
    ensure_publication_keys,
    ensure_publications,
    find_generic_article,
    record_publication_citations,
    resolve_publication,
)
from mavedb.models.mixins.publication_citation import PublicationKey
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.view_models.publication_identifier import PublicationIdentifierCreate
from tests.helpers.constants import TEST_BIORXIV_IDENTIFIER, TEST_CROSSREF_IDENTIFIER, TEST_PUBMED_IDENTIFIER

PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def mock_pubmed_success(requests_mock, identifier):
    requests_mock.post(
        PUBMED_EFETCH_URL,
        text=f"""<?xml version="1.0"?>
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation>
                    <PMID Version="1">{identifier}</PMID>
                    <Article>
                        <Journal>
                            <Title>test</Title>
                            <JournalIssue>
                                <PubDate>
                                <Year>1999</Year>
                                </PubDate>
                            </JournalIssue>
                        </Journal>
                        <Abstract>
                            <AbstractText>test</AbstractText>
                        </Abstract>
                    </Article>
                </MedlineCitation>
                <PubmedData>
                    <ArticleIdList>
                        <ArticleId IdType="doi">test</ArticleId>
                    </ArticleIdList>
                </PubmedData>
            </PubmedArticle>
        </PubmedArticleSet>
        """,
    )


def medrxiv_url(identifier):
    return f"https://api.biorxiv.org/details/medrxiv/10.1101/{identifier}/na/json"


def biorxiv_url(identifier):
    return f"https://api.biorxiv.org/details/biorxiv/10.1101/{identifier}/na/json"


def test_fetch_pubmed_article_unknown_pmid_returns_none(requests_mock):
    requests_mock.post(PUBMED_EFETCH_URL, text='<?xml version="1.0" ?><PubmedArticleSet></PubmedArticleSet>')

    assert fetch_pubmed_article(TEST_PUBMED_IDENTIFIER) is None


@pytest.mark.parametrize(
    "ncbi_error",
    [
        {
            "status_code": 429,
            "json": {"error": "API rate limit exceeded"},
            "headers": {"Content-Type": "application/json"},
        },
        {
            "status_code": 503,
            "text": "<html><body>Service unavailable</body></html>",
            "headers": {"Content-Type": "text/html"},
        },
    ],
    ids=["rate_limited", "outage_page"],
)
def test_fetch_pubmed_article_ncbi_error_raises_service_error(requests_mock, ncbi_error):
    requests_mock.post(PUBMED_EFETCH_URL, **ncbi_error)

    with pytest.raises(PublicationServiceError):
        fetch_pubmed_article(TEST_PUBMED_IDENTIFIER)


@pytest.mark.parametrize(
    "fetch, identifier, url, not_found",
    [
        (
            fetch_crossref_work,
            TEST_CROSSREF_IDENTIFIER,
            f"https://api.crossref.org/works/{TEST_CROSSREF_IDENTIFIER}",
            {"status_code": 404},
        ),
        (
            fetch_biorxiv_article,
            TEST_BIORXIV_IDENTIFIER,
            biorxiv_url(TEST_BIORXIV_IDENTIFIER),
            {"json": {"messages": [{"status": "no posts found"}], "collection": []}},
        ),
    ],
    ids=["crossref", "biorxiv"],
)
def test_fetch_publication_request_sets_timeout(requests_mock, fetch, identifier, url, not_found):
    """A stalled publication database must time out, so the caller gets a 504 rather than a hung request."""
    requests_mock.get(url, **not_found)

    fetch(identifier)

    assert requests_mock.last_request.timeout == PUBLICATION_FETCH_TIMEOUT


def test_find_generic_article_drops_candidate_with_malformed_response(session, requests_mock):
    """
    A PMID like TEST_PUBMED_IDENTIFIER is also a plausible (8 digit) legacy medRxiv
    identifier, so find_generic_article fans out to both databases. If medRxiv responds
    with a body that can't be parsed as JSON, that candidate should be dropped (and logged)
    rather than failing the whole lookup when PubMed already has a match.
    """
    mock_pubmed_success(requests_mock, TEST_PUBMED_IDENTIFIER)
    requests_mock.get(medrxiv_url(TEST_PUBMED_IDENTIFIER), text="not json")

    matches = find_generic_article(session, TEST_PUBMED_IDENTIFIER)

    assert matches["PubMed"] is not None
    assert matches.get("medRxiv") is None


def test_find_generic_article_drops_candidate_on_request_exception(session, requests_mock):
    """
    The same fan-out should also tolerate a candidate database being entirely unreachable
    (timeouts, connection errors, bad status codes), not just malformed JSON bodies.
    """
    mock_pubmed_success(requests_mock, TEST_PUBMED_IDENTIFIER)
    requests_mock.get(medrxiv_url(TEST_PUBMED_IDENTIFIER), exc=requests.exceptions.ConnectTimeout)

    matches = find_generic_article(session, TEST_PUBMED_IDENTIFIER)

    assert matches["PubMed"] is not None
    assert matches.get("medRxiv") is None


def test_find_generic_article_reraises_error_when_no_candidate_succeeds(session, requests_mock):
    """
    When every candidate database fails and none produces a match, the underlying error
    should propagate instead of being silently reported as "not found", so callers can
    still surface it as a gateway error (eg. a 504 on a Rxiv timeout).
    """
    requests_mock.get(biorxiv_url(TEST_BIORXIV_IDENTIFIER), exc=requests.exceptions.ConnectTimeout)

    with pytest.raises(PublicationServiceTimeoutError) as exc_info:
        find_generic_article(session, TEST_BIORXIV_IDENTIFIER)

    assert isinstance(exc_info.value.__cause__, requests.exceptions.ConnectTimeout)


def store_publication(session, db_name, identifier, **fields):
    publication = PublicationIdentifier(
        db_name=db_name, identifier=identifier, title=fields.pop("title", "stored title"), authors=[], **fields
    )
    session.add(publication)
    session.commit()
    return publication


@contextmanager
def statements_on(session, table):
    """Collect the SQL statements run against ``table`` inside the block."""
    statements: list[str] = []

    def collect(conn, cursor, statement, parameters, context, executemany):
        if table in statement:
            statements.append(statement)

    engine = session.get_bind()
    event.listen(engine, "before_cursor_execute", collect)
    try:
        yield statements
    finally:
        event.remove(engine, "before_cursor_execute", collect)


def publication_count(session):
    return session.scalar(select(func.count()).select_from(PublicationIdentifier))


def test_ensure_publication_keys_new_publication_inserts_without_returning(session, requests_mock):
    mock_pubmed_success(requests_mock, TEST_PUBMED_IDENTIFIER)

    with statements_on(session, "publication_identifiers") as statements:
        keys = ensure_publication_keys(
            session, [PublicationIdentifierCreate(identifier=TEST_PUBMED_IDENTIFIER, db_name="PubMed")]
        )

    assert keys == [[PublicationKey("PubMed", TEST_PUBMED_IDENTIFIER)]]
    stored = session.scalars(select(PublicationIdentifier)).one()
    assert (stored.db_name, stored.identifier) == keys[0][0]

    # Under row-level security, RETURNING on a row nothing cites yet fails.
    inserts = [statement for statement in statements if statement.startswith("INSERT")]
    assert len(inserts) == 1
    assert "RETURNING" not in inserts[0]


def test_ensure_publication_keys_stored_publication_returns_key_without_fetching(session, requests_mock):
    store_publication(session, "PubMed", TEST_PUBMED_IDENTIFIER)

    keys = ensure_publication_keys(
        session, [PublicationIdentifierCreate(identifier=TEST_PUBMED_IDENTIFIER, db_name="PubMed")]
    )

    assert keys == [[PublicationKey("PubMed", TEST_PUBMED_IDENTIFIER)]]
    assert not requests_mock.called
    assert publication_count(session) == 1


def test_ensure_publication_keys_repeated_submission_returns_one_key(session, requests_mock):
    mock_pubmed_success(requests_mock, TEST_PUBMED_IDENTIFIER)
    submitted = PublicationIdentifierCreate(identifier=TEST_PUBMED_IDENTIFIER, db_name="PubMed")

    keys = ensure_publication_keys(session, [submitted, submitted])

    assert keys == [[PublicationKey("PubMed", TEST_PUBMED_IDENTIFIER)]]
    assert publication_count(session) == 1


def test_ensure_publication_keys_several_lists_returns_keys_per_list(session, requests_mock):
    mock_pubmed_success(requests_mock, TEST_PUBMED_IDENTIFIER)
    store_publication(session, "bioRxiv", TEST_BIORXIV_IDENTIFIER)
    pubmed = PublicationIdentifierCreate(identifier=TEST_PUBMED_IDENTIFIER, db_name="PubMed")
    biorxiv = PublicationIdentifierCreate(identifier=TEST_BIORXIV_IDENTIFIER, db_name="bioRxiv")

    keys = ensure_publication_keys(session, [pubmed], [biorxiv, pubmed], [])

    assert keys == [
        [PublicationKey("PubMed", TEST_PUBMED_IDENTIFIER)],
        [PublicationKey("bioRxiv", TEST_BIORXIV_IDENTIFIER), PublicationKey("PubMed", TEST_PUBMED_IDENTIFIER)],
        [],
    ]
    assert publication_count(session) == 2


def test_ensure_publications_existing_key_leaves_stored_row_unchanged(session, requests_mock):
    """A row the caller couldn't see when resolving, such as one cited only by private records, is left alone."""
    mock_pubmed_success(requests_mock, TEST_PUBMED_IDENTIFIER)
    fetched = resolve_publication(session, TEST_PUBMED_IDENTIFIER, "PubMed")
    assert isinstance(fetched, ExternalPublication)
    store_publication(session, "PubMed", TEST_PUBMED_IDENTIFIER, title="stored title")

    ensure_publications(session, [fetched])
    session.commit()

    stored = session.scalars(select(PublicationIdentifier)).one()
    assert stored.title == "stored title"


def test_record_publication_citations_publication_in_both_lists_is_cited_once_as_primary():
    shared = PublicationKey("PubMed", "1")
    secondary_only = PublicationKey("bioRxiv", "2")

    citations = record_publication_citations(
        ScoreSetPublicationIdentifierAssociation, primary=[shared], secondary=[secondary_only, shared, shared]
    )

    assert {(citation.key, citation.primary) for citation in citations} == {(shared, True), (secondary_only, False)}
    assert len(citations) == 2


@pytest.mark.parametrize(
    "stored, expected",
    [
        ([("PubMed", "39975210"), ("bioRxiv", "2024.04.01.587474")], ("PubMed", "39975210")),
        (
            [("PubMed", "39975210"), ("Crossref", "10.1101/2024.04.01.587474")],
            ("Crossref", "10.1101/2024.04.01.587474"),
        ),
    ],
    ids=["oldest", "crossref-first"],
)
def test_find_generic_article_doi_shared_by_two_publications_returns_one(session, stored, expected):
    for db_name, identifier in stored:
        store_publication(session, db_name, identifier, doi="10.1101/2024.04.01.587474")

    matches = find_generic_article(session, "10.1101/2024.04.01.587474")

    (match,) = matches.values()
    assert isinstance(match, PublicationIdentifier)
    assert (match.db_name, match.identifier) == expected
