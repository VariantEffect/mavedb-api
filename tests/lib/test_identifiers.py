# ruff: noqa: E402

import pytest
import requests

from mavedb.lib.identifiers import find_generic_article
from tests.helpers.constants import TEST_BIORXIV_IDENTIFIER, TEST_PUBMED_IDENTIFIER

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


@pytest.mark.asyncio
async def test_find_generic_article_drops_candidate_with_malformed_response(session, requests_mock):
    """
    A PMID like TEST_PUBMED_IDENTIFIER is also a plausible (8 digit) legacy medRxiv
    identifier, so find_generic_article fans out to both databases. If medRxiv responds
    with a body that can't be parsed as JSON, that candidate should be dropped (and logged)
    rather than failing the whole lookup when PubMed already has a match.
    """
    mock_pubmed_success(requests_mock, TEST_PUBMED_IDENTIFIER)
    requests_mock.get(medrxiv_url(TEST_PUBMED_IDENTIFIER), text="not json")

    matches = await find_generic_article(session, TEST_PUBMED_IDENTIFIER)

    assert matches["PubMed"] is not None
    assert matches.get("medRxiv") is None


@pytest.mark.asyncio
async def test_find_generic_article_drops_candidate_on_request_exception(session, requests_mock):
    """
    The same fan-out should also tolerate a candidate database being entirely unreachable
    (timeouts, connection errors, bad status codes), not just malformed JSON bodies.
    """
    mock_pubmed_success(requests_mock, TEST_PUBMED_IDENTIFIER)
    requests_mock.get(medrxiv_url(TEST_PUBMED_IDENTIFIER), exc=requests.exceptions.ConnectTimeout)

    matches = await find_generic_article(session, TEST_PUBMED_IDENTIFIER)

    assert matches["PubMed"] is not None
    assert matches.get("medRxiv") is None


@pytest.mark.asyncio
async def test_find_generic_article_reraises_error_when_no_candidate_succeeds(session, requests_mock):
    """
    When every candidate database fails and none produces a match, the underlying error
    should propagate instead of being silently reported as "not found", so callers can
    still surface it as a gateway error (eg. a 504 on a Rxiv timeout).
    """
    requests_mock.get(biorxiv_url(TEST_BIORXIV_IDENTIFIER), exc=requests.exceptions.ConnectTimeout)

    with pytest.raises(requests.exceptions.ConnectTimeout):
        await find_generic_article(session, TEST_BIORXIV_IDENTIFIER)
