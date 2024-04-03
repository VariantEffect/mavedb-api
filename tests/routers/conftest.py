from pathlib import Path
import pytest
from shutil import copytree
from mavedb.models.reference_genome import ReferenceGenome
from mavedb.models.license import License
from mavedb.models.user import User
from tests.helpers.constants import TEST_USER, EXTRA_USER, TEST_REFERENCE_GENOME, TEST_LICENSE

import requests_mock


@pytest.fixture
def setup_router_db(session):
    """Set up the database with information needed to create a score set.

    This fixture creates ReferenceGenome and License, each with id 1.
    It also creates a new test experiment and yields it as a JSON object.
    """
    db = session
    db.add(User(**TEST_USER))
    db.add(User(**EXTRA_USER))
    db.add(ReferenceGenome(**TEST_REFERENCE_GENOME))
    db.add(License(**TEST_LICENSE))
    db.commit()


@pytest.fixture
def data_files(tmp_path):
    copytree(Path(__file__).absolute().parent / "data", tmp_path / "data")
    return tmp_path / "data"


@pytest.fixture
def mock_publication_fetch(request, requests_mock):
    """
    Mocks the request that would be sent for the provided publication.

    To use this fixture for a test on which you would like to mock the creation of a publication identifier,
    mark the test with:

    @pytest.mark.parametrize(
        "mock_publication_fetch",
        [
            {
                "dbName": "<name of database to which the publication identifier belongs>",
                "identifier": "<identifier of the publication>"
            },
            ...
        ],
        indirect=["mock_publication_fetch"],
    )
    def test_needing_publication_identifier_mock(mock_publication_fetch, ...):
        ...

    If your test requires use of the mocked publication identifier, this fixture returns it. Just assign the fixture
    to a variable (or use it directly).

    def test_needing_publication_identifier_mock(mock_publication_fetch, ...):
        ...
        mocked_publication = mock_publication_fetch
        experiment = create_experiment(client, {"primaryPublicationIdentifiers": [mocked_publication]})
        ...
    """
    publication_to_mock = request.param

    if publication_to_mock["dbName"] == "PubMed":
        # minimal xml to pass validation
        requests_mock.post(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
            text=f"""<?xml version="1.0"?>
                <PubmedArticleSet>
                <PubmedArticle>
                    <MedlineCitation>
                    <PMID Version="1">{publication_to_mock["identifier"]}</PMID>
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

        # Since 6 digit PubMed identifiers may also be valid bioRxiv identifiers, the code checks that this isn't also a valid bioxriv ID. We return nothing.
        requests_mock.get(
            f"https://api.biorxiv.org/details/medrxiv/10.1101/{publication_to_mock['identifier']}/na/json",
            json={"collection": []},
        )

    elif publication_to_mock["dbName"] == "bioRxiv":
        requests_mock.get(
            f"https://api.biorxiv.org/details/biorxiv/10.1101/{publication_to_mock['identifier']}/na/json",
            json={
                "collection": [
                    {
                        "title": "test1",
                        "doi": "test2",
                        "category": "test3",
                        "authors": "test4; test5",
                        "author_corresponding": "test6",
                        "author_corresponding_institution": "test7",
                        "date": "1999-12-31",
                        "version": "test8",
                        "type": "test9",
                        "license": "test10",
                        "jatsxml": "test11",
                        "abstract": "test12",
                        "published": "test13",
                        "server": "test14",
                    }
                ]
            },
        )
    elif publication_to_mock["dbName"] == "medRxiv":
        requests_mock.get(
            f"https://api.biorxiv.org/details/medrxiv/10.1101/{publication_to_mock['identifier']}/na/json",
            json={
                "collection": [
                    {
                        "title": "test1",
                        "doi": "test2",
                        "category": "test3",
                        "authors": "test4; test5",
                        "author_corresponding": "test6",
                        "author_corresponding_institution": "test7",
                        "date": "1999-12-31",
                        "version": "test8",
                        "type": "test9",
                        "license": "test10",
                        "jatsxml": "test11",
                        "abstract": "test12",
                        "published": "test13",
                        "server": "test14",
                    }
                ]
            },
        )

    return publication_to_mock
