from pathlib import Path
from shutil import copytree

import pytest

from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.controlled_keyword import ControlledKeyword
from mavedb.models.contributor import Contributor
from mavedb.models.enums.user_role import UserRole
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.license import License
from mavedb.models.role import Role
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User

from tests.helpers.constants import (
    ADMIN_USER,
    TEST_CLINVAR_CONTROL,
    TEST_GENERIC_CLINICAL_CONTROL,
    EXTRA_USER,
    EXTRA_CONTRIBUTOR,
    TEST_DB_KEYWORDS,
    TEST_LICENSE,
    TEST_INACTIVE_LICENSE,
    EXTRA_LICENSE,
    TEST_SAVED_TAXONOMY,
    TEST_USER,
    TEST_GNOMAD_VARIANT,
)


@pytest.fixture
def setup_router_db(session):
    """Set up the database with information needed to create a score set.

    This fixture creates ReferenceGenome and License, each with id 1.
    It also creates a new test experiment and yields it as a JSON object.
    """
    db = session
    db.add(User(**TEST_USER))
    db.add(User(**EXTRA_USER))
    db.add(User(**ADMIN_USER, role_objs=[Role(name=UserRole.admin)]))
    db.add(Taxonomy(**TEST_SAVED_TAXONOMY))
    db.add(License(**TEST_LICENSE))
    db.add(License(**TEST_INACTIVE_LICENSE))
    db.add(License(**EXTRA_LICENSE))
    db.add(Contributor(**EXTRA_CONTRIBUTOR))
    db.add(ClinicalControl(**TEST_CLINVAR_CONTROL))
    db.add(ClinicalControl(**TEST_GENERIC_CLINICAL_CONTROL))
    db.add(GnomADVariant(**TEST_GNOMAD_VARIANT))
    db.bulk_save_objects([ControlledKeyword(**keyword_obj) for keyword_obj in TEST_DB_KEYWORDS])
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
    elif publication_to_mock["dbName"] == "Crossref":
        requests_mock.get(
            f"https://api.crossref.org/works/{publication_to_mock['identifier']}",
            json={
                "status": "ok",
                "message-type": "work",
                "message-version": "1.0.0",
                "message": {
                    "DOI": "10.10/1.2.3",
                    "source": "Crossref",
                    "title": ["Crossref test pub title"],
                    "prefix": "10.10",
                    "author": [
                        {"given": "author", "family": "one", "sequence": "first", "affiliation": []},
                        {"given": "author", "family": "two", "sequence": "additional", "affiliation": []},
                    ],
                    "container-title": ["American Heart Journal"],
                    "abstract": "<jats:title>Abstract</jats:title><jats:p>text test</jats:p>",
                    "URL": "http://dx.doi.org/10.10/1.2.3",
                    "published": {"date-parts": [[2024, 5]]},
                },
            },
        )

    return publication_to_mock
