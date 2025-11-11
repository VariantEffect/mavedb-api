import logging  # noqa: F401
from datetime import datetime
from unittest import mock
import sys

import email_validator
import pytest
import pytest_postgresql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from mavedb.db.base import Base
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.models.user import User, UserRole, Role
from mavedb.models.license import License
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.experiment import Experiment
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet

from mavedb.models import *  # noqa: F403

from tests.helpers.constants import (
    ADMIN_USER,
    EXTRA_USER,
    TEST_LICENSE,
    TEST_INACTIVE_LICENSE,
    TEST_SAVED_TAXONOMY,
    TEST_USER,
    VALID_VARIANT_URN,
    VALID_SCORE_SET_URN,
    VALID_EXPERIMENT_URN,
    VALID_EXPERIMENT_SET_URN,
    TEST_PUBMED_IDENTIFIER,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_BRNICH_SCORE_CALIBRATION,
    TEST_PATHOGENICITY_SCORE_CALIBRATION,
)

sys.path.append(".")

# Attempt to import optional top level fixtures. If the modules they depend on are not installed,
# we won't have access to our full fixture suite and only a limited subset of tests can be run.
try:
    from tests.conftest_optional import *  # noqa: F401, F403

except ModuleNotFoundError:
    pass

# needs the pytest_postgresql plugin installed
assert pytest_postgresql.factories

# Allow the @test domain name through our email validator.
email_validator.SPECIAL_USE_DOMAIN_NAMES.remove("test")


@pytest.fixture()
def session(postgresql):
    # Un-comment this line to log all database queries:
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    connection = (
        f"postgresql+psycopg2://{postgresql.info.user}:"
        f"@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    )

    engine = create_engine(connection, echo=False, poolclass=NullPool)
    session = sessionmaker(autocommit=False, autoflush=False, bind=engine)()

    Base.metadata.create_all(bind=engine)

    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture
def setup_lib_db(session):
    """
    Sets up the lib test db with a user, reference, and license. Its more straightforward to use
    the well tested client methods to insert experiments and score sets to the db for testing.
    """
    db = session
    db.add(User(**TEST_USER))
    db.add(User(**EXTRA_USER))
    db.add(User(**ADMIN_USER, role_objs=[Role(name=UserRole.admin)]))
    db.add(Taxonomy(**TEST_SAVED_TAXONOMY))
    db.add(License(**TEST_LICENSE))
    db.add(License(**TEST_INACTIVE_LICENSE))
    db.commit()


@pytest.fixture
def mock_user():
    mv = mock.Mock(spec=User)
    mv.username = TEST_USER["username"]
    return mv


@pytest.fixture
def mock_publication():
    mv = mock.Mock(spec=PublicationIdentifier)
    mv.identifier = TEST_PUBMED_IDENTIFIER
    mv.url = f"http://www.ncbi.nlm.nih.gov/pubmed/{TEST_PUBMED_IDENTIFIER}"
    return mv


@pytest.fixture
def mock_publication_associations(mock_publication):
    mv = mock.Mock(spec=ScoreSetPublicationIdentifierAssociation)
    mv.publication = mock_publication
    mv.primary = True
    return [mv]


@pytest.fixture
def mock_experiment_set():
    resource = mock.Mock(spec=ExperimentSet)
    resource.urn = VALID_EXPERIMENT_SET_URN
    resource.creation_date = datetime(2023, 1, 1)
    resource.modification_date = datetime(2023, 1, 2)
    return resource


@pytest.fixture
def mock_experiment():
    experiment = mock.Mock(spec=Experiment)
    experiment.title = "Mock Experiment"
    experiment.short_description = "Mock experiment"
    experiment.urn = VALID_EXPERIMENT_URN
    experiment.creation_date = datetime(2023, 1, 1)
    experiment.modification_date = datetime(2023, 1, 2)
    experiment.private = False
    return experiment


@pytest.fixture
def mock_score_set(mock_user, mock_experiment, mock_publication_associations):
    score_set = mock.Mock(spec=ScoreSet)
    score_set.urn = VALID_SCORE_SET_URN
    score_set.score_calibrations = [TEST_BRNICH_SCORE_CALIBRATION, TEST_PATHOGENICITY_SCORE_CALIBRATION]
    score_set.license.short_name = "MIT"
    score_set.created_by = mock_user
    score_set.modified_by = mock_user
    score_set.published_date = datetime(2023, 1, 1)
    score_set.title = "Mock score set"
    score_set.short_description = "Mock score set"
    score_set.creation_date = datetime(2023, 1, 2)
    score_set.modification_date = datetime(2023, 1, 3)
    score_set.private = False
    score_set.experiment = mock_experiment
    score_set.publication_identifier_associations = mock_publication_associations
    return score_set


@pytest.fixture
def mock_variant(mock_score_set):
    variant = mock.Mock(spec=Variant)
    variant.urn = VALID_VARIANT_URN
    variant.score_set = mock_score_set
    variant.data = {"score_data": {"score": 1.0}}
    variant.creation_date = datetime(2023, 1, 2)
    variant.modification_date = datetime(2023, 1, 3)
    return variant


@pytest.fixture
def mock_mapped_variant(mock_variant):
    mv = mock.Mock(spec=MappedVariant)
    mv.mapping_api_version = "pytest.mapping.1.0"
    mv.mapped_date = datetime(2023, 1, 1)
    mv.variant = mock_variant
    mv.pre_mapped = TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X
    mv.post_mapped = TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X
    mv.mapped_date = datetime(2023, 1, 2)
    mv.modification_date = datetime(2023, 1, 3)
    return mv


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
    # Support passing either a single publication dict or an iterable (list/tuple) of them.
    raw_param = request.param
    if isinstance(raw_param, (list, tuple)):
        publications_to_mock = list(raw_param)
    else:
        publications_to_mock = [raw_param]

    mocked_publications = []

    for publication_to_mock in publications_to_mock:
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
                            "title": "test biorxiv",
                            "doi": "test:test:test",
                            "category": "test3",
                            "authors": "",
                            "author_corresponding": "test6",
                            "author_corresponding_institution": "test7",
                            "date": "1999-12-31",
                            "version": "test8",
                            "type": "test9",
                            "license": "test10",
                            "jatsxml": "test11",
                            "abstract": "test abstract",
                            "published": "Preprint",
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
        mocked_publications.append(publication_to_mock)
    # Return a single dict (original behavior) if only one was provided; otherwise the list.
    return mocked_publications[0] if len(mocked_publications) == 1 else mocked_publications
