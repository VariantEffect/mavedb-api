from tempfile import TemporaryDirectory
from pathlib import Path
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import date
from humps import camelize

from mavedb.models.reference_genome import ReferenceGenome
from mavedb.models.license import License
from mavedb.server_main import app
from mavedb.db.base import Base
from mavedb.deps import get_db
from mavedb.lib.authentication import get_current_user
from mavedb.models.user import User

TEST_USER = {
    "username": "0000-1111-2222-3333",
    "first_name": "First",
    "last_name": "Last",
    "is_active": True,
    "is_staff": False,
    "is_superuser": False,
}

EXTRA_USER = {
    "username": "1234-5678-8765-4321",
    "first_name": "Extra",
    "last_name": "User",
    "is_active": True,
    "is_staff": False,
    "is_superuser": False,
}

TEST_MINIMAL_EXPERIMENT = {
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
}

TEST_MINIMAL_EXPERIMENT_RESPONSE = {
    "title": "Test Experiment Title",
    "shortDescription": "Test experiment",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "numScoreSets": 0,
    "createdBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "keywords": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "rawReadIdentifiers": [],
    # keys to be set after receiving response
    "urn": None,
    "experimentSetUrn": None,
}

TEST_REFERENCE_GENOME = {
    "id": 1,
    "short_name": "Name",
    "organism_name": "Organism",
}

TEST_LICENSE = {
    "id": 1,
    "short_name": "Short",
    "long_name": "Long",
    "text": "Don't be evil.",
    "link": "localhost",
    "version": "1.0",
}

TEST_MINIMAL_SCORE_SET = {
    "title": "Test Score Set Title",
    "shortDescription": "Test score set",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "licenseId": 1,
    "targetGene": {
        "name": "TEST1",
        "category": "Protein coding",
        "externalIdentifiers": [],
        "referenceMaps": [{"genomeId": TEST_REFERENCE_GENOME["id"]}],
        "wtSequence": {
            "sequenceType": "dna",
            "sequence": "ACGT",
        },
    },
    # keys to be set after setting up
    "experimentUrn": None,
}

TEST_MINIMAL_SCORE_SET_RESPONSE = {
    "title": "Test Score Set Title",
    "shortDescription": "Test score set",
    "abstractText": "Abstract",
    "methodText": "Methods",
    "createdBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "modifiedBy": {
        "firstName": TEST_USER["first_name"],
        "lastName": TEST_USER["last_name"],
        "orcidId": TEST_USER["username"],
    },
    "creationDate": date.today().isoformat(),
    "modificationDate": date.today().isoformat(),
    "license": {camelize(k): v for k, v in TEST_LICENSE.items() if k not in ("text",)},
    "numVariants": 0,
    "targetGene": {
        "name": "TEST1",
        "category": "Protein coding",
        "externalIdentifiers": [],
        "referenceMaps": [
            {
                "creationDate": date.today().isoformat(),
                "modificationDate": date.today().isoformat(),
                "genomeId": TEST_REFERENCE_GENOME["id"],
                "id": 1,
                "targetId": 1,
                "isPrimary": False,
                "genome": {camelize(k): v for k, v in TEST_REFERENCE_GENOME.items()}
                | {
                    "creationDate": date.today().isoformat(),
                    "modificationDate": date.today().isoformat(),
                },
            }
        ],
        "wtSequence": {
            "sequenceType": "dna",
            "sequence": "ACGT",
        },
    },
    "metaAnalysisSourceScoreSets": [],
    "metaAnalyses": [],
    "keywords": [],
    "doiIdentifiers": [],
    "primaryPublicationIdentifiers": [],
    "secondaryPublicationIdentifiers": [],
    "datasetColumns": {},
    "private": True,
    "experiment": TEST_MINIMAL_EXPERIMENT_RESPONSE,
    # keys to be set after receiving response
    "urn": None,
}


def override_get_db():
    try:
        # db = TestingSessionLocal()
        yield db
    finally:
        db.close()


def override_current_user():
    # db = TestingSessionLocal()
    default_user = db.query(User).filter(User.username == TEST_USER["username"]).one_or_none()
    db.expire(default_user)
    yield default_user


@pytest.fixture()
def test_empty_db():
    Base.metadata.create_all(bind=engine)
    # db = TestingSessionLocal()
    default_user = User(**TEST_USER)
    db.add(default_user)
    db.commit()
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def test_score_set_db(test_empty_db):
    """Set up the empty database with information needed to create a score set.

    This fixture creates ReferenceGenome and License, each with id 1.
    It also creates a new test experiment and yields it as a JSON object.
    """
    # db = TestingSessionLocal()

    new_reference_genome = ReferenceGenome(**TEST_REFERENCE_GENOME)
    db.add(new_reference_genome)
    db.commit()

    new_license = License(**TEST_LICENSE)
    db.add(new_license)
    db.commit()

    extra_user = User(**EXTRA_USER)
    db.add(extra_user)
    db.commit()

    response = client.post("/api/v1/experiments/", json=TEST_MINIMAL_EXPERIMENT)
    yield response.json()


# create the test database
db_directory = TemporaryDirectory()
engine = create_engine(f"sqlite:///{Path(db_directory.name, 'test.db')}", connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db = TestingSessionLocal()

# set up the test environment by overriding the db and user behavior
app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[get_current_user] = override_current_user
client = TestClient(app)
