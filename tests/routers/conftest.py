from tempfile import TemporaryDirectory
from pathlib import Path
from fastapi.testclient import TestClient
import pytest
from shutil import copytree
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
from tests.helpers.constants import TEST_USER, EXTRA_USER, TEST_REFERENCE_GENOME, TEST_LICENSE, TEST_MINIMAL_EXPERIMENT


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
