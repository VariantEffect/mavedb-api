from pathlib import Path
import pytest
from shutil import copytree
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.license import License
from mavedb.models.user import User
from tests.helpers.constants import TEST_USER, EXTRA_USER, TEST_TAXONOMY, TEST_LICENSE


@pytest.fixture
def setup_router_db(session):
    """Set up the database with information needed to create a score set.

    This fixture creates ReferenceGenome and License, each with id 1.
    It also creates a new test experiment and yields it as a JSON object.
    """
    db = session
    db.add(User(**TEST_USER))
    db.add(User(**EXTRA_USER))
    db.add(Taxonomy(**TEST_TAXONOMY))
    db.add(License(**TEST_LICENSE))
    db.commit()


@pytest.fixture
def data_files(tmp_path):
    copytree(Path(__file__).absolute().parent / "data", tmp_path / "data")
    return tmp_path / "data"
