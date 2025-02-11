import pytest
from pathlib import Path
from shutil import copytree
from unittest import mock
from datetime import datetime

from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.experiment import Experiment
from mavedb.models.license import License
from mavedb.models.role import Role
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from tests.helpers.constants import (
    ADMIN_USER,
    EXTRA_USER,
    TEST_LICENSE,
    TEST_INACTIVE_LICENSE,
    TEST_SAVED_TAXONOMY,
    TEST_USER,
    VALID_SCORE_SET_URN,
    VALID_EXPERIMENT_URN,
    VALID_EXPERIMENT_SET_URN,
)


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
def data_files(tmp_path):
    copytree(Path(__file__).absolute().parent / "data", tmp_path / "data")
    return tmp_path / "data"


@pytest.fixture
def mock_user():
    mv = mock.Mock(spec=User)
    mv.username = TEST_USER["username"]
    return mv


@pytest.fixture
def mock_mapped_variant():
    mv = mock.Mock(spec=MappedVariant)
    mv.mapping_api_version = "1.0"
    mv.mapped_date = datetime(2023, 1, 1)
    return mv


@pytest.fixture
def mock_variant():
    variant = mock.Mock(spec=Variant)
    variant.urn = f"{VALID_SCORE_SET_URN}#1"
    variant.score_set.urn = VALID_SCORE_SET_URN
    return variant


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
    experiment.title = "Test Experiment"
    experiment.urn = VALID_EXPERIMENT_URN
    experiment.creation_date = datetime(2023, 1, 1)
    experiment.modification_date = datetime(2023, 1, 2)
    return experiment


@pytest.fixture
def mock_score_set(mock_user):
    score_set = mock.Mock(spec=ScoreSet)
    score_set.urn = VALID_SCORE_SET_URN
    score_set.license.short_name = "MIT"
    score_set.created_by = mock_user
    score_set.modified_by = mock_user
    score_set.published_date = datetime(2023, 1, 1)
    score_set.title = "Mock score set"
    score_set.creation_date = datetime(2023, 1, 2)
    score_set.modification_date = datetime(2023, 1, 3)
    return score_set
