from pathlib import Path
from shutil import copytree

import pytest

from mavedb.models.license import License
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User
from tests.helpers.constants import EXTRA_USER, TEST_LICENSE, TEST_INACTIVE_LICENSE, TEST_SAVED_TAXONOMY, TEST_USER
from tests.helpers.util import create_experiment, create_seq_score_set


@pytest.fixture
def setup_worker_db(session):
    db = session
    db.add(User(**TEST_USER))
    db.add(User(**EXTRA_USER))
    db.add(Taxonomy(**TEST_SAVED_TAXONOMY))
    db.add(License(**TEST_LICENSE))
    db.add(License(**TEST_INACTIVE_LICENSE))
    db.commit()


@pytest.fixture
def populate_worker_db(data_files, client):
    # create score set via API. In production, the API would invoke this worker job
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])

    return score_set["urn"]


@pytest.fixture
def data_files(tmp_path):
    copytree(Path(__file__).absolute().parent / "data", tmp_path / "data")
    return tmp_path / "data"
