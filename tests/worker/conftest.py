from pathlib import Path
from shutil import copytree
from unittest.mock import Mock

import pytest

from mavedb.models.license import License
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User

from tests.helpers.constants import (
    EXTRA_USER,
    TEST_LICENSE,
    TEST_INACTIVE_LICENSE,
    TEST_SAVED_TAXONOMY,
    TEST_USER,
    TEST_MAVEDB_ATHENA_ROW,
)


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
def data_files(tmp_path):
    copytree(Path(__file__).absolute().parent / "data", tmp_path / "data")
    return tmp_path / "data"


@pytest.fixture
def mocked_gnomad_variant_row():
    gnomad_variant = Mock()

    for key, value in TEST_MAVEDB_ATHENA_ROW.items():
        setattr(gnomad_variant, key, value)

    return gnomad_variant
