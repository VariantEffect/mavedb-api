from pathlib import Path
from shutil import copytree

import pytest

from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.contributor import Contributor
from mavedb.models.controlled_keyword import ControlledKeyword
from mavedb.models.enums.user_role import UserRole
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.license import License
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.role import Role
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User
from tests.helpers.constants import (
    ADMIN_USER,
    EXTRA_CONTRIBUTOR,
    EXTRA_LICENSE,
    EXTRA_USER,
    TEST_CLINVAR_CONTROL,
    TEST_DB_KEYWORDS,
    TEST_GENERIC_CLINICAL_CONTROL,
    TEST_GNOMAD_VARIANT,
    TEST_INACTIVE_LICENSE,
    TEST_LICENSE,
    TEST_PUBMED_PUBLICATION,
    TEST_SAVED_TAXONOMY,
    TEST_USER,
)

try:
    from .conftest_optional import *  # noqa: F403, F401
except ImportError:
    pass


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
    db.add(PublicationIdentifier(**TEST_PUBMED_PUBLICATION))
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
