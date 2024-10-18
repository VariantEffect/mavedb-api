import pytest

from mavedb.models.enums.user_role import UserRole
from mavedb.models.license import License
from mavedb.models.role import Role
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User
from tests.helpers.constants import ADMIN_USER, EXTRA_USER, TEST_LICENSE, TEST_TAXONOMY, TEST_USER


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
    db.add(Taxonomy(**TEST_TAXONOMY))
    db.add(License(**TEST_LICENSE))
    db.commit()
