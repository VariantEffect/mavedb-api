import pytest

from mavedb.models.license import License
from mavedb.models.reference_genome import ReferenceGenome
from mavedb.models.user import User

from tests.helpers.constants import EXTRA_USER, TEST_LICENSE, TEST_REFERENCE_GENOME, TEST_USER


@pytest.fixture
def setup_lib_db(session):
    """
    Sets up the lib test db with a user, reference, and license. Its more straightforward to use
    the well tested client methods to insert experiments and score sets to the db for testing.
    """
    db = session
    db.add(User(**TEST_USER))
    db.add(User(**EXTRA_USER))
    db.add(ReferenceGenome(**TEST_REFERENCE_GENOME))
    db.add(License(**TEST_LICENSE))
    db.commit()
