from datetime import date

from mavedb.view_models.license import License

from tests.helpers.util import dummy_attributed_object_from_dict
from tests.helpers.constants import TEST_LICENSE


def test_minimal_license():
    license = TEST_LICENSE.copy()
    license["creation_date"] = date.today()
    license["modification_date"] = date.today()

    saved_license = License.model_validate(dummy_attributed_object_from_dict(license))

    assert all(saved_license.__getattribute__(k) == v for k, v in license.items())
