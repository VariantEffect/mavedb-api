import os

import pytest


# Unset test mode flag before each test to ensure decorator logic is executed
# during unit testing of the decorator itself.
@pytest.fixture(autouse=True)
def unset_test_mode_flag():
    os.environ.pop("MAVEDB_TEST_MODE", None)
