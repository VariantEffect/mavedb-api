import pytest
from fastapi.encoders import jsonable_encoder

from mavedb.view_models.user import CurrentUserUpdate
from tests.helpers.constants import TEST_USER


# There are lots of potentially invalid emails, but this test is intented to ensure
# the validator is active, so just use a simple one.
def test_cannot_update_user_with_invalid_email(client):
    with pytest.raises(ValueError):
        CurrentUserUpdate(**jsonable_encoder(TEST_USER, exclude={"email"}), email="invalidemail@")
