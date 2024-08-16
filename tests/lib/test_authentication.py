import pytest

from fastapi import HTTPException
from sqlalchemy.exc import MultipleResultsFound
from unittest.mock import patch

from mavedb.lib.authentication import get_current_user_data_from_api_key, get_current_user
from mavedb.models.user import User
from mavedb.models.enums.user_role import UserRole

from tests.helpers.constants import TEST_USER, TEST_USER_DECODED_JWT, ADMIN_USER, ADMIN_USER_DECODED_JWT
from tests.helpers.util import create_api_key_for_current_user, mark_user_inactive


@pytest.mark.asyncio
async def test_get_current_user_data_from_key_valid_token(session, setup_lib_db, client):
    access_key = create_api_key_for_current_user(client)
    user_data = await get_current_user_data_from_api_key(session, access_key)
    assert user_data.user.username == TEST_USER["username"]

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_data_from_key_invalid_token(session, setup_lib_db, client):
    access_key = create_api_key_for_current_user(client)
    user_data = await get_current_user_data_from_api_key(session, f"invalid_{access_key}")
    assert user_data is None

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_data_from_key_nonetype_token(session, setup_lib_db, client):
    access_key = create_api_key_for_current_user(client)
    user_data = await get_current_user_data_from_api_key(session, None)
    assert user_data is None

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_via_api_key(session, setup_lib_db, client):
    access_key = create_api_key_for_current_user(client)
    user_data = await get_current_user_data_from_api_key(session, access_key)

    user_data = await get_current_user(user_data, None, session, None)
    assert user_data.user.username == TEST_USER["username"]

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_via_token_payload(session, setup_lib_db):
    user_data = await get_current_user(None, TEST_USER_DECODED_JWT, session, None)
    assert user_data.user.username == TEST_USER["username"]

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_no_api_no_jwt(session, setup_lib_db):
    user_data = await get_current_user(None, None, session, None)
    assert user_data is None

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_no_username(session, setup_lib_db):
    # Remove the username key from the JWT
    jwt_without_sub = TEST_USER_DECODED_JWT.copy()
    jwt_without_sub.pop("sub")

    user_data = await get_current_user(None, jwt_without_sub, session, None)
    assert user_data is None

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("with_email", [True, False])
async def test_get_current_user_nonexistent_user(session, setup_lib_db, with_email):
    new_user_jwt = {
        "sub": "5555-5555-5555-5555",
        "given_name": "Temporary",
        "family_name": "User",
    }

    email = "tempemail@test.com" if with_email else None
    with patch("mavedb.lib.authentication.fetch_orcid_user_email") as orc_email:
        orc_email.return_value = email
        user_data = await get_current_user(None, new_user_jwt, session, None)
        orc_email.assert_called_once()

    assert user_data.user.username == new_user_jwt["sub"]
    assert user_data.user.first_name == new_user_jwt["given_name"]
    assert user_data.user.last_name == new_user_jwt["family_name"]
    assert user_data.user.email == email

    # Ensure one user record is in the database
    session.query(User).filter(User.username == new_user_jwt["sub"]).one()

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_user_is_inactive(session, setup_lib_db):
    mark_user_inactive(session, TEST_USER["username"])
    user_data = await get_current_user(None, TEST_USER_DECODED_JWT, session, None)

    assert user_data is None

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_set_active_roles(session, setup_lib_db):
    user_data = await get_current_user(None, ADMIN_USER_DECODED_JWT, session, "admin")

    assert user_data.user.username == ADMIN_USER["username"]
    assert UserRole.admin in user_data.active_roles

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_user_with_invalid_role_membership(session, setup_lib_db):
    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(None, TEST_USER_DECODED_JWT, session, "admin")
    assert "This user is not a member of the requested acting role." in str(exc_info.value.detail)

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


@pytest.mark.asyncio
async def test_get_current_user_user_extraneous_roles(session, setup_lib_db):
    user_data = await get_current_user(None, TEST_USER_DECODED_JWT, session, "extra_role")

    assert user_data.user.username == TEST_USER["username"]
    assert user_data.active_roles == []

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()
