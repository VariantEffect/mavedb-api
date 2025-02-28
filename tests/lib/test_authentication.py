# ruff: noqa: E402

import pytest
from unittest.mock import patch

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.authentication import get_current_user, get_current_user_data_from_api_key
from mavedb.models.enums.user_role import UserRole
from mavedb.models.user import User
from tests.helpers.constants import ADMIN_USER, ADMIN_USER_DECODED_JWT, TEST_USER, TEST_USER_DECODED_JWT

from tests.helpers.util.access_key import create_api_key_for_user
from tests.helpers.util.user import mark_user_inactive


@pytest.mark.asyncio
async def test_get_current_user_data_from_key_valid_token(session, setup_lib_db):
    access_key = create_api_key_for_user(session, TEST_USER["username"])
    user_data = await get_current_user_data_from_api_key(session, access_key)
    assert user_data.user.username == TEST_USER["username"]


@pytest.mark.asyncio
async def test_get_current_user_data_from_key_invalid_token(session, setup_lib_db):
    access_key = create_api_key_for_user(session, TEST_USER["username"])
    user_data = await get_current_user_data_from_api_key(session, f"invalid_{access_key}")
    assert user_data is None


@pytest.mark.asyncio
async def test_get_current_user_data_from_key_nonetype_token(session, setup_lib_db):
    create_api_key_for_user(session, TEST_USER["username"])
    user_data = await get_current_user_data_from_api_key(session, None)
    assert user_data is None


@pytest.mark.asyncio
async def test_get_current_user_via_api_key(session, setup_lib_db):
    access_key = create_api_key_for_user(session, TEST_USER["username"])
    user_data = await get_current_user_data_from_api_key(session, access_key)

    user_data = await get_current_user(user_data, None, session, None)
    assert user_data.user.username == TEST_USER["username"]


@pytest.mark.asyncio
async def test_get_current_user_via_token_payload(session, setup_lib_db):
    user_data = await get_current_user(None, TEST_USER_DECODED_JWT, session, None)
    assert user_data.user.username == TEST_USER["username"]


@pytest.mark.asyncio
async def test_get_current_user_no_api_no_jwt(session, setup_lib_db):
    user_data = await get_current_user(None, None, session, None)
    assert user_data is None


@pytest.mark.asyncio
async def test_get_current_user_no_username(session, setup_lib_db):
    # Remove the username key from the JWT
    jwt_without_sub = TEST_USER_DECODED_JWT.copy()
    jwt_without_sub.pop("sub")

    user_data = await get_current_user(None, jwt_without_sub, session, None)
    assert user_data is None


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


@pytest.mark.asyncio
async def test_get_current_user_user_is_inactive(session, setup_lib_db):
    mark_user_inactive(session, TEST_USER["username"])
    user_data = await get_current_user(None, TEST_USER_DECODED_JWT, session, None)

    assert user_data is None


@pytest.mark.asyncio
async def test_get_current_user_set_active_roles(session, setup_lib_db):
    user_data = await get_current_user(None, ADMIN_USER_DECODED_JWT, session, "admin")

    assert user_data.user.username == ADMIN_USER["username"]
    assert UserRole.admin in user_data.active_roles


@pytest.mark.asyncio
async def test_get_current_user_user_with_invalid_role_membership(session, setup_lib_db):
    with pytest.raises(Exception) as exc_info:
        await get_current_user(None, TEST_USER_DECODED_JWT, session, "admin")
    assert "This user is not a member of the requested acting role." in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_get_current_user_user_extraneous_roles(session, setup_lib_db):
    user_data = await get_current_user(None, TEST_USER_DECODED_JWT, session, "extra_role")

    assert user_data.user.username == TEST_USER["username"]
    assert user_data.active_roles == []
