# ruff: noqa: E402

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy.exc import IntegrityError

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.authentication import (
    LAST_LOGIN_REFRESH_INTERVAL,
    get_current_user,
    get_current_user_data_from_api_key,
)
from mavedb.models.enums.user_role import UserRole
from mavedb.models.user import User
from tests.helpers.constants import ADMIN_USER, ADMIN_USER_DECODED_JWT, TEST_USER, TEST_USER_DECODED_JWT
from tests.helpers.util.access_key import create_api_key_for_user
from tests.helpers.util.user import mark_user_inactive


def test_get_current_user_data_from_key_valid_token(session, setup_lib_db):
    access_key = create_api_key_for_user(session, TEST_USER["username"])
    user_data = get_current_user_data_from_api_key(session, access_key)
    assert user_data.user.username == TEST_USER["username"]


def test_get_current_user_data_from_key_invalid_token(session, setup_lib_db):
    access_key = create_api_key_for_user(session, TEST_USER["username"])
    user_data = get_current_user_data_from_api_key(session, f"invalid_{access_key}")
    assert user_data is None


def test_get_current_user_data_from_key_nonetype_token(session, setup_lib_db):
    create_api_key_for_user(session, TEST_USER["username"])
    user_data = get_current_user_data_from_api_key(session, None)
    assert user_data is None


def test_get_current_user_via_api_key(session, setup_lib_db):
    access_key = create_api_key_for_user(session, TEST_USER["username"])
    user_data = get_current_user_data_from_api_key(session, access_key)

    user_data = get_current_user(user_data, None, session, None)
    assert user_data.user.username == TEST_USER["username"]


def test_get_current_user_via_token_payload(session, setup_lib_db):
    user_data = get_current_user(None, TEST_USER_DECODED_JWT, session, None)
    assert user_data.user.username == TEST_USER["username"]


def test_get_current_user_no_api_no_jwt(session, setup_lib_db):
    user_data = get_current_user(None, None, session, None)
    assert user_data is None


def test_get_current_user_no_username(session, setup_lib_db):
    # Remove the username key from the JWT
    jwt_without_sub = TEST_USER_DECODED_JWT.copy()
    jwt_without_sub.pop("sub")

    user_data = get_current_user(None, jwt_without_sub, session, None)
    assert user_data is None


@pytest.mark.parametrize("with_email", [True, False])
def test_get_current_user_nonexistent_user(session, setup_lib_db, with_email):
    new_user_jwt = {
        "sub": "5555-5555-5555-5555",
        "given_name": "Temporary",
        "family_name": "User",
    }

    email = "tempemail@test.com" if with_email else None
    with patch("mavedb.lib.authentication.fetch_orcid_user_email") as orc_email:
        orc_email.return_value = email
        user_data = get_current_user(None, new_user_jwt, session, None)
        orc_email.assert_called_once()

    assert user_data.user.username == new_user_jwt["sub"]
    assert user_data.user.first_name == new_user_jwt["given_name"]
    assert user_data.user.last_name == new_user_jwt["family_name"]
    assert user_data.user.email == email

    # Ensure one user record is in the database
    session.query(User).filter(User.username == new_user_jwt["sub"]).one()


def test_get_current_user_user_is_inactive(session, setup_lib_db):
    mark_user_inactive(session, TEST_USER["username"])
    user_data = get_current_user(None, TEST_USER_DECODED_JWT, session, None)

    assert user_data is None


def test_get_current_user_set_active_roles(session, setup_lib_db):
    user_data = get_current_user(None, ADMIN_USER_DECODED_JWT, session, "admin")

    assert user_data.user.username == ADMIN_USER["username"]
    assert UserRole.admin in user_data.active_roles


def test_get_current_user_user_with_invalid_role_membership(session, setup_lib_db):
    with pytest.raises(Exception) as exc_info:
        get_current_user(None, TEST_USER_DECODED_JWT, session, "admin")
    assert "This user is not a member of the requested acting role." in str(exc_info.value.detail)


def test_get_current_user_user_extraneous_roles(session, setup_lib_db):
    user_data = get_current_user(None, TEST_USER_DECODED_JWT, session, "extra_role")

    assert user_data.user.username == TEST_USER["username"]
    assert user_data.active_roles == []


def test_get_current_user_concurrent_first_login_integrity_error_returns_existing_user(session, setup_lib_db):
    """
    Simulate two servers racing on first login: the commit raises IntegrityError because a
    concurrent request already inserted the row. The handler should roll back and return the
    existing user rather than surfacing the error.
    """
    new_user_jwt = {
        "sub": "9999-0000-0000-9999",
        "given_name": "Race",
        "family_name": "Condition",
    }

    # Insert the user as if a concurrent request already committed it.
    pre_existing = User(
        username=new_user_jwt["sub"],
        first_name=new_user_jwt["given_name"],
        last_name=new_user_jwt["family_name"],
        is_active=True,
        is_first_login=True,
    )
    session.add(pre_existing)
    session.commit()

    # Wrap the real session so we can intercept the first commit call and raise IntegrityError,
    # letting subsequent calls (rollback, refresh, etc.) pass through to the real session.
    original_commit = session.commit
    commit_calls = []

    def fake_commit():
        commit_calls.append(1)
        if len(commit_calls) == 1:
            raise IntegrityError(statement=None, params=None, orig=Exception("duplicate key"))
        return original_commit()

    session.commit = fake_commit

    with patch("mavedb.lib.authentication.fetch_orcid_user_email", return_value=None):
        user_data = get_current_user(None, new_user_jwt, session, None)

    assert user_data is not None
    assert user_data.user.username == new_user_jwt["sub"]

    # Only one user record should exist in the database.
    users = session.query(User).filter(User.username == new_user_jwt["sub"]).all()
    assert len(users) == 1


def _set_login_state(session, username, *, last_login, is_first_login=False):
    user = session.query(User).filter(User.username == username).one()
    user.last_login = last_login
    user.is_first_login = is_first_login
    session.commit()
    return user


def test_get_current_user_skips_write_when_last_login_is_recent(session, setup_lib_db):
    recent = datetime.now() - timedelta(minutes=1)
    _set_login_state(session, TEST_USER["username"], last_login=recent)

    with patch.object(session, "commit", wraps=session.commit) as commit:
        user_data = get_current_user(None, TEST_USER_DECODED_JWT, session, None)

    assert user_data.user.username == TEST_USER["username"]
    commit.assert_not_called()
    assert user_data.user.last_login == recent


def test_get_current_user_refreshes_stale_last_login(session, setup_lib_db):
    stale = datetime.now() - LAST_LOGIN_REFRESH_INTERVAL - timedelta(minutes=1)
    _set_login_state(session, TEST_USER["username"], last_login=stale)

    user_data = get_current_user(None, TEST_USER_DECODED_JWT, session, None)

    assert user_data.user.last_login > stale


def test_get_current_user_writes_when_last_login_is_missing(session, setup_lib_db):
    _set_login_state(session, TEST_USER["username"], last_login=None)

    user_data = get_current_user(None, TEST_USER_DECODED_JWT, session, None)

    assert user_data.user.last_login is not None


def test_get_current_user_clears_first_login_flag_even_when_last_login_is_recent(session, setup_lib_db):
    _set_login_state(session, TEST_USER["username"], last_login=datetime.now(), is_first_login=True)

    user_data = get_current_user(None, TEST_USER_DECODED_JWT, session, None)

    assert user_data.user.is_first_login is False
