import pytest

from tests.helpers.constants import EXTRA_USER
from tests.helpers.dependency_overrider import DependencyOverrider

from mavedb.models.access_key import AccessKey
from mavedb.models.enums.user_role import UserRole
from mavedb.models.user import User

from tests.helpers.util import create_api_key_for_current_user, create_admin_key_for_current_user


def test_create_user_access_key(client, setup_router_db, session):
    key_id = create_api_key_for_current_user(client)

    saved_access_key = session.query(AccessKey).filter(AccessKey.key_id == key_id).one_or_none()
    assert saved_access_key is not None

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_anonymous_user_cannot_create_access_key(client, setup_router_db, session, anonymous_app_overrides):
    with DependencyOverrider(anonymous_app_overrides):
        response = client.post("api/v1/users/me/access-keys")

    assert response.status_code == 401
    response_value = response.json()
    assert response_value["detail"] in "Could not validate credentials"

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_user_cannot_create_access_key_for_role_they_dont_have(client, setup_router_db, session):
    response = client.post("api/v1/users/me/access-keys/admin")
    assert response.status_code == 403
    response_value = response.json()
    assert response_value["detail"] in "User cannot create an API key for a role they do not have."

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_admin_can_create_admin_key(client, setup_router_db, session, admin_app_overrides):
    with DependencyOverrider(admin_app_overrides):
        key_id = create_admin_key_for_current_user(client)

    saved_access_key = session.query(AccessKey).filter(AccessKey.key_id == key_id).one_or_none()
    assert saved_access_key is not None
    assert saved_access_key.role is UserRole.admin

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_user_can_delete_access_key(client, setup_router_db, session):
    key_id = create_api_key_for_current_user(client)
    saved_access_key = session.query(AccessKey).filter(AccessKey.key_id == key_id).one_or_none()
    assert saved_access_key is not None

    del_response = client.delete(f"api/v1/users/me/access-keys/{key_id}")
    assert del_response.status_code == 200
    saved_access_key = session.query(AccessKey).filter(AccessKey.key_id == key_id).one_or_none()
    assert saved_access_key is None

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_anonymous_user_cannot_delete_access_key(client, setup_router_db, session, anonymous_app_overrides):
    key_id = create_api_key_for_current_user(client)
    saved_access_key = session.query(AccessKey).filter(AccessKey.key_id == key_id).one_or_none()
    assert saved_access_key is not None

    with DependencyOverrider(anonymous_app_overrides):
        del_response = client.delete(f"api/v1/users/me/access-keys/{key_id}")

    assert del_response.status_code == 401
    response_value = del_response.json()
    assert response_value["detail"] in "Could not validate credentials"

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_user_cannot_delete_other_users_access_key(client, setup_router_db, session):
    key_id = create_api_key_for_current_user(client)
    saved_access_key = session.query(AccessKey).filter(AccessKey.key_id == key_id).one_or_none()
    assert saved_access_key is not None

    extra_user = session.query(User).filter(User.username == EXTRA_USER["username"]).one_or_none()
    assert extra_user is not None
    saved_access_key.user = extra_user
    session.add(saved_access_key)
    session.commit()

    del_response = client.delete(f"api/v1/users/me/access-keys/{key_id}")
    assert del_response.status_code == 200
    saved_access_key = session.query(AccessKey).filter(AccessKey.key_id == key_id).one_or_none()
    assert saved_access_key is not None

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()
