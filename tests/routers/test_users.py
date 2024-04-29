import pytest

from fastapi import Header

from mavedb.models.enums.user_role import UserRole
from mavedb.lib.authentication import get_current_user
from mavedb.lib.authorization import require_current_user
from unittest import mock

from tests.helpers.constants import TEST_USER, ADMIN_USER, EXTRA_USER, camelize
from tests.helpers.dependency_overrider import DependencyOverrider


def test_cannot_list_users_as_anonymous_user(client, setup_router_db, anonymous_app_overrides):
    with DependencyOverrider(anonymous_app_overrides):
        response = client.get("/api/v1/users/")

    assert response.status_code == 401
    response_value = response.json()
    assert response_value["detail"] in "Could not validate credentials"


def test_cannot_list_users_as_normal_user(client, setup_router_db):
    response = client.get("/api/v1/users/")
    assert response.status_code == 401
    response_value = response.json()
    assert response_value["detail"] in "You are not authorized to use this feature"


def test_can_list_users_as_admin_user(admin_app_overrides, setup_router_db, client):
    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/users")

    assert response.status_code == 200
    response_value = response.json()
    user_ids = [user["orcidId"] for user in response_value]
    assert len(response_value) == 3
    assert all(
        test_id in user_ids for test_id in (TEST_USER["username"], ADMIN_USER["username"], EXTRA_USER["username"])
    )


def test_cannot_get_anonymous_user(client, setup_router_db, session, anonymous_app_overrides):
    with DependencyOverrider(anonymous_app_overrides):
        response = client.get("/api/v1/users/me")

    assert response.status_code == 401
    response_value = response.json()
    assert response_value["detail"] in "Could not validate credentials"

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_get_current_user(client, setup_router_db, session):
    response = client.get("/api/v1/users/me")
    assert response.status_code == 200
    response_value = response.json()
    assert response_value["orcidId"] == TEST_USER["username"]

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_get_current_admin_user(client, admin_app_overrides, setup_router_db, session):
    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/users/me")

    assert response.status_code == 200
    response_value = response.json()
    assert response_value["orcidId"] == ADMIN_USER["username"]
    assert response_value["roles"] == ["admin"]

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_cannot_impersonate_admin_user_as_default_user(client, setup_router_db, session):
    # NOTE: We can't mock JWTBearer directly because the object is created when the `get_current_user` function is called.
    #       Instead, mock the function that decodes the JWT and present a fake `Bearer test` string that
    #       lets us reach the `decode_jwt` function call without raising exceptions.
    with DependencyOverrider(
        {
            get_current_user: get_current_user,
            require_current_user: require_current_user,
        }
    ), mock.patch("mavedb.lib.authentication.decode_jwt", lambda _: {"sub": TEST_USER["username"]}):
        response = client.get(
            "/api/v1/users/me",
            headers={"Authorization": "Bearer test", "X-Active-Roles": f"{UserRole.admin.name},ordinary user"},
        )

    assert response.status_code == 403
    assert response.json()["detail"] in "This user is not a member of the requested acting role."

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_cannot_fetch_single_user_as_anonymous_user(client, setup_router_db, session, anonymous_app_overrides):
    with DependencyOverrider(anonymous_app_overrides):
        response = client.get("/api/v1/users/2")

    assert response.status_code == 401
    assert response.json()["detail"] in "Could not validate credentials"

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_cannot_fetch_single_user_as_normal_user(client, setup_router_db, session):
    response = client.get("/api/v1/users/2")
    assert response.status_code == 401
    assert response.json()["detail"] in "You are not authorized to use this feature"

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_can_fetch_single_user_as_admin_user(client, setup_router_db, session, admin_app_overrides):
    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/users/2")

    assert response.status_code == 200
    response_value = response.json()
    assert response_value["orcidId"] == EXTRA_USER["username"]

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_fetching_nonexistent_user_as_admin_raises_exception(client, setup_router_db, session, admin_app_overrides):
    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/users/0")

    assert response.status_code == 404
    response_value = response.json()
    assert "User with ID 0 not found" in response_value["detail"]

    # Some lingering db transaction holds this test open unless it is explicitly closed.
    session.commit()


def test_anonymous_user_cannot_update_self(client, setup_router_db, anonymous_app_overrides):
    user_update = TEST_USER.copy()
    user_update.update({"email": "updated@test.com"})
    with DependencyOverrider(anonymous_app_overrides):
        response = client.put("/api/v1/users/me", json=user_update)

    assert response.status_code == 401
    response_value = response.json()
    assert response_value["detail"] == "Could not validate credentials"


def test_user_can_update_self(client, setup_router_db):
    user_update = TEST_USER.copy()
    user_update.update({"email": "updated@test.com"})
    response = client.put("/api/v1/users/me", json=user_update)
    assert response.status_code == 200
    response_value = response.json()
    assert response_value["email"] == "updated@test.com"


def test_admin_can_update_self(client, setup_router_db, admin_app_overrides):
    user_update = ADMIN_USER.copy()
    user_update.update({"email": "updated@test.com"})
    with DependencyOverrider(admin_app_overrides):
        response = client.put("/api/v1/users/me", json=user_update)

    assert response.status_code == 200
    response_value = response.json()
    assert response_value["email"] == "updated@test.com"


@pytest.mark.parametrize(
    "field_name,field_value",
    [
        ("email", "updated@test.com"),
        ("first_name", "Updated"),
        ("last_name", "User"),
        ("roles", ["admin"]),
    ],
)
def test_anonymous_user_cannot_update_other_users(
    client, setup_router_db, field_name, field_value, anonymous_app_overrides
):
    user_update = EXTRA_USER.copy()
    user_update.update({field_name: field_value})
    with DependencyOverrider(anonymous_app_overrides):
        response = client.put("/api/v1/users//2", json=user_update)

    assert response.status_code == 401
    response_value = response.json()
    assert response_value["detail"] in "Could not validate credentials"


@pytest.mark.parametrize(
    "field_name,field_value",
    [
        ("email", "updated@test.com"),
        ("first_name", "Updated"),
        ("last_name", "User"),
        ("roles", ["admin"]),
    ],
)
def test_user_cannot_update_other_users(client, setup_router_db, field_name, field_value):
    user_update = EXTRA_USER.copy()
    user_update.update({field_name: field_value})
    response = client.put("/api/v1/users//2", json=user_update)
    assert response.status_code == 403
    response_value = response.json()
    assert response_value["detail"] in "Insufficient permissions for user update."


@pytest.mark.parametrize(
    "field_name,field_value",
    [
        ("email", "updated@test.com"),
        ("first_name", "Updated"),
        ("last_name", "User"),
        ("roles", ["admin"]),
    ],
)
def test_admin_user_can_update_other_users(client, setup_router_db, field_name, field_value, admin_app_overrides):
    user_update = TEST_USER.copy()
    user_update.update({field_name: field_value})
    with DependencyOverrider(admin_app_overrides):
        response = client.put("/api/v1/users//2", json=user_update)

    assert response.status_code == 200
    response_value = response.json()
    assert response_value[camelize(field_name)] == field_value
