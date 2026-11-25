# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.enums.user_role import UserRole
from tests.helpers.authentication import ApiKey, Jwt
from tests.helpers.constants import ADMIN_USER, EXTRA_USER, TEST_USER, camelize


def test_cannot_list_users_as_anonymous_user(anonymous_client, setup_router_db):
    response = anonymous_client.get("/api/v1/users/")

    assert response.status_code == 401
    response_value = response.json()
    assert "Could not validate credentials" in response_value["detail"]


def test_cannot_list_users_as_normal_user(client, setup_router_db):
    response = client.get("/api/v1/users/")
    assert response.status_code == 403
    response_value = response.json()
    assert "You are not authorized to use this feature" in response_value["detail"]


def test_can_list_users_as_admin_user(admin_client):
    response = admin_client.get("/api/v1/users")

    assert response.status_code == 200
    response_value = response.json()
    user_ids = [user["orcidId"] for user in response_value]
    assert len(response_value) == 3
    assert all(
        test_id in user_ids for test_id in (TEST_USER["username"], ADMIN_USER["username"], EXTRA_USER["username"])
    )


def test_cannot_get_anonymous_user(anonymous_client, setup_router_db, session):
    response = anonymous_client.get("/api/v1/users/me")

    assert response.status_code == 401
    response_value = response.json()
    assert "Could not validate credentials" in response_value["detail"]


def test_get_current_user(client, setup_router_db, session):
    response = client.get("/api/v1/users/me")
    assert response.status_code == 200
    response_value = response.json()
    assert response_value["orcidId"] == TEST_USER["username"]


def test_get_current_admin_user(admin_client, session):
    response = admin_client.get("/api/v1/users/me")

    assert response.status_code == 200
    response_value = response.json()
    assert response_value["orcidId"] == ADMIN_USER["username"]
    assert response_value["roles"] == ["admin"]


def test_cannot_impersonate_admin_user_as_default_user(client_for, client, setup_router_db, session):
    impersonating_client = client_for(TEST_USER, via=Jwt(active_roles=[UserRole.admin, "ordinary user"]))
    response = impersonating_client.get("/api/v1/users/me")

    assert response.status_code == 403
    assert response.json()["detail"] in "This user is not a member of the requested acting role."


def test_cannot_fetch_single_user_as_anonymous_user(anonymous_client, setup_router_db, session):
    response = anonymous_client.get("/api/v1/users/2")

    assert response.status_code == 401
    assert response.json()["detail"] in "Could not validate credentials"


def test_cannot_fetch_single_user_as_normal_user(client, setup_router_db, session):
    response = client.get("/api/v1/users/2")
    assert response.status_code == 403
    assert response.json()["detail"] in "You are not authorized to use this feature"


def test_can_fetch_single_user_as_admin_user(admin_client, session):
    response = admin_client.get("/api/v1/users/2")

    assert response.status_code == 200
    response_value = response.json()
    assert response_value["orcidId"] == EXTRA_USER["username"]


def test_fetching_nonexistent_user_as_admin_raises_exception(admin_client, session):
    response = admin_client.get("/api/v1/users/0")

    assert response.status_code == 404
    response_value = response.json()
    assert "user profile with ID 0 not found" in response_value["detail"]


def test_anonymous_user_cannot_update_self(anonymous_client, setup_router_db):
    user_update = TEST_USER.copy()
    user_update.update({"email": "updated@test.com"})
    response = anonymous_client.put("/api/v1/users/me", json=user_update)

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


def test_admin_can_update_self(admin_client):
    user_update = ADMIN_USER.copy()
    user_update.update({"email": "updated@test.com"})
    response = admin_client.put("/api/v1/users/me", json=user_update)

    assert response.status_code == 200
    response_value = response.json()
    assert response_value["email"] == "updated@test.com"


def test_anonymous_user_cannot_set_logged_in_property(anonymous_client, setup_router_db):
    response = anonymous_client.put("/api/v1/users/me/has-logged-in")

    assert response.status_code == 401
    response_value = response.json()
    assert response_value["detail"] == "Could not validate credentials"


def test_user_can_set_logged_in_property_on_self(client_for, client, setup_router_db):
    # A JWT sign-in clears is_first_login on its own; an access key leaves it set for the route to clear.
    user_key_client = client_for(TEST_USER, via=ApiKey(role=None))
    response = user_key_client.put("/api/v1/users/me/has-logged-in")

    assert response.status_code == 200
    response_value = response.json()
    assert not response_value[camelize("is_first_login")]


def test_admin_can_set_logged_in_property_on_self(client_for, client, setup_router_db):
    admin_key_client = client_for(ADMIN_USER, via=ApiKey(role=None))
    response = admin_key_client.put("/api/v1/users/me/has-logged-in")

    assert response.status_code == 200
    response_value = response.json()
    assert not response_value[camelize("is_first_login")]


@pytest.mark.parametrize(
    "field_name,field_value",
    [
        ("email", "updated@test.com"),
        ("first_name", "Updated"),
        ("last_name", "user profile"),
        ("roles", ["admin"]),
    ],
)
def test_anonymous_user_cannot_update_other_users(anonymous_client, setup_router_db, field_name, field_value):
    user_update = EXTRA_USER.copy()
    user_update.update({field_name: field_value})
    response = anonymous_client.put("/api/v1/users//2", json=user_update)

    assert response.status_code == 401
    response_value = response.json()
    assert "Could not validate credentials" in response_value["detail"]


@pytest.mark.parametrize(
    "field_name,field_value",
    [
        ("email", "updated@test.com"),
        ("first_name", "Updated"),
        ("last_name", "user profile"),
        ("roles", ["admin"]),
    ],
)
def test_user_cannot_update_other_users(client, setup_router_db, field_name, field_value):
    user_update = EXTRA_USER.copy()
    user_update.update({field_name: field_value})
    response = client.put("/api/v1/users//2", json=user_update)
    assert response.status_code == 403
    response_value = response.json()
    assert "insufficient permissions on user profile with ID '2'" in response_value["detail"]


@pytest.mark.parametrize(
    "field_name,field_value",
    [
        ("email", "updated@test.com"),
        ("first_name", "Updated"),
        ("last_name", "user profile"),
        ("roles", ["admin"]),
    ],
)
def test_admin_user_can_update_other_users(admin_client, field_name, field_value):
    user_update = TEST_USER.copy()
    user_update.update({field_name: field_value})
    response = admin_client.put("/api/v1/users//2", json=user_update)

    assert response.status_code == 200
    response_value = response.json()
    assert response_value[camelize(field_name)] == field_value
