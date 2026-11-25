# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from tests.helpers.constants import ADMIN_USER, ANONYMOUS, TEST_LICENSE, TEST_USER


@pytest.mark.parametrize("user", [TEST_USER, ANONYMOUS, ADMIN_USER], ids=["user", "anonymous", "admin"])
def test_can_list_licenses_as_any_user_class(client_for, setup_router_db, client, user):
    user_client = client_for(user)
    response = user_client.get("/api/v1/licenses/")

    assert response.status_code == 200
    response_value = response.json()
    assert len(response_value) == 3


@pytest.mark.parametrize("user", [TEST_USER, ANONYMOUS, ADMIN_USER], ids=["user", "anonymous", "admin"])
def test_can_list_active_licenses_as_any_user_class(client_for, setup_router_db, client, user):
    user_client = client_for(user)
    response = user_client.get("/api/v1/licenses/active")

    assert response.status_code == 200
    response_value = response.json()
    assert len(response_value) == 2
    license_state = [_license["active"] for _license in response_value]
    assert all(license_state)


def test_can_fetch_arbitrary_license(setup_router_db, client):
    response = client.get("/api/v1/licenses/1")

    assert response.status_code == 200
    response_value = response.json()
    response_value["text"] == TEST_LICENSE["text"]


def test_cannot_fetch_nonexistent_license(setup_router_db, client):
    response = client.get("/api/v1/licenses/100")
    assert response.status_code == 404
