import pytest

from tests.helpers.constants import TEST_LICENSE
from tests.helpers.dependency_overrider import DependencyOverrider


@pytest.mark.parametrize("user_overrides", [None, "anonymous_app_overrides", "admin_app_overrides"])
def test_can_list_licenses_as_any_user_class(setup_router_db, client, user_overrides, request):
    if user_overrides is not None:
        dep_overrides = request.getfixturevalue(user_overrides)
        with DependencyOverrider(dep_overrides):
            response = client.get("/api/v1/licenses/")
    else:
        response = client.get("/api/v1/licenses/")

    assert response.status_code == 200
    response_value = response.json()
    assert len(response_value) == 2


@pytest.mark.parametrize("user_overrides", [None, "anonymous_app_overrides", "admin_app_overrides"])
def test_can_list_active_licenses_as_any_user_class(setup_router_db, client, user_overrides, request):
    if user_overrides is not None:
        dep_overrides = request.getfixturevalue(user_overrides)
        with DependencyOverrider(dep_overrides):
            response = client.get("/api/v1/licenses/active")
    else:
        response = client.get("/api/v1/licenses/active")

    assert response.status_code == 200
    response_value = response.json()
    assert len(response_value) == 1
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
