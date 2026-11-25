# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.enums.user_role import UserRole
from tests.helpers.authentication import ApiKey, Jwt, generate_signing_key
from tests.helpers.constants import ADMIN_USER, ANONYMOUS, EXTRA_USER, TEST_USER


@pytest.mark.parametrize(
    "user, via, expected_status",
    [
        (ADMIN_USER, Jwt(), 200),
        (ADMIN_USER, Jwt(active_roles=[UserRole.admin]), 200),
        (ADMIN_USER, Jwt(active_roles=[]), 403),
        (TEST_USER, Jwt(), 403),
        (ADMIN_USER, ApiKey(role=UserRole.admin), 200),
        (ADMIN_USER, ApiKey(role=None), 403),
        (TEST_USER, ApiKey(role=None), 403),
        (ANONYMOUS, Jwt(), 401),
    ],
    ids=[
        "admin-jwt-every-role",
        "admin-jwt-acting-as-admin",
        "admin-jwt-acting-with-no-role",
        "user-jwt",
        "admin-key-with-admin-role",
        "admin-key-with-no-role",
        "user-key",
        "anonymous",
    ],
)
def test_list_users_by_credential_allows_only_a_caller_acting_as_admin(
    client_for, setup_router_db, user, via, expected_status
):
    """Admin and ordinary users sign in by JWT, with and without ``X-Active-Roles``, and by access key with and
    without a role; only a caller whose credential leaves them acting as admin lists users."""
    response = client_for(user, via=via).get("/api/v1/users/")

    assert response.status_code == expected_status


@pytest.mark.parametrize("via", [Jwt(), ApiKey(role=None)], ids=["jwt", "api-key"])
def test_get_current_user_with_real_credential_returns_that_user(client_for, setup_router_db, via):
    """A JWT or an access key for ``EXTRA_USER`` resolves to them, not to the client's default ``TEST_USER``."""
    response = client_for(EXTRA_USER, via=via).get("/api/v1/users/me")

    assert response.status_code == 200
    assert response.json()["orcidId"] == EXTRA_USER["username"]


def test_get_current_user_with_token_signed_by_another_key_is_rejected(anonymous_client, setup_router_db):
    """A well-formed JWT for ``ADMIN_USER`` signed with a key the app doesn't trust gets 403, not the admin."""
    forged = generate_signing_key().token_for(ADMIN_USER["username"])

    response = anonymous_client.get("/api/v1/users/me", headers={"Authorization": f"Bearer {forged}"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Invalid token or expired token."
