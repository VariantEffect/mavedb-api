from mavedb.view_models.orcid import OrcidAuthTokenRequest, OrcidAuthTokenResponse, OrcidUser

from tests.helpers.constants import (
    TEST_MINIMAL_ORCID_AUTH_TOKEN_RESPONSE,
    TEST_MINIMAL_ORCID_AUTH_TOKEN_REQUEST,
    TEST_MINIMAL_ORCID_USER,
)


def test_minimal_orcid_auth_token_request():
    orcid_auth_token_request = OrcidAuthTokenRequest(**TEST_MINIMAL_ORCID_AUTH_TOKEN_REQUEST)
    assert all(
        orcid_auth_token_request.__getattribute__(k) == v for k, v in TEST_MINIMAL_ORCID_AUTH_TOKEN_REQUEST.items()
    )


def test_minimal_orcid_auth_token_response():
    orcid_auth_token_response = OrcidAuthTokenResponse(**TEST_MINIMAL_ORCID_AUTH_TOKEN_RESPONSE)
    assert all(
        orcid_auth_token_response.__getattribute__(k) == v for k, v in TEST_MINIMAL_ORCID_AUTH_TOKEN_RESPONSE.items()
    )


def test_minimal_orcid_user():
    orcid_user = OrcidUser(**TEST_MINIMAL_ORCID_USER)
    assert all(orcid_user.__getattribute__(k) == v for k, v in TEST_MINIMAL_ORCID_USER.items())
