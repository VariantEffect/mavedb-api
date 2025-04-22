import os
from urllib import parse
import pytest
import requests
from datetime import datetime
from unittest.mock import patch, MagicMock
from mavedb.lib.clingen.constants import LDH_LINKED_DATA_URL
from mavedb.lib.clingen.linked_data_hub import ClinGenLdhService, get_clingen_variation


@pytest.fixture
def clingen_service():
    yield ClinGenLdhService(url="https://pytest.clingen.com")


class TestClinGenLdhService:
    def test_init(self, clingen_service):
        assert clingen_service.url == "https://pytest.clingen.com"

    ### Test the authenticate method

    def test_authenticate_with_existing_jwt(self, clingen_service: ClinGenLdhService):
        with patch.object(ClinGenLdhService, "_existing_jwt", return_value="existing_jwt_token") as mock_existing_jwt:
            jwt = clingen_service.authenticate()

        assert jwt == "existing_jwt_token"
        mock_existing_jwt.assert_called_once()

    @patch("mavedb.lib.clingen.linked_data_hub.requests.post")
    @patch("mavedb.lib.clingen.linked_data_hub.ClinGenLdhService._existing_jwt")
    @patch.dict(
        os.environ, {"GENBOREE_ACCOUNT_NAME": "test_account", "GENBOREE_ACCOUNT_PASSWORD": "test_password"}, clear=True
    )
    def test_authenticate_with_new_jwt(self, mock_existing_jwt, mock_post, clingen_service):
        mock_existing_jwt.return_value = None

        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {"jwt": "new_jwt_token"}}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        jwt = clingen_service.authenticate()
        assert jwt == "new_jwt_token"
        assert os.environ["GENBOREE_JWT"] == "new_jwt_token"
        mock_post.assert_called_once_with(
            "https://genboree.org/auth/usr/gb:test_account/auth",
            json={"type": "plain", "val": "test_password"},
        )

    @patch("mavedb.lib.clingen.linked_data_hub.requests.post")
    @patch("mavedb.lib.clingen.linked_data_hub.ClinGenLdhService._existing_jwt")
    @patch.dict(os.environ, {}, clear=True)
    def test_authenticate_missing_credentials(self, mock_existing_jwt, mock_post, clingen_service):
        mock_existing_jwt.return_value = None
        with pytest.raises(ValueError, match="Genboree account name and/or password are not set"):
            clingen_service.authenticate()
        mock_post.assert_not_called()

    @patch("mavedb.lib.clingen.linked_data_hub.requests.post")
    @patch("mavedb.lib.clingen.linked_data_hub.ClinGenLdhService._existing_jwt")
    @patch.dict(
        os.environ, {"GENBOREE_ACCOUNT_NAME": "test_account", "GENBOREE_ACCOUNT_PASSWORD": "test_password"}, clear=True
    )
    def test_authenticate_http_error(mock_existing_jwt, mock_post, clingen_service):
        mock_existing_jwt.return_value = None

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP Error")
        mock_post.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError, match="HTTP Error"):
            clingen_service.authenticate()
        mock_post.assert_called_once()

    @patch("mavedb.lib.clingen.linked_data_hub.requests.post")
    @patch("mavedb.lib.clingen.linked_data_hub.ClinGenLdhService._existing_jwt")
    @patch.dict(os.environ, {"GENBOREE_ACCOUNT_NAME": "test_account", "GENBOREE_ACCOUNT_PASSWORD": "test_password"})
    def test_authenticate_missing_jwt_in_response(mock_existing_jwt, mock_post, clingen_service):
        mock_existing_jwt.return_value = None

        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {}}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        with pytest.raises(ValueError, match="Could not parse JWT from valid response"):
            clingen_service.authenticate()
        mock_post.assert_called_once()

    ### Test the _existing_jwt method

    @patch("mavedb.lib.clingen.linked_data_hub.os.getenv")
    @patch("mavedb.lib.clingen.linked_data_hub.jwt.get_unverified_claims")
    def test_existing_jwt_valid(mock_get_unverified_claims, mock_getenv, clingen_service):
        mock_getenv.return_value = "valid_jwt_token"
        mock_get_unverified_claims.return_value = {"exp": (datetime.now().timestamp() + 3600)}

        jwt = clingen_service._existing_jwt()
        assert jwt == "valid_jwt_token"
        mock_getenv.assert_called_once_with("GENBOREE_JWT")
        mock_get_unverified_claims.assert_called_once_with("valid_jwt_token")

    @patch("mavedb.lib.clingen.linked_data_hub.os.getenv")
    @patch("mavedb.lib.clingen.linked_data_hub.jwt.get_unverified_claims")
    def test_existing_jwt_expired(mock_get_unverified_claims, mock_getenv, clingen_service):
        mock_getenv.return_value = "expired_jwt_token"
        mock_get_unverified_claims.return_value = {"exp": (datetime.now().timestamp() - 3600)}

        jwt = clingen_service._existing_jwt()
        assert jwt is None
        mock_getenv.assert_called_once_with("GENBOREE_JWT")
        mock_get_unverified_claims.assert_called_once_with("expired_jwt_token")

    @patch("mavedb.lib.clingen.linked_data_hub.os.getenv")
    def test_existing_jwt_not_set(mock_getenv, clingen_service):
        mock_getenv.return_value = None

        jwt = clingen_service._existing_jwt()
        assert jwt is None
        mock_getenv.assert_called_once_with("GENBOREE_JWT")

    ### Test the dispatch_submissions method

    @patch("mavedb.lib.clingen.linked_data_hub.request_with_backoff")
    @patch("mavedb.lib.clingen.linked_data_hub.ClinGenLdhService.authenticate")
    @patch("mavedb.lib.clingen.linked_data_hub.batched")
    def test_dispatch_submissions_success(mock_batched, mock_authenticate, mock_request_with_backoff, clingen_service):
        mock_authenticate.return_value = "test_jwt_token"
        mock_request_with_backoff.return_value.json.return_value = {"success": True}

        content_submissions = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_batched.return_value = [[{"id": 1}, {"id": 2}], [{"id": 3}]]  # Simulate batching

        successes, failures = clingen_service.dispatch_submissions(content_submissions, batch_size=2)

        assert len(successes) == 3
        assert len(failures) == 0
        mock_batched.assert_called_once_with(content_submissions, 2)
        mock_request_with_backoff.assert_called_with(
            method="PUT",
            url=clingen_service.url,
            json={"id": 1},
            headers={"Authorization": "Bearer test_jwt_token", "Content-Type": "application/json"},
        )

    @patch("mavedb.lib.clingen.linked_data_hub.request_with_backoff")
    @patch("mavedb.lib.clingen.linked_data_hub.ClinGenLdhService.authenticate")
    def test_dispatch_submissions_failure(mock_authenticate, mock_request_with_backoff, clingen_service):
        mock_authenticate.return_value = "test_jwt_token"
        mock_request_with_backoff.side_effect = requests.exceptions.RequestException("Request failed")

        content_submissions = [{"id": 1}, {"id": 2}, {"id": 3}]

        successes, failures = clingen_service.dispatch_submissions(content_submissions)

        assert len(successes) == 0
        assert len(failures) == 3
        mock_request_with_backoff.assert_called_with(
            method="PUT",
            url=clingen_service.url,
            json={"id": 1},
            headers={"Authorization": "Bearer test_jwt_token", "Content-Type": "application/json"},
        )

    @patch("mavedb.lib.clingen.linked_data_hub.request_with_backoff")
    @patch("mavedb.lib.clingen.linked_data_hub.ClinGenLdhService.authenticate")
    def test_dispatch_submissions_partial_success(mock_authenticate, mock_request_with_backoff, clingen_service):
        mock_authenticate.return_value = "test_jwt_token"

        def mock_request_with_backoff_side_effect(*args, **kwargs):
            if kwargs["json"]["id"] == 2:
                raise requests.exceptions.RequestException("Request failed")
            return MagicMock(json=MagicMock(return_value={"success": True}))

        mock_request_with_backoff.side_effect = mock_request_with_backoff_side_effect

        content_submissions = [{"id": 1}, {"id": 2}, {"id": 3}]

        successes, failures = clingen_service.dispatch_submissions(content_submissions)

        assert len(successes) == 2
        assert len(failures) == 1
        assert failures[0]["id"] == 2

    @patch("mavedb.lib.clingen.linked_data_hub.request_with_backoff")
    @patch("mavedb.lib.clingen.linked_data_hub.ClinGenLdhService.authenticate")
    @patch("mavedb.lib.clingen.linked_data_hub.batched")
    def test_dispatch_submissions_no_batching(
        mock_batched, mock_authenticate, mock_request_with_backoff, clingen_service
    ):
        mock_authenticate.return_value = "test_jwt_token"
        mock_request_with_backoff.return_value.json.return_value = {"success": True}

        content_submissions = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_batched.return_value = content_submissions  # No batching

        successes, failures = clingen_service.dispatch_submissions(content_submissions)

        assert len(successes) == 3
        assert len(failures) == 0
        mock_batched.assert_not_called()
        mock_request_with_backoff.assert_called_with(
            method="PUT",
            url=clingen_service.url,
            json={"id": 1},
            headers={"Authorization": "Bearer test_jwt_token", "Content-Type": "application/json"},
        )


@patch("mavedb.lib.clingen.linked_data_hub.requests.get")
def test_get_clingen_variation_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"ldFor": {"Variant": [{"id": "variant_1", "name": "Test Variant"}]}}}
    mock_get.return_value = mock_response

    urn = "urn:example:variant"
    result = get_clingen_variation(urn)

    assert result == {"id": "variant_1", "name": "Test Variant"}
    mock_get.assert_called_once_with(
        f"{LDH_LINKED_DATA_URL}/{parse.quote_plus(urn)}",
        headers={"Accept": "application/json"},
    )


@patch("mavedb.lib.clingen.linked_data_hub.requests.get")
def test_get_clingen_variation_failure(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_get.return_value = mock_response

    urn = "urn:example:nonexistent_variant"
    result = get_clingen_variation(urn)

    assert result is None
    mock_get.assert_called_once_with(
        f"{LDH_LINKED_DATA_URL}/{parse.quote_plus(urn)}",
        headers={"Accept": "application/json"},
    )


@patch("mavedb.lib.clingen.linked_data_hub.requests.get")
def test_get_clingen_variation_invalid_response(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"ldFor": {}}}  # Missing "Variant" key
    mock_get.return_value = mock_response

    urn = "urn:example:variant"
    with pytest.raises(KeyError):
        get_clingen_variation(urn)

    mock_get.assert_called_once_with(
        f"{LDH_LINKED_DATA_URL}/{parse.quote_plus(urn)}",
        headers={"Accept": "application/json"},
    )
