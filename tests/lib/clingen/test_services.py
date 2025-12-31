# ruff: noqa: E402

import os
from datetime import datetime
from unittest.mock import MagicMock, patch
from urllib import parse

import pytest
import requests

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.clingen.constants import GENBOREE_ACCOUNT_NAME, GENBOREE_ACCOUNT_PASSWORD, LDH_MAVE_ACCESS_ENDPOINT
from mavedb.lib.clingen.services import (
    ClinGenAlleleRegistryService,
    ClinGenLdhService,
    clingen_allele_id_from_ldh_variation,
    get_allele_registry_associations,
    get_clingen_variation,
)
from mavedb.lib.utils import batched
from tests.helpers.constants import VALID_CLINGEN_CA_ID

TEST_CLINGEN_URL = "https://pytest.clingen.com"
TEST_CAR_URL = "https://pytest.car.clingen.com"


@pytest.fixture
def clingen_service():
    yield ClinGenLdhService(url=TEST_CLINGEN_URL)


@pytest.fixture
def car_service():
    return ClinGenAlleleRegistryService(url=TEST_CAR_URL)


class TestClinGenLdhService:
    def test_init(self, clingen_service):
        assert clingen_service.url == TEST_CLINGEN_URL

    ### Test the authenticate method

    def test_authenticate_with_existing_jwt(self, clingen_service: ClinGenLdhService):
        with patch.object(ClinGenLdhService, "_existing_jwt", return_value="existing_jwt_token") as mock_existing_jwt:
            jwt = clingen_service.authenticate()

        assert jwt == "existing_jwt_token"
        mock_existing_jwt.assert_called_once()

    @patch("mavedb.lib.clingen.services.requests.post")
    @patch("mavedb.lib.clingen.services.ClinGenLdhService._existing_jwt")
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
            f"https://genboree.org/auth/usr/gb:{GENBOREE_ACCOUNT_NAME}/auth",
            json={"type": "plain", "val": GENBOREE_ACCOUNT_PASSWORD},
        )

    @patch("mavedb.lib.clingen.services.requests.post")
    @patch("mavedb.lib.clingen.services.ClinGenLdhService._existing_jwt")
    def test_authenticate_http_error(self, mock_existing_jwt, mock_post, clingen_service):
        mock_existing_jwt.return_value = None

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP Error")
        mock_post.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError, match="HTTP Error"):
            clingen_service.authenticate()

        mock_post.assert_called_once()

    @patch("mavedb.lib.clingen.services.requests.post")
    @patch("mavedb.lib.clingen.services.ClinGenLdhService._existing_jwt")
    def test_authenticate_missing_jwt_in_response(self, mock_existing_jwt, mock_post, clingen_service):
        mock_existing_jwt.return_value = None

        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {}}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        with pytest.raises(ValueError, match="Could not parse JWT from valid response"):
            clingen_service.authenticate()

        mock_post.assert_called_once()

    ### Test the _existing_jwt method

    @patch("mavedb.lib.clingen.services.os.getenv")
    @patch("mavedb.lib.clingen.services.jwt.get_unverified_claims")
    def test_existing_jwt_valid(self, mock_get_unverified_claims, mock_getenv, clingen_service):
        mock_getenv.return_value = "valid_jwt_token"
        mock_get_unverified_claims.return_value = {"exp": (datetime.now().timestamp() + 3600)}

        jwt = clingen_service._existing_jwt()

        assert jwt == "valid_jwt_token"
        mock_getenv.assert_called_once_with("GENBOREE_JWT")
        mock_get_unverified_claims.assert_called_once_with("valid_jwt_token")

    @patch("mavedb.lib.clingen.services.os.getenv")
    @patch("mavedb.lib.clingen.services.jwt.get_unverified_claims")
    def test_existing_jwt_expired(self, mock_get_unverified_claims, mock_getenv, clingen_service):
        mock_getenv.return_value = "expired_jwt_token"
        mock_get_unverified_claims.return_value = {"exp": (datetime.now().timestamp() - 3600)}

        jwt = clingen_service._existing_jwt()

        assert jwt is None
        mock_getenv.assert_called_once_with("GENBOREE_JWT")
        mock_get_unverified_claims.assert_called_once_with("expired_jwt_token")

    @patch("mavedb.lib.clingen.services.os.getenv")
    def test_existing_jwt_not_set(self, mock_getenv, clingen_service):
        mock_getenv.return_value = None

        jwt = clingen_service._existing_jwt()

        assert jwt is None
        mock_getenv.assert_called_once_with("GENBOREE_JWT")

    ### Test the dispatch_submissions method

    @patch("mavedb.lib.clingen.services.requests.put")
    @patch("mavedb.lib.clingen.services.ClinGenLdhService.authenticate")
    @patch("mavedb.lib.clingen.services.batched")
    def test_dispatch_submissions_success(self, mock_batched, mock_authenticate, mock_request, clingen_service):
        mock_authenticate.return_value = "test_jwt_token"
        mock_request.return_value.json.return_value = {"success": True}

        content_submissions = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_batched.return_value = [[{"id": 1}, {"id": 2}], [{"id": 3}]]  # Simulate batching

        batch_size = 2
        successes, failures = clingen_service.dispatch_submissions(content_submissions, batch_size=batch_size)

        assert len(successes) == 2  # 2 batches
        assert len(failures) == 0
        mock_batched.assert_called_once_with(content_submissions, 2)
        for submission in batched(content_submissions, batch_size):
            mock_request.assert_any_call(
                url=clingen_service.url,
                json=submission,
                headers={"Authorization": "Bearer test_jwt_token", "Content-Type": "application/json"},
            )

    @patch("mavedb.lib.clingen.services.requests.put")
    @patch("mavedb.lib.clingen.services.ClinGenLdhService.authenticate")
    def test_dispatch_submissions_failure(self, mock_authenticate, mock_request, clingen_service):
        mock_authenticate.return_value = "test_jwt_token"
        mock_request.side_effect = requests.exceptions.RequestException("Request failed")

        content_submissions = [{"id": 1}, {"id": 2}, {"id": 3}]

        successes, failures = clingen_service.dispatch_submissions(content_submissions)

        assert len(successes) == 0
        assert len(failures) == 3
        for submission in content_submissions:
            mock_request.assert_any_call(
                url=clingen_service.url,
                json=submission,
                headers={"Authorization": "Bearer test_jwt_token", "Content-Type": "application/json"},
            )

    @patch("mavedb.lib.clingen.services.requests.put")
    @patch("mavedb.lib.clingen.services.ClinGenLdhService.authenticate")
    def test_dispatch_submissions_partial_success(self, mock_authenticate, mock_request, clingen_service):
        mock_authenticate.return_value = "test_jwt_token"

        def mock_request_side_effect(*args, **kwargs):
            if kwargs["json"]["id"] == 2:
                raise requests.exceptions.RequestException("Request failed")
            return MagicMock(json=MagicMock(return_value={"success": True}))

        mock_request.side_effect = mock_request_side_effect

        content_submissions = [{"id": 1}, {"id": 2}, {"id": 3}]

        successes, failures = clingen_service.dispatch_submissions(content_submissions)

        assert len(successes) == 2
        assert len(failures) == 1
        assert failures[0]["id"] == 2

    @patch("mavedb.lib.clingen.services.requests.put")
    @patch("mavedb.lib.clingen.services.ClinGenLdhService.authenticate")
    @patch("mavedb.lib.clingen.services.batched")
    def test_dispatch_submissions_no_batching(self, mock_batched, mock_authenticate, mock_request, clingen_service):
        mock_authenticate.return_value = "test_jwt_token"
        mock_request.return_value.json.return_value = {"success": True}

        content_submissions = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_batched.return_value = content_submissions  # No batching

        successes, failures = clingen_service.dispatch_submissions(content_submissions)

        assert len(successes) == 3
        assert len(failures) == 0
        mock_batched.assert_not_called()
        for submission in content_submissions:
            mock_request.assert_any_call(
                url=clingen_service.url,
                json=submission,
                headers={"Authorization": "Bearer test_jwt_token", "Content-Type": "application/json"},
            )


@patch("mavedb.lib.clingen.services.requests.get")
def test_get_clingen_variation_success(mock_get):
    mocked_response_json = {"data": {"ldFor": {"Variant": [{"id": "variant_1", "name": "Test Variant"}]}}}
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mocked_response_json
    mock_get.return_value = mock_response

    urn = "urn:example:variant"
    result = get_clingen_variation(urn)

    assert result == mocked_response_json
    mock_get.assert_called_once_with(
        f"{LDH_MAVE_ACCESS_ENDPOINT}/{parse.quote_plus(urn)}",
        headers={"Accept": "application/json"},
    )


@patch("mavedb.lib.clingen.services.requests.get")
def test_get_clingen_variation_failure(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_get.return_value = mock_response

    urn = "urn:example:nonexistent_variant"
    result = get_clingen_variation(urn)

    assert result is None
    mock_get.assert_called_once_with(
        f"{LDH_MAVE_ACCESS_ENDPOINT}/{parse.quote_plus(urn)}",
        headers={"Accept": "application/json"},
    )


def test_clingen_allele_id_from_ldh_variation_success():
    variation = {"data": {"ldFor": {"Variant": [{"entId": VALID_CLINGEN_CA_ID}]}}}
    result = clingen_allele_id_from_ldh_variation(variation)
    assert result == VALID_CLINGEN_CA_ID


def test_clingen_allele_id_from_ldh_variation_missing_key():
    variation = {"data": {"ldFor": {"Variant": []}}}

    result = clingen_allele_id_from_ldh_variation(variation)
    assert result is None


def test_clingen_allele_id_from_ldh_variation_no_variation():
    result = clingen_allele_id_from_ldh_variation(None)
    assert result is None


def test_clingen_allele_id_from_ldh_variation_key_error():
    variation = {"data": {}}

    result = clingen_allele_id_from_ldh_variation(variation)
    assert result is None


class TestClinGenAlleleRegistryService:
    def test_init(self, car_service):
        assert car_service.url == TEST_CAR_URL

    @patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_NAME", "testuser")
    @patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_PASSWORD", "testpass")
    def test_construct_auth_url(self, car_service):
        url = "https://example.com/api?param=1"
        result = car_service.construct_auth_url(url)
        assert result.startswith(url)
        assert "gbLogin=testuser" in result
        assert "gbTime=" in result
        assert "gbToken=" in result

    @patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_NAME", None)
    @patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_PASSWORD", None)
    def test_construct_auth_url_missing_env(self, car_service):
        with pytest.raises(ValueError, match="Genboree account name and password must be set"):
            car_service.construct_auth_url("https://example.com/api")

    @patch("mavedb.lib.clingen.services.requests.put")
    @patch("mavedb.lib.clingen.services.ClinGenAlleleRegistryService.construct_auth_url")
    def test_dispatch_submissions_success(self, mock_auth_url, mock_put, car_service):
        mock_auth_url.return_value = "https://example.com/api?auth"
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "@id": "http://reg.test.genome.network/allele/CA123",
                "genomicAlleles": [{"hgvs": "NM_0001:c.1A>G"}],
                "transcriptAlleles": [],
            }
        ]
        mock_response.raise_for_status = MagicMock()
        mock_put.return_value = mock_response

        content_submissions = ["NM_0001:c.1A>G"]
        result = car_service.dispatch_submissions(content_submissions)
        assert isinstance(result, list)
        assert result[0]["@id"].endswith("CA123")
        mock_put.assert_called_once()

    @patch("mavedb.lib.clingen.services.requests.put")
    @patch("mavedb.lib.clingen.services.ClinGenAlleleRegistryService.construct_auth_url")
    def test_dispatch_submissions_failure(self, mock_auth_url, mock_put, car_service):
        mock_auth_url.return_value = "https://example.com/api?auth"
        mock_put.side_effect = requests.exceptions.RequestException("Failed")

        content_submissions = ["NM_0001:c.1A>G"]
        result = car_service.dispatch_submissions(content_submissions)
        assert result == []


def test_get_allele_registry_associations_success():
    content_submissions = ["NM_0001:c.1A>G", "NM_0002:c.2T>C"]
    submission_response = [
        {
            "@id": "http://reg.test.genome.network/allele/CA123",
            "genomicAlleles": [{"hgvs": "NM_0001:c.1A>G"}],
            "transcriptAlleles": [],
        },
        {
            "@id": "http://reg.test.genome.network/allele/CA456",
            "genomicAlleles": [],
            "transcriptAlleles": [{"hgvs": "NM_0002:c.2T>C"}],
        },
    ]
    result = get_allele_registry_associations(content_submissions, submission_response)
    assert result == {"NM_0001:c.1A>G": "CA123", "NM_0002:c.2T>C": "CA456"}


def test_get_allele_registry_associations_empty():
    result = get_allele_registry_associations([], [])
    assert result == {}


def test_get_allele_registry_associations_no_match():
    content_submissions = ["NM_0001:c.1A>G"]
    submission_response = [
        {
            "@id": "http://reg.test.genome.network/allele/CA123",
            "genomicAlleles": [{"hgvs": "NM_0002:c.2T>C"}],
            "transcriptAlleles": [],
        }
    ]
    result = get_allele_registry_associations(content_submissions, submission_response)
    assert result == {}


def test_get_allele_registry_associations_mixed():
    content_submissions = ["NM_0001:c.1A>G", "NM_0002:c.2T>C", "NM_0003:c.3G>A"]
    submission_response = [
        {
            "@id": "http://reg.test.genome.network/allele/CA123",
            "genomicAlleles": [{"hgvs": "NM_0001:c.1A>G"}],
            "transcriptAlleles": [],
        },
        {
            "errorType": "InvalidHGVS",
            "hgvs": "NM_0002:c.2T>C",
            "message": "The HGVS string is invalid.",
        },
        {
            "@id": "http://reg.test.genome.network/allele/CA789",
            "genomicAlleles": [],
            "transcriptAlleles": [{"hgvs": "NM_0003:c.3G>A"}],
        },
    ]

    result = get_allele_registry_associations(content_submissions, submission_response)
    assert result == {"NM_0001:c.1A>G": "CA123", "NM_0003:c.3G>A": "CA789"}
