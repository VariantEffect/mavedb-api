# ruff: noqa: E402

import pytest
from unittest import mock
from requests.exceptions import HTTPError

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.uniprot.id_mapping import UniProtIDMappingAPI

from tests.helpers.constants import (
    TEST_UNIPROT_JOB_ID,
    TEST_UNIPROT_JOB_SUBMISSION_RESPONSE,
    TEST_UNIPROT_JOB_SUBMISSION_ERROR_RESPONSE,
    TEST_UNIPROT_REDIRECT_RESPONSE,
    VALID_NT_ACCESSION,
    TEST_UNIPROT_FINISHED_JOB_STATUS_RESPONSE,
    TEST_UNIPROT_RUNNING_JOB_STATUS_RESPONSE,
    TEST_UNIPROT_ID_MAPPING_RESPONSE,
    TEST_UNIPROT_ID_FAILED_ID_MAPPING_RESPONSE,
)


@pytest.fixture
def uniprot_id_mapping_api():
    return UniProtIDMappingAPI(polling_interval=0, polling_tries=2)


### UniProtIDMappingAPI.submit_id_mapping tests


def test_submit_id_mapping_success(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "post") as mock_post:
        mock_post.return_value.json.return_value = TEST_UNIPROT_JOB_SUBMISSION_RESPONSE
        mock_post.return_value.raise_for_status = mock.Mock()
        job_id = uniprot_id_mapping_api.submit_id_mapping("RefSeq", "UniProtKB", ["ID1", "ID2"])
        assert job_id == TEST_UNIPROT_JOB_ID
        mock_post.assert_called_once()


def test_submit_id_mapping_no_ids(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "post") as mock_post:
        job_id = uniprot_id_mapping_api.submit_id_mapping("RefSeq", "UniProtKB", [])
        assert job_id is None
        mock_post.assert_not_called()


def test_submit_id_mapping_no_jobid(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "post") as mock_post:
        mock_post.return_value.json.return_value = {}
        mock_post.return_value.raise_for_status = mock.Mock()
        job_id = uniprot_id_mapping_api.submit_id_mapping("RefSeq", "UniProtKB", ["ID1"])
        assert job_id is None


def test_submit_id_mapping_http_error(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "post") as mock_post:
        mock_post.return_value.json.return_value = TEST_UNIPROT_JOB_SUBMISSION_ERROR_RESPONSE
        mock_post.return_value.raise_for_status = mock.Mock(side_effect=HTTPError("Mocked HTTP error"))

        with pytest.raises(HTTPError):
            uniprot_id_mapping_api.submit_id_mapping("RefSeq", "UniProtKB", ["ID1"])


### UniProtIDMappingAPI.check_id_mapping_results_ready tests


def test_check_id_mapping_results_ready_finished(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        mock_get.return_value.json.return_value = TEST_UNIPROT_FINISHED_JOB_STATUS_RESPONSE
        mock_get.return_value.raise_for_status = mock.Mock()
        assert uniprot_id_mapping_api.check_id_mapping_results_ready(VALID_NT_ACCESSION) is True


def test_check_id_mapping_results_ready_not_finished(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        # First call: not finished, Second call: not finished
        mock_get.return_value.json.return_value = TEST_UNIPROT_RUNNING_JOB_STATUS_RESPONSE
        mock_get.return_value.raise_for_status = mock.Mock()
        assert uniprot_id_mapping_api.check_id_mapping_results_ready(VALID_NT_ACCESSION) is False


def test_check_id_mapping_results_ready_results_key(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        mock_get.return_value.json.return_value = TEST_UNIPROT_ID_MAPPING_RESPONSE
        mock_get.return_value.raise_for_status = mock.Mock()
        assert uniprot_id_mapping_api.check_id_mapping_results_ready(VALID_NT_ACCESSION) is True


def test_check_id_mapping_results_ready_failed_ids(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        mock_get.return_value.json.return_value = TEST_UNIPROT_ID_FAILED_ID_MAPPING_RESPONSE
        mock_get.return_value.raise_for_status = mock.Mock()
        assert uniprot_id_mapping_api.check_id_mapping_results_ready(VALID_NT_ACCESSION) is True


def test_check_id_mapping_results_ready_retry(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        # First call: not finished, Second call: finished
        mock_get.side_effect = [
            mock.Mock(
                json=mock.Mock(return_value=TEST_UNIPROT_RUNNING_JOB_STATUS_RESPONSE), raise_for_status=mock.Mock()
            ),
            mock.Mock(
                json=mock.Mock(return_value=TEST_UNIPROT_FINISHED_JOB_STATUS_RESPONSE), raise_for_status=mock.Mock()
            ),
        ]
        assert uniprot_id_mapping_api.check_id_mapping_results_ready(VALID_NT_ACCESSION) is True


def test_check_id_mapping_results_ready_retry_not_ready(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        # Never finishes, retries until max attempts
        mock_get.side_effect = [
            mock.Mock(
                json=mock.Mock(return_value=TEST_UNIPROT_RUNNING_JOB_STATUS_RESPONSE), raise_for_status=mock.Mock()
            )
        ] * (uniprot_id_mapping_api.polling_tries + 1)
        assert uniprot_id_mapping_api.check_id_mapping_results_ready(VALID_NT_ACCESSION) is False


def test_check_id_mapping_results_ready_http_error(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        mock_get.return_value.raise_for_status = mock.Mock(side_effect=HTTPError("Mocked HTTP error"))

        with pytest.raises(HTTPError):
            uniprot_id_mapping_api.check_id_mapping_results_ready(VALID_NT_ACCESSION)


def test_check_id_mapping_results_ready_http_error_on_retry(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        # First call: not finished, Second call: http error
        mock_get.side_effect = [
            mock.Mock(
                json=mock.Mock(return_value=TEST_UNIPROT_RUNNING_JOB_STATUS_RESPONSE), raise_for_status=mock.Mock()
            ),
            mock.Mock(
                json=mock.Mock(return_value={}), raise_for_status=mock.Mock(side_effect=HTTPError("Mocked HTTP error"))
            ),
        ]

        with pytest.raises(HTTPError):
            uniprot_id_mapping_api.check_id_mapping_results_ready(VALID_NT_ACCESSION)


### UniProtIDMappingAPI.get_id_mapping_results tests


def test_get_id_mapping_results_success(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        # First call returns redirectURL, second call returns results
        mock_get.side_effect = [
            mock.Mock(json=mock.Mock(return_value=TEST_UNIPROT_REDIRECT_RESPONSE), raise_for_status=mock.Mock()),
            mock.Mock(
                json=mock.Mock(return_value=TEST_UNIPROT_ID_MAPPING_RESPONSE),
                raise_for_status=mock.Mock(),
            ),
        ]
        results = uniprot_id_mapping_api.get_id_mapping_results(VALID_NT_ACCESSION)
        assert "results" in results


def test_get_id_mapping_results_missing_redirect(uniprot_id_mapping_api: UniProtIDMappingAPI):
    redirect_response_without_url = TEST_UNIPROT_REDIRECT_RESPONSE.copy()
    redirect_response_without_url.pop("redirectURL")

    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        mock_get.return_value.json.return_value = redirect_response_without_url
        mock_get.return_value.raise_for_status = mock.Mock()
        with pytest.raises(KeyError):
            uniprot_id_mapping_api.get_id_mapping_results(VALID_NT_ACCESSION)


def test_get_id_mapping_results_http_error(uniprot_id_mapping_api: UniProtIDMappingAPI):
    with mock.patch.object(uniprot_id_mapping_api.session, "get") as mock_get:
        mock_get.side_effect = HTTPError("Mocked HTTP error")
        with pytest.raises(HTTPError):
            uniprot_id_mapping_api.get_id_mapping_results(VALID_NT_ACCESSION)


### UniProtIDMappingAPI.extract_uniprot_id_from_results tests


def test_extract_uniprot_id_from_result():
    results = {
        "results": [
            {"from": "A", "to": {"primaryAccession": "P1"}},
        ]
    }
    mappings = UniProtIDMappingAPI.extract_uniprot_id_from_results(results)
    assert mappings == [{"A": "P1"}]


def test_extract_uniprot_id_from_results():
    results = {
        "results": [
            {"from": "A", "to": {"primaryAccession": "P1"}},
            {"from": "B", "to": {"primaryAccession": "P2"}},
        ]
    }
    mappings = UniProtIDMappingAPI.extract_uniprot_id_from_results(results)
    assert mappings == [{"A": "P1"}, {"B": "P2"}]


def test_extract_uniprot_id_results_not_present():
    results = {
        "not_results": [
            {"from": "A", "to": {"primaryAccession": "P1"}},
            {"from": "B", "to": {"primaryAccession": "P2"}},
        ]
    }
    mappings = UniProtIDMappingAPI.extract_uniprot_id_from_results(results)
    assert mappings == []


def test_extract_uniprot_id_invalid_result_structure():
    results = {
        "results": [
            {"not_from": "A", "to": {"primaryAccession": "P1"}},
            {"from": "B", "not_to": {"primaryAccession": "P2"}},
            {"from": "B", "to": {"notPrimaryAccession": "P2"}},
        ]
    }
    mappings = UniProtIDMappingAPI.extract_uniprot_id_from_results(results)
    assert mappings == []


def test_extract_uniprot_id_from_results_empty():
    results = {}
    mappings = UniProtIDMappingAPI.extract_uniprot_id_from_results(results)
    assert mappings == []
