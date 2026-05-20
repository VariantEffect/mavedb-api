# ruff: noqa: E402

import pytest
from unittest import mock

import requests

from mavedb.lib.exceptions import HGNCGeneNotFoundError, HGNCServiceError
from mavedb.lib.hgnc.client import HGNCGeneInfo, fetch_gene_info


def _valid_doc(symbol="BRCA1"):
    return {
        "symbol": symbol,
        "name": "BRCA1 DNA repair associated",
        "hgnc_id": "HGNC:1100",
        "locus_group": "protein-coding gene",
        "location": "17q21.31",
        "omim_id": ["113705"],
    }


def _valid_response(symbol="BRCA1"):
    return {
        "response": {
            "numFound": 1,
            "docs": [_valid_doc(symbol)],
        }
    }


@pytest.mark.unit
@mock.patch("mavedb.lib.hgnc.client.requests.get")
class TestFetchGeneInfo:
    def test_valid_response_returns_gene_info(self, mock_get):
        mock_response = mock.Mock()
        mock_response.json.return_value = _valid_response()
        mock_get.return_value = mock_response

        result = fetch_gene_info("BRCA1")

        assert isinstance(result, HGNCGeneInfo)
        assert result.symbol == "BRCA1"
        assert result.name == "BRCA1 DNA repair associated"
        assert result.hgnc_id == "HGNC:1100"
        assert result.locus_group == "protein-coding gene"
        assert result.location == "17q21.31"
        assert result.omim_id == "113705"

    def test_optional_fields_absent_returns_none(self, mock_get):
        doc = {"symbol": "BRCA1", "name": "BRCA1 DNA repair associated"}
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"numFound": 1, "docs": [doc]}}
        mock_get.return_value = mock_response

        result = fetch_gene_info("BRCA1")

        assert result.hgnc_id is None
        assert result.locus_group is None
        assert result.location is None
        assert result.omim_id is None

    def test_hyphen_symbol_is_preserved_in_request_url(self, mock_get):
        mock_response = mock.Mock()
        mock_response.json.return_value = _valid_response("HLA-A")
        mock_get.return_value = mock_response

        fetch_gene_info("HLA-A")

        call_url = mock_get.call_args[0][0]
        assert "HLA-A" in call_url

    def test_empty_docs_raises_not_found(self, mock_get):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"numFound": 0, "docs": []}}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCGeneNotFoundError):
            fetch_gene_info("NOTAREAL")

    def test_http_error_raises_service_error(self, mock_get):
        mock_response = mock.Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("503 Service Unavailable")
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_connection_error_raises_service_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_invalid_json_raises_service_error(self, mock_get):
        mock_response = mock.Mock()
        mock_response.json.side_effect = ValueError("No JSON object could be decoded")
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_non_dict_top_level_response_raises_service_error(self, mock_get):
        mock_response = mock.Mock()
        mock_response.json.return_value = ["not", "a", "dict"]
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_missing_response_key_raises_service_error(self, mock_get):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"unexpected": "shape"}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_non_dict_response_data_raises_service_error(self, mock_get):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": "not a dict"}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_non_list_docs_raises_service_error(self, mock_get):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"docs": "not a list"}}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_non_dict_doc_raises_service_error(self, mock_get):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"docs": ["not a dict"]}}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_missing_symbol_key_falls_back_to_input(self, mock_get):
        """When the doc lacks a 'symbol' key, the input symbol is used as a fallback."""
        doc = {"name": "BRCA1 DNA repair associated"}
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"docs": [doc]}}
        mock_get.return_value = mock_response

        result = fetch_gene_info("BRCA1")

        assert result.symbol == "BRCA1"

    def test_empty_symbol_raises_service_error(self, mock_get):
        doc = {**_valid_doc(), "symbol": ""}
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"docs": [doc]}}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_missing_name_raises_service_error(self, mock_get):
        doc = {"symbol": "BRCA1"}
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"docs": [doc]}}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_non_list_omim_id_raises_service_error(self, mock_get):
        doc = {**_valid_doc(), "omim_id": "113705"}
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"docs": [doc]}}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_non_string_omim_id_element_raises_service_error(self, mock_get):
        doc = {**_valid_doc(), "omim_id": [113705]}
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"docs": [doc]}}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")

    def test_non_string_optional_field_raises_service_error(self, mock_get):
        doc = {**_valid_doc(), "locus_group": ["protein-coding gene"]}
        mock_response = mock.Mock()
        mock_response.json.return_value = {"response": {"docs": [doc]}}
        mock_get.return_value = mock_response

        with pytest.raises(HGNCServiceError):
            fetch_gene_info("BRCA1")
