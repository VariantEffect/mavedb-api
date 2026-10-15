# ruff: noqa: E402
from unittest.mock import patch

import pytest
import requests
import requests_mock

pytest.importorskip("boto3")
pytest.importorskip("cdot")
pytest.importorskip("hgvs")
pytest.importorskip("variant_annotation")

from mavedb.data_providers.services import CDOT_REQUEST_TIMEOUT, StrictRESTDataProvider, cdot_rest
from mavedb.worker.lib.translation_ports import WorkerCoordinateTranslator

CDOT_URL = "http://cdot.test"
TRANSCRIPT = "NM_007294.4"
TRANSCRIPT_URL = f"{CDOT_URL}/transcript/{TRANSCRIPT}"
GENOMIC_HGVS = "NC_000017.11:g.43045712T>C"
REGION_URL = f"{CDOT_URL}/transcripts/region/NC_000017.11/splign/43045711/43045712"
MANE_TRANSCRIPT_DATA = {"id": TRANSCRIPT, "genome_builds": {"GRCh38": {"tag": "MANE Select"}}}


@pytest.fixture
def provider() -> StrictRESTDataProvider:
    return StrictRESTDataProvider(url=CDOT_URL)


@pytest.mark.unit
def test_cdot_rest_builds_the_strict_provider():
    # The genome FASTA files behind the default seqfetcher exist only in the container.
    with patch("mavedb.data_providers.services.seqfetcher", return_value=None):
        assert isinstance(cdot_rest(), StrictRESTDataProvider)


@pytest.mark.unit
def test_a_missing_transcript_is_none_and_cached(provider):
    with requests_mock.Mocker() as m:
        m.get(TRANSCRIPT_URL, status_code=404)

        assert provider._get_transcript(TRANSCRIPT) is None
        assert provider._get_transcript(TRANSCRIPT) is None
        assert m.call_count == 1


@pytest.mark.unit
def test_an_outage_raises_and_is_not_cached(provider):
    with requests_mock.Mocker() as m:
        m.get(TRANSCRIPT_URL, status_code=503)
        with pytest.raises(requests.HTTPError):
            provider._get_transcript(TRANSCRIPT)

        m.get(TRANSCRIPT_URL, json=MANE_TRANSCRIPT_DATA)
        assert provider._get_transcript(TRANSCRIPT) == MANE_TRANSCRIPT_DATA


@pytest.mark.unit
def test_requests_carry_a_timeout(provider):
    with requests_mock.Mocker() as m:
        m.get(TRANSCRIPT_URL, json=MANE_TRANSCRIPT_DATA)
        provider._get_transcript(TRANSCRIPT)

        assert m.last_request.timeout == CDOT_REQUEST_TIMEOUT


@pytest.mark.unit
def test_mane_select_lookup_raises_during_an_outage(provider):
    coordinates = WorkerCoordinateTranslator(provider)
    with requests_mock.Mocker() as m:
        m.get(REGION_URL, status_code=503)

        with pytest.raises(requests.HTTPError):
            coordinates.mane_select_transcript(GENOMIC_HGVS)


@pytest.mark.unit
def test_mane_select_lookup_finds_the_tagged_transcript(provider):
    coordinates = WorkerCoordinateTranslator(provider)
    with requests_mock.Mocker() as m:
        m.get(REGION_URL, json={"results": [{"tx_ac": TRANSCRIPT}]})
        m.get(TRANSCRIPT_URL, json=MANE_TRANSCRIPT_DATA)

        assert coordinates.mane_select_transcript(GENOMIC_HGVS) == TRANSCRIPT


@pytest.mark.unit
def test_mane_select_lookup_skips_a_listed_transcript_cdot_no_longer_has(provider):
    coordinates = WorkerCoordinateTranslator(provider)
    with requests_mock.Mocker() as m:
        m.get(REGION_URL, json={"results": [{"tx_ac": TRANSCRIPT}]})
        m.get(TRANSCRIPT_URL, status_code=404)

        assert coordinates.mane_select_transcript(GENOMIC_HGVS) is None
