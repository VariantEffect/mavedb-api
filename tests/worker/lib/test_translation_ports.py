# ruff: noqa: E402
import pytest

pytest.importorskip("arq")

from unittest.mock import MagicMock, patch

from mavedb.worker.lib import translation_ports
from mavedb.worker.lib.translation_ports import uta_transcript_source


@pytest.mark.unit
class TestUtaTranscriptSource:
    def test_retries_long_enough_to_ride_out_a_uta_outage(self, monkeypatch):
        monkeypatch.setenv("UTA_DB_URL", "postgresql://anonymous@uta.example.org:5432/uta/uta_20241220")
        client = MagicMock()
        client.__enter__.return_value = client

        with patch.object(translation_ports.UtaClient, "from_url", return_value=client) as from_url:
            with uta_transcript_source() as source:
                assert source is client

        from_url.assert_called_once_with(
            "postgresql://anonymous@uta.example.org:5432/uta/uta_20241220", max_attempts=6, backoff_seconds=2.0
        )

    def test_requires_uta_db_url(self, monkeypatch):
        monkeypatch.delenv("UTA_DB_URL", raising=False)

        with pytest.raises(RuntimeError, match="UTA_DB_URL must be set"):
            with uta_transcript_source():
                pass
