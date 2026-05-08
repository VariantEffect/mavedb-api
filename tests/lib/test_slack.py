# ruff: noqa: E402

"""Tests for Slack notification utilities."""

from unittest.mock import patch

import pytest

pytest.importorskip("slack_sdk", reason="slack_sdk is required to test Slack notification utilities")

from mavedb.lib.slack import _retry_status_text, send_slack_error, send_slack_job_error, send_slack_job_failure


@pytest.mark.unit
class TestSendSlackError:
    """Tests for send_slack_error resilience."""

    def test_send_slack_error_does_not_propagate_exceptions(self):
        """send_slack_error should catch and log any internal exceptions rather than propagating them."""
        with (
            patch("mavedb.lib.slack.send_slack_message", side_effect=RuntimeError("Slack is down")),
            patch("mavedb.lib.slack.logger") as mock_logger,
        ):
            # Should not raise
            send_slack_error(ValueError("original error"))

        mock_logger.critical.assert_called_once_with("Failed to send Slack error notification", exc_info=True)

    def test_send_slack_error_calls_send_slack_message(self):
        """send_slack_error should format and send the error via send_slack_message."""
        with patch("mavedb.lib.slack.send_slack_message") as mock_send:
            send_slack_error(ValueError("test error"))

        mock_send.assert_called_once()
        sent_text = mock_send.call_args[0][0]
        assert "ValueError" in sent_text
        assert "test error" in sent_text

    def test_send_slack_error_with_string_error(self):
        """send_slack_error should handle non-exception inputs gracefully."""
        with patch("mavedb.lib.slack.send_slack_message") as mock_send:
            send_slack_error("plain string error")

        mock_send.assert_called_once()
        sent_text = mock_send.call_args[0][0]
        assert "plain string error" in sent_text


@pytest.mark.unit
class TestRetryStatusText:
    """Tests for _retry_status_text helper."""

    def test_will_retry_first_attempt(self):
        assert _retry_status_text(retry_count=0, max_retries=3, will_retry=True) == "Attempt 1 of 4 — will retry"

    def test_will_retry_second_attempt(self):
        assert _retry_status_text(retry_count=1, max_retries=3, will_retry=True) == "Attempt 2 of 4 — will retry"

    def test_final_retry_exhausted(self):
        assert (
            _retry_status_text(retry_count=3, max_retries=3, will_retry=False)
            == "Attempt 4 of 4 — this job will not be retried"
        )

    def test_no_retries_configured(self):
        assert (
            _retry_status_text(retry_count=0, max_retries=0, will_retry=False)
            == "Attempt 1 of 1 — this job will not be retried"
        )


@pytest.mark.unit
class TestSendSlackJobFailure:
    """Tests for send_slack_job_failure."""

    def test_includes_retry_context_when_will_retry(self):
        with patch("mavedb.lib.slack._send_slack_blocks") as mock_send:
            send_slack_job_failure(
                job_urn="urn:mavedb:00000001-a-1",
                job_function="map_variants",
                reason="timeout",
                failure_category="TIMEOUT",
                retry_count=0,
                max_retries=3,
                will_retry=True,
            )

        mock_send.assert_called_once()
        fallback, blocks = mock_send.call_args[0]
        assert "will retry" in fallback
        fields = blocks[1]["fields"]
        retry_field = next(f for f in fields if "*Retry*" in f["text"])
        assert "Attempt 1 of 4" in retry_field["text"]
        assert "will retry" in retry_field["text"]

    def test_includes_retry_context_when_no_more_retries(self):
        with patch("mavedb.lib.slack._send_slack_blocks") as mock_send:
            send_slack_job_failure(
                job_urn="urn:mavedb:00000001-a-1",
                job_function="map_variants",
                reason="timeout",
                failure_category="TIMEOUT",
                retry_count=3,
                max_retries=3,
                will_retry=False,
            )

        mock_send.assert_called_once()
        fallback, blocks = mock_send.call_args[0]
        assert "will not be retried" in fallback
        fields = blocks[1]["fields"]
        retry_field = next(f for f in fields if "*Retry*" in f["text"])
        assert "Attempt 4 of 4" in retry_field["text"]
        assert "will not be retried" in retry_field["text"]

    def test_defaults_produce_no_retry_text(self):
        """Default parameters (retry_count=0, max_retries=0, will_retry=False) show attempt 1 of 1."""
        with patch("mavedb.lib.slack._send_slack_blocks") as mock_send:
            send_slack_job_failure(
                job_urn="urn:mavedb:00000001-a-1",
                job_function="map_variants",
                reason="bad data",
                failure_category="VALIDATION_ERROR",
            )

        mock_send.assert_called_once()
        _, blocks = mock_send.call_args[0]
        fields = blocks[1]["fields"]
        retry_field = next(f for f in fields if "*Retry*" in f["text"])
        assert "Attempt 1 of 1" in retry_field["text"]

    def test_does_not_propagate_exceptions(self):
        with (
            patch("mavedb.lib.slack._send_slack_blocks", side_effect=RuntimeError("Slack is down")),
            patch("mavedb.lib.slack.logger") as mock_logger,
        ):
            send_slack_job_failure(
                job_urn="urn:test",
                job_function="fn",
                reason="r",
                failure_category="c",
            )

        mock_logger.critical.assert_called_once_with("Failed to send Slack job failure notification", exc_info=True)


@pytest.mark.unit
class TestSendSlackJobError:
    """Tests for send_slack_job_error."""

    def test_includes_retry_context_when_will_retry(self):
        with patch("mavedb.lib.slack._send_slack_blocks") as mock_send:
            send_slack_job_error(
                job_urn="urn:mavedb:00000001-a-1",
                job_function="create_variants",
                err=RuntimeError("boom"),
                failure_category="NETWORK_ERROR",
                retry_count=1,
                max_retries=3,
                will_retry=True,
            )

        mock_send.assert_called_once()
        fallback, blocks = mock_send.call_args[0]
        assert "will retry" in fallback
        fields = blocks[1]["fields"]
        retry_field = next(f for f in fields if "*Retry*" in f["text"])
        assert "Attempt 2 of 4" in retry_field["text"]
        assert "will retry" in retry_field["text"]

    def test_includes_retry_context_when_exhausted(self):
        with patch("mavedb.lib.slack._send_slack_blocks") as mock_send:
            send_slack_job_error(
                job_urn="urn:mavedb:00000001-a-1",
                job_function="create_variants",
                err=RuntimeError("boom"),
                failure_category="NETWORK_ERROR",
                retry_count=3,
                max_retries=3,
                will_retry=False,
            )

        mock_send.assert_called_once()
        _, blocks = mock_send.call_args[0]
        fields = blocks[1]["fields"]
        retry_field = next(f for f in fields if "*Retry*" in f["text"])
        assert "Attempt 4 of 4" in retry_field["text"]
        assert "will not be retried" in retry_field["text"]

    def test_does_not_propagate_exceptions(self):
        with (
            patch("mavedb.lib.slack._send_slack_blocks", side_effect=RuntimeError("Slack is down")),
            patch("mavedb.lib.slack.logger") as mock_logger,
        ):
            send_slack_job_error(job_urn="urn:test", job_function="fn", err=ValueError("e"))

        mock_logger.critical.assert_called_once_with("Failed to send Slack job error notification", exc_info=True)
