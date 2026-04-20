# ruff: noqa: E402

"""Tests for Slack notification utilities."""

from unittest.mock import patch

import pytest

pytest.importorskip("slack_sdk", reason="slack_sdk is required to test Slack notification utilities")

from mavedb.lib.slack import send_slack_error


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
