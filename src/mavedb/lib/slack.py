import json
import logging
import os
import sys
import traceback
from typing import Any

from slack_sdk.webhook import WebhookClient

logger = logging.getLogger(__name__)

_BLOCK_TEXT_MAX = 2000


def find_traceback_locations():
    _, _, tb = sys.exc_info()
    return [
        (fs.filename, fs.lineno, fs.name)
        for fs in traceback.extract_tb(tb)
        # attempt to show only *our* code, not the many layers of library code
        if "/mavedb/" in fs.filename and "/.direnv/" not in fs.filename
    ]


def _send_slack_blocks(fallback_text: str, blocks: list[dict]) -> None:
    """Send a Slack message with Block Kit formatting. Falls back to print when no webhook URL is set."""
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook_url:
        client = WebhookClient(url=slack_webhook_url)
        client.send(text=fallback_text, blocks=blocks)
    else:
        print(f"SLACK: {fallback_text}")


def send_slack_message(text: str):
    _send_slack_blocks(
        fallback_text=text,
        blocks=[{"type": "section", "text": {"type": "plain_text", "text": text}}],
    )


def send_slack_error(err, request=None):
    try:
        text = {"type": err.__class__.__name__, "exception": str(err), "location": find_traceback_locations()}

        if request:
            text["client"] = str(request.client.host)
            text["request"] = f"{request.method} {request.url}"

        text = json.dumps(text)
        send_slack_message(text)
    except Exception:
        logger.critical("Failed to send Slack error notification", exc_info=True)


def _retry_status_text(retry_count: int, max_retries: int, will_retry: bool) -> str:
    """Format a human-readable retry status string for Slack notifications.

    retry_count is 0-indexed (0 = first attempt). total attempts = max_retries + 1.
    """
    attempt = retry_count + 1
    total = max_retries + 1
    if will_retry:
        return f"Attempt {attempt} of {total} — will retry"

    return f"Attempt {attempt} of {total} — this job will not be retried"


def send_slack_job_failure(
    job_urn: str,
    job_function: str,
    reason: str,
    failure_category: str,
    retry_count: int = 0,
    max_retries: int = 0,
    will_retry: bool = False,
) -> None:
    """Send a structured Slack alert for a controlled job failure (FAILED outcome)."""
    try:
        retry_text = _retry_status_text(retry_count, max_retries, will_retry)
        blocks: list[dict] = [
            {"type": "header", "text": {"type": "plain_text", "text": "⚠️ Job Failed"}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Job URN*\n`{job_urn}`"},
                    {"type": "mrkdwn", "text": f"*Function*\n`{job_function}`"},
                    {"type": "mrkdwn", "text": f"*Category*\n{failure_category or 'unknown'}"},
                    {"type": "mrkdwn", "text": f"*Retry*\n{retry_text}"},
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Reason*\n{reason or 'No reason provided'}"[:_BLOCK_TEXT_MAX],
                },
            },
        ]
        fallback = f"Job Failed: {job_urn} ({job_function}) — {reason} [{retry_text}]"
        _send_slack_blocks(fallback, blocks)
    except Exception:
        logger.critical("Failed to send Slack job failure notification", exc_info=True)


def send_slack_job_error(
    job_urn: str,
    job_function: str,
    err: Exception,
    failure_category: str = "",
    retry_count: int = 0,
    max_retries: int = 0,
    will_retry: bool = False,
) -> None:
    """Send a structured Slack alert for an unhandled job exception (ERRORED outcome)."""
    try:
        locations = find_traceback_locations()
        location_lines = [f"`{fn}:{lineno}` in `{name}`" for fn, lineno, name in locations]
        retry_text = _retry_status_text(retry_count, max_retries, will_retry)

        blocks: list[dict] = [
            {"type": "header", "text": {"type": "plain_text", "text": "\U0001f6a8 Job Errored"}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Job URN*\n`{job_urn}`"},
                    {"type": "mrkdwn", "text": f"*Function*\n`{job_function}`"},
                    {"type": "mrkdwn", "text": f"*Exception*\n`{err.__class__.__name__}`"},
                    {"type": "mrkdwn", "text": f"*Category*\n{failure_category or 'unknown'}"},
                    {"type": "mrkdwn", "text": f"*Retry*\n{retry_text}"},
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Message*\n```{str(err)}```"[:_BLOCK_TEXT_MAX],
                },
            },
        ]
        if location_lines:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ("*Location*\n" + "\n".join(location_lines))[:_BLOCK_TEXT_MAX],
                    },
                }
            )

        fallback = f"Job Errored: {job_urn} ({job_function}) — {err.__class__.__name__}: {err} [{retry_text}]"
        _send_slack_blocks(fallback, blocks)
    except Exception:
        logger.critical("Failed to send Slack job error notification", exc_info=True)


def log_and_send_slack_message(msg: str, ctx: dict[str, Any], level: int):
    """
    Log a message and send it to Slack if the SLACK_WEBHOOK_URL environment variable is set.
    """
    logger.log(level, msg, extra=ctx)

    if os.getenv("SLACK_WEBHOOK_URL"):
        send_slack_message(msg)
    else:
        print(f"SLACK_WEBHOOK_URL not set, not sending message: {msg}.")
