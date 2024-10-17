import json
import os
import sys
import traceback

from slack_sdk.webhook import WebhookClient


def find_traceback_locations():
    _, _, tb = sys.exc_info()
    return [
        (fs.filename, fs.lineno, fs.name)
        for fs in traceback.extract_tb(tb)
        # attempt to show only *our* code, not the many layers of library code
        if "/mavedb/" in fs.filename and "/.direnv/" not in fs.filename
    ]


def send_slack_message(err, request=None):
    text = {"type": err.__class__.__name__, "exception": str(err), "location": find_traceback_locations()}

    if request:
        text["client"] = str(request.client.host)
        text["request"] = f"{request.method} {request.url}"

    text = json.dumps(text)
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook_url is not None and len(slack_webhook_url) > 0:
        client = WebhookClient(url=slack_webhook_url)
        client.send(
            text=text,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "plain_text", "text": text},
                }
            ],
        )
    else:
        print(f"EXCEPTION_HANDLER: {text}")
