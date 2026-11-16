"""Middleware that opens the logging context for each request."""

import os
import time
import uuid
from typing import Any

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from mavedb import __project__, __version__
from mavedb.lib.logging.context import logging_context_scope
from mavedb.lib.logging.models import Source

FRONTEND_URL = os.getenv("FRONTEND_URL", "")
API_URL = os.getenv("API_URL", "")

CORRELATION_ID_HEADER = "X-Correlation-ID"
REQUEST_ID_HEADER = "X-Request-ID"
USER_AGENT_HEADER = "User-Agent"

# Echoed on every response so a caller can quote the id that finds their request in the logs.
_RESPONSE_ID_HEADERS = (CORRELATION_ID_HEADER, REQUEST_ID_HEADER)


class RequestContextMiddleware:
    """Open a logging context for each HTTP or websocket request and seed it with the request's identity.

    Both ids are minted fresh for every request; ids sent by the caller are ignored. Pure ASGI, like
    ``CatchAllErrorMiddleware``, so streamed responses pass through untouched.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        with logging_context_scope(self._initial_context(scope)) as ctx:

            async def send_wrapper(message: Message) -> None:
                if message["type"] == "http.response.start":
                    headers = MutableHeaders(scope=message)
                    for key in _RESPONSE_ID_HEADERS:
                        headers.append(key, str(ctx.get(key)))
                await send(message)

            await self.app(scope, receive, send_wrapper)

    @staticmethod
    def _initial_context(scope: Scope) -> dict[str, Any]:
        headers = Headers(scope=scope)

        source = Source.other
        if headers.get("origin") == FRONTEND_URL:
            source = Source.web
        elif headers.get("referer") == API_URL + "/docs":
            source = Source.docs

        ctx: dict[str, Any] = {
            "request_ns": time.time_ns(),
            # The ASGI scope rather than request.url.path, which truncates at a '#'. Starlette builds
            # request.url by re-parsing, so a variant URN's '#' opens a fragment there and the variant
            # number and sub-resource are dropped: three /variants routes all logged the same path.
            "path": scope["path"],
            "source": source,
            "application": __project__,
            "version": __version__,
            CORRELATION_ID_HEADER: uuid.uuid4().hex,
            REQUEST_ID_HEADER: uuid.uuid4().hex,
            USER_AGENT_HEADER: headers.get(USER_AGENT_HEADER),
        }
        # Websocket scopes carry no method.
        if "method" in scope:
            ctx["method"] = scope["method"]

        return ctx
