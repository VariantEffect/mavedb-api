import json
import logging
import time
import os
from typing import Any, Union, Optional


from starlette.requests import Request, HTTPConnection
from starlette_context.middleware import RawContextMiddleware
from starlette_context import context

from mavedb import __project__, __version__
from mavedb.lib.logging.models import Source


FRONTEND_URL = os.getenv("FRONTEND_URL", "")
API_URL = os.getenv("API_URL", "")

logger = logging.getLogger(__name__)


class PopulatedRawContextMiddleware(RawContextMiddleware):
    async def set_context(self, request: Union[Request, HTTPConnection]) -> dict:
        ctx: dict[str, Any] = {}

        ctx["request_ns"] = time.time_ns()
        ctx["path"] = request.url.path

        if isinstance(request, Request):
            ctx["method"] = request.method
        else:
            try:
                ctx["method"] = request.scope["method"]
            except KeyError:
                pass

        source = Source.other
        if request.headers.get("origin") == FRONTEND_URL:
            source = Source.web
        elif request.headers.get("referer") == API_URL + "/docs":
            source = Source.docs

        ctx["source"] = source
        ctx["application"] = __project__
        ctx["version"] = __version__

        ctx["host"] = request.client.host if request.client else None

        # Retain plugin functionality.
        plugin_ctx = {plugin.key: await plugin.process_request(request) for plugin in self.plugins}

        return {**ctx, **plugin_ctx}


def save_to_context(ctx: dict) -> dict:
    if not context.exists():
        logger.debug("Skipped saving to context. Context does not exist.")
        return {}

    for k, v in ctx.items():
        # Don't overwrite existing context mappings but create a list if a duplicated key is added.
        if k in context:
            existing_ctx = context[k]
            if isinstance(existing_ctx, list):
                context[k].append(v)
            else:
                context[k] = [existing_ctx, k]
        else:
            context[k] = v

    return context.data


def logging_context() -> dict:
    if not context.exists():
        logger.debug("Could not access logging context. Context does not exist.")
        return {}

    return context.data


def dump_context() -> str:
    return json.dumps(logging_context())


def correlation_id_for_context() -> Optional[str]:
    return logging_context().get("X-Correlation-ID", None)
