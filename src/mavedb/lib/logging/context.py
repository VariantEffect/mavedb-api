import logging
import os
import sys
import time
import traceback
from typing import Any, Optional, Union

from starlette.requests import HTTPConnection, Request
from starlette_context import context
from starlette_context.middleware import RawContextMiddleware

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

        # Retain plugin functionality.
        plugin_ctx = {plugin.key: await plugin.process_request(request) for plugin in self.plugins}

        return {**ctx, **plugin_ctx}


def save_to_logging_context(ctx: dict) -> dict:
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
                context[k] = [existing_ctx, v]
        else:
            context[k] = v

    return context.data


def logging_context() -> dict:
    if not context.exists():
        logger.debug("Could not access logging context. Context does not exist.")
        return {}

    return context.data


def correlation_id_for_context() -> Optional[str]:
    return logging_context().get("X-Correlation-ID", None)


def format_raised_exception_info_as_dict(err: BaseException) -> dict:
    _, _, tb = sys.exc_info()

    exc_ctx: dict = {
        "captured_exception_info": {
            "type": err.__class__.__name__,
            "string": str(err),
        }
    }

    try:
        exc_ctx["captured_exception_info"] = {
            **exc_ctx["captured_exception_info"],
            **[
                {"file": fs.filename, "line": fs.lineno, "func": fs.name}
                for fs in traceback.extract_tb(tb)
                # attempt to show only *our* code, not the many layers of library code
                if "/mavedb/" in fs.filename and "/.direnv/" not in fs.filename
            ][-1],
        }

    # We did our best to construct useful traceback info
    except IndexError:
        pass

    return exc_ctx
