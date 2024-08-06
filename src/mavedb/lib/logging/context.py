import json
import logging
import time
import os
import sys
import traceback

from contextlib import contextmanager
from typing import Any, Generator, Union, Optional


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

        # Retain plugin functionality.
        plugin_ctx = {plugin.key: await plugin.process_request(request) for plugin in self.plugins}

        return {**ctx, **plugin_ctx}


@contextmanager
def managed_global_context(managed_ctx: dict, **kwargs) -> Generator[dict, Any, None]:
    global_context = logging_context()

    # Retain any colliding context.
    existing_data = {}
    for k, v in managed_ctx.items():
        if k in global_context.keys():
            existing_data[k] = global_context[k]

        global_context[k] = v

    try:
        yield global_context

    # Return global context to its previous state.
    finally:
        for k in set(managed_ctx.keys()).intersection(set(global_context.keys())):
            global_context.pop(k)

        global_context = {**global_context, **existing_data}


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
                context[k] = [existing_ctx, v]
        else:
            context[k] = v

    return context.data


def logging_context() -> dict:
    if not context.exists():
        logger.debug("Could not access logging context. Context does not exist.")
        return {}

    return context.data


def dump_context(message: Optional[str] = None, local_ctx: Optional[dict] = None) -> str:
    # No local context to manage.
    if not message and not local_ctx:
        return json.dumps(logging_context())

    local_ctx = local_ctx if local_ctx else {}

    # A passed message will take priority over an existing message in the local context.
    if message:
        local_ctx = {**local_ctx, "message": message}

    with managed_global_context(local_ctx) as managed_ctx:
        return json.dumps(managed_ctx)


def correlation_id_for_context() -> Optional[str]:
    return logging_context().get("X-Correlation-ID", None)


def exc_info_as_dict(err: BaseException) -> dict:
    _, _, tb = sys.exc_info()

    exc_ctx: dict = {
        "exc_info": {
            "type": err.__class__.__name__,
            "string": str(err),
        }
    }

    try:
        exc_ctx["exc_info"] = {
            **exc_ctx["exc_info"],
            **[
                {"file": fs.filename, "line": fs.lineno, "func": fs.name}
                for fs in traceback.extract_tb(tb)
                # attempt to show only *our* code, not the many layers of library code
                if "/mavedb/" in fs.filename and "/.direnv/" not in fs.filename
            ][-1],
        }

    # We did our best to construct useful traceback info
    except IndexError:
        exc_ctx["exc_info"]["stringified"] = str(err)

    return exc_ctx
