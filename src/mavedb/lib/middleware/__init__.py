"""ASGI middleware for the MaveDB application."""

from mavedb.lib.middleware.errors import CatchAllErrorMiddleware

__all__ = [
    "CatchAllErrorMiddleware",
]
