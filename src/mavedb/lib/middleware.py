from uuid import uuid4

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware


class InteractionMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        request.state.interaction_id = uuid4().hex

        # process the request and get the response
        response = await call_next(request)

        return response
