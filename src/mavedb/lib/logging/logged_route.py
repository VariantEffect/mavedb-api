from fastapi import Response, Request
from fastapi.routing import APIRoute
from starlette.background import BackgroundTask, BackgroundTasks
from typing import Callable

import time

from mavedb.lib.logging.canonical import log_request
from mavedb.lib.logging.context import save_to_context
from mavedb.lib.logging.models import LogType


class LoggedRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def logging_route_handler(request: Request) -> Response:
            start = time.time_ns()
            response = await original_route_handler(request)
            end = time.time_ns()

            save_to_context(
                {
                    "log_type": LogType.api_request,
                    "time_ns": start,
                    "duration_ns": end - start,
                    "response_code": response.status_code,
                }
            )

            # Add logging task to list of background tasks to be completed after the response is sent.
            old_background = response.background

            task = BackgroundTask(log_request)
            if old_background:
                if isinstance(old_background, BackgroundTasks):
                    old_background.add_task(task)
                    response.background = old_background
                else:
                    tasks = BackgroundTasks()
                    tasks.add_task(old_background)
                    tasks.add_task(task)
                    response.background = tasks
            else:
                response.background = task

            return response

        return logging_route_handler
