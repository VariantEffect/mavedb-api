from fastapi import Depends, Response, Request
from fastapi.routing import APIRoute
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask
from typing import Callable

from mavedb import deps
from mavedb.models.log_record import LogRecord
from mavedb.models.enums.logs import LogType, Method, Source

import time


def log_info(db, request, response, start, end):
    print('LOG RECORD')
    source = Source.other
    if 'origin' in request.headers and request.headers['origin'] == 'https://mavedb.org':
        source = Source.web
    elif 'referer' in request.headers and request.headers['referer'] == 'https://api.mavedb.org/docs':
        source = Source.docs
    
    record = LogRecord(
        log_type=LogType.api_request,
        source=source,
        time_ns=start,
        duration_ns=end - start,
        path=request.url.path,
    )

    print('path: ' + request.url.path)
    print('method: ' + request.method)
    print('source: ' + str(source))
    print(response)
    print('start: ' + str(start))
    print('end: ' + str(end))
    print('duration: ' + str(end - start))
    db.add(record)
    db.commit()

class LoggedRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def logging_route_handler(request: Request, db: Session = Depends(deps.get_db)) -> Response:
            start = time.time_ns()
            response = await original_route_handler(request)
            end = time.time_ns()

            # Add logging task to list of background tasks to be completed after the response is sent.
            tasks = response.background
            task = BackgroundTask(log_info, db, request, response, start, end)
            if tasks:
                tasks.add_task(task)
                response.background = tasks
            else:
                response.background = task
                
            return response
            
        return logging_route_handler