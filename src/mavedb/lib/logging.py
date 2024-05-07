from enum import Enum
from fastapi import Response, Request
from fastapi.routing import APIRoute
from starlette.background import BackgroundTask
from typing import Callable, TypedDict
from watchtower import CloudWatchLogHandler


import boto3
import json
import logging
import os
import time


FRONTEND_URL = os.getenv("FRONTEND_URL", "")
API_URL = os.getenv("API_URL", "")
CLOUDWATCH_LOG_GROUP = os.getenv("CLOUDWATCH_LOG_GROUP", "")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "")

logger = logging.getLogger(__name__)
if AWS_REGION_NAME and CLOUDWATCH_LOG_STREAM:
    boto3_logs_client = boto3.client("logs", region_name=AWS_REGION_NAME)
    logger.addHandler(CloudWatchLogHandler(
        boto3_client=boto3_logs_client,
        log_group_name=CLOUDWATCH_LOG_GROUP))

class LogType(str, Enum):
    api_request = "api_request"


class Source(str, Enum):
    docs = "docs"
    other = "other"
    web = "web"


class LogRecord(TypedDict):
    log_type = LogType
    source = Source
    time_ns = int
    duration_ns = int

    # Fields specific to API calls
    path = str
    method = str
    response_code = int


def log_info(request, response, start, end):
    print("LOG RECORD")
    source = Source.other
    if "origin" in request.headers and request.headers["origin"] == FRONTEND_URL:
        source = Source.web
    elif "referer" in request.headers and request.headers["referer"] == API_URL + "/docs":
        source = Source.docs
    
    record: LogRecord = {
        "log_type": LogType.api_request,
        "source": source,
        "time_ns": start,
        "duration_ns": end - start,
        "path": request.url.path,
        "method": request.method,
        "response_code": response.status_code,
    }
    print("record: " + json.dumps(record))
    logger.info(json.dumps(record))


class LoggedRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def logging_route_handler(request: Request) -> Response:
            start = time.time_ns()
            response = await original_route_handler(request)
            end = time.time_ns()

            # Add logging task to list of background tasks to be completed after the response is sent.
            tasks = response.background
            task = BackgroundTask(log_info, request, response, start, end)
            if tasks:
                tasks.add_task(task)
                response.background = tasks
            else:
                response.background = task
                
            return response
            
        return logging_route_handler