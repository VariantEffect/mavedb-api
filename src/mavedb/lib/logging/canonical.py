import json
import logging
import os

import boto3
from arq import ArqRedis
from arq.jobs import Job
from fastapi.requests import Request
from fastapi.responses import Response
from watchtower import CloudWatchLogHandler

from mavedb.lib.logging.models import WorkerRecord, APIRecord, Source, LogType

logger = logging.getLogger(__name__)

CLOUDWATCH_LOG_GROUP = os.getenv("CLOUDWATCH_LOG_GROUP", "")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "")

if AWS_REGION_NAME and CLOUDWATCH_LOG_GROUP:
    boto3_logs_client = boto3.client("logs", region_name=AWS_REGION_NAME)
    logger.addHandler(CloudWatchLogHandler(boto3_client=boto3_logs_client, log_group_name=CLOUDWATCH_LOG_GROUP))

else:
    logger.warning("Canonical CloudWatch Handler is not defined. Canonical logs will not be sent to CloudWatch.")

FRONTEND_URL = os.getenv("FRONTEND_URL", "")
API_URL = os.getenv("API_URL", "")


async def log_job(ctx: dict):
    redis: ArqRedis = ctx["redis"]

    completed_job = Job(ctx["job_id"], redis=redis)
    result = await completed_job.result_info()

    if not result:
        logger.warning(f"Job finished, but could not retrieve a job result for job {ctx['job_id']}")

    assert result
    record: WorkerRecord = {
        "id": completed_job.job_id,
        "log_type": LogType.worker_job,
        "source": Source.worker,
        "time_ns": int(result.enqueue_time.timestamp()),
        "queued_ns": (result.start_time - result.enqueue_time).microseconds,
        "duration_ns": (result.finish_time - result.start_time).microseconds,
        "job": result.function,
        "attempt": result.job_try,
        "success": result.success,
        "result": result.result,
    }

    if not result.success:
        logger.warning(json.dumps(record))
    else:
        logger.info(json.dumps(record))


def log_request(request: Request, response: Response, start: int, end: int):
    source = Source.other
    if request.headers.get("origin") == FRONTEND_URL:
        source = Source.web
    elif request.headers.get("referer") == API_URL + "/docs":
        source = Source.docs

    user_agent = request.headers.get("user-agent", "None")

    record: APIRecord = {
        "log_type": LogType.api_request,
        "source": source,
        "time_ns": start,
        "duration_ns": end - start,
        "path": request.url.path,
        "method": request.method,
        "response_code": response.status_code,
        "user_agent": user_agent,
        **request.state._state,
    }
    logger.info(json.dumps(record))
