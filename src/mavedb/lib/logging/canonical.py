import logging
import os
from typing import Any

import boto3
from arq import ArqRedis
from arq.jobs import Job
from starlette.requests import Request
from starlette.responses import Response
from watchtower import CloudWatchLogHandler

from mavedb import __version__
from mavedb.lib.logging.models import Source, LogType
from mavedb.lib.logging.context import save_to_context, dump_context


logger = logging.getLogger(__name__)

CLOUDWATCH_LOG_GROUP = os.getenv("CLOUDWATCH_LOG_GROUP", "")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "")

if AWS_REGION_NAME and CLOUDWATCH_LOG_GROUP:
    boto3_logs_client = boto3.client("logs", region_name=AWS_REGION_NAME)
    logger.addHandler(CloudWatchLogHandler(boto3_client=boto3_logs_client, log_group_name=CLOUDWATCH_LOG_GROUP))
else:
    logger.warning("Canonical CloudWatch Handler is not defined. Canonical logs will only be emitted to stderr.")


async def log_job(ctx: dict):
    redis: ArqRedis = ctx["redis"]
    job_id = ctx["job_id"]

    log_context: dict[str, Any] = ctx["state"].pop(job_id) if job_id in ctx["state"] else {}
    save_to_context(log_context)

    completed_job = Job(job_id, redis=redis)
    result = await completed_job.result_info()

    if not result:
        logger.warning(dump_context(message=f"Job finished, but could not retrieve a job result for job {job_id}."))

    assert result
    save_to_context(
        {
            "job_id": completed_job.job_id,
            "version": __version__,
            "log_type": LogType.worker_job,
            "source": Source.worker,
            "time_ns": int(result.enqueue_time.timestamp()),
            "queued_ns": (result.start_time - result.enqueue_time).microseconds,
            "duration_ns": (result.finish_time - result.start_time).microseconds,
            "job_name": result.function,
            "job_attempt": result.job_try,
            "process_result": result.success,
            "job_result": result.result,
        }
    )

    if not result.success:
        logger.warning(dump_context(message="Job completed unsuccessfully."))
    else:
        logger.info(dump_context(message="Job completed successfully."))


def log_request(request: Request, response: Response, start: int, end: int):
    save_to_context(
        {
            "log_type": LogType.api_request,
            "time_ns": start,
            "duration_ns": end - start,
            "response_code": response.status_code,
        }
    )

    logger.info(dump_context(message="Request comleted."))
