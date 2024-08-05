import logging
import os
import json
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

    # NOTE: Worker shut down will prevent canonical logs from being emitted if queues are used. Since only one log event is output
    #       per job, this shouldn't represent an oppressive performance issue. If it eventually does, we should look into how we
    #       might log worker jobs without allowing worker shutdowns (Perhaps canonical log events are processed in their own jobs).
    use_queues = False if "worker" in CLOUDWATCH_LOG_GROUP else True
    logger.addHandler(
        CloudWatchLogHandler(boto3_client=boto3_logs_client, log_group_name=CLOUDWATCH_LOG_GROUP, use_queues=use_queues)
    )
else:
    logger.warning("Canonical CloudWatch Handler is not defined. Canonical logs will only be emitted to stderr.")


# NOTE: Starlette context will not be initialized in the worker, so maintain a local dictionary of context. We
#       may eventually want to harden this by using the backing Redis cache or with Python's contextvars.
async def log_job(ctx: dict):
    redis: ArqRedis = ctx["redis"]
    job_id = ctx["job_id"]

    log_context: dict[str, Any] = ctx["state"].pop(job_id) if job_id in ctx["state"] else {}

    completed_job = Job(job_id, redis=redis)
    result = await completed_job.result_info()

    if not result:
        log_context["message"] = f"Job finished, but could not retrieve a job result for job {job_id}."
        logger.warning(json.dumps(log_context))
        log_context.pop("message")
    else:
        log_context = {
            **log_context,
            **{
                "time_ns": int(result.enqueue_time.timestamp()),
                "queued_ns": (result.start_time - result.enqueue_time).microseconds,
                "duration_ns": (result.finish_time - result.start_time).microseconds,
                "job_name": result.function,
                "job_attempt": result.job_try,
                "process_result": result.success,
                "job_result": result.result,
            },
        }

    log_context = {
        **log_context,
        **{
            "job_id": completed_job.job_id,
            "version": __version__,
            "log_type": LogType.worker_job,
            "source": Source.worker,
        },
    }

    if result is None:
        log_context["message"] = "Job result could not be found."
        logger.error(json.dumps(log_context))
    elif result.result == "success":
        log_context["message"] = "Job completed successfully."
        logger.info(json.dumps(log_context))
    elif result.result != "success":
        log_context["message"] = "Job completed with handled exception."
        logger.warning(json.dumps(log_context))
    else:
        log_context["message"] = "Job completed with unhandled exception."
        logger.error(json.dumps(log_context))

    log_context.pop("message")
    flush_cloudwatch_logs()


def log_request(request: Request, response: Response, start: int, end: int):
    save_to_context(
        {
            "log_type": LogType.api_request,
            "time_ns": start,
            "duration_ns": end - start,
            "response_code": response.status_code,
        }
    )

    if response.status_code < 400:
        logger.info(dump_context(message="Request comleted."))
    elif response.status_code < 500:
        logger.warning(dump_context(message="Request comleted."))
    else:
        logger.error(dump_context(message="Request comleted with exception."))


def flush_cloudwatch_logs():
    """Force flush of all cloudwatch logging handlers. For example at the end of a process just before it is killed."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, CloudWatchLogHandler):
            handler.flush()
