import logging
import os
from typing import Any

import boto3
from arq import ArqRedis
from arq.jobs import Job
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

    completed_job = Job(job_id, redis=redis)
    log_context: dict[str, Any] = ctx["state"].pop(job_id) if job_id in ctx["state"] else {}
    result = await completed_job.result_info()

    if not result:
        logger.warning(f"Job finished, but could not retrieve a job result for job {job_id}")

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
            **log_context,
        }
    )

    if not result.success:
        logger.warning(f"Job completed unsuccessfully. {dump_context()}")
    else:
        logger.info(f"Job completed successfully. {dump_context()}")


def log_request():
    logger.info(f"Request completed. {dump_context()}")
