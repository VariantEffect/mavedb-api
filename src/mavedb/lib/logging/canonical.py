import logging
from typing import Any, Optional

from arq import ArqRedis
from arq.jobs import Job
from starlette.requests import Request
from starlette.responses import Response

from mavedb import __version__
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.logging.models import LogType, Source

logger = logging.getLogger(__name__)


# NOTE: Starlette context will not be initialized in the worker, so maintain a local dictionary of context. We
#       may eventually want to harden this by using the backing Redis cache or with Python's contextvars.
async def log_job(ctx: dict) -> None:
    redis: ArqRedis = ctx["redis"]
    job_id: str = ctx["job_id"]

    log_context: dict[str, Any] = ctx["state"].pop(job_id) if job_id in ctx["state"] else {}

    completed_job = Job(job_id, redis=redis)
    result = await completed_job.result_info()

    if not result:
        logger.warning(msg=f"Job finished, but could not retrieve a job result for job {job_id}.", extra=log_context)
    else:
        log_context = {
            **log_context,
            **{
                "time_ns": int(result.enqueue_time.timestamp()),
                "queued_ns": (result.start_time - result.enqueue_time).microseconds,
                "duration_ns": (result.finish_time - result.start_time).microseconds,
                "job_name": result.function,
                "job_attempt": result.job_try,
                "arq_success": result.success,
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
            "canonical": True,
        },
    }

    if result is None:
        logger.error(msg="Job result could not be found.", extra=log_context)
    elif result.result is not None:
        logger.info(msg="Job completed with result.", extra=log_context)
    else:
        logger.error(msg="Job completed with unhandled exception.", extra=log_context)


def log_request(request: Request, response: Response, end: int) -> None:
    save_to_logging_context({"log_type": LogType.api_request, "response_code": response.status_code})

    start: Optional[int] = logging_context().get("time_ns")
    if start:
        save_to_logging_context({"duration_ns": end - start})

    save_to_logging_context({"canonical": True})
    if response.status_code < 400:
        logger.info(msg="Request completed.", extra=logging_context())
    elif response.status_code < 500:
        logger.warning(msg="Request completed.", extra=logging_context())
    else:
        logger.error(msg="Request completed.", extra=logging_context())
