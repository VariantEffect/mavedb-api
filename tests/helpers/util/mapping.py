from mavedb.worker.jobs.utils.constants import MAPPING_QUEUE_NAME


async def sanitize_mapping_queue(standalone_worker_context, score_set):
    queued_job = await standalone_worker_context["redis"].rpop(MAPPING_QUEUE_NAME)
    assert int(queued_job.decode("utf-8")) == score_set.id
