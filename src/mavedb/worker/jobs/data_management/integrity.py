"""Scheduled checks that data maintained by database triggers still matches what it caches (#809)."""

import logging

from mavedb.lib.alleles import first_published_at_drift
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import FailureCategory
from mavedb.worker.lib.decorators.job_guarantee import with_guaranteed_job_run_record
from mavedb.worker.lib.decorators.job_management import with_job_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)

# Enough ids to start an investigation without flooding the Slack alert, which a bad restore could fill with
# millions.
DRIFT_IDS_REPORTED = 20


# TODO(#1003): Fold this into one job running the shared registry of allele-table invariant checks, so a new check
# is a registry entry rather than another cron job.
@with_guaranteed_job_run_record("cron_job")
@with_job_management
async def check_allele_first_published_at(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Fail, and so alert Slack, when an allele's ``first_published_at`` disagrees with its links.

    Triggers keep the column in step, but anything that skips them can break it: a data-only restore with
    triggers disabled, ``session_replication_role = replica``, or a hand-written ``UPDATE``. One set too early
    shows everyone an allele only private score sets produced, so this runs on a schedule rather than when someone
    remembers to.
    """
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "check_allele_first_published_at",
            "resource": "alleles",
            "correlation_id": None,
        }
    )

    job_manager.update_progress(0, 100, "Checking alleles' first_published_at against their links.")

    drifted = first_published_at_drift(job_manager.db)
    job_manager.update_progress(100, 100, f"Found {len(drifted)} alleles with drifted first_published_at.")
    if not drifted:
        return JobExecutionOutcome.succeeded(data={"drifted_allele_count": 0})

    shown = ", ".join(str(allele_id) for allele_id in drifted[:DRIFT_IDS_REPORTED])
    more = f" and {len(drifted) - DRIFT_IDS_REPORTED} more" if len(drifted) > DRIFT_IDS_REPORTED else ""
    reason = (
        f"{len(drifted)} alleles have a first_published_at that disagrees with their links "
        f"(ids {shown}{more}). Set too early, the alleles RLS policy shows them to everyone."
    )
    logger.error(msg=reason, extra=job_manager.logging_context())
    return JobExecutionOutcome.failed(
        reason=reason,
        data={"drifted_allele_count": len(drifted), "drifted_allele_ids": drifted[:DRIFT_IDS_REPORTED]},
        failure_category=FailureCategory.DATA_ERROR,
    )
