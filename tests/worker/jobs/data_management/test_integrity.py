# ruff: noqa: E402

import pytest

pytest.importorskip("arq")  # Skip tests if arq is not installed

from datetime import date

from sqlalchemy import select

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.allele import Allele
from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.jobs.data_management.integrity import DRIFT_IDS_REPORTED, check_allele_first_published_at

# A failed outcome is what makes the job decorator send its Slack alert, so these tests assert the outcome rather
# than patching the alert.
pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("patch_db_session_ctxmgr")]


def _alleles(session, count: int, first_published_at=None, prefix: str = "a") -> list[int]:
    """Add ``count`` alleles with no links. Setting ``first_published_at`` on them is what a restore with triggers
    disabled could leave behind, a state the app can't produce, so it's written directly."""
    alleles = [
        Allele(
            vrs_digest=f"ga4gh:VA.{prefix}{i}",
            level="genomic",
            hgvs_g=f"NC_000001.11:g.{i + 1}{prefix.upper()}>G",
            first_published_at=first_published_at,
        )
        for i in range(count)
    ]
    session.add_all(alleles)
    session.commit()
    return sorted(allele.id for allele in alleles)


def _job_run(session) -> JobRun:
    return session.execute(select(JobRun).where(JobRun.job_function == "check_allele_first_published_at")).scalar_one()


@pytest.mark.asyncio
async def test_check_allele_first_published_at_consistent_alleles_succeeds(standalone_worker_context, session):
    _alleles(session, 2)

    result = await check_allele_first_published_at(standalone_worker_context)

    assert isinstance(result, JobExecutionOutcome)
    assert result.status == JobStatus.SUCCEEDED
    assert result.data == {"drifted_allele_count": 0}
    job = _job_run(session)
    assert job.status == JobStatus.SUCCEEDED
    assert job.job_type == "cron_job"


@pytest.mark.asyncio
async def test_check_allele_first_published_at_drift_fails(standalone_worker_context, session):
    drifted = _alleles(session, 1, first_published_at=date(2026, 1, 1), prefix="d")
    _alleles(session, 1, prefix="c")

    result = await check_allele_first_published_at(standalone_worker_context)

    assert result.status == JobStatus.FAILED
    assert result.failure_category == FailureCategory.DATA_ERROR
    assert result.data == {"drifted_allele_count": 1, "drifted_allele_ids": drifted}
    job = _job_run(session)
    assert job.status == JobStatus.FAILED
    assert job.job_type == "cron_job"


@pytest.mark.asyncio
async def test_check_allele_first_published_at_many_drifted_reports_a_bounded_id_list(
    standalone_worker_context, session
):
    drifted = _alleles(session, DRIFT_IDS_REPORTED + 5, first_published_at=date(2026, 1, 1), prefix="d")

    result = await check_allele_first_published_at(standalone_worker_context)

    assert result.status == JobStatus.FAILED
    assert result.data == {
        "drifted_allele_count": DRIFT_IDS_REPORTED + 5,
        "drifted_allele_ids": drifted[:DRIFT_IDS_REPORTED],
    }


@pytest.mark.asyncio
async def test_check_allele_first_published_at_through_arq_succeeds(
    arq_redis, arq_worker, standalone_worker_context, session
):
    await arq_redis.enqueue_job("check_allele_first_published_at")
    await arq_worker.async_run()
    await arq_worker.run_check()

    job = _job_run(session)
    assert job.status == JobStatus.SUCCEEDED
    assert job.job_type == "cron_job"
