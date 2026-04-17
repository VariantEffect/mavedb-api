# ruff: noqa: E402
"""
Unit and integration tests for the with_guaranteed_job_run_record async decorator.
Covers JobRun creation, status transitions, error handling, and DB persistence.
"""

import pytest

pytest.importorskip("arq")  # Skip tests if arq is not installed

from sqlalchemy import select

from mavedb import __version__
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.lib.decorators.job_guarantee import with_guaranteed_job_run_record
from tests.helpers.transaction_spy import TransactionSpy

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@with_guaranteed_job_run_record("test_job")
async def sample_job(ctx: dict, job_id: int):
    """Sample job function to test the decorator.

    NOTE: The job_id parameter is injected by the decorator
          and is not passed explicitly when calling the function.

    Args:
        ctx (dict): Worker context dictionary.
        job_id (int): ID of the JobRun record created by the decorator.
    """
    return JobExecutionOutcome.succeeded()


@pytest.mark.asyncio
@pytest.mark.unit
class TestJobGuaranteeDecoratorUnit:
    async def test_decorator_must_receive_ctx_as_first_argument(self, mock_worker_ctx):
        with pytest.raises(ValueError) as exc_info:
            await sample_job()

        assert "Managed functions must receive context as first argument" in str(exc_info.value)

    async def test_decorator_calls_wrapped_function(self, mock_worker_ctx):
        result = await sample_job(mock_worker_ctx)
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

    async def test_decorator_creates_job_run(self, mock_worker_ctx, session):
        with (
            TransactionSpy.spy(session, expect_flush=True, expect_commit=True),
        ):
            await sample_job(mock_worker_ctx)

        job_run = session.execute(select(JobRun)).scalars().first()
        assert job_run is not None
        assert job_run.status == JobStatus.PENDING
        assert job_run.job_type == "test_job"
        assert job_run.job_function == "sample_job"
        assert job_run.mavedb_version == __version__


@pytest.mark.asyncio
@pytest.mark.integration
class TestJobGuaranteeDecoratorIntegration:
    async def test_decorator_persists_job_run_record(self, session, standalone_worker_context):
        # Flush called implicitly by commit
        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            job_task = await sample_job(standalone_worker_context)

        assert isinstance(job_task, JobExecutionOutcome)
        assert job_task.status == JobStatus.SUCCEEDED

        job_run = session.execute(select(JobRun).order_by(JobRun.id.desc())).scalars().first()
        assert job_run.status == JobStatus.PENDING
        assert job_run.job_type == "test_job"
        assert job_run.job_function == "sample_job"
        assert job_run.mavedb_version is not None

    async def test_decorator_skips_creation_when_job_id_provided(self, session, standalone_worker_context):
        """When a job_id is already provided (e.g. from run_job script), the decorator
        should use it instead of creating a new JobRun record."""
        # Pre-create a JobRun like run_job.py does
        existing_job = JobRun(
            job_type="test_job",
            job_function="sample_job",
            status=JobStatus.PENDING,
            mavedb_version=__version__,
        )  # type: ignore[call-arg]
        session.add(existing_job)
        session.flush()
        existing_job_id = existing_job.id

        job_count_before = session.execute(select(JobRun)).scalars().all()

        # Call with the pre-existing job_id as the second argument
        result = await sample_job(standalone_worker_context, existing_job_id)

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # No new JobRun should have been created
        job_count_after = session.execute(select(JobRun)).scalars().all()
        assert len(job_count_after) == len(job_count_before)

    async def test_decorator_raises_on_invalid_job_id(self, session, standalone_worker_context):
        """When a job_id int is provided but doesn't correspond to a real JobRun,
        the decorator should raise immediately to uphold its guarantee."""
        nonexistent_job_id = 999999

        job_count_before = len(session.execute(select(JobRun)).scalars().all())

        with pytest.raises(ValueError, match="does not correspond to an existing JobRun"):
            await sample_job(standalone_worker_context, nonexistent_job_id)

        # No new JobRun should have been created by the decorator
        job_count_after = len(session.execute(select(JobRun)).scalars().all())
        assert job_count_after == job_count_before
