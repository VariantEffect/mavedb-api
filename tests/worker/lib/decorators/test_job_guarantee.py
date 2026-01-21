# ruff: noqa: E402
"""
Unit and integration tests for the with_guaranteed_job_run_record async decorator.
Covers JobRun creation, status transitions, error handling, and DB persistence.
"""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from mavedb import __version__
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.lib.decorators.job_guarantee import with_guaranteed_job_run_record
from tests.helpers.transaction_spy import TransactionSpy


@pytest.mark.asyncio
@pytest.mark.unit
class TestJobGuaranteeDecoratorUnit:
    async def test_decorator_must_receive_ctx_as_first_argument(self, mock_worker_ctx):
        @with_guaranteed_job_run_record("test_job")
        async def sample_job(not_ctx: dict):
            return {"status": "ok"}

        with pytest.raises(ValueError) as exc_info:
            await sample_job()

        assert "Managed job functions must receive context as first argument" in str(exc_info.value)

    async def test_decorator_must_receive_db_in_ctx(self, mock_worker_ctx):
        del mock_worker_ctx["db"]

        @with_guaranteed_job_run_record("test_job")
        async def sample_job(not_ctx: dict):
            return {"status": "ok"}

        with pytest.raises(ValueError) as exc_info:
            await sample_job(mock_worker_ctx)

        assert "DB session not found in job context" in str(exc_info.value)

    async def test_decorator_calls_wrapped_function(self, mock_worker_ctx):
        @with_guaranteed_job_run_record("test_job")
        async def sample_job(ctx: dict):
            return {"status": "ok"}

        with patch("mavedb.worker.lib.decorators.job_guarantee.JobRun") as MockJobRunClass:
            MockJobRunClass.return_value = MagicMock(spec=JobRun)

            result = await sample_job(mock_worker_ctx)

        assert result == {"status": "ok"}

    async def test_decorator_creates_job_run(self, mock_worker_ctx, mock_job_run):
        @with_guaranteed_job_run_record("test_job")
        async def sample_job(ctx: dict):
            return {"status": "ok"}

        with (
            TransactionSpy.spy(mock_worker_ctx["db"], expect_commit=True),
            patch("mavedb.worker.lib.decorators.job_guarantee.JobRun") as mock_job_run_class,
        ):
            mock_job_run_class.return_value = MagicMock(spec=JobRun)

            await sample_job(mock_worker_ctx)

        mock_job_run_class.assert_called_with(
            job_type="test_job",
            job_function="sample_job",
            status=JobStatus.PENDING,
            mavedb_version=__version__,
        )
        mock_worker_ctx["db"].add.assert_called()


@pytest.mark.asyncio
@pytest.mark.integration
class TestJobGuaranteeDecoratorIntegration:
    async def test_decorator_persists_job_run_record(self, session, standalone_worker_context):
        @with_guaranteed_job_run_record("integration_job")
        async def sample_job(ctx: dict):
            return {"status": "ok"}

        # Flush called implicitly by commit
        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            job_task = await sample_job(standalone_worker_context)

        assert job_task == {"status": "ok"}

        job_run = session.execute(select(JobRun).order_by(JobRun.id.desc())).scalars().first()
        assert job_run.status == JobStatus.PENDING
        assert job_run.job_type == "integration_job"
        assert job_run.job_function == "sample_job"
        assert job_run.mavedb_version is not None
