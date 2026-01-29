# ruff: noqa: E402

import pytest

pytest.importorskip("arq")  # Skip tests if arq is not installed

from unittest.mock import call, patch

from sqlalchemy import select

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.models.published_variant import PublishedVariantsMV
from mavedb.worker.jobs.data_management.views import refresh_materialized_views, refresh_published_variants_view
from tests.helpers.transaction_spy import TransactionSpy

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")

############################################################################################################################################
# refresh_materialized_views
############################################################################################################################################


@pytest.mark.asyncio
@pytest.mark.unit
class TestRefreshMaterializedViewsUnit:
    """Unit tests for the refresh_materialized_views function."""

    async def test_refresh_materialized_views_calls_refresh_function(self, mock_worker_ctx, mock_job_manager):
        """Test that refresh_materialized_views calls the refresh function."""
        with (
            patch("mavedb.worker.jobs.data_management.views.refresh_all_mat_views") as mock_refresh,
            TransactionSpy.spy(mock_job_manager.db, expect_commit=False, expect_flush=True),
        ):
            result = await refresh_materialized_views(mock_worker_ctx, 999, job_manager=mock_job_manager)

        mock_refresh.assert_called_once_with(mock_job_manager.db)
        assert result == {"status": "ok", "data": {}, "exception": None}

    async def test_refresh_materialized_views_updates_progress(self, mock_worker_ctx, mock_job_manager):
        """Test that refresh_materialized_views updates progress correctly."""
        with (
            patch("mavedb.worker.jobs.data_management.views.refresh_all_mat_views"),
            patch.object(mock_job_manager, "update_progress", return_value=None) as mock_update_progress,
            TransactionSpy.spy(mock_job_manager.db, expect_commit=False, expect_flush=True),
        ):
            result = await refresh_materialized_views(mock_worker_ctx, 999, job_manager=mock_job_manager)

        expected_calls = [
            call(0, 100, "Starting refresh of all materialized views."),
            call(100, 100, "Completed refresh of all materialized views."),
        ]
        mock_update_progress.assert_has_calls(expected_calls)
        assert result == {"status": "ok", "data": {}, "exception": None}


@pytest.mark.asyncio
@pytest.mark.integration
class TestRefreshMaterializedViewsIntegration:
    """Integration tests for the refresh_materialized_views function and decorator logic."""

    async def test_refresh_materialized_views_integration(self, standalone_worker_context, session):
        """Integration test that runs refresh_materialized_views end-to-end."""

        # Flush will be called implicitly when the transaction is committed
        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await refresh_materialized_views(standalone_worker_context)

        job = session.execute(
            select(JobRun).where(JobRun.job_function == "refresh_materialized_views")
        ).scalar_one_or_none()
        assert job is not None
        assert job.status == JobStatus.SUCCEEDED
        assert job.job_type == "cron_job"

        assert result == {"status": "ok", "data": {}, "exception": None}

    async def test_refresh_materialized_views_handles_exceptions(self, standalone_worker_context, session):
        """Integration test that ensures exceptions during refresh are handled properly."""

        with (
            patch(
                "mavedb.worker.jobs.data_management.views.refresh_all_mat_views",
                side_effect=Exception("Test exception during refresh"),
            ),
            TransactionSpy.spy(session, expect_rollback=True, expect_flush=True, expect_commit=True),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await refresh_materialized_views(standalone_worker_context)
            mock_send_slack_error.assert_called_once()

        job = session.execute(
            select(JobRun).where(JobRun.job_function == "refresh_materialized_views")
        ).scalar_one_or_none()

        assert job is not None
        assert job.status == JobStatus.FAILED
        assert job.job_type == "cron_job"
        assert job.error_message == "Test exception during refresh"
        assert result["status"] == "exception"
        assert isinstance(result["exception"], Exception)


@pytest.mark.asyncio
@pytest.mark.integration
class TestRefreshMaterializedViewsArqContext:
    """Integration tests for refresh_materialized_views within an ARQ worker context."""

    async def test_refresh_materialized_views_arq_integration(
        self, arq_redis, arq_worker, standalone_worker_context, session
    ):
        """Integration test that runs refresh_materialized_views end-to-end using ARQ context."""
        await arq_redis.enqueue_job("refresh_materialized_views")
        await arq_worker.async_run()
        await arq_worker.run_check()

        job = session.execute(
            select(JobRun).where(JobRun.job_function == "refresh_materialized_views")
        ).scalar_one_or_none()
        assert job is not None
        assert job.status == JobStatus.SUCCEEDED
        assert job.job_type == "cron_job"


############################################################################################################################################
# refresh_published_variants_view
############################################################################################################################################


@pytest.mark.asyncio
@pytest.mark.unit
class TestRefreshPublishedVariantsViewUnit:
    """Unit tests for the refresh_published_variants_view function."""

    async def test_refresh_published_variants_view_calls_refresh_function(
        self, mock_worker_ctx, mock_job_manager, mock_job_run
    ):
        """Test that refresh_published_variants_view calls the refresh function."""
        mock_job_run.job_params = {"correlation_id": "test-corr-id"}

        with (
            patch.object(PublishedVariantsMV, "refresh") as mock_refresh,
            patch("mavedb.worker.jobs.data_management.views.validate_job_params"),
            TransactionSpy.spy(mock_job_manager.db, expect_commit=False, expect_flush=True),
        ):
            result = await refresh_published_variants_view(mock_worker_ctx, 999, job_manager=mock_job_manager)

        mock_refresh.assert_called_once_with(mock_job_manager.db)
        assert result == {"status": "ok", "data": {}, "exception": None}

    async def test_refresh_published_variants_view_updates_progress(
        self, mock_worker_ctx, mock_job_manager, mock_job_run
    ):
        """Test that refresh_published_variants_view updates progress correctly."""
        mock_job_run.job_params = {"correlation_id": "test-corr-id"}

        with (
            patch.object(PublishedVariantsMV, "refresh"),
            patch("mavedb.worker.jobs.data_management.views.validate_job_params"),
            patch.object(mock_job_manager, "update_progress", return_value=None) as mock_update_progress,
            TransactionSpy.spy(mock_job_manager.db, expect_commit=False, expect_flush=True),
        ):
            result = await refresh_published_variants_view(mock_worker_ctx, 999, job_manager=mock_job_manager)

        expected_calls = [
            call(0, 100, "Starting refresh of published variants materialized view."),
            call(100, 100, "Completed refresh of published variants materialized view."),
        ]
        mock_update_progress.assert_has_calls(expected_calls)
        assert result == {"status": "ok", "data": {}, "exception": None}


@pytest.mark.asyncio
@pytest.mark.integration
class TestRefreshPublishedVariantsViewIntegration:
    """Integration tests for the refresh_published_variants_view function and decorator logic."""

    @pytest.fixture()
    def setup_refresh_job_run(self, session):
        """Add a refresh_published_variants_view job run to the DB before each test."""
        job_run = JobRun(
            job_type="data_management",
            job_function="refresh_published_variants_view",
            status=JobStatus.PENDING,
            job_params={"correlation_id": "test-corr-id"},
        )
        session.add(job_run)
        session.commit()
        return job_run

    async def test_refresh_published_variants_view_integration_standalone(
        self, standalone_worker_context, session, setup_refresh_job_run
    ):
        """Integration test that runs refresh_published_variants_view end-to-end."""
        # Flush will be called implicitly when the transaction is committed
        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await refresh_published_variants_view(standalone_worker_context, setup_refresh_job_run.id)

        session.refresh(setup_refresh_job_run)
        assert setup_refresh_job_run.status == JobStatus.SUCCEEDED
        assert result == {"status": "ok", "data": {}, "exception": None}

    async def test_refresh_published_variants_view_integration_pipeline(
        self, standalone_worker_context, session, setup_refresh_job_run
    ):
        """Integration test that runs refresh_published_variants_view end-to-end."""
        # Create a pipeline for the job run and associate it
        pipeline = Pipeline(
            name="Test Pipeline for Published Variants View Refresh",
        )
        session.add(pipeline)
        session.commit()
        session.refresh(pipeline)
        setup_refresh_job_run.pipeline_id = pipeline.id
        session.add(setup_refresh_job_run)
        session.commit()

        # Flush will be called implicitly when the transaction is committed
        with TransactionSpy.spy(session, expect_flush=True, expect_commit=True):
            result = await refresh_published_variants_view(standalone_worker_context, setup_refresh_job_run.id)

        session.refresh(setup_refresh_job_run)
        assert setup_refresh_job_run.status == JobStatus.SUCCEEDED
        assert result == {"status": "ok", "data": {}, "exception": None}
        session.refresh(pipeline)
        assert pipeline.status == PipelineStatus.SUCCEEDED

    async def test_refresh_published_variants_view_handles_exceptions(
        self, standalone_worker_context, session, setup_refresh_job_run
    ):
        """Integration test that ensures exceptions during refresh are handled properly."""
        with (
            patch.object(
                PublishedVariantsMV,
                "refresh",
                side_effect=Exception("Test exception during published variants view refresh"),
            ),
            TransactionSpy.spy(session, expect_rollback=True, expect_flush=True, expect_commit=True),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await refresh_published_variants_view(standalone_worker_context, setup_refresh_job_run.id)
            mock_send_slack_error.assert_called_once()

        session.refresh(setup_refresh_job_run)
        assert setup_refresh_job_run.status == JobStatus.FAILED
        assert setup_refresh_job_run.error_message == "Test exception during published variants view refresh"
        assert result["status"] == "exception"
        assert isinstance(result["exception"], Exception)

    async def test_refresh_published_variants_view_requires_params(
        self, setup_refresh_job_run, standalone_worker_context, session
    ):
        """Integration test that ensures required job params are validated."""
        setup_refresh_job_run.job_params = {}  # Clear required params
        session.add(setup_refresh_job_run)
        session.commit()

        with (
            TransactionSpy.spy(session, expect_rollback=True, expect_flush=True, expect_commit=True),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await refresh_published_variants_view(standalone_worker_context, setup_refresh_job_run.id)
            mock_send_slack_error.assert_called_once()

        session.refresh(setup_refresh_job_run)
        assert setup_refresh_job_run.status == JobStatus.FAILED
        assert "Job has no job_params defined" in setup_refresh_job_run.error_message
        assert result["status"] == "exception"
        assert isinstance(result["exception"], Exception)


@pytest.mark.asyncio
@pytest.mark.integration
class TestRefreshPublishedVariantsViewArqContext:
    """Integration tests for refresh_published_variants_view within an ARQ worker context."""

    @pytest.fixture()
    def setup_refresh_job_run(self, session):
        """Add a refresh_published_variants_view job run to the DB before each test."""
        job_run = JobRun(
            job_type="data_management",
            job_function="refresh_published_variants_view",
            status=JobStatus.PENDING,
            job_params={"correlation_id": "test-corr-id"},
        )
        session.add(job_run)
        session.commit()
        return job_run

    async def test_refresh_published_variants_view_arq_integration(
        self, arq_redis, arq_worker, standalone_worker_context, session, setup_refresh_job_run
    ):
        """Integration test that runs refresh_published_variants_view end-to-end using ARQ context."""
        await arq_redis.enqueue_job("refresh_published_variants_view", setup_refresh_job_run.id)
        await arq_worker.async_run()
        await arq_worker.run_check()

        session.refresh(setup_refresh_job_run)
        assert setup_refresh_job_run.status == JobStatus.SUCCEEDED
