from unittest.mock import call, patch

import pytest
from sqlalchemy import select

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.jobs.pipeline_management.start_pipeline import start_pipeline
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.pipeline_manager import PipelineManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.unit
@pytest.mark.asyncio
class TestStartPipelineUnit:
    """Unit tests for starting pipelines."""

    @pytest.fixture(autouse=True)
    def setup_start_pipeline_job_run(self, session, with_dummy_pipeline, sample_dummy_pipeline):
        """Fixture to ensure a start pipeline job run exists in the database."""
        job_run = JobRun(
            pipeline_id=sample_dummy_pipeline.id,
            job_type="start_pipeline",
            job_function="start_pipeline",
        )
        session.add(job_run)
        session.commit()

        return job_run

    async def test_start_pipeline_raises_exception_when_no_pipeline_associated_with_job(
        self,
        session,
        mock_worker_ctx,
        setup_start_pipeline_job_run,
    ):
        """Test that starting a pipeline raises an exception when no pipeline is associated with the job."""

        # Remove pipeline association from job run
        setup_start_pipeline_job_run.pipeline_id = None
        session.commit()

        with pytest.raises(ValueError, match="No pipeline associated with job"):
            await start_pipeline(
                mock_worker_ctx,
                setup_start_pipeline_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], setup_start_pipeline_job_run.id),
            )

    async def test_start_pipeline_starts_pipeline_successfully(
        self,
        session,
        mock_worker_ctx,
        mock_pipeline_manager,
        setup_start_pipeline_job_run,
    ):
        """Test that starting a pipeline completes successfully."""

        with (
            patch("mavedb.worker.lib.managers.pipeline_manager.PipelineManager") as mock_pipeline_manager_class,
            patch.object(PipelineManager, "coordinate_pipeline", return_value=None) as mock_coordinate_pipeline,
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager

            result = await start_pipeline(
                mock_worker_ctx,
                setup_start_pipeline_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], setup_start_pipeline_job_run.id),
            )

        assert result["status"] == "ok"
        mock_coordinate_pipeline.assert_called_once()

    async def test_start_pipeline_updates_progress(
        self,
        session,
        mock_worker_ctx,
        mock_pipeline_manager,
        setup_start_pipeline_job_run,
    ):
        """Test that starting a pipeline updates job progress."""

        with (
            patch("mavedb.worker.lib.managers.pipeline_manager.PipelineManager") as mock_pipeline_manager_class,
            patch.object(PipelineManager, "coordinate_pipeline", return_value=None),
            patch.object(
                JobManager,
                "update_progress",
                return_value=None,
            ) as mock_update_progress,
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager

            result = await start_pipeline(
                mock_worker_ctx,
                setup_start_pipeline_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], setup_start_pipeline_job_run.id),
            )

        assert result["status"] == "ok"

        mock_update_progress.assert_has_calls(
            [
                call(0, 100, "Coordinating pipeline for the first time."),
                call(100, 100, "Initial pipeline coordination complete."),
            ]
        )

    async def test_start_pipeline_raises_exception(
        self,
        session,
        mock_worker_ctx,
        mock_pipeline_manager,
        setup_start_pipeline_job_run,
    ):
        """Test that starting a pipeline raises an exception."""

        with (
            patch("mavedb.worker.lib.managers.pipeline_manager.PipelineManager") as mock_pipeline_manager_class,
            patch.object(
                PipelineManager,
                "coordinate_pipeline",
                side_effect=Exception("Simulated pipeline start failure"),
            ),
            pytest.raises(Exception, match="Simulated pipeline start failure"),
        ):
            mock_pipeline_manager_class.return_value = mock_pipeline_manager

            await start_pipeline(
                mock_worker_ctx,
                setup_start_pipeline_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], setup_start_pipeline_job_run.id),
            )


@pytest.mark.integration
@pytest.mark.asyncio
class TestStartPipelineIntegration:
    """Integration tests for starting pipelines."""

    async def test_start_pipeline_on_job_without_pipeline_fails(
        self,
        session,
        mock_worker_ctx,
        with_full_dummy_pipeline,
        sample_dummy_pipeline_start,
    ):
        """Test that starting a pipeline on a job without an associated pipeline fails."""

        sample_dummy_pipeline_start.pipeline_id = None
        session.commit()

        result = await start_pipeline(mock_worker_ctx, sample_dummy_pipeline_start.id)
        assert result["status"] == "failed"

        # Verify the start job run status
        session.refresh(sample_dummy_pipeline_start)
        assert sample_dummy_pipeline_start.status == JobStatus.FAILED

    async def test_start_pipeline_on_valid_job_succeeds_and_coordinates_pipeline(
        self, session, mock_worker_ctx, with_full_dummy_pipeline, sample_dummy_pipeline_start, sample_dummy_pipeline
    ):
        """Test that starting a pipeline on a valid job succeeds and coordinates the pipeline."""

        result = await start_pipeline(mock_worker_ctx, sample_dummy_pipeline_start.id)
        assert result["status"] == "ok"

        # Verify the start job run status
        session.refresh(sample_dummy_pipeline_start)
        assert sample_dummy_pipeline_start.status == JobStatus.SUCCEEDED

        # Verify that the pipeline state is updated appropriately
        session.refresh(sample_dummy_pipeline)
        assert sample_dummy_pipeline.status == PipelineStatus.RUNNING

    async def test_start_pipeline_handles_exceptions_gracefully(
        self,
        session,
        mock_worker_ctx,
        with_full_dummy_pipeline,
        sample_dummy_pipeline,
        sample_dummy_pipeline_start,
    ):
        """Test that starting a pipeline handles exceptions gracefully."""
        # Mock a coordination failure during pipeline start. Realistically if this failed in pipeline start
        # it would likely also fail during the final coordination attempt in the exception handler, but for testing purposes
        # we only mock the initial failure here. In a real-world scenario, we'd likely have to rely on our alerting here and
        # intervene manually or via a separate recovery job to fix the pipeline state.
        real_coordinate_pipeline = PipelineManager.coordinate_pipeline
        call_count = {"n": 0}

        async def custom_side_effect(*args, **kwargs):
            if call_count["n"] == 0:
                call_count["n"] += 1
                raise Exception("Simulated pipeline start failure")
            return await real_coordinate_pipeline(
                PipelineManager(session, session, sample_dummy_pipeline.id), *args, **kwargs
            )  # Allow the final coordination attempt to proceed 'normally'

        with patch(
            "mavedb.worker.lib.managers.pipeline_manager.PipelineManager.coordinate_pipeline",
            side_effect=custom_side_effect,
        ):
            result = await start_pipeline(mock_worker_ctx, sample_dummy_pipeline_start.id)
            assert result["status"] == "failed"

        # Verify the start job run status
        session.refresh(sample_dummy_pipeline_start)
        assert sample_dummy_pipeline_start.status == JobStatus.FAILED

        # Verify that the pipeline state is updated to CANCELLED
        session.refresh(sample_dummy_pipeline)
        assert sample_dummy_pipeline.status == PipelineStatus.FAILED

    async def test_start_pipeline_no_jobs_in_pipeline(
        self,
        session,
        mock_worker_ctx,
        with_dummy_pipeline,
        sample_dummy_pipeline_start,
        sample_dummy_pipeline,
    ):
        """Test starting a pipeline that has no jobs defined."""

        result = await start_pipeline(mock_worker_ctx, sample_dummy_pipeline_start.id)
        assert result["status"] == "ok"

        # Verify that a JobRun was created for the start_pipeline job and it succeeded
        session.refresh(sample_dummy_pipeline_start)
        assert sample_dummy_pipeline_start.status == JobStatus.SUCCEEDED

        # Verify that the pipeline state is updated appropriately
        session.refresh(sample_dummy_pipeline)
        assert sample_dummy_pipeline.status == PipelineStatus.SUCCEEDED


@pytest.mark.integration
@pytest.mark.asyncio
class TestStartPipelineArqContext:
    """Test starting pipelines using an ARQ worker context."""

    async def test_start_pipeline_with_arq_context(
        self,
        session,
        arq_redis,
        arq_worker,
        with_full_dummy_pipeline,
        sample_dummy_pipeline_start,
        sample_dummy_pipeline,
    ):
        """Test starting a pipeline using an ARQ worker context."""

        await arq_redis.enqueue_job("start_pipeline", sample_dummy_pipeline_start.id)
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify the start job run status
        session.refresh(sample_dummy_pipeline_start)
        assert sample_dummy_pipeline_start.status == JobStatus.SUCCEEDED

        # Verify that the pipeline state is updated appropriately
        session.refresh(sample_dummy_pipeline)
        assert sample_dummy_pipeline.status == PipelineStatus.RUNNING

        # Verify that other pipeline steps have been queued
        pipeline_steps = (
            session.execute(
                select(JobRun).where(
                    JobRun.pipeline_id == sample_dummy_pipeline.id, JobRun.id != sample_dummy_pipeline_start.id
                )
            )
            .scalars()
            .all()
        )
        assert len(pipeline_steps) == 1
        assert pipeline_steps[0].job_type == "dummy_step"
        assert pipeline_steps[0].status == JobStatus.QUEUED

    async def test_start_pipeline_with_arq_context_no_jobs_in_pipeline(
        self,
        session,
        arq_redis,
        arq_worker,
        with_dummy_pipeline,
        sample_dummy_pipeline_start,
        sample_dummy_pipeline,
    ):
        """Test starting a pipeline with no jobs using an ARQ worker context."""

        await arq_redis.enqueue_job("start_pipeline", sample_dummy_pipeline_start.id)
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify that a JobRun was created for the start_pipeline job and it succeeded
        session.refresh(sample_dummy_pipeline_start)
        assert sample_dummy_pipeline_start.status == JobStatus.SUCCEEDED

        # Verify that the pipeline state is updated appropriately
        session.refresh(sample_dummy_pipeline)
        assert sample_dummy_pipeline.status == PipelineStatus.SUCCEEDED
