# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from unittest.mock import patch

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.worker.jobs.external_services.hgvs import populate_hgvs_for_score_set
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")

SAMPLE_CA_ALLELE_DATA = {
    "genomicAlleles": [
        {
            "referenceGenome": "GRCh38",
            "hgvs": ["NC_000001.11:g.12345A>G"],
        }
    ],
    "transcriptAlleles": [
        {
            "hgvs": ["NM_000000.1:c.1A>G"],
            "proteinEffect": {"hgvs": "NP_000000.1:p.Met1Val"},
            "MANE": {
                "nucleotide": {"RefSeq": {"hgvs": "NM_000000.1:c.1A>G"}},
                "protein": {"RefSeq": {"hgvs": "NP_000000.1:p.Met1Val"}},
            },
        }
    ],
}

SAMPLE_PA_ALLELE_DATA = {
    "aminoAcidAlleles": [
        {
            "hgvs": ["NP_000000.1:p.Met1Val"],
        }
    ],
}


@pytest.mark.asyncio
@pytest.mark.unit
class TestPopulateHgvsForScoreSetUnit:
    """Unit tests for the populate_hgvs_for_score_set job."""

    async def test_no_mapped_variants(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
    ):
        """Test populating HGVS when no mapped variants exist."""
        with patch.object(JobManager, "update_progress") as mock_update_progress:
            result = await populate_hgvs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_hgvs_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        mock_update_progress.assert_any_call(100, 100, "No current mapped variants found. Nothing to do.")

    async def test_variant_without_caid_skipped(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that a variant without a CAID gets a skipped annotation."""
        _, mapped_variant = setup_sample_variants_with_caid_for_hgvs
        mapped_variant.clingen_allele_id = None
        session.commit()

        with patch.object(JobManager, "update_progress"):
            result = await populate_hgvs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_hgvs_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["skipped_count"] == 1

    async def test_variant_with_multi_caid_skipped(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that a variant with a multi-variant CAID gets a skipped annotation."""
        _, mapped_variant = setup_sample_variants_with_caid_for_hgvs
        mapped_variant.clingen_allele_id = "CA123,CA456"
        session.commit()

        with patch.object(JobManager, "update_progress"):
            result = await populate_hgvs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_hgvs_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["skipped_count"] == 1

    async def test_successful_ca_allele_hgvs_population(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test successful HGVS population for a CA allele."""
        with (
            patch.object(JobManager, "update_progress"),
            patch(
                "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
                return_value=SAMPLE_CA_ALLELE_DATA,
            ),
        ):
            result = await populate_hgvs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_hgvs_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["populated_count"] == 1

        _, mapped_variant = setup_sample_variants_with_caid_for_hgvs
        session.refresh(mapped_variant)
        assert mapped_variant.hgvs_g == "NC_000001.11:g.12345A>G"

    async def test_clingen_api_error_recorded_as_failed(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that ClinGen API errors are recorded as failed annotations."""
        import requests

        with (
            patch.object(JobManager, "update_progress"),
            patch(
                "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
                side_effect=requests.exceptions.ConnectionError("Connection refused"),
            ),
        ):
            result = await populate_hgvs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_hgvs_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["failed_count"] == 1

    async def test_clingen_allele_not_found_skipped(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that a 404 from ClinGen results in a skipped annotation."""
        with (
            patch.object(JobManager, "update_progress"),
            patch(
                "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
                return_value=None,
            ),
        ):
            result = await populate_hgvs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_hgvs_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["skipped_count"] == 1

    async def test_updates_progress(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that progress updates are made during the population process."""
        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
            patch(
                "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
                return_value=SAMPLE_CA_ALLELE_DATA,
            ),
        ):
            result = await populate_hgvs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_hgvs_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        mock_update_progress.assert_any_call(0, 100, "Starting mapped HGVS population.")
        mock_update_progress.assert_any_call(100, 100, "Completed mapped HGVS population.")

    async def test_propagates_exceptions(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that unexpected exceptions are propagated."""
        with patch(
            "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
            side_effect=Exception("Test exception"),
        ):
            with pytest.raises(Exception) as exc_info:
                await populate_hgvs_for_score_set(
                    mock_worker_ctx,
                    1,
                    JobManager(session, mock_worker_ctx["redis"], sample_populate_hgvs_run.id),
                )

        assert str(exc_info.value) == "Test exception"


@pytest.mark.asyncio
@pytest.mark.integration
class TestPopulateHgvsForScoreSetIntegration:
    """Integration tests for the populate_hgvs_for_score_set job."""

    async def test_no_mapped_variants(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
    ):
        """Test end-to-end when no mapped variants exist."""
        result = await populate_hgvs_for_score_set(mock_worker_ctx, sample_populate_hgvs_run.id)
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        session.refresh(sample_populate_hgvs_run)
        assert sample_populate_hgvs_run.status == JobStatus.SUCCEEDED

    async def test_successful_hgvs_population(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test end-to-end successful HGVS population."""
        with patch(
            "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
            return_value=SAMPLE_CA_ALLELE_DATA,
        ):
            result = await populate_hgvs_for_score_set(mock_worker_ctx, sample_populate_hgvs_run.id)

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify mapped variant was updated with HGVS
        mapped_variant = session.query(MappedVariant).first()
        assert mapped_variant.hgvs_g == "NC_000001.11:g.12345A>G"

        # Verify annotation status was rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "mapped_hgvs"

        session.refresh(sample_populate_hgvs_run)
        assert sample_populate_hgvs_run.status == JobStatus.SUCCEEDED

    async def test_successful_hgvs_population_pipeline(
        self,
        session,
        with_populated_domain_data,
        mock_worker_ctx,
        sample_populate_hgvs_run_pipeline,
        sample_populate_hgvs_pipeline,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test end-to-end HGVS population in a pipeline."""
        with patch(
            "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
            return_value=SAMPLE_CA_ALLELE_DATA,
        ):
            result = await populate_hgvs_for_score_set(mock_worker_ctx, sample_populate_hgvs_run_pipeline.id)

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify mapped variant was updated
        mapped_variant = session.query(MappedVariant).first()
        assert mapped_variant.hgvs_g == "NC_000001.11:g.12345A>G"

        # Verify annotation status
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "mapped_hgvs"

        # Verify job and pipeline status
        session.refresh(sample_populate_hgvs_run_pipeline)
        assert sample_populate_hgvs_run_pipeline.status == JobStatus.SUCCEEDED

        session.refresh(sample_populate_hgvs_pipeline)
        assert sample_populate_hgvs_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_variant_without_caid_creates_skipped_annotation(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that variants without CAIDs get a skipped annotation status."""
        _, mapped_variant = setup_sample_variants_with_caid_for_hgvs
        mapped_variant.clingen_allele_id = None
        session.commit()

        result = await populate_hgvs_for_score_set(mock_worker_ctx, sample_populate_hgvs_run.id)
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "skipped"
        assert annotation_statuses[0].annotation_type == "mapped_hgvs"

        session.refresh(sample_populate_hgvs_run)
        assert sample_populate_hgvs_run.status == JobStatus.SUCCEEDED

    async def test_exceptions_handled_by_decorators(
        self,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        mock_worker_ctx,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that unexpected exceptions are handled by decorators."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await populate_hgvs_for_score_set(
                mock_worker_ctx,
                sample_populate_hgvs_run.id,
            )

        mock_send_slack_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.ERRORED
        assert isinstance(result.exception, Exception)

        session.refresh(sample_populate_hgvs_run)
        assert sample_populate_hgvs_run.status == JobStatus.ERRORED


@pytest.mark.asyncio
@pytest.mark.integration
class TestPopulateHgvsForScoreSetArqContext:
    """Tests for populate_hgvs_for_score_set job using the ARQ context fixture."""

    async def test_with_arq_context_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that the job works with the ARQ context fixture."""
        with patch(
            "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
            return_value=SAMPLE_CA_ALLELE_DATA,
        ):
            await arq_redis.enqueue_job("populate_hgvs_for_score_set", sample_populate_hgvs_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify mapped variant was updated
        mapped_variant = session.query(MappedVariant).first()
        assert mapped_variant.hgvs_g == "NC_000001.11:g.12345A>G"

        # Verify annotation status
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "mapped_hgvs"

        # Verify job completed
        session.refresh(sample_populate_hgvs_run)
        assert sample_populate_hgvs_run.status == JobStatus.SUCCEEDED

    async def test_with_arq_context_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        sample_populate_hgvs_run_pipeline,
        sample_populate_hgvs_pipeline,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that the job works with the ARQ context fixture in a pipeline."""
        with patch(
            "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
            return_value=SAMPLE_CA_ALLELE_DATA,
        ):
            await arq_redis.enqueue_job("populate_hgvs_for_score_set", sample_populate_hgvs_run_pipeline.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify mapped variant was updated
        mapped_variant = session.query(MappedVariant).first()
        assert mapped_variant.hgvs_g == "NC_000001.11:g.12345A>G"

        # Verify annotation status
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "mapped_hgvs"

        # Verify job and pipeline status
        session.refresh(sample_populate_hgvs_run_pipeline)
        assert sample_populate_hgvs_run_pipeline.status == JobStatus.SUCCEEDED

        session.refresh(sample_populate_hgvs_pipeline)
        assert sample_populate_hgvs_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_with_arq_context_exception_handling_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_populate_hgvs_job,
        sample_populate_hgvs_run,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that exceptions are handled with the ARQ context fixture."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job("populate_hgvs_for_score_set", sample_populate_hgvs_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()

        # Verify no annotations were rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify job errored
        session.refresh(sample_populate_hgvs_run)
        assert sample_populate_hgvs_run.status == JobStatus.ERRORED

    async def test_with_arq_context_exception_handling_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        sample_populate_hgvs_pipeline,
        sample_populate_hgvs_run_pipeline,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Test that exceptions in pipeline context are handled."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.hgvs.get_clingen_allele_data",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job("populate_hgvs_for_score_set", sample_populate_hgvs_run_pipeline.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()

        # Verify no annotations were rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify job errored
        session.refresh(sample_populate_hgvs_run_pipeline)
        assert sample_populate_hgvs_run_pipeline.status == JobStatus.ERRORED

        # Verify pipeline failed
        session.refresh(sample_populate_hgvs_pipeline)
        assert sample_populate_hgvs_pipeline.status == PipelineStatus.FAILED
