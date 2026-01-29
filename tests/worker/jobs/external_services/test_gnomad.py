# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from unittest.mock import MagicMock, call, patch

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.worker.jobs.external_services.gnomad import link_gnomad_variants
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.unit
class TestLinkGnomadVariantsUnit:
    """Unit tests for the link_gnomad_variants job."""

    @pytest.fixture
    def setup_sample_variants_with_caid(
        self, session, with_populated_domain_data, mock_worker_ctx, sample_link_gnomad_variants_run
    ):
        """Setup variants and mapped variants in the database for testing."""
        score_set = session.get(ScoreSet, sample_link_gnomad_variants_run.job_params["score_set_id"])

        # Add a variant and mapped variant to the database with a CAID
        variant = Variant(
            urn="urn:variant:test-variant-with-caid",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={"hgvs_c": "NM_000000.1:c.1A>G", "hgvs_p": "NP_000000.1:p.Met1Val"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA123",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

    async def test_link_gnomad_variants_no_variants_with_caids(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
    ):
        """Test linking gnomAD variants when no mapped variants have CAIDs."""
        with patch.object(JobManager, "update_progress") as mock_update_progress:
            result = await link_gnomad_variants(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_link_gnomad_variants_run.id),
            )

        assert result["status"] == "ok"
        mock_update_progress.assert_any_call(
            100, 100, "No variants with CAIDs found to link to gnomAD variants. Nothing to do."
        )

    async def test_link_gnomad_variants_no_gnomad_matches(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_variants_with_caid,
        athena_engine,
    ):
        """Test linking gnomAD variants when no gnomAD variants match the CAIDs."""

        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                return_value={},
            ),
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            result = await link_gnomad_variants(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_link_gnomad_variants_run.id),
            )

        assert result["status"] == "ok"
        mock_update_progress.assert_any_call(100, 100, "Linked 0 mapped variants to gnomAD variants.")

    async def test_link_gnomad_variants_call_linking_method(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_variants_with_caid,
        athena_engine,
    ):
        """Test that the linking method is called when gnomAD variants match CAIDs."""

        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                return_value=[MagicMock()],
            ),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.link_gnomad_variants_to_mapped_variants",
                return_value=1,
            ) as mock_linking_method,
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            result = await link_gnomad_variants(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_link_gnomad_variants_run.id),
            )

        assert result["status"] == "ok"
        mock_linking_method.assert_called_once()
        mock_update_progress.assert_any_call(100, 100, "Linked 1 mapped variants to gnomAD variants.")

    async def test_link_gnomad_variants_updates_progress(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_variants_with_caid,
        athena_engine,
    ):
        """Test that progress updates are made during the linking process."""

        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                return_value=[MagicMock()],
            ),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.link_gnomad_variants_to_mapped_variants",
                return_value=1,
            ),
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            result = await link_gnomad_variants(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_link_gnomad_variants_run.id),
            )

        assert result["status"] == "ok"
        mock_update_progress.assert_has_calls(
            [
                call(0, 100, "Starting gnomAD mapped resource linkage."),
                call(10, 100, "Found 1 variants with CAIDs to link to gnomAD variants."),
                call(75, 100, "Found 1 gnomAD variants matching CAIDs."),
                call(100, 100, "Linked 1 mapped variants to gnomAD variants."),
            ]
        )

    async def test_link_gnomad_variants_propagates_exceptions(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_variants_with_caid,
        athena_engine,
    ):
        """Test that exceptions during the linking process are propagated."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            with pytest.raises(Exception) as exc_info:
                await link_gnomad_variants(
                    mock_worker_ctx,
                    1,
                    JobManager(session, mock_worker_ctx["redis"], sample_link_gnomad_variants_run.id),
                )

        assert str(exc_info.value) == "Test exception"


@pytest.mark.asyncio
@pytest.mark.integration
class TestLinkGnomadVariantsIntegration:
    """Integration tests for the link_gnomad_variants job."""

    async def test_link_gnomad_variants_no_variants_with_caids(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
    ):
        """Test the end-to-end functionality of the link_gnomad_variants job when no variants have CAIDs."""

        result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run.id)
        assert result["status"] == "ok"

        # Verify that no gnomAD variants were linked
        gnomad_variants = session.query(GnomADVariant).all()
        assert len(gnomad_variants) == 0

        # Verify no annotations were rendered (since there were no variants with CAIDs)
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_no_matching_caids(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_variants_with_caid,
        athena_engine,
    ):
        """Test the end-to-end functionality of the link_gnomad_variants job when no matching CAIDs are found."""
        # Update the created mapped variant to have a CAID that won't match any gnomAD data
        mapped_variant = session.query(MappedVariant).first()
        mapped_variant.clingen_allele_id = "NON_MATCHING_CAID"
        session.commit()

        # Patch the athena engine to use the mock athena_engine fixture
        with patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine):
            result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run.id)

        assert result["status"] == "ok"

        # Verify that no gnomAD variants were linked
        gnomad_variants = session.query(GnomADVariant).all()
        assert len(gnomad_variants) == 0

        # Verify a skipped annotation status was rendered (since there were variants with CAIDs)
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "skipped"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_successful_linking_independent(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_variants_with_caid,
        athena_engine,
    ):
        """Test the end-to-end functionality of the link_gnomad_variants job with successful linking."""

        # Patch the athena engine to use the mock athena_engine fixture
        with patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine):
            result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run.id)

        assert result["status"] == "ok"

        # Verify that gnomAD variants were linked
        gnomad_variants = session.query(GnomADVariant).all()
        assert len(gnomad_variants) > 0

        # Verify annotation status was rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_successful_linking_pipeline(
        self,
        session,
        with_populated_domain_data,
        mock_worker_ctx,
        sample_link_gnomad_variants_run_pipeline,
        sample_link_gnomad_variants_pipeline,
        setup_sample_variants_with_caid,
        athena_engine,
    ):
        """Test the end-to-end functionality of the link_gnomad_variants job with successful linking in a pipeline."""

        # Patch the athena engine to use the mock athena_engine fixture
        with patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine):
            result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run_pipeline.id)

        assert result["status"] == "ok"

        # Verify that gnomAD variants were linked
        gnomad_variants = session.query(GnomADVariant).all()
        assert len(gnomad_variants) > 0

        # Verify annotation status was rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run_pipeline)
        assert sample_link_gnomad_variants_run_pipeline.status == JobStatus.SUCCEEDED

        # Verify pipeline status updates
        session.refresh(sample_link_gnomad_variants_pipeline)
        assert sample_link_gnomad_variants_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_link_gnomad_variants_exceptions_handled_by_decorators(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_variants_with_caid,
        athena_engine,
    ):
        """Test that exceptions during the linking process are handled by decorators."""

        # Patch the athena engine to use the mock athena_engine fixture
        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await link_gnomad_variants(
                mock_worker_ctx,
                sample_link_gnomad_variants_run.id,
            )

        mock_send_slack_error.assert_called_once()
        assert result["status"] == "exception"
        assert isinstance(result["exception"], Exception)

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.FAILED


@pytest.mark.asyncio
@pytest.mark.integration
class TestLinkGnomadVariantsArqContext:
    """Tests for link_gnomad_variants job using the ARQ context fixture."""

    async def test_link_gnomad_variants_with_arq_context_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        athena_engine,
        sample_link_gnomad_variants_run,
        setup_sample_variants_with_caid,
    ):
        """Test that the link_gnomad_variants job works with the ARQ context fixture."""

        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            await arq_redis.enqueue_job("link_gnomad_variants", sample_link_gnomad_variants_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that gnomAD variants were linked
        gnomad_variants = session.query(GnomADVariant).all()
        assert len(gnomad_variants) > 0

        # Verify annotation status was rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify that the job completed successfully
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_with_arq_context_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        athena_engine,
        sample_link_gnomad_variants_run_pipeline,
        sample_link_gnomad_variants_pipeline,
        setup_sample_variants_with_caid,
    ):
        """Test that the link_gnomad_variants job works with the ARQ context fixture in a pipeline."""

        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            await arq_redis.enqueue_job("link_gnomad_variants", sample_link_gnomad_variants_run_pipeline.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that gnomAD variants were linked
        gnomad_variants = session.query(GnomADVariant).all()
        assert len(gnomad_variants) > 0

        # Verify annotation status was rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify that the job completed successfully
        session.refresh(sample_link_gnomad_variants_run_pipeline)
        assert sample_link_gnomad_variants_run_pipeline.status == JobStatus.SUCCEEDED

        # Verify pipeline status updates
        session.refresh(sample_link_gnomad_variants_pipeline)
        assert sample_link_gnomad_variants_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_link_gnomad_variants_with_arq_context_exception_handling_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        athena_engine,
        sample_link_gnomad_variants_run,
        setup_sample_variants_with_caid,
    ):
        """Test that exceptions in the link_gnomad_variants job are handled with the ARQ context fixture."""

        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job("link_gnomad_variants", sample_link_gnomad_variants_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify that no gnomAD variants were linked
        gnomad_variants = session.query(GnomADVariant).all()
        assert len(gnomad_variants) == 0

        # Verify no annotations were rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job failed
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.FAILED

    async def test_link_gnomad_variants_with_arq_context_exception_handling_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        athena_engine,
        sample_link_gnomad_variants_pipeline,
        sample_link_gnomad_variants_run_pipeline,
        setup_sample_variants_with_caid,
    ):
        """Test that exceptions in the link_gnomad_variants job are handled with the ARQ context fixture."""

        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job("link_gnomad_variants", sample_link_gnomad_variants_run_pipeline.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify that no gnomAD variants were linked
        gnomad_variants = session.query(GnomADVariant).all()
        assert len(gnomad_variants) == 0

        # Verify no annotations were rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job failed
        session.refresh(sample_link_gnomad_variants_run_pipeline)
        assert sample_link_gnomad_variants_run_pipeline.status == JobStatus.FAILED

        # Verify that the pipeline failed
        session.refresh(sample_link_gnomad_variants_pipeline)
        assert sample_link_gnomad_variants_pipeline.status == PipelineStatus.FAILED
