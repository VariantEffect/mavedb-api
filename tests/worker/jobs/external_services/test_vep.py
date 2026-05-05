# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from unittest.mock import patch

from sqlalchemy import select

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationFailureCategory, AnnotationStatus, JobStatus
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.worker.jobs.external_services.vep import populate_vep_for_score_set
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.unit
class TestPopulateVepForScoreSetUnit:
    """Unit tests for populate_vep_for_score_set."""

    async def test_no_mapped_variants(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
    ):
        """Job succeeds with zero counts when no mapped variants exist."""
        result = await populate_vep_for_score_set(
            mock_worker_ctx,
            1,
            JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
        )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["variants_processed"] == 0
        assert result.data["variants_with_consequences"] == 0
        assert result.data["variants_recoder_failed"] == 0

    async def test_variant_without_hgvs_assay_level_skipped(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_variants_for_vep,
    ):
        """A mapped variant with no hgvs_assay_level gets a SKIPPED annotation."""
        _, mapped_variant = setup_sample_variants_for_vep
        mapped_variant.hgvs_assay_level = None
        session.commit()

        result = await populate_vep_for_score_set(
            mock_worker_ctx,
            1,
            JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
        )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["variants_processed"] == 0

        annotation = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.variant_id == mapped_variant.variant_id,
                VariantAnnotationStatus.annotation_type == AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            )
        ).one()
        assert annotation.status == AnnotationStatus.SKIPPED
        assert annotation.failure_category == AnnotationFailureCategory.MISSING_IDENTIFIER

    async def test_vep_api_success_sets_consequence_and_annotation(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_variants_for_vep,
    ):
        """VEP returns a consequence: mapped variant and SUCCESS annotation are updated."""
        _, mapped_variant = setup_sample_variants_for_vep
        hgvs = mapped_variant.hgvs_assay_level

        with patch(
            "mavedb.worker.jobs.external_services.vep.get_functional_consequence",
            return_value={hgvs: "missense_variant"},
        ):
            result = await populate_vep_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["variants_processed"] == 1
        assert result.data["variants_with_consequences"] == 1
        assert result.data["variants_recoder_failed"] == 0

        session.refresh(mapped_variant)
        assert mapped_variant.vep_functional_consequence == "missense_variant"
        assert mapped_variant.vep_access_date is not None

        annotation = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.variant_id == mapped_variant.variant_id,
                VariantAnnotationStatus.annotation_type == AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            )
        ).one()
        assert annotation.status == AnnotationStatus.SUCCESS

    async def test_vep_missing_triggers_variant_recoder_fallback(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_variants_for_vep,
    ):
        """When VEP misses a variant, Variant Recoder is called and its result fed back to VEP."""
        _, mapped_variant = setup_sample_variants_for_vep
        hgvs = mapped_variant.hgvs_assay_level
        genomic_hgvs = "NC_000017.11:g.43094692C>T"

        with (
            patch(
                "mavedb.worker.jobs.external_services.vep.get_functional_consequence",
                side_effect=[
                    {},  # initial VEP pass returns nothing
                    {genomic_hgvs: "missense_variant"},  # second VEP pass on recoded HGVS
                ],
            ),
            patch(
                "mavedb.worker.jobs.external_services.vep.run_variant_recoder",
                return_value={hgvs: [genomic_hgvs]},
            ),
        ):
            result = await populate_vep_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["variants_with_consequences"] == 1
        assert result.data["variants_recoder_failed"] == 0

        session.refresh(mapped_variant)
        assert mapped_variant.vep_functional_consequence == "missense_variant"

    async def test_variant_recoder_failure_annotated_as_failed(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_variants_for_vep,
    ):
        """Variant Recoder returning no result for an HGVS produces a FAILED annotation."""
        _, mapped_variant = setup_sample_variants_for_vep

        with (
            patch(
                "mavedb.worker.jobs.external_services.vep.get_functional_consequence",
                return_value={},
            ),
            patch(
                "mavedb.worker.jobs.external_services.vep.run_variant_recoder",
                return_value={},  # recoder has no result
            ),
        ):
            result = await populate_vep_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["variants_without_consequences"] == 0
        assert result.data["variants_recoder_failed"] == 1

        annotation = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.variant_id == mapped_variant.variant_id,
                VariantAnnotationStatus.annotation_type == AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            )
        ).one()
        assert annotation.status == AnnotationStatus.FAILED
        assert annotation.failure_category == AnnotationFailureCategory.EXTERNAL_REFERENCE_NOT_FOUND

    async def test_vep_failure_after_recoder_annotated_as_failed(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_variants_for_vep,
    ):
        """VEP returning no consequence even after Variant Recoder produces a FAILED annotation."""
        _, mapped_variant = setup_sample_variants_for_vep
        hgvs = mapped_variant.hgvs_assay_level
        genomic_hgvs = "NC_000017.11:g.43094692C>T"

        with (
            patch(
                "mavedb.worker.jobs.external_services.vep.get_functional_consequence",
                return_value={},  # VEP returns nothing in both passes
            ),
            patch(
                "mavedb.worker.jobs.external_services.vep.run_variant_recoder",
                return_value={hgvs: [genomic_hgvs]},
            ),
        ):
            result = await populate_vep_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["variants_without_consequences"] == 1
        assert result.data["variants_recoder_failed"] == 0

        annotation = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.variant_id == mapped_variant.variant_id,
                VariantAnnotationStatus.annotation_type == AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            )
        ).one()
        assert annotation.status == AnnotationStatus.FAILED
        assert annotation.failure_category == AnnotationFailureCategory.EXTERNAL_REFERENCE_NOT_FOUND

    async def test_vep_batch_api_exception_raises(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_variants_for_vep,
    ):
        """An unexpected exception from the VEP API propagates to the job management decorator."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.vep.get_functional_consequence",
                side_effect=RuntimeError("VEP API unreachable"),
            ),
            pytest.raises(RuntimeError, match="VEP API unreachable"),
        ):
            await populate_vep_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
            )

    async def test_variant_recoder_api_exception_raises(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_variants_for_vep,
    ):
        """An unexpected exception from the Variant Recoder API propagates to the job management decorator."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.vep.get_functional_consequence",
                return_value={},
            ),
            patch(
                "mavedb.worker.jobs.external_services.vep.run_variant_recoder",
                side_effect=RuntimeError("Recoder API unreachable"),
            ),
            pytest.raises(RuntimeError, match="Recoder API unreachable"),
        ):
            await populate_vep_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
            )


@pytest.mark.asyncio
@pytest.mark.integration
class TestPopulateVepForScoreSetIntegration:
    """Integration tests for populate_vep_for_score_set run through an ARQ worker context."""

    async def test_populate_vep_with_arq_context(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        sample_populate_vep_run,
        setup_sample_variants_for_vep,
    ):
        """Job completes successfully within an ARQ worker context."""
        _, mapped_variant = setup_sample_variants_for_vep
        hgvs = mapped_variant.hgvs_assay_level

        with patch(
            "mavedb.worker.jobs.external_services.vep.get_functional_consequence",
            return_value={hgvs: "missense_variant"},
        ):
            await arq_redis.enqueue_job("populate_vep_for_score_set", sample_populate_vep_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        session.refresh(sample_populate_vep_run)
        assert sample_populate_vep_run.status == JobStatus.SUCCEEDED

        session.refresh(mapped_variant)
        assert mapped_variant.vep_functional_consequence == "missense_variant"

        annotation = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.variant_id == mapped_variant.variant_id,
                VariantAnnotationStatus.annotation_type == AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            )
        ).one()
        assert annotation.status == AnnotationStatus.SUCCESS

    async def test_populate_vep_in_pipeline_context(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        sample_populate_vep_run_pipeline,
        sample_populate_vep_pipeline,
        setup_sample_variants_for_vep,
    ):
        """Job completes and advances the pipeline when run in a pipeline context."""
        from mavedb.models.enums.job_pipeline import PipelineStatus

        _, mapped_variant = setup_sample_variants_for_vep
        hgvs = mapped_variant.hgvs_assay_level

        with patch(
            "mavedb.worker.jobs.external_services.vep.get_functional_consequence",
            return_value={hgvs: "synonymous_variant"},
        ):
            await arq_redis.enqueue_job("populate_vep_for_score_set", sample_populate_vep_run_pipeline.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        session.refresh(sample_populate_vep_run_pipeline)
        assert sample_populate_vep_run_pipeline.status == JobStatus.SUCCEEDED

        session.refresh(sample_populate_vep_pipeline)
        assert sample_populate_vep_pipeline.status == PipelineStatus.SUCCEEDED
