# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from sqlalchemy import select

from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus, JobStatus, PipelineStatus
from mavedb.models.variant_annotation_status import VariantAnnotationStatus

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
class TestE2EPopulateVariantTranslationsForScoreSet:
    """End-to-end test for variant translation population against the real ClinGen API."""

    async def test_populate_variant_translations_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_populate_variant_translations_run_pipeline,
        sample_populate_variant_translations_pipeline,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Enqueue the variant translation job, run the worker, and verify translations are created."""
        _, mapped_variant = setup_sample_variants_with_caid_for_translation

        await arq_redis.enqueue_job(
            "populate_variant_translations_for_score_set",
            sample_populate_variant_translations_run_pipeline.id,
        )
        await arq_worker.async_run()
        await arq_worker.run_check()

        session.refresh(sample_populate_variant_translations_run_pipeline)
        assert sample_populate_variant_translations_run_pipeline.status == JobStatus.SUCCEEDED

        session.refresh(sample_populate_variant_translations_pipeline)
        assert sample_populate_variant_translations_pipeline.status == PipelineStatus.SUCCEEDED

        annotation = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.variant_id == mapped_variant.variant_id,
                VariantAnnotationStatus.annotation_type == AnnotationType.VARIANT_TRANSLATION,
                VariantAnnotationStatus.current.is_(True),
            )
        ).one_or_none()
        assert annotation is not None
        assert annotation.status in (AnnotationStatus.SUCCESS, AnnotationStatus.SKIPPED)
