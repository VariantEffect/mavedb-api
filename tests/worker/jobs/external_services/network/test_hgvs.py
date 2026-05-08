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
class TestE2EPopulateHgvsForScoreSet:
    """End-to-end test for HGVS population against the real ClinGen API."""

    async def test_populate_hgvs_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_populate_hgvs_run_pipeline,
        sample_populate_hgvs_pipeline,
        setup_sample_variants_with_caid_for_hgvs,
    ):
        """Enqueue the HGVS population job, run the worker, and verify HGVS fields are populated."""
        _, mapped_variant = setup_sample_variants_with_caid_for_hgvs

        await arq_redis.enqueue_job("populate_hgvs_for_score_set", sample_populate_hgvs_run_pipeline.id)
        await arq_worker.async_run()
        await arq_worker.run_check()

        session.refresh(sample_populate_hgvs_run_pipeline)
        assert sample_populate_hgvs_run_pipeline.status == JobStatus.SUCCEEDED

        session.refresh(sample_populate_hgvs_pipeline)
        assert sample_populate_hgvs_pipeline.status == PipelineStatus.SUCCEEDED

        session.refresh(mapped_variant)
        annotation = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.variant_id == mapped_variant.variant_id,
                VariantAnnotationStatus.annotation_type == AnnotationType.MAPPED_HGVS,
                VariantAnnotationStatus.current.is_(True),
            )
        ).one_or_none()
        assert annotation is not None
        assert annotation.status in (AnnotationStatus.SUCCESS, AnnotationStatus.SKIPPED)
