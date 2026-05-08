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
class TestE2EPopulateVepForScoreSet:
    """End-to-end test for VEP functional consequence prediction against the real Ensembl API."""

    async def test_populate_vep_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_populate_vep_run_pipeline,
        sample_populate_vep_pipeline,
        setup_sample_variants_for_vep,
    ):
        """Enqueue the VEP job, run the worker, and verify consequence and annotation are populated."""
        _, mapped_variant = setup_sample_variants_for_vep

        await arq_redis.enqueue_job("populate_vep_for_score_set", sample_populate_vep_run_pipeline.id)
        await arq_worker.async_run()
        await arq_worker.run_check()

        session.refresh(sample_populate_vep_run_pipeline)
        assert sample_populate_vep_run_pipeline.status == JobStatus.SUCCEEDED

        session.refresh(sample_populate_vep_pipeline)
        assert sample_populate_vep_pipeline.status == PipelineStatus.SUCCEEDED

        session.refresh(mapped_variant)
        assert mapped_variant.vep_functional_consequence is not None
        assert mapped_variant.vep_access_date is not None

        annotation = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.variant_id == mapped_variant.variant_id,
                VariantAnnotationStatus.annotation_type == AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
                VariantAnnotationStatus.current.is_(True),
            )
        ).one()
        assert annotation.status == AnnotationStatus.SUCCESS
