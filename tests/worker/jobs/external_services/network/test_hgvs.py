"""End-to-end network integration tests for HGVS nomenclature mapping jobs."""

# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.network
class TestE2EHgvsMappingJobs:
    """End-to-end tests for HGVS nomenclature mapping jobs."""

    async def test_populate_hgvs_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_populate_hgvs_pipeline,
        sample_populate_hgvs_pipeline,
        sample_populate_hgvs_run_in_pipeline,
    ):
        """Test the end-to-end flow of populating HGVS nomenclature for mapped variants."""

        from mavedb.models.variant import Variant
        from mavedb.models.mapped_variant import MappedVariant
        from sqlalchemy import select

        # Verify that the score set has mapped variants
        mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
            )
        ).all()

        assert len(mapped_variants) > 0, "Score set should have mapped variants"
        initial_variant_count = len(mapped_variants)

        # Enqueue the HGVS population job
        await arq_redis.enqueue_job("populate_hgvs_for_score_set", sample_populate_hgvs_run_in_pipeline.id)

        # Run the worker to process the job
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify that the job completed successfully
        session.refresh(sample_populate_hgvs_run_in_pipeline)
        assert sample_populate_hgvs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that job metadata contains processing information
        metadata = sample_populate_hgvs_run_in_pipeline.metadata_
        assert "variants_processed" in metadata or "variants_processed_so_far" in metadata

        # Verify that the pipeline run status is succeeded
        session.refresh(sample_populate_hgvs_pipeline)
        assert sample_populate_hgvs_pipeline.status == PipelineStatus.SUCCEEDED

        # Verify that at least some mapped variants have post_mapped HGVS data
        session.refresh(sample_score_set)
        updated_mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
                MappedVariant.post_mapped.isnot(None),
            )
        ).all()

        # Should have populated at least some HGVS data or have no variants to process
        assert len(updated_mapped_variants) > 0 or initial_variant_count == 0

    async def test_populate_hgvs_metadata_tracking(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_populate_hgvs_pipeline,
        sample_populate_hgvs_run_in_pipeline,
    ):
        """Test that HGVS population jobs properly track metadata."""

        from mavedb.models.variant import Variant
        from mavedb.models.mapped_variant import MappedVariant
        from sqlalchemy import select

        # Verify initial state
        mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
            )
        ).all()

        # Enqueue the job
        await arq_redis.enqueue_job("populate_hgvs_for_score_set", sample_populate_hgvs_run_in_pipeline.id)

        # Run the worker
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job metadata
        session.refresh(sample_populate_hgvs_run_in_pipeline)
        metadata = sample_populate_hgvs_run_in_pipeline.metadata_

        # Check for expected metadata fields
        assert "variants_processed" in metadata or "variants_with_hgvs" in metadata or len(mapped_variants) == 0
        assert sample_populate_hgvs_run_in_pipeline.status == JobStatus.SUCCEEDED

    async def test_populate_hgvs_jobs_progress_reporting(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_populate_hgvs_pipeline,
        sample_populate_hgvs_run_in_pipeline,
    ):
        """Test that HGVS population jobs properly report progress."""

        # Enqueue the job
        await arq_redis.enqueue_job("populate_hgvs_for_score_set", sample_populate_hgvs_run_in_pipeline.id)

        # Run the worker
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job completed
        session.refresh(sample_populate_hgvs_run_in_pipeline)
        assert sample_populate_hgvs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify job has context with application metadata
        context = sample_populate_hgvs_run_in_pipeline.context_
        assert context is not None
        assert context.get("application") == "mavedb-worker"
        assert context.get("function") == "populate_hgvs_for_score_set"
        assert context.get("resource") == sample_score_set.urn
        assert context.get("correlation_id") is not None
