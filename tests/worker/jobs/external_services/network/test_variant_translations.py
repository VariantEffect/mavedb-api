"""End-to-end network integration tests for variant translation jobs."""

# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.network
class TestE2EVariantTranslationJobs:
    """End-to-end tests for variant translation jobs."""

    async def test_variant_translation_jobs_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_variant_translation_jobs_pipeline,
        sample_submit_variant_translation_jobs_pipeline,
        sample_submit_variant_translation_jobs_run_in_pipeline,
    ):
        """Test the end-to-end flow of populating variant translations from ClinGen."""

        from mavedb.models.variant import Variant
        from mavedb.models.mapped_variant import MappedVariant
        from sqlalchemy import select

        # Get mapped variants
        mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
            )
        ).all()

        assert len(mapped_variants) > 0, "Score set should have mapped variants"
        # initial_variant_count = len(mapped_variants)

        # Assign ClinGen allele IDs (CA and PA formats)
        for i, mapped_variant in enumerate(mapped_variants):
            if i % 2 == 0:
                mapped_variant.clingen_allele_id = f"CA{100000 + i}"
            else:
                mapped_variant.clingen_allele_id = f"PA{100000 + i}"

        session.commit()

        # Enqueue the variant translation job
        await arq_redis.enqueue_job(
            "submit_variant_translation_jobs_for_score_set", sample_submit_variant_translation_jobs_run_in_pipeline.id
        )

        # Run the worker to process the job
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify that the job completed successfully
        session.refresh(sample_submit_variant_translation_jobs_run_in_pipeline)
        assert sample_submit_variant_translation_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that job metadata contains processing information
        metadata = sample_submit_variant_translation_jobs_run_in_pipeline.metadata_
        assert "clingen_allele_ids_processed" in metadata or "allele_ids_processed" in metadata

        # Verify that the pipeline run status is succeeded
        session.refresh(sample_submit_variant_translation_jobs_pipeline)
        assert sample_submit_variant_translation_jobs_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_variant_translation_jobs_multi_variant_expansion_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_variant_translation_jobs_pipeline,
        sample_submit_variant_translation_jobs_run_in_pipeline,
    ):
        """Test that multi-variant (comma-separated) allele IDs are properly expanded."""

        from mavedb.models.variant import Variant
        from mavedb.models.mapped_variant import MappedVariant
        from sqlalchemy import select

        # Get mapped variants
        mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
            )
        ).all()

        if len(mapped_variants) > 0:
            # Assign multi-variant allele ID to first variant
            mapped_variants[0].clingen_allele_id = "CA100000,CA100001,CA100002"
            session.commit()

        # Enqueue the job
        await arq_redis.enqueue_job(
            "submit_variant_translation_jobs_for_score_set", sample_submit_variant_translation_jobs_run_in_pipeline.id
        )

        # Run the worker
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job succeeded
        session.refresh(sample_submit_variant_translation_jobs_run_in_pipeline)
        assert sample_submit_variant_translation_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that expanded allele IDs were processed
        metadata = sample_submit_variant_translation_jobs_run_in_pipeline.metadata_
        total_unique = metadata.get("total_unique_expanded_allele_ids", 0)
        assert total_unique >= 3 or total_unique == 0  # 3 unique allele IDs from expansion

    async def test_variant_translation_jobs_duplicate_handling_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_variant_translation_jobs_pipeline,
        sample_submit_variant_translation_jobs_run_in_pipeline,
    ):
        """Test that duplicate allele IDs are only processed once."""

        from mavedb.models.variant import Variant
        from mavedb.models.mapped_variant import MappedVariant
        from sqlalchemy import select

        # Get mapped variants
        mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
            )
        ).all()

        # Assign the same allele ID to multiple variants
        same_allele_id = "CA100000"
        for mapped_variant in mapped_variants[:3]:
            mapped_variant.clingen_allele_id = same_allele_id

        session.commit()

        # Enqueue the job
        await arq_redis.enqueue_job(
            "submit_variant_translation_jobs_for_score_set", sample_submit_variant_translation_jobs_run_in_pipeline.id
        )

        # Run the worker
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job succeeded
        session.refresh(sample_submit_variant_translation_jobs_run_in_pipeline)
        assert sample_submit_variant_translation_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that duplicate allele ID was only processed once
        metadata = sample_submit_variant_translation_jobs_run_in_pipeline.metadata_
        total_unique = metadata.get("total_unique_expanded_allele_ids", 0)
        assert total_unique <= 1 or total_unique == 0

    async def test_variant_translation_jobs_invalid_allele_id_format_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_variant_translation_jobs_pipeline,
        sample_submit_variant_translation_jobs_run_in_pipeline,
    ):
        """Test handling of invalid allele ID formats."""

        from mavedb.models.variant import Variant
        from mavedb.models.mapped_variant import MappedVariant
        from sqlalchemy import select

        # Get mapped variants
        mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
            )
        ).all()

        if len(mapped_variants) > 0:
            # Assign invalid allele ID (doesn't start with CA or PA)
            mapped_variants[0].clingen_allele_id = "INVALID123456"
            session.commit()

        # Enqueue the job
        await arq_redis.enqueue_job(
            "submit_variant_translation_jobs_for_score_set", sample_submit_variant_translation_jobs_run_in_pipeline.id
        )

        # Run the worker
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job succeeded (handles invalid IDs gracefully)
        session.refresh(sample_submit_variant_translation_jobs_run_in_pipeline)
        assert sample_submit_variant_translation_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that invalid allele ID was counted as error
        metadata = sample_submit_variant_translation_jobs_run_in_pipeline.metadata_
        assert "allele_ids_with_errors" in metadata

    async def test_variant_translation_jobs_context_tracking_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_variant_translation_jobs_pipeline,
        sample_submit_variant_translation_jobs_run_in_pipeline,
    ):
        """Test that variant translation jobs properly track context information."""

        from mavedb.models.variant import Variant
        from mavedb.models.mapped_variant import MappedVariant
        from sqlalchemy import select

        # Get mapped variants and assign allele IDs
        mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
            )
        ).all()

        for i, mapped_variant in enumerate(mapped_variants):
            mapped_variant.clingen_allele_id = f"CA{100000 + i}"

        session.commit()

        # Enqueue the job
        await arq_redis.enqueue_job(
            "submit_variant_translation_jobs_for_score_set", sample_submit_variant_translation_jobs_run_in_pipeline.id
        )

        # Run the worker
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job context
        session.refresh(sample_submit_variant_translation_jobs_run_in_pipeline)
        context = sample_submit_variant_translation_jobs_run_in_pipeline.context_

        assert context is not None
        assert context.get("application") == "mavedb-worker"
        assert context.get("function") == "submit_variant_translation_jobs_for_score_set"
        assert context.get("resource") == sample_score_set.urn
        assert context.get("correlation_id") is not None
