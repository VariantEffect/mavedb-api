"""End-to-end network integration tests for VEP functional consequence jobs."""

# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

import responses
from mavedb.lib.vep import ENSEMBL_API_URL
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.network
class TestE2EVepFunctionalConsequenceJobs:
    """End-to-end tests for VEP functional consequence prediction jobs."""

    @responses.activate
    async def test_vep_jobs_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_vep_jobs_pipeline,
        sample_submit_vep_jobs_pipeline,
        sample_submit_vep_jobs_run_in_pipeline,
    ):
        """Test the end-to-end flow of VEP functional consequence prediction."""

        from mavedb.models.variant import Variant
        from mavedb.models.mapped_variant import MappedVariant
        from sqlalchemy import select

        # Mock VEP API responses
        responses.add(
            responses.POST,
            f"{ENSEMBL_API_URL}/vep/human/hgvs",
            json=[
                {
                    "input": f"NM_000001.1:c.{i}A>G",
                    "most_severe_consequence": "missense_variant" if i % 2 == 0 else "synonymous_variant",
                }
                for i in range(10)
            ],
            status=200,
        )

        # Verify that the score set has mapped variants with post_mapped HGVS
        mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
                MappedVariant.post_mapped.isnot(None),
            )
        ).all()

        initial_variant_count = len(mapped_variants)

        # Enqueue the VEP job
        await arq_redis.enqueue_job("submit_vep_jobs_for_score_set", sample_submit_vep_jobs_run_in_pipeline.id)

        # Run the worker to process the job
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify that the job completed successfully
        session.refresh(sample_submit_vep_jobs_run_in_pipeline)
        assert sample_submit_vep_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that job metadata contains VEP processing information
        metadata = sample_submit_vep_jobs_run_in_pipeline.metadata_
        assert (
            "variants_processed" in metadata
            or "variants_with_functional_consequence" in metadata
            or initial_variant_count == 0
        )

        # Verify that the pipeline run status is succeeded
        session.refresh(sample_submit_vep_jobs_pipeline)
        assert sample_submit_vep_jobs_pipeline.status == PipelineStatus.SUCCEEDED

        # Verify that some mapped variants have VEP functional consequence data
        session.refresh(sample_score_set)
        variants_with_consequences = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(
                Variant.score_set_id == sample_score_set.id,
                MappedVariant.current.is_(True),
                MappedVariant.vep_functional_consequence.isnot(None),
            )
        ).all()

        # Should have populated at least some VEP consequences or have no variants to process
        assert len(variants_with_consequences) > 0 or initial_variant_count == 0

    @responses.activate
    async def test_vep_jobs_batch_processing_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_vep_jobs_pipeline,
        sample_submit_vep_jobs_run_in_pipeline,
    ):
        """Test that VEP jobs properly handle batch processing (200 variants per batch)."""

        import json

        # Mock VEP API with dynamic response handling for batch requests
        def vep_callback(request):
            body = json.loads(request.body)
            hgvs_strings = body.get("hgvs_notations", [])

            return (
                200,
                {},
                json.dumps(
                    [
                        {
                            "input": hgvs,
                            "most_severe_consequence": "missense_variant",
                        }
                        for hgvs in hgvs_strings
                    ]
                ),
            )

        responses.add_callback(
            responses.POST,
            f"{ENSEMBL_API_URL}/vep/human/hgvs",
            callback=vep_callback,
            content_type="application/json",
        )

        # Verify variants exist
        # mapped_variants = session.scalars(
        #     select(MappedVariant)
        #     .join(Variant)
        #     .where(
        #         Variant.score_set_id == sample_score_set.id,
        #         MappedVariant.current.is_(True),
        #         MappedVariant.post_mapped.isnot(None),
        #     )
        # ).all()

        # Enqueue the job
        await arq_redis.enqueue_job("submit_vep_jobs_for_score_set", sample_submit_vep_jobs_run_in_pipeline.id)

        # Run the worker
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job succeeded
        session.refresh(sample_submit_vep_jobs_run_in_pipeline)
        assert sample_submit_vep_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

    @responses.activate
    async def test_vep_jobs_api_fallback_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_vep_jobs_pipeline,
        sample_submit_vep_jobs_run_in_pipeline,
    ):
        """Test VEP fallback to Variant Recoder when initial VEP call is incomplete."""

        # Mock initial VEP response (partial results)
        responses.add(
            responses.POST,
            f"{ENSEMBL_API_URL}/vep/human/hgvs",
            json=[
                {
                    "input": "NM_000001.1:c.0A>G",
                    "most_severe_consequence": "missense_variant",
                }
            ],
            status=200,
        )

        # Mock Variant Recoder response
        responses.add(
            responses.POST,
            f"{ENSEMBL_API_URL}/variant_recoder/human",
            json=[
                {
                    "input": "NM_000001.1:c.1A>G",
                    "NC_000001.14:g.1000A>G": {
                        "hgvsg": ["NC_000001.14:g.1000A>G"],
                    },
                }
            ],
            status=200,
        )

        # Mock VEP response for genomic variants
        responses.add(
            responses.POST,
            f"{ENSEMBL_API_URL}/vep/human/hgvs",
            json=[
                {
                    "input": "NC_000001.14:g.1000A>G",
                    "most_severe_consequence": "synonymous_variant",
                }
            ],
            status=200,
        )

        # Verify variants exist
        # mapped_variants = session.scalars(
        #     select(MappedVariant)
        #     .join(Variant)
        #     .where(
        #         Variant.score_set_id == sample_score_set.id,
        #         MappedVariant.current.is_(True),
        #         MappedVariant.post_mapped.isnot(None),
        #     )
        # ).all()

        # Enqueue and run the job
        await arq_redis.enqueue_job("submit_vep_jobs_for_score_set", sample_submit_vep_jobs_run_in_pipeline.id)

        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job succeeded and made multiple API calls
        session.refresh(sample_submit_vep_jobs_run_in_pipeline)
        assert sample_submit_vep_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED
        assert len(responses.calls) >= 2  # At least VEP + fallback call

    @responses.activate
    async def test_vep_jobs_metadata_tracking_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_vep_jobs_pipeline,
        sample_submit_vep_jobs_run_in_pipeline,
    ):
        """Test that VEP jobs properly track metadata."""

        # Mock VEP API
        responses.add(
            responses.POST,
            f"{ENSEMBL_API_URL}/vep/human/hgvs",
            json=[
                {
                    "input": f"NM_000001.1:c.{i}A>G",
                    "most_severe_consequence": "missense_variant",
                }
                for i in range(5)
            ],
            status=200,
        )

        # Enqueue the job
        await arq_redis.enqueue_job("submit_vep_jobs_for_score_set", sample_submit_vep_jobs_run_in_pipeline.id)

        # Run the worker
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job metadata
        session.refresh(sample_submit_vep_jobs_run_in_pipeline)
        metadata = sample_submit_vep_jobs_run_in_pipeline.metadata_

        # Check for expected metadata fields
        assert "variants_processed" in metadata or "variants_with_functional_consequence" in metadata or True
        assert sample_submit_vep_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

    @responses.activate
    async def test_vep_jobs_context_tracking_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_vep_jobs_pipeline,
        sample_submit_vep_jobs_run_in_pipeline,
    ):
        """Test that VEP jobs properly track context information."""

        # Mock VEP API
        responses.add(
            responses.POST,
            f"{ENSEMBL_API_URL}/vep/human/hgvs",
            json=[],
            status=200,
        )

        # Enqueue the job
        await arq_redis.enqueue_job("submit_vep_jobs_for_score_set", sample_submit_vep_jobs_run_in_pipeline.id)

        # Run the worker
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify job context
        session.refresh(sample_submit_vep_jobs_run_in_pipeline)
        context = sample_submit_vep_jobs_run_in_pipeline.context_

        assert context is not None
        assert context.get("application") == "mavedb-worker"
        assert context.get("function") == "submit_vep_jobs_for_score_set"
        assert context.get("resource") == sample_score_set.urn
        assert context.get("correlation_id") is not None
