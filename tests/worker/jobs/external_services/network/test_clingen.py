from unittest.mock import patch

import pytest
from sqlalchemy import select

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.mapped_variant import MappedVariant
from tests.helpers.util.setup.worker import create_mappings_in_score_set

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


# TODO#XXX: Connect with ClinGen to resolve the invalid credentials issue on test site.
@pytest.mark.skip(reason="invalid credentials, despite what is provided in documentation.")
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.network
class TestE2EClingenSubmitScoreSetMappingsToCar:
    """End-to-end tests for ClinGen CAR submission jobs."""

    async def test_clingen_car_submission_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        mock_s3_client,
        sample_score_set,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_pipeline,
        submit_score_set_mappings_to_car_sample_job_run_in_pipeline,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        """Test the end-to-end flow of submitting score set mappings to ClinGen CAR."""
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            standalone_worker_context,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
                "https://reg.test.genome.network",
            ),
            patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_NAME", "testuser"),
            patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_PASSWORD", "testuser"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_car", submit_score_set_mappings_to_car_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that the submission job was completed successfully
        session.refresh(submit_score_set_mappings_to_car_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_car_sample_job_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that the pipeline run status is succeeded
        session.refresh(submit_score_set_mappings_to_car_sample_pipeline)
        assert submit_score_set_mappings_to_car_sample_pipeline.status == PipelineStatus.SUCCEEDED

        # Verify that variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 4
        for variant in variants:
            assert variant.clingen_allele_id is not None


# TODO#XXX: Connect with ClinGen to resolve the invalid credentials issue on test site.
@pytest.mark.skip(reason="invalid credentials, despite what is provided in documentation.")
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.network
class TestE2EClingenSubmitScoreSetMappingsToLdh:
    """End-to-end tests for ClinGen LDH submission jobs."""

    async def test_clingen_ldh_submission_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        mock_s3_client,
        sample_score_set,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_pipeline,
        submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        """Test the end-to-end flow of submitting score set mappings to ClinGen LDH."""
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            standalone_worker_context,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Patch ClinGenLdhService to simulate all submissions failing
        with (
            patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_NAME", "testuser"),
            patch("mavedb.lib.clingen.services.GENBOREE_ACCOUNT_PASSWORD", "testpassword"),
            patch("mavedb.lib.clingen.constants.LDH_ACCESS_ENDPOINT", "https://genboree.org/ldh-stg/srvc"),
            patch("mavedb.lib.clingen.constants.CLIN_GEN_TENANT", "dev-clingen"),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_ldh", submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that the submission job succeeded
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that the pipeline run status is succeeded
        session.refresh(submit_score_set_mappings_to_ldh_sample_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_pipeline.status == PipelineStatus.SUCCEEDED
