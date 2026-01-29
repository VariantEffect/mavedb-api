# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from tests.helpers.constants import TEST_REFSEQ_IDENTIFIER

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.network
class TestE2EUniprotMappingJobs:
    """End-to-end tests for UniProt mapping jobs."""

    async def test_uniprot_mapping_jobs_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_score_set,
        with_submit_uniprot_mapping_jobs_pipeline,
        sample_submit_uniprot_mapping_jobs_pipeline,
        sample_submit_uniprot_mapping_jobs_run_in_pipeline,
        sample_polling_job_for_submission_run_in_pipeline,
    ):
        """Test the end-to-end flow of submitting and polling UniProt mapping jobs."""

        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [TEST_REFSEQ_IDENTIFIER]}}
        session.commit()

        await arq_redis.enqueue_job(
            "submit_uniprot_mapping_jobs_for_score_set", sample_submit_uniprot_mapping_jobs_run_in_pipeline.id
        )
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify that the job metadata contains the submitted job
        session.refresh(sample_submit_uniprot_mapping_jobs_run_in_pipeline)
        submitted_jobs = sample_submit_uniprot_mapping_jobs_run_in_pipeline.metadata_["submitted_jobs"]
        assert "1" in submitted_jobs
        assert submitted_jobs["1"]["job_id"] is not None
        assert submitted_jobs["1"]["accession"] == TEST_REFSEQ_IDENTIFIER

        # Verify that polling job params have been updated correctly
        session.refresh(sample_polling_job_for_submission_run_in_pipeline)
        assert sample_polling_job_for_submission_run_in_pipeline.job_params["mapping_jobs"] == {
            "1": {"job_id": submitted_jobs["1"]["job_id"], "accession": TEST_REFSEQ_IDENTIFIER}
        }

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run_in_pipeline)
        assert sample_submit_uniprot_mapping_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job has run and is succeeded (pipeline ctx)
        session.refresh(sample_polling_job_for_submission_run_in_pipeline)
        assert sample_polling_job_for_submission_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that the pipeline run status is running
        session.refresh(sample_submit_uniprot_mapping_jobs_pipeline)
        assert sample_submit_uniprot_mapping_jobs_pipeline.status == PipelineStatus.SUCCEEDED
