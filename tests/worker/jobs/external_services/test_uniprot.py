# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from unittest.mock import call, patch

from mavedb.lib.exceptions import (
    NonExistentTargetGeneError,
    UniprotAmbiguousMappingResultError,
    UniprotMappingResultNotFoundError,
    UniProtPollingEnqueueError,
)
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.worker.jobs.external_services.uniprot import (
    poll_uniprot_mapping_jobs_for_score_set,
    submit_uniprot_mapping_jobs_for_score_set,
)
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.constants import (
    TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
    TEST_UNIPROT_SWISS_PROT_TYPE,
    VALID_NT_ACCESSION,
    VALID_UNIPROT_ACCESSION,
)

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.unit
@pytest.mark.asyncio
class TestSubmitUniprotMappingJobsForScoreSetUnit:
    """Unit tests for submit_uniprot_mapping_jobs_for_score_set function."""

    async def test_submit_uniprot_mapping_jobs_no_targets(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
    ):
        """Test submitting UniProt mapping jobs when no target genes are present."""

        # Ensure the sample score set has no target genes
        sample_score_set.target_genes = []
        session.commit()

        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_submit_uniprot_mapping_jobs_run.id,
                ),
            )

        mock_update_progress.assert_called_with(
            100, 100, "No target genes found. Skipped UniProt mapping job submission."
        )
        assert job_result["status"] == "ok"

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {}

    async def test_submit_uniprot_mapping_jobs_no_acs_in_post_mapped_metadata(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
    ):
        """Test submitting UniProt mapping jobs when no ACs are present in post mapped metadata."""

        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_submit_uniprot_mapping_jobs_run.id,
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "No UniProt mapping jobs were submitted.")
        assert job_result["status"] == "ok"

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {}

    async def test_submit_uniprot_mapping_jobs_too_many_acs_in_post_mapped_metadata(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
    ):
        """Test submitting UniProt mapping jobs when too many ACs are present in post mapped metadata."""

        # Arrange the post mapped metadata to have multiple ACs
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION, "P67890"]}}
        session.commit()

        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_submit_uniprot_mapping_jobs_run.id,
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "No UniProt mapping jobs were submitted.")
        assert job_result["status"] == "ok"

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {}

    async def test_submit_uniprot_mapping_jobs_no_jobs_submitted(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
    ):
        """Test submitting UniProt mapping jobs when no jobs are submitted."""

        # Arrange the post mapped metadata to have a single AC
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value=None,
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_submit_uniprot_mapping_jobs_run.id,
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "No UniProt mapping jobs were submitted.")
        assert job_result["status"] == "ok"

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {
            "1": {"job_id": None, "accession": VALID_NT_ACCESSION}
        }

    async def test_submit_uniprot_mapping_jobs_api_failure_raises(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
    ):
        """Test handling of UniProt API failure during job submission."""

        # Arrange the post mapped metadata to have a single AC
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                side_effect=Exception("UniProt API failure"),
            ),
            patch.object(JobManager, "update_progress"),
            pytest.raises(Exception, match="UniProt API failure"),
        ):
            await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_submit_uniprot_mapping_jobs_run.id,
                ),
            )

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {}

    async def test_submit_uniprot_mapping_jobs_raises_dependent_job_not_available(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
    ):
        """Test handling when dependent polling job is not available."""

        # Arrange the post mapped metadata to have a single AC
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_submit_uniprot_mapping_jobs_run.id,
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "Failed to submit UniProt mapping jobs.")
        assert result["status"] == "failed"
        assert isinstance(result["exception"], UniProtPollingEnqueueError)

        # Verify that the job metadata contains the submitted jobs (which were submitted before the error)
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }

    async def test_submit_uniprot_mapping_jobs_successful_submission(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Test successful submission of UniProt mapping jobs."""

        # Arrange the post mapped metadata to have a single AC
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ),
            patch.object(JobManager, "update_progress"),
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_submit_uniprot_mapping_jobs_run.id,
                ),
            )

        assert job_result["status"] == "ok"

        expected_submitted_jobs = {"1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}}

        # Verify that the job metadata contains the submitted job
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == expected_submitted_jobs

        # Verify that polling job params have been updated correctly
        session.refresh(sample_dummy_polling_job_for_submission_run)
        assert sample_dummy_polling_job_for_submission_run.job_params["mapping_jobs"] == expected_submitted_jobs

    async def test_submit_uniprot_mapping_jobs_partial_submission(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Test partial submission of UniProt mapping jobs."""

        # Add another target gene to the score set to simulate multiple submissions
        new_target_gene = TargetGene(
            score_set_id=sample_score_set.id,
            name="TP53",
            category="protein_coding",
            target_sequence=TargetSequence(sequence="MEEPQSDPSV", sequence_type="protein"),
        )
        session.add(new_target_gene)
        session.commit()

        # Arrange the post mapped metadata to have a single AC for both target genes
        target_gene_1 = sample_score_set.target_genes[0]
        target_gene_1.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        target_gene_2 = new_target_gene
        target_gene_2.post_mapped_metadata = {"protein": {"sequence_accessions": ["NM_000546"]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                side_effect=["job_12345", None],
            ),
            patch.object(JobManager, "update_progress"),
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_submit_uniprot_mapping_jobs_run.id,
                ),
            )

        assert job_result["status"] == "ok"

        expected_submitted_jobs = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION},
            "2": {"job_id": None, "accession": "NM_000546"},
        }

        # Verify that the job metadata contains both submitted and failed jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == expected_submitted_jobs

        # Verify that polling job params have been updated correctly
        session.refresh(sample_dummy_polling_job_for_submission_run)
        assert sample_dummy_polling_job_for_submission_run.job_params["mapping_jobs"] == expected_submitted_jobs

    async def test_submit_uniprot_mapping_jobs_updates_progress(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
    ):
        """Test that progress updates are made during UniProt mapping job submission."""

        # Arrange the post mapped metadata to have a single AC
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_submit_uniprot_mapping_jobs_run.id,
                ),
            )

        assert job_result["status"] == "ok"

        # Verify that progress updates were made
        mock_update_progress.assert_has_calls(
            [
                call(0, 100, "Starting UniProt mapping job submission."),
                call(
                    95, 100, f"Submitted UniProt mapping job for target gene {sample_score_set.target_genes[0].name}."
                ),
                call(100, 100, "Completed submission of UniProt mapping jobs."),
            ]
        )


@pytest.mark.integration
@pytest.mark.asyncio
class TestSubmitUniprotMappingJobsForScoreSetIntegration:
    """Integration tests for submit_uniprot_mapping_jobs_for_score_set function."""

    async def test_submit_uniprot_mapping_jobs_success_independent_ctx(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Integration test for submitting UniProt mapping jobs."""

        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ) as mock_submit_id_mapping,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_submit_uniprot_mapping_jobs_run.id
            )

        mock_submit_id_mapping.assert_called_once()
        assert job_result["status"] == "ok"

        expected_submitted_jobs = {"1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}}

        # Verify that the job metadata contains the submitted job
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == expected_submitted_jobs

        # Verify that polling job params have been updated correctly
        session.refresh(sample_dummy_polling_job_for_submission_run)
        assert sample_dummy_polling_job_for_submission_run.job_params["mapping_jobs"] == expected_submitted_jobs

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job is still pending (non-pipeline ctx)
        session.refresh(sample_dummy_polling_job_for_submission_run)
        assert sample_dummy_polling_job_for_submission_run.status == JobStatus.PENDING

    async def test_submit_uniprot_mapping_jobs_success_pipeline_ctx(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_jobs_pipeline,
        with_dummy_polling_job_for_submission_run,
        sample_submit_uniprot_mapping_jobs_run_in_pipeline,
        sample_submit_uniprot_mapping_jobs_pipeline,
        sample_dummy_polling_job_for_submission_run_in_pipeline,
        sample_score_set,
    ):
        """Integration test for submitting UniProt mapping jobs in a pipeline context."""

        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ) as mock_submit_id_mapping,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_submit_uniprot_mapping_jobs_run_in_pipeline.id
            )

        mock_submit_id_mapping.assert_called_once()
        assert job_result["status"] == "ok"

        expected_submitted_jobs = {"1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}}

        # Verify that the job metadata contains the submitted job
        session.refresh(sample_submit_uniprot_mapping_jobs_run_in_pipeline)
        sample_submit_uniprot_mapping_jobs_run_in_pipeline.metadata_["submitted_jobs"] == expected_submitted_jobs

        # Verify that polling job params have been updated correctly
        session.refresh(sample_dummy_polling_job_for_submission_run_in_pipeline)
        assert (
            sample_dummy_polling_job_for_submission_run_in_pipeline.job_params["mapping_jobs"]
            == expected_submitted_jobs
        )

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run_in_pipeline)
        assert sample_submit_uniprot_mapping_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job is now queued (pipeline ctx)
        session.refresh(sample_dummy_polling_job_for_submission_run_in_pipeline)
        assert sample_dummy_polling_job_for_submission_run_in_pipeline.status == JobStatus.QUEUED

        # Verify that the pipeline run status is running
        session.refresh(sample_submit_uniprot_mapping_jobs_pipeline)
        assert sample_submit_uniprot_mapping_jobs_pipeline.status == PipelineStatus.RUNNING

    async def test_submit_uniprot_mapping_jobs_no_targets(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Integration test for submitting UniProt mapping jobs when no target genes are present."""

        # Ensure the sample score set has no target genes
        sample_score_set.target_genes = []
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ) as mock_submit_id_mapping,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_submit_uniprot_mapping_jobs_run.id
            )

        mock_submit_id_mapping.assert_not_called()
        assert job_result["status"] == "ok"

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {}

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job is still pending and no param changes were made
        assert sample_dummy_polling_job_for_submission_run.status == JobStatus.PENDING
        assert sample_dummy_polling_job_for_submission_run.job_params.get("mapping_jobs") == {}

    async def test_submit_uniprot_mapping_jobs_no_acs_in_post_mapped_metadata(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Integration test for submitting UniProt mapping jobs when no ACs are present in post mapped metadata."""

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ) as mock_submit_id_mapping,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_submit_uniprot_mapping_jobs_run.id
            )

        mock_submit_id_mapping.assert_not_called()
        assert job_result["status"] == "ok"

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {}

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job is still pending and no param changes were made
        assert sample_dummy_polling_job_for_submission_run.status == JobStatus.PENDING
        assert sample_dummy_polling_job_for_submission_run.job_params.get("mapping_jobs") == {}

    async def test_submit_uniprot_mapping_jobs_too_many_acs_in_post_mapped_metadata(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Integration test for submitting UniProt mapping jobs when too many ACs are present in post mapped metadata."""

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ) as mock_submit_id_mapping,
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_submit_uniprot_mapping_jobs_run.id
            )

        mock_submit_id_mapping.assert_not_called()
        assert job_result["status"] == "ok"

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {}

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job is still pending and no param changes were made
        assert sample_dummy_polling_job_for_submission_run.status == JobStatus.PENDING
        assert sample_dummy_polling_job_for_submission_run.job_params.get("mapping_jobs") == {}

    async def test_submit_uniprot_mapping_jobs_propagates_exceptions(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Integration test to ensure exceptions during UniProt mapping job submission are propagated to decorators."""

        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                side_effect=Exception("UniProt API failure"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_submit_uniprot_mapping_jobs_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert result["status"] == "exception"
        assert isinstance(result["exception"], Exception)

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_.get("submitted_jobs") is None

        # Verify that the submission job failed
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.FAILED

        # Verify that the dependent polling job is still pending and no param changes were made
        assert sample_dummy_polling_job_for_submission_run.status == JobStatus.PENDING
        assert sample_dummy_polling_job_for_submission_run.job_params.get("mapping_jobs") == {}

    async def test_submit_uniprot_mapping_jobs_no_jobs_submitted(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Integration test for submitting UniProt mapping jobs when no jobs are submitted."""

        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value=None,
            ),
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_submit_uniprot_mapping_jobs_run.id
            )

        assert job_result["status"] == "ok"

        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {
            "1": {"job_id": None, "accession": VALID_NT_ACCESSION}
        }

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job is still pending and no param changes were made
        assert sample_dummy_polling_job_for_submission_run.status == JobStatus.PENDING
        assert sample_dummy_polling_job_for_submission_run.job_params.get("mapping_jobs") == {}

    async def test_submit_uniprot_mapping_jobs_partial_submission(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Integration test for partial submission of UniProt mapping jobs."""

        # Add another target gene to the score set to simulate multiple submissions
        new_target_gene = TargetGene(
            score_set_id=sample_score_set.id,
            name="TP53",
            category="protein_coding",
            target_sequence=TargetSequence(sequence="MEEPQSDPSV", sequence_type="protein"),
        )
        session.add(new_target_gene)
        session.commit()

        # Add accessions to both target genes' post mapped metadata
        for idx, tg in enumerate(sample_score_set.target_genes):
            tg.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION + f"{idx:05d}"]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                side_effect=["job_12345", None],
            ),
        ):
            job_result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_submit_uniprot_mapping_jobs_run.id
            )

        assert job_result["status"] == "ok"

        expected_submitted_jobs = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION + "00000"},
            "2": {"job_id": None, "accession": VALID_NT_ACCESSION + "00001"},
        }

        # Verify that the job metadata contains both submitted and failed jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == expected_submitted_jobs

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job is still pending and params were updated correctly
        assert sample_dummy_polling_job_for_submission_run.status == JobStatus.PENDING
        assert sample_dummy_polling_job_for_submission_run.job_params.get("mapping_jobs") == expected_submitted_jobs

    async def test_submit_uniprot_mapping_jobs_no_dependent_job_raises(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
    ):
        """Integration test to ensure error is raised to the decorator when dependent polling job is not available."""

        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await submit_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_submit_uniprot_mapping_jobs_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert result["status"] == "failed"
        assert isinstance(result["exception"], UniProtPollingEnqueueError)

        # Verify that the job metadata contains the job we submitted before the error
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }

        # Verify that the submission job failed
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.FAILED

        # nothing to verify for dependent polling job since it does not exist


@pytest.mark.integration
@pytest.mark.asyncio
class TestSubmitUniprotMappingJobsArqContext:
    """Integration tests for submit_uniprot_mapping_jobs_for_score_set function in ARQ context."""

    async def test_submit_uniprot_mapping_jobs_with_arq_context_independent(
        self,
        session,
        arq_redis,
        arq_worker,
        athena_engine,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_uniprot_mapping_jobs_for_score_set", sample_submit_uniprot_mapping_jobs_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        expected_submitted_jobs = {"1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}}

        # Verify that the job metadata contains the submitted job
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        sample_submit_uniprot_mapping_jobs_run.metadata_["submitted_jobs"] == expected_submitted_jobs

        # Verify that polling job params have been updated correctly
        session.refresh(sample_dummy_polling_job_for_submission_run)
        assert sample_dummy_polling_job_for_submission_run.job_params["mapping_jobs"] == expected_submitted_jobs

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job is still pending (non-pipeline ctx)
        session.refresh(sample_dummy_polling_job_for_submission_run)
        assert sample_dummy_polling_job_for_submission_run.status == JobStatus.PENDING

    async def test_submit_uniprot_mapping_jobs_with_arq_context_pipeline(
        self,
        session,
        arq_redis,
        arq_worker,
        athena_engine,
        with_populated_domain_data,
        with_submit_uniprot_mapping_jobs_pipeline,
        with_dummy_polling_job_for_submission_run,
        sample_submit_uniprot_mapping_jobs_run_in_pipeline,
        sample_submit_uniprot_mapping_jobs_pipeline,
        sample_dummy_polling_job_for_submission_run_in_pipeline,
        sample_score_set,
    ):
        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                return_value="job_12345",
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_uniprot_mapping_jobs_for_score_set", sample_submit_uniprot_mapping_jobs_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        expected_submitted_jobs = {"1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}}

        # Verify that the job metadata contains the submitted job
        session.refresh(sample_submit_uniprot_mapping_jobs_run_in_pipeline)
        sample_submit_uniprot_mapping_jobs_run_in_pipeline.metadata_["submitted_jobs"] == expected_submitted_jobs

        # Verify that polling job params have been updated correctly
        session.refresh(sample_dummy_polling_job_for_submission_run_in_pipeline)
        assert (
            sample_dummy_polling_job_for_submission_run_in_pipeline.job_params["mapping_jobs"]
            == expected_submitted_jobs
        )

        # Verify that the submission job was completed successfully
        session.refresh(sample_submit_uniprot_mapping_jobs_run_in_pipeline)
        assert sample_submit_uniprot_mapping_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that the dependent polling job is now queued (pipeline ctx)
        session.refresh(sample_dummy_polling_job_for_submission_run_in_pipeline)
        assert sample_dummy_polling_job_for_submission_run_in_pipeline.status == JobStatus.QUEUED

        # Verify that the pipeline run status is running
        session.refresh(sample_submit_uniprot_mapping_jobs_pipeline)
        assert sample_submit_uniprot_mapping_jobs_pipeline.status == PipelineStatus.RUNNING

    async def test_submit_uniprot_mapping_jobs_with_arq_context_exception_handling_independent(
        self,
        session,
        arq_redis,
        arq_worker,
        athena_engine,
        with_populated_domain_data,
        with_submit_uniprot_mapping_job,
        with_dummy_polling_job_for_submission_run,
        sample_score_set,
        sample_submit_uniprot_mapping_jobs_run,
        sample_dummy_polling_job_for_submission_run,
    ):
        """Integration test to ensure exceptions during UniProt mapping job submission are propagated to decorators."""

        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                side_effect=Exception("UniProt API failure"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job(
                "submit_uniprot_mapping_jobs_for_score_set", sample_submit_uniprot_mapping_jobs_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.metadata_.get("submitted_jobs") is None

        # Verify that the submission job failed
        session.refresh(sample_submit_uniprot_mapping_jobs_run)
        assert sample_submit_uniprot_mapping_jobs_run.status == JobStatus.FAILED

        # Verify that the dependent polling job is still pending and no param changes were made
        assert sample_dummy_polling_job_for_submission_run.status == JobStatus.PENDING
        assert sample_dummy_polling_job_for_submission_run.job_params.get("mapping_jobs") == {}

    async def test_submit_uniprot_mapping_jobs_with_arq_context_exception_handling_pipeline(
        self,
        session,
        arq_redis,
        arq_worker,
        athena_engine,
        with_populated_domain_data,
        with_submit_uniprot_mapping_jobs_pipeline,
        with_dummy_polling_job_for_submission_run,
        sample_submit_uniprot_mapping_jobs_run_in_pipeline,
        sample_submit_uniprot_mapping_jobs_pipeline,
        sample_dummy_polling_job_for_submission_run_in_pipeline,
        sample_score_set,
    ):
        """Integration test to ensure exceptions during UniProt mapping job submission are propagated to decorators."""

        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.submit_id_mapping",
                side_effect=Exception("UniProt API failure"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job(
                "submit_uniprot_mapping_jobs_for_score_set", sample_submit_uniprot_mapping_jobs_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify that the job metadata contains no submitted jobs
        session.refresh(sample_submit_uniprot_mapping_jobs_run_in_pipeline)
        assert sample_submit_uniprot_mapping_jobs_run_in_pipeline.metadata_.get("submitted_jobs") is None

        # Verify that the submission job failed
        session.refresh(sample_submit_uniprot_mapping_jobs_run_in_pipeline)
        assert sample_submit_uniprot_mapping_jobs_run_in_pipeline.status == JobStatus.FAILED

        # Verify that the dependent polling job is now cancelled and no param changes were made
        assert sample_dummy_polling_job_for_submission_run_in_pipeline.status == JobStatus.SKIPPED
        assert sample_dummy_polling_job_for_submission_run_in_pipeline.job_params.get("mapping_jobs") == {}

        # Verify that the pipeline run status is failed
        session.refresh(sample_submit_uniprot_mapping_jobs_pipeline)
        assert sample_submit_uniprot_mapping_jobs_pipeline.status == PipelineStatus.FAILED


@pytest.mark.unit
@pytest.mark.asyncio
class TestPollUniprotMappingJobsForScoreSetUnit:
    """Unit tests for poll_uniprot_mapping_jobs_for_score_set function."""

    async def test_poll_uniprot_mapping_jobs_no_mapping_jobs(
        self,
        session,
        mock_worker_ctx,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Ensure there are no mapping jobs in the polling job params
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {}
        session.commit()

        with (
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_polling_job_for_submission_run.id,
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "No mapping jobs found to poll.")
        assert job_result["status"] == "ok"

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None

    # TODO:XXX -- We will eventually want to make sure the job indicates to the manager
    #             its desire to be retried. For now, we just verify that no changes are made
    #             when results are not ready.
    async def test_poll_uniprot_mapping_jobs_results_not_ready(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=False,
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_polling_job_for_submission_run.id,
                ),
            )

        assert job_result["status"] == "ok"

        # Verify that progress updates were made
        mock_update_progress.assert_called_with(100, 100, "Completed polling of UniProt mapping jobs.")

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None

    async def test_poll_uniprot_mapping_jobs_no_results(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value={"results": []},  # minimal response with no results
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(UniprotMappingResultNotFoundError),
        ):
            await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_polling_job_for_submission_run.id,
                ),
            )

        mock_update_progress.assert_called_with(
            100, 100, f"No UniProt ID found for accession {VALID_NT_ACCESSION}. Cannot add UniProt ID."
        )

    async def test_poll_uniprot_mapping_jobs_ambiguous_results(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value={
                    "results": [
                        {
                            "from": VALID_NT_ACCESSION,
                            "to": {
                                "primaryAccession": f"{VALID_UNIPROT_ACCESSION}",
                                "entryType": TEST_UNIPROT_SWISS_PROT_TYPE,
                            },
                        },
                        {
                            "from": VALID_NT_ACCESSION,
                            "to": {
                                "primaryAccession": "P67890",
                                "entryType": TEST_UNIPROT_SWISS_PROT_TYPE,
                            },
                        },
                    ]
                },
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(UniprotAmbiguousMappingResultError),
        ):
            await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_polling_job_for_submission_run.id,
                ),
            )

        mock_update_progress.assert_called_with(
            100,
            100,
            f"Ambiguous UniProt ID mapping results for accession {VALID_NT_ACCESSION}. Cannot add UniProt ID.",
        )

    async def test_poll_uniprot_mapping_jobs_nonexistent_target(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job with a non-existent target gene ID
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "999": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(NonExistentTargetGeneError),
        ):
            await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_polling_job_for_submission_run.id,
                ),
            )

        mock_update_progress.assert_called_with(
            100,
            100,
            f"Target gene ID 999 not found in score set {sample_score_set.urn}. Cannot add UniProt ID.",
        )

    async def test_poll_uniprot_mapping_jobs_successful_update(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_polling_job_for_submission_run.id,
                ),
            )

        assert job_result["status"] == "ok"

        # Verify that progress updates were made
        mock_update_progress.assert_called_with(100, 100, "Completed polling of UniProt mapping jobs.")

        # Verify the target gene uniprot id has been updated
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata == VALID_UNIPROT_ACCESSION

    async def test_poll_uniprot_mapping_jobs_partial_success(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have two mapping jobs
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION},
            "2": {"job_id": "job_67890", "accession": "NONEXISTENT_AC"},
        }
        session.commit()

        # Add another target gene to the score set to correspond to the second mapping job
        new_target_gene = TargetGene(
            score_set_id=sample_score_set.id,
            name="TP53",
            category="protein_coding",
            target_sequence=TargetSequence(sequence="MEEPQSDPSV", sequence_type="protein"),
        )
        session.add(new_target_gene)
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                side_effect=[True, False],
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                side_effect=[
                    TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,  # Successful result for the first mapping job
                    {"results": []},  # No results for the second mapping job
                ],
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_polling_job_for_submission_run.id,
                ),
            )

        assert job_result["status"] == "ok"

        # Verify that progress updates were made
        mock_update_progress.assert_called_with(100, 100, "Completed polling of UniProt mapping jobs.")

        # Verify the target gene uniprot id has been updated for the successful mapping and
        # remains None for the failed mapping
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata == VALID_UNIPROT_ACCESSION
        assert sample_score_set.target_genes[1].uniprot_id_from_mapped_metadata is None

    async def test_poll_uniprot_mapping_jobs_updates_progress(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have one mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_11111", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                side_effect=[True, True, True],
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                side_effect=[TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE],
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            job_result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_polling_job_for_submission_run.id,
                ),
            )

        assert job_result["status"] == "ok"

        # Verify that progress updates were made incrementally
        mock_update_progress.assert_has_calls(
            [
                call(0, 100, "Starting UniProt mapping job polling."),
                call(95, 100, "Polled UniProt mapping job for target gene Sample Gene."),
                call(100, 100, "Completed polling of UniProt mapping jobs."),
            ]
        )

        # Verify the target gene uniprot ids have been updated
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata == VALID_UNIPROT_ACCESSION

    async def test_poll_uniprot_mapping_jobs_propagates_exceptions(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                side_effect=Exception("UniProt API failure"),
            ),
            pytest.raises(Exception) as exc_info,
        ):
            await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(
                    db=session,
                    redis=mock_worker_ctx["redis"],
                    job_id=sample_polling_job_for_submission_run.id,
                ),
            )

        assert str(exc_info.value) == "UniProt API failure"


@pytest.mark.integration
@pytest.mark.asyncio
class TestPollUniprotMappingJobsForScoreSetIntegration:
    """Integration tests for poll_uniprot_mapping_jobs_for_score_set function."""

    async def test_poll_uniprot_mapping_jobs_success_independent_ctx(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        with_submit_uniprot_mapping_job,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
            ),
        ):
            job_result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_polling_job_for_submission_run.id
            )

        assert job_result["status"] == "ok"

        # Verify the target gene uniprot id has been updated
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata == VALID_UNIPROT_ACCESSION

        # Verify that the polling job was completed successfully
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.SUCCEEDED

    async def test_poll_uniprot_mapping_jobs_success_pipeline_ctx(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_poll_uniprot_mapping_jobs_pipeline,
        sample_score_set,
        sample_poll_uniprot_mapping_jobs_run_in_pipeline,
        sample_poll_uniprot_mapping_jobs_pipeline,
    ):
        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        # Arrange the polling job params to have a single mapping job
        sample_poll_uniprot_mapping_jobs_run_in_pipeline.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
            ),
        ):
            job_result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_poll_uniprot_mapping_jobs_run_in_pipeline.id
            )

        assert job_result["status"] == "ok"

        # Verify the target gene uniprot id has been updated
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata == VALID_UNIPROT_ACCESSION

        # Verify that the polling job was completed successfully
        session.refresh(sample_poll_uniprot_mapping_jobs_run_in_pipeline)
        assert sample_poll_uniprot_mapping_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that the pipeline run status is succeeded (this is the only job in the test pipeline)
        session.refresh(sample_poll_uniprot_mapping_jobs_pipeline)
        assert sample_poll_uniprot_mapping_jobs_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_poll_uniprot_mapping_jobs_no_mapping_jobs(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Ensure there are no mapping jobs in the polling job params
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {}
        session.commit()

        job_result = await poll_uniprot_mapping_jobs_for_score_set(
            mock_worker_ctx, sample_polling_job_for_submission_run.id
        )

        assert job_result["status"] == "ok"

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None

        # Verify that the polling job succeeded
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.SUCCEEDED

    async def test_poll_uniprot_mapping_jobs_partial_mapping_jobs(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have two mapping jobs
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION},
            "2": {"job_id": None, "accession": "NONEXISTENT_AC"},
        }
        session.commit()

        # Add another target gene to the score set to correspond to the second mapping job
        new_target_gene = TargetGene(
            score_set_id=sample_score_set.id,
            name="TP53",
            category="protein_coding",
            target_sequence=TargetSequence(sequence="MEEPQSDPSV", sequence_type="protein"),
        )
        session.add(new_target_gene)
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                side_effect=[True],
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                side_effect=[TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE],
            ),
        ):
            job_result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_polling_job_for_submission_run.id
            )

        assert job_result["status"] == "ok"

        # Verify the target gene uniprot id has been updated for the successful mapping and
        # remains None for the mapping with no job id
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata == VALID_UNIPROT_ACCESSION
        assert sample_score_set.target_genes[1].uniprot_id_from_mapped_metadata is None

        # Verify that the polling job succeeded
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.SUCCEEDED

    async def test_poll_uniprot_mapping_jobs_results_not_ready(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with patch(
            "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
            return_value=False,
        ):
            job_result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_polling_job_for_submission_run.id
            )

        assert job_result["status"] == "ok"

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None

        # Verify that the polling job succeeded
        # TODO#XXX -- For now, we mark the job as succeeded even if no updates were made.
        #             In the future, we may want to have the job indicate it should be retried.
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.SUCCEEDED

    async def test_poll_uniprot_mapping_jobs_no_results(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value={"results": []},  # minimal response with no results
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_polling_job_for_submission_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert result["status"] == "exception"
        assert isinstance(result["exception"], UniprotMappingResultNotFoundError)

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None

        # Verify that the polling job failed
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.FAILED

    async def test_poll_uniprot_mapping_jobs_ambiguous_results(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value={
                    "results": [
                        {
                            "from": VALID_NT_ACCESSION,
                            "to": {
                                "primaryAccession": f"{VALID_UNIPROT_ACCESSION}",
                                "entryType": TEST_UNIPROT_SWISS_PROT_TYPE,
                            },
                        },
                        {
                            "from": VALID_NT_ACCESSION,
                            "to": {
                                "primaryAccession": "P67890",
                                "entryType": TEST_UNIPROT_SWISS_PROT_TYPE,
                            },
                        },
                    ]
                },
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_polling_job_for_submission_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert result["status"] == "exception"
        assert isinstance(result["exception"], UniprotAmbiguousMappingResultError)

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None

        # Verify that the polling job failed
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.FAILED

    async def test_poll_uniprot_mapping_jobs_nonexistent_target(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job with a non-existent target gene ID
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "999": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_polling_job_for_submission_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert result["status"] == "exception"
        assert isinstance(result["exception"], NonExistentTargetGeneError)

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None

        # Verify that the polling job failed
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.FAILED

    async def test_poll_uniprot_mapping_jobs_propagates_exceptions_to_decorator(
        self,
        session,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                side_effect=Exception("UniProt API failure"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await poll_uniprot_mapping_jobs_for_score_set(
                mock_worker_ctx, sample_polling_job_for_submission_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert result["status"] == "exception"
        assert isinstance(result["exception"], Exception)

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None

        # Verify that the polling job failed
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.FAILED


@pytest.mark.integration
@pytest.mark.asyncio
class TestPollUniprotMappingJobsForScoreSetArqContext:
    """Integration tests for poll_uniprot_mapping_jobs_for_score_set function with ARQ context."""

    async def test_poll_uniprot_mapping_jobs_with_arq_context_independent(
        self,
        session,
        arq_worker,
        arq_redis,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        with_submit_uniprot_mapping_job,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
            ),
        ):
            await arq_redis.enqueue_job(
                "poll_uniprot_mapping_jobs_for_score_set", sample_polling_job_for_submission_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the target gene uniprot id has been updated
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata == VALID_UNIPROT_ACCESSION

        # Verify that the polling job was completed successfully
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.SUCCEEDED

    async def test_poll_uniprot_mapping_jobs_with_arq_context_pipeline(
        self,
        session,
        arq_worker,
        arq_redis,
        with_populated_domain_data,
        with_poll_uniprot_mapping_jobs_pipeline,
        sample_score_set,
        sample_poll_uniprot_mapping_jobs_run_in_pipeline,
        sample_poll_uniprot_mapping_jobs_pipeline,
    ):
        # Add an accession to the target gene's post mapped metadata
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"protein": {"sequence_accessions": [VALID_NT_ACCESSION]}}
        session.commit()

        # Arrange the polling job params to have a single mapping job
        sample_poll_uniprot_mapping_jobs_run_in_pipeline.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                return_value=True,
            ),
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.get_id_mapping_results",
                return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
            ),
        ):
            await arq_redis.enqueue_job(
                "poll_uniprot_mapping_jobs_for_score_set",
                sample_poll_uniprot_mapping_jobs_run_in_pipeline.id,
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the target gene uniprot id has been updated
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata == VALID_UNIPROT_ACCESSION

        # Verify that the polling job was completed successfully
        session.refresh(sample_poll_uniprot_mapping_jobs_run_in_pipeline)
        assert sample_poll_uniprot_mapping_jobs_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify that the pipeline run status is succeeded (this is the only job in the test pipeline)
        session.refresh(sample_poll_uniprot_mapping_jobs_pipeline)
        assert sample_poll_uniprot_mapping_jobs_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_poll_uniprot_mapping_jobs_with_arq_context_exception_handling_independent(
        self,
        session,
        arq_worker,
        arq_redis,
        mock_worker_ctx,
        with_populated_domain_data,
        with_independent_polling_job_for_submission_run,
        sample_score_set,
        sample_polling_job_for_submission_run,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_polling_job_for_submission_run.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                side_effect=Exception("UniProt API failure"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job(
                "poll_uniprot_mapping_jobs_for_score_set", sample_polling_job_for_submission_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify that the polling job failed
        session.refresh(sample_polling_job_for_submission_run)
        assert sample_polling_job_for_submission_run.status == JobStatus.FAILED

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None

    async def test_poll_uniprot_mapping_jobs_with_arq_context_exception_handling_pipeline(
        self,
        session,
        arq_worker,
        arq_redis,
        mock_worker_ctx,
        with_populated_domain_data,
        with_poll_uniprot_mapping_jobs_pipeline,
        sample_score_set,
        sample_poll_uniprot_mapping_jobs_run_in_pipeline,
        sample_poll_uniprot_mapping_jobs_pipeline,
    ):
        # Arrange the polling job params to have a single mapping job
        sample_poll_uniprot_mapping_jobs_run_in_pipeline.job_params["mapping_jobs"] = {
            "1": {"job_id": "job_12345", "accession": VALID_NT_ACCESSION}
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.uniprot.UniProtIDMappingAPI.check_id_mapping_results_ready",
                side_effect=Exception("UniProt API failure"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job(
                "poll_uniprot_mapping_jobs_for_score_set",
                sample_poll_uniprot_mapping_jobs_run_in_pipeline.id,
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify that the polling job failed
        session.refresh(sample_poll_uniprot_mapping_jobs_run_in_pipeline)
        assert sample_poll_uniprot_mapping_jobs_run_in_pipeline.status == JobStatus.FAILED

        # Verify that the pipeline run status is failed
        session.refresh(sample_poll_uniprot_mapping_jobs_pipeline)
        assert sample_poll_uniprot_mapping_jobs_pipeline.status == PipelineStatus.FAILED

        # Verify the target gene uniprot id remains unchanged
        session.refresh(sample_score_set)
        assert sample_score_set.target_genes[0].uniprot_id_from_mapped_metadata is None
