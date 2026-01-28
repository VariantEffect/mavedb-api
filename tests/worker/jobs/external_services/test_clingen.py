from asyncio.unix_events import _UnixSelectorEventLoop
from unittest.mock import call, patch

import pytest
from sqlalchemy import select

from mavedb.lib.exceptions import LDHSubmissionFailureError
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.worker.jobs.external_services.clingen import (
    submit_score_set_mappings_to_car,
    submit_score_set_mappings_to_ldh,
)
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.util.setup.worker import create_mappings_in_score_set


@pytest.mark.unit
@pytest.mark.asyncio
class TestClingenSubmitScoreSetMappingsToCarUnit:
    """Tests for the Clingen submit_score_set_mappings_to_car function."""

    async def test_submit_score_set_mappings_to_car_submission_disabled(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
    ):
        # Patch to disable ClinGen submission endpoint
        with (
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", False),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "ClinGen submission is disabled. Skipping CAR submission.")
        assert result["status"] == "ok"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

    async def test_submit_score_set_mappings_to_car_no_mappings(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
    ):
        """Test submitting score set mappings to ClinGen when there are no mappings."""
        with (
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "No mapped variants to submit to CAR. Skipped submission.")
        assert result["status"] == "ok"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

    async def test_submit_score_set_mappings_to_car_submission_endpoint_not_set(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
    ):
        # Patch to disable ClinGen submission endpoint
        with (
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", ""),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
            pytest.raises(ValueError),
        ):
            await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(
            100, 100, "CAR submission endpoint not configured. Can't complete submission."
        )

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

    async def test_submit_score_set_mappings_to_car_no_registered_alleles(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Patch ClinGenAlleleRegistryService to return no registered alleles
        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=[],
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "Completed CAR mapped resource submission.")
        assert result["status"] == "ok"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

    async def test_submit_score_set_mappings_to_car_no_linked_alleles(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Patch ClinGenAlleleRegistryService to return registered alleles that do not match submitted HGVS
        registered_alleles_mock = [
            {"@id": "CA123456", "type": "nucleotide", "genomicAlleles": [{"hgvs": "NC_000007.14:g.140453136A>C"}]},
            {"@id": "CA234567", "type": "nucleotide", "genomicAlleles": [{"hgvs": "NC_000007.14:g.140453136A>G"}]},
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "Completed CAR mapped resource submission.")
        assert result["status"] == "ok"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

    async def test_submit_score_set_mappings_to_car_repeated_hgvs(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Patch ClinGenAlleleRegistryService to return registered alleles with repeated HGVS
        mapped_variants = session.scalars(select(MappedVariant)).all()
        registered_alleles_mock = [
            {
                "@id": "CA_DUPLICATE",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(mapped_variants[0].post_mapped)}],
            }
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            # Patch get_hgvs_from_post_mapped to return the same HGVS for all variants
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped",
                return_value=get_hgvs_from_post_mapped(mapped_variants[0].post_mapped),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "Completed CAR mapped resource submission.")
        assert result["status"] == "ok"

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 4
        for variant in variants:
            assert variant.clingen_allele_id == "CA_DUPLICATE"

    async def test_submit_score_set_mappings_to_car_hgvs_not_found(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Get the mapped variants from score set before submission
        mapped_variants = session.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(Variant.score_set_id == submit_score_set_mappings_to_car_sample_job_run.job_params["score_set_id"])
        ).all()

        # Patch ClinGenAlleleRegistryService to return registered alleles
        registered_alleles_mock = [
            {
                "@id": f"CA{mv.id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(mv.post_mapped)}],
            }
            for mv in mapped_variants
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            # Patch get_hgvs_from_post_mapped to not find any HGVS in registered alleles
            patch("mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "Completed CAR mapped resource submission.")
        assert result["status"] == "ok"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

    async def test_submit_score_set_mappings_to_car_propagates_exception(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Patch ClinGenAlleleRegistryService to raise an exception
        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                side_effect=Exception("ClinGen service error"),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            pytest.raises(Exception) as exc_info,
        ):
            await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        assert str(exc_info.value) == "ClinGen service error"

    async def test_submit_score_set_mappings_to_car_success(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        sample_score_set,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Get the mapped variants from score set before submission
        mapped_variants = session.scalars(
            select(MappedVariant).join(Variant).where(Variant.score_set_id == sample_score_set.id)
        ).all()
        assert len(mapped_variants) == 4

        # Patch ClinGenAlleleRegistryService to return registered alleles
        registered_alleles_mock = [
            {
                "@id": f"CA{mv.id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(mv.post_mapped)}],
            }
            for mv in mapped_variants
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "Completed CAR mapped resource submission.")
        assert result["status"] == "ok"

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 4
        for variant in variants:
            assert variant.clingen_allele_id == f"CA{variant.id}"

    async def test_submit_score_set_mappings_to_car_updates_progress(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        sample_score_set,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Get the mapped variants from score set before submission
        mapped_variants = session.scalars(
            select(MappedVariant).join(Variant).where(Variant.score_set_id == sample_score_set.id)
        ).all()
        assert len(mapped_variants) == 4

        # Patch ClinGenAlleleRegistryService to return registered alleles
        registered_alleles_mock = [
            {
                "@id": f"CA{mv.id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(mv.post_mapped)}],
            }
            for mv in mapped_variants
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id
                ),
            )

        mock_update_progress.assert_has_calls(
            [
                call(0, 100, "Starting CAR mapped resource submission."),
                call(10, 100, "Preparing 4 mapped variants for CAR submission."),
                call(15, 100, "Submitting mapped variants to CAR."),
                call(60, 100, "Processing registered alleles from CAR."),
                call(95, 100, "Processed 4 of 4 registered alleles."),
                call(100, 100, "Completed CAR mapped resource submission."),
            ]
        )

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 4
        for variant in variants:
            assert variant.clingen_allele_id == f"CA{variant.id}"


@pytest.mark.integration
@pytest.mark.asyncio
class TestClingenSubmitScoreSetMappingsToCarIntegration:
    """Integration tests for the Clingen submit_score_set_mappings_to_car function."""

    async def test_submit_score_set_mappings_to_car_independent_ctx(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenAlleleRegistryService to return registered alleles
        mapped_variants = session.scalars(select(MappedVariant)).all()
        registered_alleles_mock = [
            {
                "@id": f"CA{mv.id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(mv.post_mapped)}],
            }
            for mv in mapped_variants
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == len(mapped_variants)
        for variant in variants:
            assert variant.clingen_allele_id == f"CA{variant.id}"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_car_pipeline_ctx(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run_in_pipeline,
        submit_score_set_mappings_to_car_sample_pipeline,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenAlleleRegistryService to return registered alleles
        mapped_variants = session.scalars(select(MappedVariant)).all()
        registered_alleles_mock = [
            {
                "@id": f"CA{mv.id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(mv.post_mapped)}],
            }
            for mv in mapped_variants
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run_in_pipeline.id
            )

        assert result["status"] == "ok"

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == len(mapped_variants)
        for variant in variants:
            assert variant.clingen_allele_id == f"CA{variant.id}"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_car_sample_job_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_pipeline)
        assert submit_score_set_mappings_to_car_sample_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_car_submission_disabled(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Patch to disable ClinGen submission endpoint
        with (
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", False),
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_car_no_submission_endpoint(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Patch to disable ClinGen submission endpoint
        with (
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", ""),
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        assert result["status"] == "failed"
        assert (
            result["exception_details"]["message"] == "ClinGen Allele Registry submission endpoint is not configured."
        )

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.FAILED

    async def test_submit_score_set_mappings_to_car_no_mappings(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
    ):
        """Test submitting score set mappings to ClinGen when there are no mappings."""
        with (
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_car_no_registered_alleles(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenAlleleRegistryService to return no registered alleles
        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=[],
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_car_no_linked_alleles(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenAlleleRegistryService to return registered alleles that do not match submitted HGVS
        registered_alleles_mock = [
            {"@id": "CA123456", "type": "nucleotide", "genomicAlleles": [{"hgvs": "NC_000007.14:g.140453136A>C"}]},
            {"@id": "CA234567", "type": "nucleotide", "genomicAlleles": [{"hgvs": "NC_000007.14:g.140453136A>G"}]},
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_car_propagates_exception_to_decorator(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenAlleleRegistryService to raise an exception
        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                side_effect=Exception("ClinGen service error"),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        assert result["status"] == "failed"
        assert result["exception_details"]["message"] == "ClinGen service error"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.FAILED


@pytest.mark.integration
@pytest.mark.asyncio
class TestClingenSubmitScoreSetMappingsToCarArqContext:
    """Tests for the Clingen submit_score_set_mappings_to_car function with ARQ context."""

    async def test_submit_score_set_mappings_to_car_with_arq_context_independent(
        self,
        standalone_worker_context,
        session,
        arq_redis,
        arq_worker,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenAlleleRegistryService to return registered alleles
        mapped_variants = session.scalars(select(MappedVariant)).all()
        registered_alleles_mock = [
            {
                "@id": f"CA{mv.id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(mv.post_mapped)}],
            }
            for mv in mapped_variants
        ]

        with (
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_car", submit_score_set_mappings_to_car_sample_job_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.SUCCEEDED

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == len(mapped_variants)
        for variant in variants:
            assert variant.clingen_allele_id == f"CA{variant.id}"

    async def test_submit_score_set_mappings_to_car_with_arq_context_pipeline(
        self,
        standalone_worker_context,
        session,
        arq_redis,
        arq_worker,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run_in_pipeline,
        submit_score_set_mappings_to_car_sample_pipeline,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenAlleleRegistryService to return registered alleles
        mapped_variants = session.scalars(select(MappedVariant)).all()
        registered_alleles_mock = [
            {
                "@id": f"CA{mv.id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(mv.post_mapped)}],
            }
            for mv in mapped_variants
        ]

        with (
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_car", submit_score_set_mappings_to_car_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_car_sample_job_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_pipeline)
        assert submit_score_set_mappings_to_car_sample_pipeline.status == PipelineStatus.SUCCEEDED

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == len(mapped_variants)
        for variant in variants:
            assert variant.clingen_allele_id == f"CA{variant.id}"

    async def test_submit_score_set_mappings_to_car_with_arq_context_exception_handling_independent(
        self,
        standalone_worker_context,
        session,
        arq_redis,
        arq_worker,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenAlleleRegistryService to raise an exception
        with (
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                side_effect=Exception("ClinGen service error"),
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_car", submit_score_set_mappings_to_car_sample_job_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.FAILED
        assert submit_score_set_mappings_to_car_sample_job_run.error_message == "ClinGen service error"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

    async def test_submit_score_set_mappings_to_car_with_arq_context_exception_handling_pipeline(
        self,
        standalone_worker_context,
        session,
        arq_redis,
        arq_worker,
        with_submit_score_set_mappings_to_car_job,
        submit_score_set_mappings_to_car_sample_job_run_in_pipeline,
        submit_score_set_mappings_to_car_sample_pipeline,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenAlleleRegistryService to raise an exception
        with (
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                side_effect=Exception("ClinGen service error"),
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_car", submit_score_set_mappings_to_car_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_car_sample_job_run_in_pipeline.status == JobStatus.FAILED
        assert submit_score_set_mappings_to_car_sample_job_run_in_pipeline.error_message == "ClinGen service error"

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_pipeline)
        assert submit_score_set_mappings_to_car_sample_pipeline.status == PipelineStatus.FAILED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0


@pytest.mark.unit
@pytest.mark.asyncio
class TestClingenSubmitScoreSetMappingsToLdhUnit:
    """Unit tests for the Clingen submit_score_set_mappings_to_car function."""

    async def test_submit_score_set_mappings_to_ldh_no_variants(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.LDH_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "No mapped variants to submit to LDH. Skipping submission.")
        assert result["status"] == "ok"

    async def test_submit_score_set_mappings_to_ldh_all_submissions_failed(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        async def dummy_submission_failure(*args, **kwargs):
            return ([], ["Submission failed"])

        # Patch ClinGenLdhService to simulate all submissions failing
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_submission_failure(),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.LDH_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
            pytest.raises(LDHSubmissionFailureError),
        ):
            await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(100, 100, "All mapped variant submissions to LDH failed.")

    async def test_submit_score_set_mappings_to_ldh_hgvs_not_found(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Patch ClinGenLdhService to raise HGVS not found exception
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.LDH_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped", return_value=None),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id
                ),
            )

        mock_update_progress.assert_called_with(
            100, 100, "No valid mapped variants to submit to LDH. Skipping submission."
        )
        assert result["status"] == "ok"

    async def test_submit_score_set_mappings_to_ldh_propagates_exception(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Patch ClinGenLdhService to raise an exception
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                side_effect=Exception("LDH service error"),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.LDH_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            pytest.raises(Exception) as exc_info,
        ):
            await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id
                ),
            )

        assert str(exc_info.value) == "LDH service error"

    async def test_submit_score_set_mappings_to_ldh_partial_submission(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        async def dummy_partial_submission(*args, **kwargs):
            return (
                [{"@id": "LDH12345"}, {"@id": "LDH23456"}],
                ["Submission failed for some variants"],
            )

        # Patch ClinGenLdhService to simulate partial submission success
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_partial_submission(),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.LDH_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id
                ),
            )

        assert result["status"] == "ok"
        mock_update_progress.assert_called_with(
            100, 100, "Finalized LDH mapped resource submission (2 successes, 1 failures)."
        )

    async def test_submit_score_set_mappings_to_ldh_all_successful_submission(
        self,
        mock_worker_ctx,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
        # Create mappings in the score set
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        async def dummy_successful_submission(*args, **kwargs):
            return (
                [{"@id": "LDH12345"}, {"@id": "LDH23456"}],
                [],
            )

        # Patch ClinGenLdhService to simulate all submissions succeeding
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_successful_submission(),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.LDH_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch.object(JobManager, "update_progress", return_value=None) as mock_update_progress,
        ):
            result = await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id
                ),
            )

        assert result["status"] == "ok"
        mock_update_progress.assert_called_with(
            100, 100, "Finalized LDH mapped resource submission (2 successes, 0 failures)."
        )


@pytest.mark.integration
@pytest.mark.asyncio
class TestClingenSubmitScoreSetMappingsToLdhIntegration:
    """Integration tests for the Clingen submit_score_set_mappings_to_ldh function."""

    async def test_submit_score_set_mappings_to_ldh_independent(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        async def dummy_ldh_submission(*args, **kwargs):
            return (
                [{"@id": "LDH12345"}, {"@id": "LDH23456"}],
                [],
            )

        # Patch to disable ClinGen submission endpoint
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_ldh_submission(),
            ),
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_ldh_pipeline_ctx(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline,
        submit_score_set_mappings_to_ldh_sample_pipeline,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        async def dummy_ldh_submission(*args, **kwargs):
            return (
                [{"@id": "LDH12345"}, {"@id": "LDH23456"}],
                [],
            )

        # Patch to disable ClinGen submission endpoint
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_ldh_submission(),
            ),
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.id
            )

        assert result["status"] == "ok"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_ldh_propagates_exception_to_decorator(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenLdhService to raise an exception
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                side_effect=Exception("LDH service error"),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        assert result["status"] == "failed"
        assert result["exception_details"]["message"] == "LDH service error"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.FAILED

    async def test_submit_score_set_mappings_to_ldh_no_linked_alleles(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        async def dummy_no_linked_alleles_submission(*args, **kwargs):
            return ([], [])

        # Patch ClinGenLdhService to simulate no linked alleles found
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_no_linked_alleles_submission(),
            ),
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_ldh_hgvs_not_found(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenLdhService to raise HGVS not found exception
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped", return_value=None),
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_ldh_all_submissions_failed(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        async def dummy_submission_failure(*args, **kwargs):
            return ([], ["Submission failed"])

        # Patch ClinGenLdhService to simulate all submissions failing
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_submission_failure(),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        assert result["status"] == "failed"
        assert "All LDH submissions failed for score set" in result["exception_details"]["message"]

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.FAILED

    async def test_submit_score_set_mappings_to_ldh_partial_submission(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        async def dummy_partial_submission(*args, **kwargs):
            return (
                [{"@id": "LDH12345"}],
                ["Submission failed for some variants"],
            )

        # Patch ClinGenLdhService to simulate partial submission success
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_partial_submission(),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_ldh_all_successful_submission(
        self,
        standalone_worker_context,
        session,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        async def dummy_successful_submission(*args, **kwargs):
            return (
                [{"@id": "LDH12345"}, {"@id": "LDH23456"}],
                [],
            )

        # Patch ClinGenLdhService to simulate all submissions succeeding
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_successful_submission(),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        assert result["status"] == "ok"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.SUCCEEDED


@pytest.mark.integration
@pytest.mark.asyncio
class TestClingenSubmitScoreSetMappingsToLdhArqIntegration:
    """ARQ Integration tests for the Clingen submit_score_set_mappings_to_ldh function."""

    async def test_submit_score_set_mappings_to_ldh_independent(
        self,
        standalone_worker_context,
        session,
        arq_redis,
        arq_worker,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        async def dummy_ldh_submission(*args, **kwargs):
            return (
                [{"@id": "LDH12345"}, {"@id": "LDH23456"}],
                [],
            )

        # Patch to disable ClinGen submission endpoint
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_ldh_submission(),
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_ldh", submit_score_set_mappings_to_ldh_sample_job_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_ldh_with_arq_context_in_pipeline(
        self,
        standalone_worker_context,
        session,
        arq_redis,
        arq_worker,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline,
        submit_score_set_mappings_to_ldh_sample_pipeline,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        async def dummy_ldh_submission(*args, **kwargs):
            return (
                [{"@id": "LDH12345"}, {"@id": "LDH23456"}],
                [],
            )

        # Patch to disable ClinGen submission endpoint
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_ldh_submission(),
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_ldh", submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_ldh_with_arq_context_exception_handling(
        self,
        standalone_worker_context,
        session,
        arq_redis,
        arq_worker,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenLdhService to raise an exception
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                side_effect=Exception("LDH service error"),
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_ldh", submit_score_set_mappings_to_ldh_sample_job_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.FAILED
        assert submit_score_set_mappings_to_ldh_sample_job_run.error_message == "LDH service error"

    async def test_submit_score_set_mappings_to_ldh_with_arq_context_exception_handling_pipeline_ctx(
        self,
        standalone_worker_context,
        session,
        arq_redis,
        arq_worker,
        with_submit_score_set_mappings_to_ldh_job,
        submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline,
        submit_score_set_mappings_to_ldh_sample_pipeline,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        with_dummy_setup_jobs,
        dummy_variant_creation_job_run,
        dummy_variant_mapping_job_run,
    ):
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

        # Patch ClinGenLdhService to raise an exception
        with (
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                side_effect=Exception("LDH service error"),
            ),
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_ldh", submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.status == JobStatus.FAILED
        assert submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.error_message == "LDH service error"

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_pipeline.status == PipelineStatus.FAILED
