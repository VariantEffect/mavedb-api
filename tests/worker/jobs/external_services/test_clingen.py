# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from asyncio.unix_events import _UnixSelectorEventLoop
from unittest.mock import patch

from sqlalchemy import select

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.worker.jobs.external_services.clingen import (
    submit_score_set_mappings_to_car,
    submit_score_set_mappings_to_ldh,
)
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.constants import TEST_CLINGEN_LDH_LINKING_RESPONSE_BAD_REQUEST
from tests.helpers.util.setup.worker import create_mappings_in_score_set

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


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
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SKIPPED

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
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

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
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

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
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify annotation statuses were rendered as failed
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "failed"
            assert ann.annotation_type == "clingen_allele_id"

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
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify annotation statuses were rendered as failed
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "failed"
            assert ann.annotation_type == "clingen_allele_id"

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
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 4
        for variant in variants:
            assert variant.clingen_allele_id == "CA_DUPLICATE"

        # Verify annotation statuses were rendered as success
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "success"
            assert ann.annotation_type == "clingen_allele_id"

    async def test_submit_score_set_mappings_to_car_partial_failure(
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
        """Test that partial CAR failures (some matched, some not) result in a succeeded outcome with failure annotations."""
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

        # Get mapped variants; return a CAR response that only matches the first variant
        mapped_variants = session.scalars(select(MappedVariant)).all()
        assert len(mapped_variants) == 4

        first_hgvs = get_hgvs_from_post_mapped(mapped_variants[0].post_mapped)
        registered_alleles_mock = [
            {
                "@id": f"CA{mapped_variants[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": first_hgvs}],
            }
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
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["matched_count"] == 1
        assert result.data["failed_count"] == 3

        # Verify only the first variant got a CAID
        variants_with_caid = session.scalars(
            select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))
        ).all()
        assert len(variants_with_caid) == 1
        assert variants_with_caid[0].clingen_allele_id == f"CA{mapped_variants[0].id}"

        # Verify annotation statuses: 1 success, 3 failed
        success_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == "clingen_allele_id",
                VariantAnnotationStatus.status == "success",
            )
        ).all()
        failed_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == "clingen_allele_id",
                VariantAnnotationStatus.status == "failed",
            )
        ).all()
        assert len(success_annotations) == 1
        assert len(failed_annotations) == 3

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
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify annotation statuses were rendered as failed
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "failed"
            assert ann.annotation_type == "clingen_allele_id"

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
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
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
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 4
        for variant in variants:
            assert variant.clingen_allele_id == f"CA{variant.id}"

        # Verify annotation statuses were rendered as success
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "success"
            assert ann.annotation_type == "clingen_allele_id"


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

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == len(mapped_variants)
        for variant in variants:
            assert variant.clingen_allele_id == f"CA{variant.id}"

        # Verify annotation statuses were rendered as success
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == len(mapped_variants)
        for ann in annotation_statuses:
            assert ann.status == "success"

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

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == len(mapped_variants)
        for variant in variants:
            assert variant.clingen_allele_id == f"CA{variant.id}"

        # Verify annotation statuses were rendered as success
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == len(mapped_variants)
        for ann in annotation_statuses:
            assert ann.status == "success"

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

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SKIPPED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(select(VariantAnnotationStatus)).all()
        assert len(annotation_statuses) == 0

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.SKIPPED

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(select(VariantAnnotationStatus)).all()
        assert len(annotation_statuses) == 0

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

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(select(VariantAnnotationStatus)).all()
        assert len(annotation_statuses) == 0

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify annotation statuses were rendered as failed
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 4

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.FAILED

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify annotation statuses were rendered as failed
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 4

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.FAILED

    async def test_submit_score_set_mappings_to_car_partial_failure(
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
        """Test that partial CAR failures result in SUCCEEDED status with per-variant failure annotations committed."""
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

        # Return a CAR response that only matches the first variant's HGVS
        mapped_variants = session.scalars(select(MappedVariant)).all()
        first_hgvs = get_hgvs_from_post_mapped(mapped_variants[0].post_mapped)
        registered_alleles_mock = [
            {
                "@id": f"CA{mapped_variants[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": first_hgvs}],
            }
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        mock_send_slack_error.assert_not_called()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["matched_count"] == 1
        assert result.data["failed_count"] == 3

        # Verify the successfully matched variant got a CAID
        variants_with_caid = session.scalars(
            select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))
        ).all()
        assert len(variants_with_caid) == 1
        assert variants_with_caid[0].clingen_allele_id == f"CA{mapped_variants[0].id}"

        # Verify annotation statuses: 1 success, 3 failed
        success_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == "clingen_allele_id",
                VariantAnnotationStatus.status == "success",
            )
        ).all()
        failed_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == "clingen_allele_id",
                VariantAnnotationStatus.status == "failed",
            )
        ).all()
        assert len(success_annotations) == 1
        assert len(failed_annotations) == 3

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.SUCCEEDED

    async def test_submit_score_set_mappings_to_car_car_error_details_stored_in_annotation_metadata(
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
        """Test that explicit CAR error details (errorType, hgvs, message) are stored in annotation_metadata."""
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

        # Return a CAR response where: first variant succeeds, second has explicit CAR error, rest are silent failures
        mapped_variants = session.scalars(select(MappedVariant)).all()
        first_hgvs = get_hgvs_from_post_mapped(mapped_variants[0].post_mapped)
        second_hgvs = get_hgvs_from_post_mapped(mapped_variants[1].post_mapped)
        registered_alleles_mock = [
            {
                "@id": f"CA{mapped_variants[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": first_hgvs}],
            },
            {
                "errorType": "InvalidHGVS",
                "hgvs": second_hgvs,
                "message": "The HGVS string is invalid.",
                "description": "error",
                "inputLine": second_hgvs,
                "position": "0",
            },
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error"),
        ):
            await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        # Verify the variant whose HGVS returned an explicit CAR error has error details in annotation_metadata.
        # Only 1 annotation should have EXTERNAL_SERVICE_REJECTED since only one CAR error was in the response.
        car_rejected_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == "clingen_allele_id",
                VariantAnnotationStatus.failure_category == "external_service_rejected",
            )
        ).all()
        assert len(car_rejected_annotations) == 1
        rejected = car_rejected_annotations[0]
        assert rejected.annotation_metadata["submitted_hgvs"] == second_hgvs
        assert rejected.annotation_metadata["car_error_type"] == "InvalidHGVS"
        assert rejected.annotation_metadata["car_error_message"] == "The HGVS string is invalid."

        # The remaining 2 failures (variants 3 and 4) got no CAR response — silent failures get EXTERNAL_API_ERROR.
        silent_failure_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == "clingen_allele_id",
                VariantAnnotationStatus.failure_category == "external_api_error",
            )
        ).all()
        assert len(silent_failure_annotations) == 2
        for ann in silent_failure_annotations:
            assert ann.annotation_metadata["submitted_hgvs"] is not None
            assert "car_error_type" not in ann.annotation_metadata

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.ERRORED
        assert isinstance(result.exception, Exception)
        assert str(result.exception) == "ClinGen service error"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.ERRORED


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

        # Verify annotation statuses were rendered as success
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "success"

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

        # Verify annotation statuses were rendered as success
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "success"

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_car", submit_score_set_mappings_to_car_sample_job_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.ERRORED
        assert submit_score_set_mappings_to_car_sample_job_run.error_message == "ClinGen service error"

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 0

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_car", submit_score_set_mappings_to_car_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_car_sample_job_run_in_pipeline.status == JobStatus.ERRORED
        assert submit_score_set_mappings_to_car_sample_job_run_in_pipeline.error_message == "ClinGen service error"

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_pipeline)
        assert submit_score_set_mappings_to_car_sample_pipeline.status == PipelineStatus.FAILED

        # Verify no variants have CAIDs assigned
        variants = session.scalars(select(MappedVariant).where(MappedVariant.clingen_allele_id.isnot(None))).all()
        assert len(variants) == 0

        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "clingen_allele_id")
        ).all()
        assert len(annotation_statuses) == 0


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
        ):
            result = await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

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
            return ([], [TEST_CLINGEN_LDH_LINKING_RESPONSE_BAD_REQUEST] * 4)

        # Patch ClinGenLdhService to simulate all submissions failing
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_submission_failure(),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.LDH_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
        ):
            result = await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

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
        ):
            result = await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

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
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id),
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

        variants = session.scalars(select(Variant)).all()

        async def dummy_partial_submission(*args, **kwargs):
            return (
                [
                    {
                        "data": {
                            "entId": v.urn,
                            "ldhId": f"LDH123400{idx}",
                            "ldhIri": f"https://10.15.55.128/ldh-stg/MaveDBMapping/id/LDH123400{idx}",
                        },
                        "status": {"code": 200, "name": "OK"},
                    }
                    for idx, v in enumerate(variants[2:], start=1)
                ],
                [TEST_CLINGEN_LDH_LINKING_RESPONSE_BAD_REQUEST] * 2,
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
        ):
            result = await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

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

        variants = session.scalars(select(Variant)).all()

        async def dummy_successful_submission(*args, **kwargs):
            return (
                [
                    {
                        "data": {
                            "entId": v.urn,
                            "ldhId": f"LDH123400{idx}",
                            "ldhIri": f"https://10.15.55.128/ldh-stg/MaveDBMapping/id/LDH123400{idx}",
                        },
                        "status": {"code": 200, "name": "OK"},
                    }
                    for idx, v in enumerate(variants, start=1)
                ],
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
        ):
            result = await submit_score_set_mappings_to_ldh(
                mock_worker_ctx,
                submit_score_set_mappings_to_ldh_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_ldh_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED


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

        variants = session.scalars(select(Variant)).all()

        async def dummy_ldh_submission(*args, **kwargs):
            return (
                [
                    {
                        "data": {
                            "entId": v.urn,
                            "ldhId": f"LDH123400{idx}",
                            "ldhIri": f"https://10.15.55.128/ldh-stg/MaveDBMapping/id/LDH123400{idx}",
                        },
                        "status": {"code": 200, "name": "OK"},
                    }
                    for idx, v in enumerate(variants, start=1)
                ],
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

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "success"

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

        variants = session.scalars(select(Variant)).all()

        async def dummy_ldh_submission(*args, **kwargs):
            return (
                [
                    {
                        "data": {
                            "entId": v.urn,
                            "ldhId": f"LDH123400{idx}",
                            "ldhIri": f"https://10.15.55.128/ldh-stg/MaveDBMapping/id/LDH123400{idx}",
                        },
                        "status": {"code": 200, "name": "OK"},
                    }
                    for idx, v in enumerate(variants, start=1)
                ],
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

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "success"

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.ERRORED
        assert isinstance(result.exception, Exception)
        assert str(result.exception) == "LDH service error"

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.ERRORED

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

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify annotation statuses were created with failures
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "failed"

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

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 0

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
            return ([], [TEST_CLINGEN_LDH_LINKING_RESPONSE_BAD_REQUEST] * 4)

        # Patch ClinGenLdhService to simulate all submissions failing
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_submission_failure(),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        mock_send_slack_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify annotation statuses were created with failures
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "failed"

        # Verify the job status is updated in the database
        # TODO:XXX: Change status to 'failed' once decorator supports it
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

        variants = session.scalars(select(Variant)).all()

        async def dummy_partial_submission(*args, **kwargs):
            return (
                [
                    {
                        "data": {
                            "entId": variants[0].urn,
                            "ldhId": f"LDH123400{1}",
                            "ldhIri": f"https://10.15.55.128/ldh-stg/MaveDBMapping/id/LDH123400{1}",
                        },
                        "status": {"code": 200, "name": "OK"},
                    }
                ],
                [TEST_CLINGEN_LDH_LINKING_RESPONSE_BAD_REQUEST] * 3,
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

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        success_count = 0
        failure_count = 0
        for ann in annotation_statuses:
            if ann.status == "success":
                success_count += 1
            elif ann.status == "failed":
                failure_count += 1

        assert success_count == 1
        assert failure_count == 3

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

        variants = session.scalars(select(Variant)).all()

        async def dummy_ldh_submission(*args, **kwargs):
            return (
                [
                    {
                        "data": {
                            "entId": v.urn,
                            "ldhId": f"LDH123400{idx}",
                            "ldhIri": f"https://10.15.55.128/ldh-stg/MaveDBMapping/id/LDH123400{idx}",
                        },
                        "status": {"code": 200, "name": "OK"},
                    }
                    for idx, v in enumerate(variants, start=1)
                ],
                [],
            )

        # Patch ClinGenLdhService to simulate all submissions succeeding
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_ldh_submission(),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.ClinGenLdhService.authenticate", return_value=None),
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "success"

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

        variants = session.scalars(select(Variant)).all()

        async def dummy_ldh_submission(*args, **kwargs):
            return (
                [
                    {
                        "data": {
                            "entId": v.urn,
                            "ldhId": f"LDH123400{idx}",
                            "ldhIri": f"https://10.15.55.128/ldh-stg/MaveDBMapping/id/LDH123400{idx}",
                        },
                        "status": {"code": 200, "name": "OK"},
                    }
                    for idx, v in enumerate(variants, start=1)
                ],
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

        # Verify annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "success"

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

        variants = session.scalars(select(Variant)).all()

        async def dummy_ldh_submission(*args, **kwargs):
            return (
                [
                    {
                        "data": {
                            "entId": v.urn,
                            "ldhId": f"LDH123400{idx}",
                            "ldhIri": f"https://10.15.55.128/ldh-stg/MaveDBMapping/id/LDH123400{idx}",
                        },
                        "status": {"code": 200, "name": "OK"},
                    }
                    for idx, v in enumerate(variants, start=1)
                ],
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

        # Verify annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.status == "success"

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_ldh", submit_score_set_mappings_to_ldh_sample_job_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 0

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run)
        assert submit_score_set_mappings_to_ldh_sample_job_run.status == JobStatus.ERRORED
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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_error") as mock_send_slack_error,
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_ldh", submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_error.assert_called_once()
        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(
            select(VariantAnnotationStatus).where(VariantAnnotationStatus.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 0

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.status == JobStatus.ERRORED
        assert submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.error_message == "LDH service error"

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_pipeline.status == PipelineStatus.FAILED
