# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from unittest.mock import patch

from sqlalchemy import select

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus, PipelineStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.models.variant_translation import VariantTranslation
from mavedb.worker.jobs.external_services.variant_translation import populate_variant_translations_for_score_set
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


# --- Unit Tests ---


@pytest.mark.asyncio
@pytest.mark.unit
class TestPopulateVariantTranslationsUnit:
    """Unit tests for the populate_variant_translations_for_score_set job."""

    async def test_no_mapped_variants(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
    ):
        """Test that the job succeeds with zero translations when no mapped variants exist."""
        result = await populate_variant_translations_for_score_set(
            mock_worker_ctx,
            1,
            JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
        )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["translations_created"] == 0

    async def test_variant_without_caid_no_translations(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that a variant without a CAID results in no translations."""
        _, mapped_variant = setup_sample_variants_with_caid_for_translation
        mapped_variant.clingen_allele_id = None
        session.commit()

        result = await populate_variant_translations_for_score_set(
            mock_worker_ctx,
            1,
            JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
        )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["translations_created"] == 0

    async def test_ca_allele_creates_translations(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that a CA allele creates translations via PA lookup."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                return_value=["PA00001"],
            ),
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=["CA11111", "CA22222"],
            ),
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        # 1 for PA00001->CA9765210 (the original CA), 2 for PA00001->CA11111 and PA00001->CA22222
        assert result.data["translations_created"] == 3

        translations = session.scalars(select(VariantTranslation)).all()
        assert len(translations) == 3

        annotation = session.scalars(select(VariantAnnotationStatus)).one()
        assert annotation is not None

    async def test_pa_allele_creates_translations(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that a PA allele creates translations via CA lookup."""
        _, mapped_variant = setup_sample_variants_with_caid_for_translation
        mapped_variant.clingen_allele_id = "PA99999"
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=["CA33333", "CA44444"],
            ),
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["translations_created"] == 2

        translations = session.scalars(select(VariantTranslation)).all()
        assert len(translations) == 2
        aa_ids = {t.aa_clingen_id for t in translations}
        assert aa_ids == {"PA99999"}

    async def test_multi_variant_caid_expanded(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that comma-separated CAIDs are expanded and each processed independently."""
        _, mapped_variant = setup_sample_variants_with_caid_for_translation
        mapped_variant.clingen_allele_id = "CA55555,CA66666"
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                return_value=["PA00002"],
            ),
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=[],
            ),
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        # PA00002->CA55555 and PA00002->CA66666
        assert result.data["translations_created"] == 2

    async def test_ca_allele_no_pa_ids_skipped(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that a CA allele with no canonical PA IDs results in a skip."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                return_value=[],
            ),
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["alleles_skipped"] == 1
        assert result.data["translations_created"] == 0

        annotation = session.scalars(select(VariantAnnotationStatus)).one()
        assert annotation.status == "skipped"

    async def test_pa_allele_no_ca_ids_skipped(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that a PA allele with no registered CA IDs results in a skip."""
        _, mapped_variant = setup_sample_variants_with_caid_for_translation
        mapped_variant.clingen_allele_id = "PA88888"
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=[],
            ),
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["alleles_skipped"] == 1
        assert result.data["translations_created"] == 0

    async def test_ca_allele_api_failure_records_failed_annotation(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that a ClinGen API failure for CA allele records a failed annotation."""
        import requests

        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                side_effect=requests.exceptions.ConnectionError("Connection failed"),
            ),
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
            )

        assert result.status == JobStatus.FAILED
        assert result.failure_category == FailureCategory.DEPENDENCY_FAILURE
        assert result.data["alleles_failed"] == 1

        annotation = session.scalars(select(VariantAnnotationStatus)).one()
        assert annotation.status == "failed"

    async def test_unrecognized_allele_format_skipped(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that an unrecognized allele ID format is skipped."""
        _, mapped_variant = setup_sample_variants_with_caid_for_translation
        mapped_variant.clingen_allele_id = "XX12345"
        session.commit()

        result = await populate_variant_translations_for_score_set(
            mock_worker_ctx,
            1,
            JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
        )

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["alleles_skipped"] == 1

    async def test_duplicate_translations_not_created(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that duplicate translations are not created on re-run."""
        # Pre-populate a translation
        session.add(VariantTranslation(aa_clingen_id="PA00003", nt_clingen_id="CA9765210"))
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                return_value=["PA00003"],
            ),
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=[],
            ),
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["translations_created"] == 0

        translations = session.scalars(
            select(VariantTranslation).where(VariantTranslation.aa_clingen_id == "PA00003")
        ).all()
        assert len(translations) == 1

    async def test_propagates_exceptions(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that unexpected exceptions are propagated."""
        with patch(
            "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
            side_effect=Exception("Test exception"),
        ):
            with pytest.raises(Exception) as exc_info:
                await populate_variant_translations_for_score_set(
                    mock_worker_ctx,
                    1,
                    JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
                )

        assert str(exc_info.value) == "Test exception"

    async def test_multiple_alleles_sharing_pa_no_duplicate_error(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that two CA alleles mapping to the same PA don't cause a UniqueViolation.

        This is a regression test for a bug where the SELECT-then-INSERT upsert pattern
        failed to detect in-session duplicates: both alleles' iterations called
        upsert_variant_translations with overlapping (PA, CA) pairs, the SELECT found
        no committed row, both staged db.add() for the same pair, and the subsequent
        update_progress commit raised a UniqueViolation.
        """
        # Add a second variant with a different CA allele under the same score set.
        score_set_id = sample_populate_variant_translations_run.job_params["score_set_id"]

        variant2 = Variant(
            urn="urn:variant:test-second-ca-allele",
            score_set_id=score_set_id,
            hgvs_nt="NM_000000.1:c.2T>G",
            hgvs_pro="NP_000000.1:p.Val2Gly",
            data={},
        )
        session.add(variant2)
        session.commit()
        mapped_variant2 = MappedVariant(
            variant_id=variant2.id,
            clingen_allele_id="CA_SECOND",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant2)
        session.commit()

        # Both CA alleles resolve to the same PA. The PA then returns the same set of
        # registered CAs for both iterations, producing fully overlapping translation pairs.
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                return_value=["PA_SHARED"],
            ),
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=["CA9765210", "CA_SECOND"],
            ),
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED

        translations = session.scalars(select(VariantTranslation)).all()
        pairs = {(t.aa_clingen_id, t.nt_clingen_id) for t in translations}
        # PA_SHARED paired with each CA: CA9765210 (original from allele 1),
        # CA_SECOND (original from allele 2), plus both as registered CAs.
        assert ("PA_SHARED", "CA9765210") in pairs
        assert ("PA_SHARED", "CA_SECOND") in pairs

    async def test_total_api_failure_returns_failed(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that the job returns FAILED when all variant translation lookups fail."""
        import requests

        with patch(
            "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
            side_effect=requests.exceptions.ConnectionError("Connection failed"),
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_variant_translations_run.id),
            )

        assert result.status == JobStatus.FAILED
        assert result.failure_category == FailureCategory.DEPENDENCY_FAILURE
        assert result.data["alleles_failed"] == 1
        assert result.data["translations_created"] == 0


# --- Integration Tests ---


@pytest.mark.asyncio
@pytest.mark.integration
class TestPopulateVariantTranslationsIntegration:
    """Integration tests that exercise the full decorator stack."""

    async def test_no_mapped_variants(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
    ):
        """Test end-to-end when no mapped variants exist."""
        result = await populate_variant_translations_for_score_set(
            mock_worker_ctx, sample_populate_variant_translations_run.id
        )
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        session.refresh(sample_populate_variant_translations_run)
        assert sample_populate_variant_translations_run.status == JobStatus.SUCCEEDED

    async def test_successful_job_updates_status(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that a successful job run updates the job status to SUCCEEDED."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                return_value=["PA00004"],
            ),
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=["CA77777"],
            ),
        ):
            await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                sample_populate_variant_translations_run.id,
            )

        session.refresh(sample_populate_variant_translations_run)
        assert sample_populate_variant_translations_run.status == JobStatus.SUCCEEDED

        translations = session.scalars(select(VariantTranslation)).all()
        assert len(translations) == 2  # PA00004->CA9765210 and PA00004->CA77777

    async def test_job_with_pipeline_updates_pipeline_status(
        self,
        session,
        with_populated_domain_data,
        mock_worker_ctx,
        sample_populate_variant_translations_run_pipeline,
        sample_populate_variant_translations_pipeline,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that a job in a pipeline updates the pipeline status on success."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                return_value=["PA00005"],
            ),
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=[],
            ),
        ):
            await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                sample_populate_variant_translations_run_pipeline.id,
            )

        session.refresh(sample_populate_variant_translations_run_pipeline)
        session.refresh(sample_populate_variant_translations_pipeline)
        assert sample_populate_variant_translations_run_pipeline.status == JobStatus.SUCCEEDED
        assert sample_populate_variant_translations_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_variant_without_caid_creates_skipped_annotation(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that variants without CAIDs produce no annotations (filtered before processing)."""
        _, mapped_variant = setup_sample_variants_with_caid_for_translation
        mapped_variant.clingen_allele_id = None
        session.commit()

        result = await populate_variant_translations_for_score_set(
            mock_worker_ctx, sample_populate_variant_translations_run.id
        )
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["translations_created"] == 0

        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        session.refresh(sample_populate_variant_translations_run)
        assert sample_populate_variant_translations_run.status == JobStatus.SUCCEEDED

    async def test_unrecognized_allele_creates_skipped_annotation(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that unrecognized allele formats create skipped annotations through the full stack."""
        _, mapped_variant = setup_sample_variants_with_caid_for_translation
        mapped_variant.clingen_allele_id = "XX12345"
        session.commit()

        result = await populate_variant_translations_for_score_set(
            mock_worker_ctx, sample_populate_variant_translations_run.id
        )
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "skipped"
        assert annotation_statuses[0].annotation_type == "variant_translation"

        session.refresh(sample_populate_variant_translations_run)
        assert sample_populate_variant_translations_run.status == JobStatus.SUCCEEDED

    async def test_exceptions_handled_by_decorators(
        self,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        mock_worker_ctx,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that unexpected exceptions are handled by decorators."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            result = await populate_variant_translations_for_score_set(
                mock_worker_ctx,
                sample_populate_variant_translations_run.id,
            )

        mock_send_slack_job_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.ERRORED
        assert isinstance(result.exception, Exception)

        session.refresh(sample_populate_variant_translations_run)
        assert sample_populate_variant_translations_run.status == JobStatus.ERRORED


# --- ARQ Context Tests ---


@pytest.mark.asyncio
@pytest.mark.integration
class TestPopulateVariantTranslationsArqContext:
    """Tests for populate_variant_translations_for_score_set job using the ARQ context fixture."""

    async def test_with_arq_context_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that the job works with the ARQ context fixture."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                return_value=["PA00006"],
            ),
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=["CA88888"],
            ),
        ):
            await arq_redis.enqueue_job(
                "populate_variant_translations_for_score_set",
                sample_populate_variant_translations_run.id,
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        session.refresh(sample_populate_variant_translations_run)
        assert sample_populate_variant_translations_run.status == JobStatus.SUCCEEDED

        translations = session.scalars(select(VariantTranslation)).all()
        assert len(translations) == 2  # PA00006->CA9765210 and PA00006->CA88888

    async def test_with_arq_context_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        sample_populate_variant_translations_run_pipeline,
        sample_populate_variant_translations_pipeline,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that the job works with the ARQ context fixture in a pipeline."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                return_value=["PA00007"],
            ),
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_matching_registered_ca_ids",
                return_value=[],
            ),
        ):
            await arq_redis.enqueue_job(
                "populate_variant_translations_for_score_set",
                sample_populate_variant_translations_run_pipeline.id,
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "variant_translation"

        session.refresh(sample_populate_variant_translations_run_pipeline)
        assert sample_populate_variant_translations_run_pipeline.status == JobStatus.SUCCEEDED

        session.refresh(sample_populate_variant_translations_pipeline)
        assert sample_populate_variant_translations_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_with_arq_context_exception_handling_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_populate_variant_translations_job,
        sample_populate_variant_translations_run,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that exceptions are handled with the ARQ context fixture."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job(
                "populate_variant_translations_for_score_set",
                sample_populate_variant_translations_run.id,
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()

        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        session.refresh(sample_populate_variant_translations_run)
        assert sample_populate_variant_translations_run.status == JobStatus.ERRORED

    async def test_with_arq_context_exception_handling_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        sample_populate_variant_translations_pipeline,
        sample_populate_variant_translations_run_pipeline,
        setup_sample_variants_with_caid_for_translation,
    ):
        """Test that exceptions in pipeline context are handled."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.variant_translation.get_canonical_pa_ids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job(
                "populate_variant_translations_for_score_set",
                sample_populate_variant_translations_run_pipeline.id,
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()

        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        session.refresh(sample_populate_variant_translations_run_pipeline)
        assert sample_populate_variant_translations_run_pipeline.status == JobStatus.ERRORED

        session.refresh(sample_populate_variant_translations_pipeline)
        assert sample_populate_variant_translations_pipeline.status == PipelineStatus.FAILED
