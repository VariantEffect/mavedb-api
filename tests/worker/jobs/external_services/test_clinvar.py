# ruff: noqa: E402

import pytest
import requests

from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus, JobStatus, PipelineStatus
from mavedb.models.variant_annotation_status import VariantAnnotationStatus

pytest.importorskip("arq")

import gzip
from asyncio.unix_events import _UnixSelectorEventLoop
from unittest.mock import call, patch

from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.external_services.clinvar import refresh_clinvar_controls
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


async def mock_fetch_tsv(*args, **kwargs):
    data = b"#AlleleID\tClinicalSignificance\tGeneSymbol\tReviewStatus\nVCV000000123\tbenign\tTEST\treviewed by expert panel"
    return gzip.compress(data)


@pytest.mark.unit
@pytest.mark.asyncio
class TestRefreshClinvarControlsUnit:
    """Tests for the refresh_clinvar_controls job function."""

    async def test_refresh_clinvar_controls_invalid_month_raises(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
    ):
        # edit the job run to have an invalid month
        sample_refresh_clinvar_controls_job_run.job_params["month"] = 13
        session.commit()

        with pytest.raises(ValueError, match="Month must be an integer between 1 and 12."):
            await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

    async def test_refresh_clinvar_controls_invalid_year_raises(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
    ):
        # edit the job run to have an invalid year
        sample_refresh_clinvar_controls_job_run.job_params["year"] = 1999
        session.commit()

        with pytest.raises(ValueError, match="ClinVar archived data is only available from February 2015 onwards."):
            await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

    async def test_refresh_clinvar_controls_propagates_exception_during_fetch(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
    ):
        # Mock the fetch_clinvar_variant_data function to raise an exception
        async def awaitable_exception(*args, **kwargs):
            raise Exception("Network error")

        with (
            pytest.raises(Exception, match="Network error"),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=awaitable_exception(),
            ),
        ):
            await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

    async def test_refresh_clinvar_controls_no_mapped_variants(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Test that the job completes successfully when there are no mapped variants."""

        async def awaitable_noop(*args, **kwargs):
            return {}

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=awaitable_noop(),
            ),
            patch("mavedb.worker.jobs.external_services.clinvar.parse_clinvar_variant_summary"),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

    async def test_refresh_clinvar_controls_no_variants_have_caids(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Test that the job completes successfully when no variants have CAIDs."""
        # Add a variant without a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant = Variant(
            urn="urn:variant:test-variant-no-caid",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.2G>A",
            hgvs_pro="NP_000000.1:p.Val2Ile",
            data={"hgvs_c": "NM_000000.1:c.2G>A", "hgvs_p": "NP_000000.1:p.Val2Ile"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

        with patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=mock_fetch_tsv(),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant without a CAID
        variant_no_caid = (
            session.query(VariantAnnotationStatus).filter(VariantAnnotationStatus.variant_id == variant.id).one()
        )
        assert variant_no_caid.status == AnnotationStatus.SKIPPED
        assert variant_no_caid.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert variant_no_caid.error_message == "Mapped variant does not have an associated ClinGen allele ID."

    async def test_refresh_clinvar_controls_variants_are_multivariants(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Test that the job completes successfully when all variants are multi-variant CAIDs."""
        # Update the mapped variant to have a multi-variant CAID
        mapped_variant = session.query(MappedVariant).first()
        mapped_variant.clingen_allele_id = "CA-MULTI-001,CA-MULTI-002"
        session.commit()

        with patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=mock_fetch_tsv(),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

        # Verify an annotation status was created for the multi-variant CAID
        variant_with_multicid = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert variant_with_multicid.status == AnnotationStatus.SKIPPED
        assert variant_with_multicid.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert (
            variant_with_multicid.error_message
            == "Multi-variant ClinGen allele IDs cannot be associated with ClinVar data."
        )

    async def test_refresh_clinvar_controls_clingen_api_failure(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Test that the job handles ClinGen API failures gracefully."""

        # Mock the get_associated_clinvar_allele_id function to raise an exception
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=requests.exceptions.RequestException("ClinGen API error"),
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant due to ClinGen API failure
        mapped_variant = session.query(MappedVariant).first()
        variant_with_api_failure = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert variant_with_api_failure.status == AnnotationStatus.FAILED
        assert variant_with_api_failure.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert "Failed to retrieve ClinVar allele ID from ClinGen API" in variant_with_api_failure.error_message

    async def test_refresh_clinvar_controls_no_associated_clinvar_allele_id(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Test that the job handles no associated ClinVar Allele ID gracefully."""

        # Mock the get_associated_clinvar_allele_id function to return None
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value=None,
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant due to no associated ClinVar Allele ID
        mapped_variant = session.query(MappedVariant).first()
        variant_no_clinvar_allele = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert variant_no_clinvar_allele.status == AnnotationStatus.SKIPPED
        assert variant_no_clinvar_allele.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert "No ClinVar allele ID found for ClinGen allele ID" in variant_no_clinvar_allele.error_message

    async def test_refresh_clinvar_controls_no_clinvar_data_found(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Test that the job handles no ClinVar data found for the associated ClinVar Allele ID."""

        async def mock_fetch_tsv(*args, **kwargs):
            data = b"#AlleleID\tClinicalSignificance\tGeneSymbol\tReviewStatus\nVCV000000001\tbenign\tTEST\treviewed by expert panel"
            return gzip.compress(data)

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant due to no ClinVar data found
        mapped_variant = session.query(MappedVariant).first()
        variant_no_clinvar_data = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert variant_no_clinvar_data.status == AnnotationStatus.SKIPPED
        assert variant_no_clinvar_data.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert "No ClinVar data found for ClinVar allele ID" in variant_no_clinvar_data.error_message

    async def test_refresh_clinvar_controls_successful_annotation_existing_control(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Test that the job successfully annotates a variant with ClinVar control data."""

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant with successful annotation
        mapped_variant = session.query(MappedVariant).first()
        annotated_variant = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert annotated_variant.status == AnnotationStatus.SUCCESS
        assert annotated_variant.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert annotated_variant.error_message is None

    async def test_refresh_clinvar_controls_successful_annotation_new_control(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Test that the job successfully annotates a variant with ClinVar control data when no prior status exists."""
        # Add a variant and mapped variant to the database with a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant = Variant(
            urn="urn:variant:test-variant-with-caid-2",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.3C>T",
            hgvs_pro="NP_000000.1:p.Ala3Val",
            data={"hgvs_c": "NM_000000.1:c.3C>T", "hgvs_p": "NP_000000.1:p.Ala3Val"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA124",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant with successful annotation
        annotated_variant = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert annotated_variant.status == AnnotationStatus.SUCCESS
        assert annotated_variant.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert annotated_variant.error_message is None

    async def test_refresh_clinvar_controls_idempotent_run(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Test that running the job multiple times does not create duplicate annotation statuses."""

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                side_effect=[mock_fetch_tsv(), mock_fetch_tsv()],
            ),
        ):
            # First run
            result1 = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

            session.commit()

            # Second run
            result2 = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result1["status"] == "ok"
        assert result2["status"] == "ok"

        # Verify only one clinical control annotation exists for the variant
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 1

        # Verify two annotated variants exist but both reflect the same successful annotation, and only
        # one is current
        annotated_variants = session.query(VariantAnnotationStatus).all()
        assert len(annotated_variants) == 2
        statuses = [av.status for av in annotated_variants]
        assert statuses.count(AnnotationStatus.SUCCESS) == 2
        current_statuses = [av for av in annotated_variants if av.current]
        assert len(current_statuses) == 1

    async def test_refresh_clinvar_controls_partial_failure(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Test that the job handles partial failures gracefully."""

        variant1, mapped_variant1 = setup_sample_variants_with_caid

        # Add an additional mapped variant to the database with a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant2 = Variant(
            urn="urn:variant:test-variant-with-caid-2",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.4G>C",
            hgvs_pro="NP_000000.1:p.Gly4Ala",
            data={"hgvs_c": "NM_000000.1:c.4G>C", "hgvs_p": "NP_000000.1:p.Gly4Ala"},
        )
        session.add(variant2)
        session.commit()
        mapped_variant2 = MappedVariant(
            variant_id=variant2.id,
            clingen_allele_id="CA125",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant2)
        session.commit()

        # Mock the get_associated_clinvar_allele_id function to raise an exception for the first call
        def side_effect_get_associated_clinvar_allele_id(clingen_allele_id):
            if clingen_allele_id == "CA125":
                raise requests.exceptions.RequestException("ClinGen API error")
            return "VCV000000123"

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=side_effect_get_associated_clinvar_allele_id,
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

        # Verify annotation statuses for both variants
        variant_with_api_failure = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant2.variant_id)
            .one()
        )
        assert variant_with_api_failure.status == AnnotationStatus.FAILED
        assert variant_with_api_failure.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert "Failed to retrieve ClinVar allele ID from ClinGen API" in variant_with_api_failure.error_message

        annotated_variant2 = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant1.variant_id)
            .one()
        )
        assert annotated_variant2.status == AnnotationStatus.SUCCESS
        assert annotated_variant2.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert annotated_variant2.error_message is None

    async def test_refresh_clinvar_controls_updates_progress(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Test that the job updates progress correctly."""

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "ok"

        mock_update_progress.assert_has_calls(
            [
                call(0, 100, "Starting ClinVar clinical control refresh for version 01_2026."),
                call(1, 100, "Fetching ClinVar variant summary TSV data."),
                call(10, 100, "Fetched and parsed ClinVar variant summary TSV data."),
                call(10, 100, "Refreshing ClinVar data for 1 variants (0 completed)."),
                call(100, 100, "Completed ClinVar clinical control refresh."),
            ]
        )


@pytest.mark.integration
@pytest.mark.asyncio
class TestRefreshClinvarControlsIntegration:
    """Integration tests for the refresh_clinvar_controls job function."""

    async def test_refresh_clinvar_controls_no_mapped_variants(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Integration test: job completes successfully when there are no mapped variants."""

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

        assert result["status"] == "ok"

        # Verify no controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 0

        # Verify no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_no_variants_with_caid(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Integration test: job completes successfully when no variants have CAIDs."""
        # Add a variant without a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant = Variant(
            urn="urn:variant:integration-test-variant-no-caid",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.5T>A",
            hgvs_pro="NP_000000.1:p.Leu5Gln",
            data={"hgvs_c": "NM_000000.1:c.5T>A", "hgvs_p": "NP_000000.1:p.Leu5Gln"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant without a CAID
        variant_no_caid = (
            session.query(VariantAnnotationStatus).filter(VariantAnnotationStatus.variant_id == variant.id).one()
        )
        assert variant_no_caid.status == AnnotationStatus.SKIPPED
        assert variant_no_caid.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert variant_no_caid.error_message == "Mapped variant does not have an associated ClinGen allele ID."

        # Verify no clinical controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 0

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controlsvariants_are_multivariants(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Integration test: job completes successfully when all variants are multi-variant CAIDs."""
        # Add a variant with a multi-variant CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant = Variant(
            urn="urn:variant:integration-test-variant-multicid",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.6A>G",
            hgvs_pro="NP_000000.1:p.Thr6Ala",
            data={"hgvs_c": "NM_000000.1:c.6A>G", "hgvs_p": "NP_000000.1:p.Thr6Ala"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA-MULTI-003,CA-MULTI-004",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

        assert result["status"] == "ok"

        # Verify an annotation status was created for the multi-variant CAID
        variant_with_multicid = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert variant_with_multicid.status == AnnotationStatus.SKIPPED
        assert variant_with_multicid.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert (
            variant_with_multicid.error_message
            == "Multi-variant ClinGen allele IDs cannot be associated with ClinVar data."
        )

        # Verify no clinical controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 0

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_no_associated_clinvar_allele_id(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Integration test: job handles no associated ClinVar Allele ID gracefully."""
        # Add a variant with a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant = Variant(
            urn="urn:variant:integration-test-variant-with-caid",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.7C>A",
            hgvs_pro="NP_000000.1:p.Ser7Tyr",
            data={"hgvs_c": "NM_000000.1:c.7C>A", "hgvs_p": "NP_000000.1:p.Ser7Tyr"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA126",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

        # Mock the get_associated_clinvar_allele_id function to return None
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value=None,
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant due to no associated ClinVar Allele ID
        variant_no_clinvar_allele = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert variant_no_clinvar_allele.status == AnnotationStatus.SKIPPED
        assert variant_no_clinvar_allele.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert "No ClinVar allele ID found for ClinGen allele ID" in variant_no_clinvar_allele.error_message

        # Verify no clinical controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 0

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_no_clinvar_data(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Integration test: job handles no ClinVar data found for the associated ClinVar Allele ID."""
        # Add a variant with a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant = Variant(
            urn="urn:variant:integration-test-variant-with-caid",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.8G>T",
            hgvs_pro="NP_000000.1:p.Val8Phe",
            data={"hgvs_c": "NM_000000.1:c.8G>T", "hgvs_p": "NP_000000.1:p.Val8Phe"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA127",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000001",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant due to no ClinVar data found
        variant_no_clinvar_data = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert variant_no_clinvar_data.status == AnnotationStatus.SKIPPED
        assert variant_no_clinvar_data.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert "No ClinVar data found for ClinVar allele ID" in variant_no_clinvar_data.error_message

        # Verify no clinical controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 0

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_successful_annotation_existing_control(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Integration test: job successfully annotates a variant with ClinVar control data."""
        # Add a variant with a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant = Variant(
            urn="urn:variant:integration-test-variant-with-caid",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.9A>C",
            hgvs_pro="NP_000000.1:p.Lys9Thr",
            data={"hgvs_c": "NM_000000.1:c.9A>C", "hgvs_p": "NP_000000.1:p.Lys9Thr"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA128",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()
        clinical_control = ClinicalControl(
            db_name="ClinVar",
            db_identifier="VCV000000123",
            clinical_significance="likely pathogenic",
            gene_symbol="TEST",
            clinical_review_status="criteria provided, single submitter",
            db_version="01_2026",
        )
        session.add(clinical_control)
        session.commit()

        mapped_variant.clinical_controls.append(clinical_control)
        session.commit()

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant with successful annotation
        annotated_variant = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert annotated_variant.status == AnnotationStatus.SUCCESS
        assert annotated_variant.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert annotated_variant.error_message is None

        # Verify the clinical control was updated
        session.refresh(clinical_control)
        assert clinical_control.clinical_significance == "benign"
        assert clinical_control.clinical_review_status == "reviewed by expert panel"
        assert mapped_variant in clinical_control.mapped_variants

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_successful_annotation_new_control(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Integration test: job successfully annotates a variant with ClinVar control data when no prior status exists."""
        # Add a variant with a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant = Variant(
            urn="urn:variant:integration-test-variant-with-caid",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.10C>G",
            hgvs_pro="NP_000000.1:p.Pro10Arg",
            data={"hgvs_c": "NM_000000.1:c.10C>G", "hgvs_p": "NP_000000.1:p.Pro10Arg"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA129",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant with successful annotation
        annotated_variant = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert annotated_variant.status == AnnotationStatus.SUCCESS
        assert annotated_variant.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert annotated_variant.error_message is None

        # Verify the clinical control was added
        clinical_control = (
            session.query(ClinicalControl).filter(ClinicalControl.mapped_variants.contains(mapped_variant)).one()
        )
        assert clinical_control.db_identifier == "VCV000000123"

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_successful_annotation_pipeline_context(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_pipeline,
        sample_refresh_clinvar_controls_job_in_pipeline,
    ):
        """Integration test: job successfully annotates a variant with ClinVar control data in a pipeline context."""
        # Add a variant with a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_in_pipeline.job_params["score_set_id"])
        variant = Variant(
            urn="urn:variant:integration-test-variant-with-caid",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.12G>A",
            hgvs_pro="NP_000000.1:p.Met12Ile",
            data={"hgvs_c": "NM_000000.1:c.12G>A", "hgvs_p": "NP_000000.1:p.Met12Ile"},
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA130",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_in_pipeline.id)

        assert result["status"] == "ok"

        # Verify an annotation status was created for the variant with successful annotation
        annotated_variant = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant.variant_id)
            .one()
        )
        assert annotated_variant.status == AnnotationStatus.SUCCESS
        assert annotated_variant.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert annotated_variant.error_message is None

        # Verify the clinical control was added
        clinical_control = (
            session.query(ClinicalControl).filter(ClinicalControl.mapped_variants.contains(mapped_variant)).one()
        )
        assert clinical_control.db_identifier == "VCV000000123"

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_in_pipeline)
        assert sample_refresh_clinvar_controls_job_in_pipeline.status == JobStatus.SUCCEEDED

        # Verify the pipeline is marked as completed
        session.refresh(sample_refresh_clinvar_controls_pipeline)
        assert sample_refresh_clinvar_controls_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_idempotent_run(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Integration test: running the job multiple times does not create duplicate annotation statuses."""

        # Mock the get_associated_clinvar_allele_id function to return a ClinVar Allele ID
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                side_effect=[mock_fetch_tsv(), mock_fetch_tsv()],
            ),
        ):
            # First run
            result1 = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

            session.commit()
            # reset the job run status to pending for the second run
            sample_refresh_clinvar_controls_job_run.status = JobStatus.PENDING
            session.commit()

            # Second run
            result2 = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

        assert result1["status"] == "ok"
        assert result2["status"] == "ok"

        # Verify only one clinical control annotation exists for the variant
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 1

        # Verify two annotated variants exist but both reflect the same successful annotation, and only
        # one is current
        annotated_variants = session.query(VariantAnnotationStatus).all()
        assert len(annotated_variants) == 2
        statuses = [av.status for av in annotated_variants]
        assert statuses.count(AnnotationStatus.SUCCESS) == 2
        current_statuses = [av for av in annotated_variants if av.current]
        assert len(current_statuses) == 1

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_partial_failure(
        self,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        mock_worker_ctx,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Integration test: job handles partial failures gracefully."""

        variant1, mapped_variant1 = setup_sample_variants_with_caid
        # Add an additional mapped variant to the database with a CAID
        score_set = session.get(ScoreSet, sample_refresh_clinvar_controls_job_run.job_params["score_set_id"])
        variant2 = Variant(
            urn="urn:variant:integration-test-variant-with-caid-2",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.11G>C",
            hgvs_pro="NP_000000.1:p.Gly11Ala",
            data={"hgvs_c": "NM_000000.1:c.11G>C", "hgvs_p": "NP_000000.1:p.Gly11Ala"},
        )
        session.add(variant2)
        session.commit()
        mapped_variant2 = MappedVariant(
            variant_id=variant2.id,
            clingen_allele_id="CA130",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add(mapped_variant2)
        session.commit()

        # Mock the get_associated_clinvar_allele_id function to raise an exception for the first call
        def side_effect_get_associated_clinvar_allele_id(clingen_allele_id):
            if clingen_allele_id == "CA130":
                raise requests.exceptions.RequestException("ClinGen API error")
            return "VCV000000123"

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=side_effect_get_associated_clinvar_allele_id,
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(mock_worker_ctx, sample_refresh_clinvar_controls_job_run.id)

        assert result["status"] == "ok"

        # Verify annotation statuses for both variants
        variant_with_api_failure = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant2.variant_id)
            .one()
        )
        assert variant_with_api_failure.status == AnnotationStatus.FAILED
        assert variant_with_api_failure.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert "Failed to retrieve ClinVar allele ID from ClinGen API" in variant_with_api_failure.error_message

        annotated_variant2 = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == mapped_variant1.variant_id)
            .one()
        )
        assert annotated_variant2.status == AnnotationStatus.SUCCESS
        assert annotated_variant2.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert annotated_variant2.error_message is None

        # Verify a clinical control was added for the successfully annotated variant and not the unsuccessful one
        clinical_control1 = (
            session.query(ClinicalControl).filter(ClinicalControl.mapped_variants.contains(mapped_variant1)).one()
        )
        assert clinical_control1.db_identifier == "VCV000000123"

        clinical_control2 = (
            session.query(ClinicalControl).filter(ClinicalControl.mapped_variants.contains(mapped_variant2)).all()
        )
        assert len(clinical_control2) == 0

        # Verify job run status is marked as completed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_propagates_exceptions_to_decorator(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Test that unexpected exceptions are propagated."""

        # Mock the get_associated_clinvar_allele_id function to raise an unexpected exception
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=ValueError("Unexpected error"),
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result["status"] == "exception"

        # Verify no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify no clinical controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 0

        # Verify job run status is marked as failed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.FAILED


@pytest.mark.asyncio
@pytest.mark.integration
class TestRefreshClinvarControlsArqContext:
    """Tests for running the refresh_clinvar_controls job function within an ARQ worker context."""

    async def test_refresh_clinvar_controls_with_arq_context_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Integration test: job completes successfully within an ARQ worker context."""

        # Patch external service calls
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            await arq_redis.enqueue_job("refresh_clinvar_controls", sample_refresh_clinvar_controls_job_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that clinical controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) > 0

        # Verify annotation status was created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == AnnotationStatus.SUCCESS
        assert annotation_statuses[0].annotation_type == AnnotationType.CLINVAR_CONTROL

        # Verify that the job completed successfully
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_refresh_clinvar_controls_with_arq_context_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Integration test: job completes successfully within an ARQ worker context in a pipeline context."""

        # Patch external service calls
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            await arq_redis.enqueue_job("refresh_clinvar_controls", sample_refresh_clinvar_controls_job_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that clinical controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) > 0

        # Verify annotation status was created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == AnnotationStatus.SUCCESS
        assert annotation_statuses[0].annotation_type == AnnotationType.CLINVAR_CONTROL

        # Verify that the job completed successfully
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

        # Verify the pipeline is marked as completed
        pass

    async def test_refresh_clinvar_controls_with_arq_context_exception_handling_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Integration test: job handles exceptions properly within an ARQ worker context."""
        # Patch external service calls to raise an exception
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=ValueError("Unexpected error"),
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            await arq_redis.enqueue_job("refresh_clinvar_controls", sample_refresh_clinvar_controls_job_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify no clinical controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 0

        # Verify job run status is marked as failed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.FAILED

    async def test_refresh_clinvar_controls_with_arq_context_exception_handling_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_variants_with_caid,
    ):
        """Integration test: job handles exceptions properly within an ARQ worker context in a pipeline context."""
        # Patch external service calls to raise an exception
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=ValueError("Unexpected error"),
            ),
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=mock_fetch_tsv(),
            ),
        ):
            await arq_redis.enqueue_job("refresh_clinvar_controls", sample_refresh_clinvar_controls_job_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify no clinical controls were added
        clinical_controls = session.query(ClinicalControl).all()
        assert len(clinical_controls) == 0

        # Verify job run status is marked as failed
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.FAILED

        # Verify the pipeline is marked as failed
        pass
