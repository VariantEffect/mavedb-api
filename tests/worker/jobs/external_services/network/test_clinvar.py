# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from sqlalchemy import select

from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus, JobStatus
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.worker.jobs.external_services.clinvar import generate_clinvar_versions

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
class TestE2ERefreshClinvarControls:
    async def test_refresh_clinvar_controls_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        setup_sample_variants_with_caid,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
    ):
        """Test the end-to-end flow of refreshing ClinVar clinical controls."""
        await arq_redis.enqueue_job("refresh_clinvar_controls", sample_refresh_clinvar_controls_job_run.id)
        await arq_worker.async_run()
        await arq_worker.run_check()

        # Verify that clinical controls were added successfully — one row per ClinVar version
        # that contains the variant, so there may be more than one.
        clinical_controls = session.scalars(select(ClinicalControl)).all()
        assert len(clinical_controls) >= 1
        assert all(cc.db_identifier == "3045425" for cc in clinical_controls)

        # Verify that at least one SUCCESS annotation was recorded for the variant.
        # The job processes 12 ClinVar versions; versions without the variant produce
        # SKIPPED annotations, so only filtering for SUCCESS gives a stable assertion.
        success_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == AnnotationType.CLINVAR_CONTROL,
                VariantAnnotationStatus.status == AnnotationStatus.SUCCESS,
            )
        ).all()
        assert len(success_annotations) >= 1

        # Verify that SKIPPED annotations are produced for versions where the variant
        # is absent — expected for any of the 12 ClinVar versions that don't contain it.
        skipped_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == AnnotationType.CLINVAR_CONTROL,
                VariantAnnotationStatus.status == AnnotationStatus.SKIPPED,
            )
        ).all()
        assert len(skipped_annotations) >= 1

        # Total annotations should equal the number of ClinVar versions processed.
        assert len(success_annotations) + len(skipped_annotations) == len(generate_clinvar_versions())

        # Verify that the job run was completed successfully
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED
