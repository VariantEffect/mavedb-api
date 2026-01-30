# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from sqlalchemy import select

from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus, JobStatus
from mavedb.models.variant_annotation_status import VariantAnnotationStatus


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

        # Verify that clinical controls were added successfully
        clinical_controls = session.scalars(select(ClinicalControl)).all()
        assert len(clinical_controls) == 1
        assert clinical_controls[0].db_identifier == "3045425"

        # Verify that annotation status was added
        annotation_statuses = session.scalars(select(VariantAnnotationStatus)).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == AnnotationStatus.SUCCESS
        assert annotation_statuses[0].annotation_type == AnnotationType.CLINVAR_CONTROL

        # Verify that the job run was completed successfully
        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED
