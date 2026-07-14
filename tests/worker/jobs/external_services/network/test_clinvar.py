# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from unittest.mock import AsyncMock, patch

from sqlalchemy import select

from mavedb.lib.clinvar.constants import CLINVAR_FIELDS_TO_KEEP
from mavedb.lib.clinvar.utils import fetch_clinvar_variant_data
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus, JobStatus
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.worker.jobs.external_services.clinvar import generate_clinvar_versions

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")

# Stable archival snapshot known to contain ClinVar allele 3045425 (CA9765210).
_E2E_VERSIONS = [(2025, 1)]

# Minimal synthetic ClinVar data for the E2E test. The real archive is 350 MB+
# and would exceed the arq job_timeout on a cold CI runner. The ClinGen API
# call (CA9765210 → 3045425) still runs live, so the happy path is genuine.
_SYNTHETIC_CLINVAR_DATA = {
    "3045425": {
        "GeneSymbol": "CA9",
        "ClinicalSignificance": "Pathogenic",
        "ReviewStatus": "criteria provided, single submitter",
    }
}


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.network
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
        """End-to-end: job resolves CA9765210 via the live ClinGen API, stores a clinical
        control, and records the correct annotation status. The NCBI archive download is
        replaced with synthetic data so the test is fast in CI.
        """
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.generate_clinvar_versions",
                return_value=_E2E_VERSIONS,
            ),
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                new=AsyncMock(return_value=_SYNTHETIC_CLINVAR_DATA),
            ),
        ):
            await arq_redis.enqueue_job("refresh_clinvar_controls", sample_refresh_clinvar_controls_job_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # The variant should be present in the 2025-01 archive.
        clinical_controls = session.scalars(select(ClinicalControl)).all()
        assert len(clinical_controls) >= 1
        assert all(cc.db_identifier == "3045425" for cc in clinical_controls)

        success_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == AnnotationType.CLINVAR_CONTROL,
                VariantAnnotationStatus.status == AnnotationStatus.SUCCESS,
            )
        ).all()
        assert len(success_annotations) == 1

        # Total annotations == versions processed.
        all_annotations = session.scalars(
            select(VariantAnnotationStatus).where(
                VariantAnnotationStatus.annotation_type == AnnotationType.CLINVAR_CONTROL,
            )
        ).all()
        assert len(all_annotations) == len(_E2E_VERSIONS)

        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.network
@pytest.mark.slow
class TestClinVarSchemaCompatibility:
    async def test_latest_clinvar_archive_has_expected_schema(self):
        """Smoke test against the live ClinVar archive.

        Fetches the most recent archival snapshot and asserts that every field in
        CLINVAR_FIELDS_TO_KEEP is present. Fails fast if ClinVar renames or removes
        a column before a deploy reaches production.
        """
        year, month = generate_clinvar_versions()[-1]
        data = await fetch_clinvar_variant_data(month, year)

        assert len(data) > 0, "ClinVar archive returned no records"

        sample_row = next(iter(data.values()))
        for field in CLINVAR_FIELDS_TO_KEEP:
            assert field in sample_row, f"Expected field {field!r} missing from ClinVar TSV"
