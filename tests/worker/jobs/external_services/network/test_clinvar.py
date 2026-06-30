# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from unittest.mock import AsyncMock, patch

from sqlalchemy import select

from mavedb.lib.clinvar.constants import CLINVAR_FIELDS_TO_KEEP
from mavedb.lib.clinvar.utils import fetch_clinvar_variant_data
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.worker.jobs.external_services.clinvar import _generate_clinvar_versions

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
        setup_sample_alleles_with_caid,
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

        # Verify that clinical controls were added successfully — one row per ClinVar version
        # that contains the variant, so there may be more than one.
        clinical_controls = session.scalars(select(ClinvarControl)).all()
        assert len(clinical_controls) >= 1
        assert all(cc.db_identifier == "3045425" for cc in clinical_controls)

        # Verify that at least one present event was recorded for the allele. The job processes one
        # event per ClinVar version; versions without the allele produce absent events, so filtering
        # for present gives a stable assertion. Events are allele-keyed (no variant_id).
        present_events = session.scalars(
            select(AnnotationEvent).where(
                AnnotationEvent.annotation_type == AnnotationType.CLINVAR_CONTROL,
                AnnotationEvent.disposition == Disposition.PRESENT,
            )
        ).all()
        assert len(present_events) >= 1
        assert all(e.variant_id is None and e.allele_id is not None for e in present_events)

        # Versions where the allele's resolved ClinVar id is absent from that release's snapshot
        # produce an absent event — expected for any version that doesn't contain it.
        absent_events = session.scalars(
            select(AnnotationEvent).where(
                AnnotationEvent.annotation_type == AnnotationType.CLINVAR_CONTROL,
                AnnotationEvent.disposition == Disposition.ABSENT,
            )
        ).all()
        assert len(absent_events) >= 1

        # Total events should equal the number of ClinVar versions processed (one allele, one per version).
        assert len(present_events) + len(absent_events) == len(_generate_clinvar_versions())

        # Verify that the job run was completed successfully
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
        year, month = _generate_clinvar_versions()[-1]
        data = await fetch_clinvar_variant_data(month, year)

        assert len(data) > 0, "ClinVar archive returned no records"

        sample_row = next(iter(data.values()))
        for field in CLINVAR_FIELDS_TO_KEEP:
            assert field in sample_row, f"Expected field {field!r} missing from ClinVar TSV"
