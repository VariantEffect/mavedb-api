# ruff: noqa: E402

import pytest
import requests

pytest.importorskip("arq")

from unittest.mock import patch

from sqlalchemy import select

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus
from mavedb.worker.jobs.external_services.clinvar import refresh_clinvar_controls
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")

MOCK_CLINVAR_DATA = {
    "VCV000000123": {
        "GeneSymbol": "TEST",
        "ClinicalSignificance": "benign",
        "ReviewStatus": "reviewed by expert panel",
        "VariationID": "987654",
    },
}


@pytest.mark.unit
@pytest.mark.asyncio
class TestRefreshClinvarControlsUnit:
    """Unit tests for the allele-model refresh_clinvar_controls job."""

    @pytest.fixture(autouse=True)
    def _mock_clinvar_versions(self):
        """Pin _generate_clinvar_versions to a single version so clinvar_version == '01_2026'."""
        with patch(
            "mavedb.worker.jobs.external_services.clinvar._generate_clinvar_versions",
            return_value=[(2026, 1)],
        ):
            yield

    async def test_no_alleles_with_caids(
        self,
        mock_worker_ctx,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
    ):
        """No current alleles carry a CAID -> the job succeeds with nothing to do."""
        with patch(
            "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
            return_value=MOCK_CLINVAR_DATA,
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert session.scalars(select(ClinvarControl)).all() == []
        assert session.scalars(select(ClinvarAlleleLink)).all() == []
        assert session.scalars(select(AnnotationEvent)).all() == []

    async def test_fetch_failure_skips_version(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """A version whose TSV fetch fails is logged and skipped, not propagated."""

        async def boom(*args, **kwargs):
            raise Exception("Network error")

        with patch(
            "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
            side_effect=boom,
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["versions_completed"] == 0
        assert session.scalars(select(ClinvarAlleleLink)).all() == []

    async def test_multi_variant_caid_skipped(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """An allele whose CAID is a multi-variant identifier is skipped (ClinVar can't key on it)."""
        _, allele = setup_sample_alleles_with_caid
        allele.clingen_allele_id = "CA-MULTI-001,CA-MULTI-002"
        session.commit()

        with patch(
            "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
            return_value=MOCK_CLINVAR_DATA,
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert session.scalars(select(ClinvarAlleleLink)).all() == []

        # Allele-keyed event: a cis-block CAID structurally can't key ClinVar — a pipeline gap, not a
        # statement about the source.
        event = session.scalars(select(AnnotationEvent)).one()
        assert event.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert event.disposition == Disposition.NOT_APPLICABLE
        assert event.reason == EventReason.MULTI_VARIANT_CAID
        assert event.allele_id == allele.id and event.variant_id is None

    async def test_protein_level_allele_skipped(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """A protein-level allele is never linked to ClinVar (a nucleotide-level DB): its clinical
        calls come from its g./c. siblings. Skipped as not-applicable before any resolution runs."""
        _, allele = setup_sample_alleles_with_caid
        allele.level = "protein"
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ) as resolve_spy,
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=MOCK_CLINVAR_DATA,
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert session.scalars(select(ClinvarAlleleLink)).all() == []
        assert session.scalars(select(ClinvarControl)).all() == []
        # Short-circuited before touching ClinGen — a protein allele can never resolve to a ClinVar id.
        resolve_spy.assert_not_awaited()

        # Allele-keyed not-applicable event: ClinVar is nucleotide-level, so a protein allele is a
        # structural gap (like a multi-variant CAID), not a statement about the source.
        event = session.scalars(select(AnnotationEvent)).one()
        assert event.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert event.disposition == Disposition.NOT_APPLICABLE
        assert event.reason == EventReason.PROTEIN_LEVEL_ALLELE
        assert event.allele_id == allele.id and event.variant_id is None

    async def test_no_associated_clinvar_allele_id_skipped(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """ClinGen returns no ClinVar allele id for the CAID -> absent/no_record event, no link."""
        _, allele = setup_sample_alleles_with_caid

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value=None,
            ),
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=MOCK_CLINVAR_DATA,
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert session.scalars(select(ClinvarAlleleLink)).all() == []
        # ClinGen had no ClinVar AlleleID for this CAID — an informative negative about the source.
        event = session.scalars(select(AnnotationEvent)).one()
        assert event.disposition == Disposition.ABSENT
        assert event.reason == EventReason.NO_RECORD
        assert event.allele_id == allele.id and event.variant_id is None

    async def test_clinvar_data_not_found_skipped(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """The resolved ClinVar allele id is absent from the version's TSV -> absent/no_record, no link."""
        _, allele = setup_sample_alleles_with_caid

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value={"VCV000000999": {"GeneSymbol": "X", "ClinicalSignificance": "y", "ReviewStatus": "z"}},
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert session.scalars(select(ClinvarAlleleLink)).all() == []
        # The CAID resolved to a ClinVar AlleleID, but it's absent from this release's snapshot — a
        # genuine, version-scoped negative.
        event = session.scalars(select(AnnotationEvent)).one()
        assert event.disposition == Disposition.ABSENT
        assert event.reason == EventReason.NO_RECORD
        assert event.allele_id == allele.id and event.variant_id is None

    async def test_clingen_api_failure_fails_when_total(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """A ClinGen API error with no successful links returns FAILED/DEPENDENCY_FAILURE."""
        _, allele = setup_sample_alleles_with_caid

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=requests.exceptions.RequestException("ClinGen API error"),
            ),
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=MOCK_CLINVAR_DATA,
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result.status == JobStatus.FAILED
        assert result.failure_category == FailureCategory.DEPENDENCY_FAILURE
        event = session.scalars(select(AnnotationEvent)).one()
        assert event.disposition == Disposition.FAILED
        assert event.reason == EventReason.API_ERROR
        assert event.allele_id == allele.id and event.variant_id is None

    async def test_successful_link_new_control(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """A resolved + present CAID creates a ClinvarControl, a live link, and a present/created event."""
        _, allele = setup_sample_alleles_with_caid

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=MOCK_CLINVAR_DATA,
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["created_link_count"] == 1

        control = session.scalars(select(ClinvarControl)).one()
        assert control.db_identifier == "VCV000000123"
        assert control.db_version == "01_2026"
        assert control.clinical_significance == "benign"
        # AlleleID stays in db_identifier; the canonical VariationID is captured alongside it.
        assert control.clinvar_variation_id == "987654"

        links = session.scalars(
            select(ClinvarAlleleLink).where(ClinvarAlleleLink.allele_id == allele.id, ClinvarAlleleLink.current)
        ).all()
        assert len(links) == 1
        assert links[0].clinvar_control_id == control.id

        event = session.scalars(select(AnnotationEvent)).one()
        assert event.disposition == Disposition.PRESENT
        assert event.reason == EventReason.CREATED
        assert event.annotation_type == AnnotationType.CLINVAR_CONTROL
        assert event.allele_id == allele.id and event.variant_id is None

    async def test_links_and_annotates_full_allele_set(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_rt_derived_allele_with_caid,
    ):
        """ClinVar linkage covers the full allele set, not just authoritative links: the RT-derived
        allele carries the matching CAID and is linked. Events are now allele-keyed, so the RT-derived
        allele's present status is recorded directly — the limitation the per-variant bandaid had
        (dropping the RT-derived allele's status) is lifted. Each allele gets its own event: present for
        the linked RT allele, absent for the unmatched authoritative one.
        """
        _, authoritative_allele, rt_allele = setup_rt_derived_allele_with_caid
        rt_caid = rt_allele.clingen_allele_id

        async def resolve(caid):
            # Only the RT-derived allele's CAID resolves to a ClinVar id present in the TSV.
            return "VCV000000123" if caid == rt_caid else None

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=resolve,
            ),
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=MOCK_CLINVAR_DATA,
            ),
        ):
            result = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED

        # The RT-derived allele IS linked — the core fix.
        rt_links = session.scalars(
            select(ClinvarAlleleLink).where(ClinvarAlleleLink.allele_id == rt_allele.id, ClinvarAlleleLink.current)
        ).all()
        assert len(rt_links) == 1
        # The authoritative allele's CAID had no ClinVar match, so it is not linked.
        assert (
            session.scalars(
                select(ClinvarAlleleLink).where(ClinvarAlleleLink.allele_id == authoritative_allele.id)
            ).all()
            == []
        )

        # One allele-keyed event per allele (never per-variant): the linked RT allele is present, the
        # unmatched authoritative allele is absent. The variant resolves its derived allele's status
        # through the live links — no variant_id on the events.
        events = session.scalars(select(AnnotationEvent)).all()
        assert all(e.annotation_type == AnnotationType.CLINVAR_CONTROL for e in events)
        assert all(e.variant_id is None for e in events)
        by_allele = {e.allele_id: e for e in events}
        assert by_allele[rt_allele.id].disposition == Disposition.PRESENT
        assert by_allele[rt_allele.id].reason == EventReason.CREATED
        assert by_allele[authoritative_allele.id].disposition == Disposition.ABSENT
        assert by_allele[authoritative_allele.id].reason == EventReason.NO_RECORD

    async def test_idempotent_rerun_skips_and_does_not_duplicate(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """A second run finds the allele already linked at the version, skips the resolution, reports
        preexisting, and creates neither a duplicate control nor a duplicate link."""
        _, allele = setup_sample_alleles_with_caid

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ) as resolve_spy,
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=MOCK_CLINVAR_DATA,
            ),
        ):
            first = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )
            session.commit()
            assert resolve_spy.await_count == 1

            second = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )
            # Second run skipped the resolution entirely (version-keyed skip).
            assert resolve_spy.await_count == 1

        assert first.data["created_link_count"] == 1
        assert second.data["preexisting_link_count"] == 1
        assert second.data["created_link_count"] == 0

        # One control, one live link — no churn.
        assert len(session.scalars(select(ClinvarControl)).all()) == 1
        links = session.scalars(select(ClinvarAlleleLink).where(ClinvarAlleleLink.allele_id == allele.id)).all()
        assert len(links) == 1
        assert links[0].valid_to is None

        # Per-version events: two allele-keyed events (one per run), no current flag. The first run
        # created the link; the second found it preexisting. Latest event (by id) wins.
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.allele_id == allele.id).order_by(AnnotationEvent.id)
        ).all()
        assert len(events) == 2
        assert events[0].reason == EventReason.CREATED
        assert events[1].reason == EventReason.PREEXISTING
        assert all(e.disposition == Disposition.PRESENT for e in events)

    async def test_release_reresolution_supersedes_newest_wins(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """Defensive guard: if an allele re-resolves to a *different* control within the same release
        (should never happen — archival data is immutable), the old live link is superseded
        newest-wins, leaving exactly one live link per (allele, version)."""
        variant, allele = setup_sample_alleles_with_caid
        sample_refresh_clinvar_controls_job_run.job_params = {
            **sample_refresh_clinvar_controls_job_run.job_params,
            "force": True,
        }
        session.commit()

        tsv = {
            "VCV000000123": {
                "GeneSymbol": "A",
                "ClinicalSignificance": "benign",
                "ReviewStatus": "ok",
                "VariationID": "111",
            },
            "VCV000000999": {
                "GeneSymbol": "B",
                "ClinicalSignificance": "pathogenic",
                "ReviewStatus": "ok",
                "VariationID": "222",
            },
        }

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=["VCV000000123", "VCV000000999"],
            ),
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=tsv,
            ),
        ):
            await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )
            session.commit()
            await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        control_a = session.scalars(select(ClinvarControl).where(ClinvarControl.db_identifier == "VCV000000123")).one()
        control_b = session.scalars(select(ClinvarControl).where(ClinvarControl.db_identifier == "VCV000000999")).one()

        all_links = session.scalars(select(ClinvarAlleleLink).where(ClinvarAlleleLink.allele_id == allele.id)).all()
        live_links = [link for link in all_links if link.valid_to is None]

        # Exactly one live link per (allele, version) — the new control — and the old one is retired.
        assert len(live_links) == 1
        assert live_links[0].clinvar_control_id == control_b.id
        retired = [link for link in all_links if link.clinvar_control_id == control_a.id]
        assert len(retired) == 1
        assert retired[0].valid_to is not None
        # Gap-free handoff: the retired link's valid_to equals the successor's valid_from.
        assert retired[0].valid_to == live_links[0].valid_from
        # Stamped from the DB clock (func.now()), so the boundary is timezone-aware and comparable to
        # every other func.now()-stamped valid-time row — a regression to a naive datetime.now() would
        # land here as a tz-naive value.
        assert live_links[0].valid_from.tzinfo is not None
        assert retired[0].valid_to.tzinfo is not None

    async def test_force_reresolves_without_duplicating_link(
        self,
        mock_worker_ctx,
        session,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """force bypasses the version-keyed skip and re-resolves, but the get-or-create link write
        does not duplicate an existing live link."""
        variant, allele = setup_sample_alleles_with_caid
        sample_refresh_clinvar_controls_job_run.job_params = {
            **sample_refresh_clinvar_controls_job_run.job_params,
            "force": True,
        }
        session.commit()

        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ) as resolve_spy,
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=MOCK_CLINVAR_DATA,
            ),
        ):
            await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )
            session.commit()
            second = await refresh_clinvar_controls(
                mock_worker_ctx,
                sample_refresh_clinvar_controls_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_refresh_clinvar_controls_job_run.id),
            )

        # force re-resolved on the second run despite an existing link.
        assert resolve_spy.await_count == 2
        assert second.data["preexisting_link_count"] == 1
        assert second.data["created_link_count"] == 0
        links = session.scalars(select(ClinvarAlleleLink).where(ClinvarAlleleLink.allele_id == allele.id)).all()
        assert len(links) == 1
        assert links[0].valid_to is None


@pytest.mark.integration
@pytest.mark.asyncio
class TestRefreshClinvarControlsArqContext:
    """End-to-end tests for refresh_clinvar_controls within an ARQ worker context."""

    @pytest.fixture(autouse=True)
    def _mock_clinvar_versions(self):
        with patch(
            "mavedb.worker.jobs.external_services.clinvar._generate_clinvar_versions",
            return_value=[(2026, 1)],
        ):
            yield

    async def test_arq_context_successful_link(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """The job links an allele and records a present event under an ARQ worker."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                return_value="VCV000000123",
            ),
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=MOCK_CLINVAR_DATA,
            ),
        ):
            await arq_redis.enqueue_job("refresh_clinvar_controls", sample_refresh_clinvar_controls_job_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        assert len(session.scalars(select(ClinvarControl)).all()) >= 1
        assert len(session.scalars(select(ClinvarAlleleLink).where(ClinvarAlleleLink.current)).all()) == 1

        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 1
        assert events[0].disposition == Disposition.PRESENT
        assert events[0].annotation_type == AnnotationType.CLINVAR_CONTROL
        assert events[0].allele_id is not None and events[0].variant_id is None

        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.SUCCEEDED

    async def test_arq_context_exception_handling(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_refresh_clinvar_controls_job,
        sample_refresh_clinvar_controls_job_run,
        setup_sample_alleles_with_caid,
    ):
        """An unexpected error during resolution is caught by the decorators and the job errors."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.clinvar.get_associated_clinvar_allele_id",
                side_effect=ValueError("Unexpected error"),
            ),
            patch(
                "mavedb.worker.jobs.external_services.clinvar.fetch_clinvar_variant_data",
                return_value=MOCK_CLINVAR_DATA,
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_slack,
        ):
            await arq_redis.enqueue_job("refresh_clinvar_controls", sample_refresh_clinvar_controls_job_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_slack.assert_called_once()
        assert session.scalars(select(ClinvarAlleleLink)).all() == []
        assert session.scalars(select(AnnotationEvent)).all() == []

        session.refresh(sample_refresh_clinvar_controls_job_run)
        assert sample_refresh_clinvar_controls_job_run.status == JobStatus.ERRORED
