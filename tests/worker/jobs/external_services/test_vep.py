# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from datetime import date, timedelta
from unittest.mock import patch

from sqlalchemy import select

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.vep_allele_consequence import VepAlleleConsequence
from mavedb.lib.vep import VepResolution
from mavedb.worker.jobs.external_services.vep import populate_vep_for_score_set
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")

_RESOLVE = "mavedb.worker.jobs.external_services.vep._resolve_consequences"
_RELEASE = "mavedb.worker.jobs.external_services.vep.get_ensembl_release"
_ENSEMBL_RELEASE = "116"


@pytest.fixture(autouse=True)
def mock_ensembl_release():
    """Stamp every job run with a fixed Ensembl release so tests version-key deterministically without
    hitting /info/software. Tests exercising a release-fetch failure override this with an inner patch."""
    with patch(_RELEASE, return_value=_ENSEMBL_RELEASE):
        yield


def _live_consequences_for(session, allele_id):
    return session.scalars(
        select(VepAlleleConsequence).where(
            VepAlleleConsequence.allele_id == allele_id,
            VepAlleleConsequence.current,
        )
    ).all()


@pytest.mark.asyncio
@pytest.mark.unit
class TestPopulateVepForScoreSetUnit:
    """Unit tests for the populate_vep_for_score_set job."""

    async def test_no_alleles_with_hgvs(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
    ):
        """No alleles with HGVS -> the job succeeds with nothing to do."""
        result = await populate_vep_for_score_set(
            mock_worker_ctx,
            1,
            JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
        )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["created_allele_count"] == 0
        assert result.data["preexisting_allele_count"] == 0
        assert result.data["absent_allele_count"] == 0
        assert result.data["errored_allele_count"] == 0

    async def test_calls_resolver_when_alleles_present(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """The VEP resolver is invoked once when the score set has HGVS-bearing alleles to query."""
        with patch(_RESOLVE, return_value=VepResolution({}, set())) as mock_resolve:
            result = await populate_vep_for_score_set(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        mock_resolve.assert_called_once()

    async def test_propagates_exceptions(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """Exceptions raised while resolving consequences are propagated."""
        with patch(_RESOLVE, side_effect=Exception("Test exception")):
            with pytest.raises(Exception) as exc_info:
                await populate_vep_for_score_set(
                    mock_worker_ctx,
                    1,
                    JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
                )

        assert str(exc_info.value) == "Test exception"

    async def test_aborts_when_release_unavailable(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """If the Ensembl release cannot be fetched the job aborts rather than mis-versioning its writes
        — the version is load-bearing for the skip, so a failure must propagate, not be swallowed."""
        with (
            patch(_RELEASE, side_effect=Exception("info/software unavailable")),
            patch(_RESOLVE) as mock_resolve,
        ):
            with pytest.raises(Exception) as exc_info:
                await populate_vep_for_score_set(
                    mock_worker_ctx,
                    1,
                    JobManager(session, mock_worker_ctx["redis"], sample_populate_vep_run.id),
                )

        assert str(exc_info.value) == "info/software unavailable"
        mock_resolve.assert_not_called()  # never queried VEP without a version to stamp


@pytest.mark.asyncio
@pytest.mark.integration
class TestPopulateVepForScoreSetIntegration:
    """Integration tests for the populate_vep_for_score_set job."""

    async def test_no_alleles_with_hgvs(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
    ):
        """End-to-end: no alleles -> no consequence rows, no annotations, job succeeds."""
        result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        assert len(session.scalars(select(VepAlleleConsequence)).all()) == 0
        assert len(session.scalars(select(AnnotationEvent)).all()) == 0

        session.refresh(sample_populate_vep_run)
        assert sample_populate_vep_run.status == JobStatus.SUCCEEDED

    async def test_no_consequence_resolved(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """A genuine empty (VEP queried successfully, found nothing) writes no consequence row and an
        ABSENT/no_record event — a trustworthy negative, now that empty is split from a failed request."""
        _, allele = setup_sample_alleles_for_vep

        with patch(_RESOLVE, return_value=VepResolution({}, set())):
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["absent_allele_count"] == 1
        assert len(_live_consequences_for(session, allele.id)) == 0

        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 1
        assert events[0].disposition == "absent"
        assert events[0].reason == "no_record"
        assert events[0].annotation_type == "vep_functional_consequence"
        assert events[0].allele_id == allele.id and events[0].variant_id is None

    async def test_request_failure_recorded_as_errored(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """An allele whose VEP/Recoder request *failed* is FAILED/api_error — distinct from a genuine
        empty — so a transient outage is never mistaken for a confirmed absence."""
        _, allele = setup_sample_alleles_for_vep

        # Resolver reports the allele's HGVS as errored (request failed), not as a hit or an empty.
        with patch(_RESOLVE, return_value=VepResolution({}, {allele.hgvs_c})):
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["errored_allele_count"] == 1
        assert result.data["absent_allele_count"] == 0
        assert len(_live_consequences_for(session, allele.id)) == 0

        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 1
        assert events[0].disposition == "failed"
        assert events[0].reason == "api_error"
        assert events[0].annotation_type == "vep_functional_consequence"
        assert events[0].allele_id == allele.id and events[0].variant_id is None

    async def test_successful_linking_independent(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """A resolved consequence creates a single live VepAlleleConsequence and a SUCCESS VAS row."""
        _, allele = setup_sample_alleles_for_vep

        with patch(_RESOLVE, return_value=VepResolution({allele.hgvs_c: "missense_variant"}, set())):
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["created_allele_count"] == 1

        live = _live_consequences_for(session, allele.id)
        assert len(live) == 1
        assert live[0].functional_consequence == "missense_variant"
        assert live[0].source_version == _ENSEMBL_RELEASE
        assert live[0].access_date == date.today()

        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 1
        assert events[0].disposition == "present"
        assert events[0].reason == "created"
        assert events[0].annotation_type == "vep_functional_consequence"
        assert events[0].allele_id == allele.id and events[0].variant_id is None

    async def test_links_and_annotates_rt_derived_allele(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_rt_derived_allele_for_vep,
    ):
        """VEP linkage covers the full allele set, not just authoritative links: the RT-derived allele's
        genomic HGVS resolves and gets a consequence row. Events are allele-keyed, so each allele in the
        score set gets its own event — present for the resolved RT allele, absent/no_record for the
        authoritative allele VEP queried but found nothing (a genuine empty, distinct from a request
        failure). The per-variant bandaid's limitation (dropping the RT-derived allele's status) is lifted."""
        variant, authoritative_allele, rt_allele = setup_rt_derived_allele_for_vep

        # VEP resolves only the RT-derived allele's genomic HGVS; the authoritative allele's coding
        # HGVS is queried successfully but yields no consequence this run (a genuine empty, not errored).
        with patch(_RESOLVE, return_value=VepResolution({rt_allele.hgvs_g: "missense_variant"}, set())):
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)

        assert result.status == JobStatus.SUCCEEDED

        # The RT-derived (non-authoritative) allele IS linked — the core fix.
        rt_live = _live_consequences_for(session, rt_allele.id)
        assert len(rt_live) == 1
        assert rt_live[0].functional_consequence == "missense_variant"
        # The authoritative allele's HGVS had no consequence, so it gets no row.
        assert len(_live_consequences_for(session, authoritative_allele.id)) == 0

        # One allele-keyed event per allele (never per-variant): the resolved RT allele is present, the
        # unclassifiable authoritative allele is absent/no_record (queried, genuinely empty).
        events = session.scalars(select(AnnotationEvent)).all()
        assert all(e.annotation_type == "vep_functional_consequence" for e in events)
        assert all(e.variant_id is None for e in events)
        by_allele = {e.allele_id: e for e in events}
        assert by_allele[rt_allele.id].disposition == "present"
        assert by_allele[authoritative_allele.id].disposition == "absent"
        assert by_allele[authoritative_allele.id].reason == "no_record"

    async def test_skips_allele_already_at_current_release(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """An allele with a live consequence already at the current Ensembl release is skipped: no VEP
        query, the status reports SUCCESS/preexisting, and the existing row is not churned."""
        _, allele = setup_sample_alleles_for_vep

        # Simulate a prior run at the current release.
        session.add(
            VepAlleleConsequence(
                allele_id=allele.id,
                functional_consequence="missense_variant",
                source_version=_ENSEMBL_RELEASE,
                access_date=date.today(),
            )
        )
        session.commit()

        with patch(_RESOLVE) as mock_resolve:
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)

        mock_resolve.assert_not_called()  # version-keyed skip avoided the external query entirely
        assert result.data["preexisting_allele_count"] == 1
        assert result.data["created_allele_count"] == 0

        # Row not churned: still exactly one, still live.
        rows = session.scalars(select(VepAlleleConsequence).where(VepAlleleConsequence.allele_id == allele.id)).all()
        assert len(rows) == 1
        assert rows[0].valid_to is None

        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 1
        assert events[0].disposition == "present"
        assert events[0].reason == "preexisting"

    async def test_new_release_same_consequence_bumps_in_place(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """An allele live at an older release is re-queried; an unchanged consequence advances
        source_version (and access_date) in place — no supersede, so a new release does not churn
        history for a categorical value that did not change."""
        _, allele = setup_sample_alleles_for_vep

        session.add(
            VepAlleleConsequence(
                allele_id=allele.id,
                functional_consequence="missense_variant",
                source_version="115",
                access_date=date.today() - timedelta(days=90),
            )
        )
        session.commit()

        with patch(_RESOLVE, return_value=VepResolution({allele.hgvs_c: "missense_variant"}, set())) as mock_resolve:
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)

        mock_resolve.assert_called_once()  # older release -> not skipped, re-queried
        assert result.data["preexisting_allele_count"] == 1
        assert result.data["created_allele_count"] == 0

        # Unchanged value -> no supersede: one row, still live, version + date advanced in place.
        rows = session.scalars(select(VepAlleleConsequence).where(VepAlleleConsequence.allele_id == allele.id)).all()
        assert len(rows) == 1
        assert rows[0].valid_to is None
        assert rows[0].source_version == _ENSEMBL_RELEASE
        assert rows[0].access_date == date.today()

    async def test_force_requeries_unchanged_without_churn(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """force bypasses the current-release skip and re-queries, but the linker only supersedes on a
        value change: a forced re-run that resolves the same consequence reports preexisting and does
        not churn the row (version/access_date advanced in place)."""
        _, allele = setup_sample_alleles_for_vep

        # Prior live consequence already at the current release (would be skipped without force).
        session.add(
            VepAlleleConsequence(
                allele_id=allele.id,
                functional_consequence="missense_variant",
                source_version=_ENSEMBL_RELEASE,
                access_date=date.today() - timedelta(days=1),
            )
        )
        session.commit()

        sample_populate_vep_run.job_params = {**sample_populate_vep_run.job_params, "force": True}
        session.commit()

        with patch(_RESOLVE, return_value=VepResolution({allele.hgvs_c: "missense_variant"}, set())) as mock_resolve:
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)

        mock_resolve.assert_called_once()  # force bypassed the current-release skip
        assert result.data["preexisting_allele_count"] == 1
        assert result.data["created_allele_count"] == 0

        # Unchanged consequence -> no supersede: one row, still live, access_date touched in place.
        rows = session.scalars(select(VepAlleleConsequence).where(VepAlleleConsequence.allele_id == allele.id)).all()
        assert len(rows) == 1
        assert rows[0].valid_to is None
        assert rows[0].access_date == date.today()

    async def test_new_release_changed_consequence_supersedes(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """An allele live at an older release is re-queried; a changed result supersedes it — one live
        row carrying the new consequence/release and one retired row preserving the old."""
        _, allele = setup_sample_alleles_for_vep

        # Prior live consequence at an older release -> not skipped, eligible for re-query.
        session.add(
            VepAlleleConsequence(
                allele_id=allele.id,
                functional_consequence="synonymous_variant",
                source_version="115",
                access_date=date.today() - timedelta(days=90),
            )
        )
        session.commit()

        with patch(_RESOLVE, return_value=VepResolution({allele.hgvs_c: "missense_variant"}, set())):
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)

        assert result.data["created_allele_count"] == 1

        live = _live_consequences_for(session, allele.id)
        assert len(live) == 1
        assert live[0].functional_consequence == "missense_variant"
        assert live[0].source_version == _ENSEMBL_RELEASE

        # Old consequence retired, not deleted.
        all_rows = session.scalars(
            select(VepAlleleConsequence).where(VepAlleleConsequence.allele_id == allele.id)
        ).all()
        assert len(all_rows) == 2
        assert len([r for r in all_rows if r.valid_to is not None]) == 1

    async def test_successful_linking_pipeline(
        self,
        session,
        with_populated_domain_data,
        mock_worker_ctx,
        sample_populate_vep_run_pipeline,
        sample_populate_vep_pipeline,
        setup_sample_alleles_for_vep,
    ):
        """End-to-end successful linking within a pipeline updates both job and pipeline status."""
        _, allele = setup_sample_alleles_for_vep

        with patch(_RESOLVE, return_value=VepResolution({allele.hgvs_c: "missense_variant"}, set())):
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run_pipeline.id)

        assert result.status == JobStatus.SUCCEEDED
        assert len(session.scalars(select(VepAlleleConsequence).where(VepAlleleConsequence.current)).all()) == 1

        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 1
        assert events[0].disposition == "present"
        assert events[0].reason == "created"
        assert events[0].annotation_type == "vep_functional_consequence"
        assert events[0].allele_id is not None and events[0].variant_id is None

        session.refresh(sample_populate_vep_run_pipeline)
        assert sample_populate_vep_run_pipeline.status == JobStatus.SUCCEEDED
        session.refresh(sample_populate_vep_pipeline)
        assert sample_populate_vep_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_exceptions_handled_by_decorators(
        self,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        mock_worker_ctx,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """Exceptions during resolution are handled by the job decorators (Slack alert, ERRORED)."""
        with (
            patch(_RESOLVE, side_effect=Exception("Test exception")),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            result = await populate_vep_for_score_set(mock_worker_ctx, sample_populate_vep_run.id)

        mock_send_slack_job_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.ERRORED
        assert isinstance(result.exception, Exception)

        session.refresh(sample_populate_vep_run)
        assert sample_populate_vep_run.status == JobStatus.ERRORED


@pytest.mark.asyncio
@pytest.mark.integration
class TestPopulateVepForScoreSetArqContext:
    """Tests for the populate_vep_for_score_set job using the ARQ context fixture."""

    async def test_populate_vep_with_arq_context_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """The VEP job links a consequence and records a SUCCESS annotation through the ARQ worker."""
        _, allele = setup_sample_alleles_for_vep

        with patch(_RESOLVE, return_value=VepResolution({allele.hgvs_c: "missense_variant"}, set())):
            await arq_redis.enqueue_job("populate_vep_for_score_set", sample_populate_vep_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        assert len(session.scalars(select(VepAlleleConsequence).where(VepAlleleConsequence.current)).all()) == 1

        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 1
        assert events[0].disposition == "present"
        assert events[0].reason == "created"
        assert events[0].annotation_type == "vep_functional_consequence"
        assert events[0].allele_id is not None and events[0].variant_id is None

        session.refresh(sample_populate_vep_run)
        assert sample_populate_vep_run.status == JobStatus.SUCCEEDED

    async def test_populate_vep_with_arq_context_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        sample_populate_vep_run_pipeline,
        sample_populate_vep_pipeline,
        setup_sample_alleles_for_vep,
    ):
        """The VEP job completes and advances the pipeline through the ARQ worker."""
        _, allele = setup_sample_alleles_for_vep

        with patch(_RESOLVE, return_value=VepResolution({allele.hgvs_c: "missense_variant"}, set())):
            await arq_redis.enqueue_job("populate_vep_for_score_set", sample_populate_vep_run_pipeline.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        assert len(session.scalars(select(VepAlleleConsequence).where(VepAlleleConsequence.current)).all()) == 1

        session.refresh(sample_populate_vep_run_pipeline)
        assert sample_populate_vep_run_pipeline.status == JobStatus.SUCCEEDED
        session.refresh(sample_populate_vep_pipeline)
        assert sample_populate_vep_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_populate_vep_with_arq_context_exception_handling_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_populate_vep_job,
        sample_populate_vep_run,
        setup_sample_alleles_for_vep,
    ):
        """Exceptions in the VEP job are handled with the ARQ context fixture."""
        with (
            patch(_RESOLVE, side_effect=Exception("Test exception")),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job("populate_vep_for_score_set", sample_populate_vep_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        assert len(session.scalars(select(VepAlleleConsequence)).all()) == 0
        assert len(session.scalars(select(AnnotationEvent)).all()) == 0

        session.refresh(sample_populate_vep_run)
        assert sample_populate_vep_run.status == JobStatus.ERRORED

    async def test_populate_vep_with_arq_context_exception_handling_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        sample_populate_vep_pipeline,
        sample_populate_vep_run_pipeline,
        setup_sample_alleles_for_vep,
    ):
        """Exceptions in the VEP job fail the pipeline with the ARQ context fixture."""
        with (
            patch(_RESOLVE, side_effect=Exception("Test exception")),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job("populate_vep_for_score_set", sample_populate_vep_run_pipeline.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        assert len(session.scalars(select(VepAlleleConsequence)).all()) == 0
        assert len(session.scalars(select(AnnotationEvent)).all()) == 0

        session.refresh(sample_populate_vep_run_pipeline)
        assert sample_populate_vep_run_pipeline.status == JobStatus.ERRORED
        session.refresh(sample_populate_vep_pipeline)
        assert sample_populate_vep_pipeline.status == PipelineStatus.FAILED
