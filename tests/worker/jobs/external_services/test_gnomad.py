# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from unittest.mock import patch

from sqlalchemy import select

from mavedb.lib import gnomad as gnomad_lib
from mavedb.lib.gnomad import GNOMAD_DATA_VERSION
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.worker.jobs.external_services.gnomad import link_gnomad_variants
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.unit
class TestLinkGnomadVariantsUnit:
    """Unit tests for the link_gnomad_variants job."""

    async def test_link_gnomad_variants_no_alleles_with_caids(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
    ):
        """No authoritative alleles with CAIDs -> the job succeeds with nothing to do."""
        result = await link_gnomad_variants(
            mock_worker_ctx,
            1,
            JobManager(session, mock_worker_ctx["redis"], sample_link_gnomad_variants_run.id),
        )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_no_gnomad_matches(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
        athena_engine,
    ):
        """Test linking gnomAD variants when no gnomAD variants match the CAIDs."""

        with (
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                return_value=[],
            ),
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            result = await link_gnomad_variants(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_link_gnomad_variants_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_call_linking_method(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
        athena_engine,
    ):
        """Test that the linking method is called when gnomAD variants match CAIDs."""

        with (
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                return_value=[object()],
            ),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.link_gnomad_variants_to_alleles",
                return_value=set(),
            ) as mock_linking_method,
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            result = await link_gnomad_variants(
                mock_worker_ctx,
                1,
                JobManager(session, mock_worker_ctx["redis"], sample_link_gnomad_variants_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        mock_linking_method.assert_called_once()

    async def test_link_gnomad_variants_propagates_exceptions(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
        athena_engine,
    ):
        """Test that exceptions during the linking process are propagated."""
        with (
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            with pytest.raises(Exception) as exc_info:
                await link_gnomad_variants(
                    mock_worker_ctx,
                    1,
                    JobManager(session, mock_worker_ctx["redis"], sample_link_gnomad_variants_run.id),
                )

        assert str(exc_info.value) == "Test exception"


@pytest.mark.asyncio
@pytest.mark.integration
class TestLinkGnomadVariantsIntegration:
    """Integration tests for the link_gnomad_variants job."""

    async def test_link_gnomad_variants_no_alleles_with_caids(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
    ):
        """Test the end-to-end functionality of the link_gnomad_variants job when no alleles have CAIDs."""

        result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run.id)
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify that no allele links were created
        assert len(session.scalars(select(GnomadAlleleLink)).all()) == 0

        # Verify no annotations were rendered (since there were no alleles with CAIDs)
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_no_matching_caids(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
        athena_engine,
    ):
        """Test the end-to-end functionality of the link_gnomad_variants job when no matching CAIDs are found."""
        # Update the allele to have a CAID that won't match any seeded gnomAD data
        _, allele = setup_sample_alleles_with_caid
        allele.clingen_allele_id = "NON_MATCHING_CAID"
        session.commit()

        # Patch the athena engine to use the mock athena_engine fixture
        with patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine):
            result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run.id)

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify that no allele links were created
        assert len(session.scalars(select(GnomadAlleleLink)).all()) == 0

        # Verify a skipped annotation status was rendered (since there was an allele with a CAID)
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "skipped"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_successful_linking_independent(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
        athena_engine,
    ):
        """Test the end-to-end functionality of the link_gnomad_variants job with successful linking."""
        _, allele = setup_sample_alleles_with_caid

        # Patch the athena engine to use the mock athena_engine fixture
        with patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine):
            result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run.id)

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify that a gnomAD variant was created and a live link to the allele established
        assert len(session.scalars(select(GnomADVariant)).all()) > 0
        live_links = session.scalars(
            select(GnomadAlleleLink).where(
                GnomadAlleleLink.allele_id == allele.id,
                GnomadAlleleLink.current,
            )
        ).all()
        assert len(live_links) == 1

        # Verify annotation status was rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_links_rt_derived_allele_but_annotates_only_authoritative(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_rt_derived_allele_with_caid,
        athena_engine,
    ):
        """gnomAD linkage must cover the full allele set, not just authoritative links: the
        RT-derived allele carries the CAID gnomAD matches and must be linked. Per-variant VAS status,
        however, is written only for the authoritative link (the interim bandaid) — so exactly one
        annotation row is produced, keyed to the variant, never a second row for the RT-derived
        allele."""
        variant, authoritative_allele, rt_allele = setup_rt_derived_allele_with_caid

        with patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine):
            result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run.id)

        assert result.status == JobStatus.SUCCEEDED

        # The RT-derived (non-authoritative) allele IS linked — the core fix.
        rt_links = session.scalars(
            select(GnomadAlleleLink).where(
                GnomadAlleleLink.allele_id == rt_allele.id,
                GnomadAlleleLink.current,
            )
        ).all()
        assert len(rt_links) == 1
        # The authoritative allele's CAID had no gnomAD match, so it gets no link.
        assert (
            len(
                session.scalars(
                    select(GnomadAlleleLink).where(GnomadAlleleLink.allele_id == authoritative_allele.id)
                ).all()
            )
            == 0
        )

        # Annotation status is written only for the authoritative link: exactly one VAS row, for the
        # variant. (Its status is "skipped" because the variant's authoritative allele had no match —
        # cross-level resolution onto the RT-derived allele is deferred to the AnnotationEvent design.)
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].variant_id == variant.id
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

    async def test_link_gnomad_variants_skips_allele_already_current(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
    ):
        """An allele already linked at the current gnomAD version is skipped: no Athena query, the
        status reports SUCCESS/preexisting, and the existing link is not churned."""
        _, allele = setup_sample_alleles_with_caid

        # Simulate a prior run: a live link at the current gnomAD version.
        gnomad_variant = GnomADVariant(
            db_name="gnomAD",
            db_identifier="1-12345-G-A",
            db_version=GNOMAD_DATA_VERSION,
            allele_count=1,
            allele_number=2,
            allele_frequency=0.5,
        )
        session.add(gnomad_variant)
        session.commit()
        session.add(GnomadAlleleLink(allele_id=allele.id, gnomad_variant_id=gnomad_variant.id))
        session.commit()

        with patch("mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids") as fetch_spy:
            result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run.id)

        fetch_spy.assert_not_called()  # version-keyed skip avoided the external query entirely
        assert result.data["preexisting_allele_count"] == 1
        assert result.data["created_allele_count"] == 0

        # Link not churned: still exactly one, still live.
        links = session.scalars(select(GnomadAlleleLink).where(GnomadAlleleLink.allele_id == allele.id)).all()
        assert len(links) == 1
        assert links[0].valid_to is None

        # Status is SUCCESS, marked preexisting.
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_metadata["action"] == "preexisting"

    async def test_link_gnomad_variants_force_refetches_without_churn(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
        athena_engine,
    ):
        """force bypasses the skip and re-fetches, but the linker supersedes only on change, so a
        forced re-run of unchanged data reports preexisting and does not churn the link."""
        _, allele = setup_sample_alleles_with_caid

        # Prior live link pointing at the same variant the Athena mock resolves to (1-12345-G-A).
        gnomad_variant = GnomADVariant(
            db_name="gnomAD",
            db_identifier="1-12345-G-A",
            db_version=GNOMAD_DATA_VERSION,
            allele_count=23,
            allele_number=32432423,
            allele_frequency=23 / 32432423,
        )
        session.add(gnomad_variant)
        session.commit()
        session.add(GnomadAlleleLink(allele_id=allele.id, gnomad_variant_id=gnomad_variant.id))
        session.commit()

        sample_link_gnomad_variants_run.job_params = {**sample_link_gnomad_variants_run.job_params, "force": True}
        session.commit()

        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                side_effect=gnomad_lib.gnomad_variant_data_for_caids,
            ) as fetch_spy,
        ):
            result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run.id)

        fetch_spy.assert_called_once()  # force bypassed the version-keyed skip
        assert result.data["preexisting_allele_count"] == 1
        assert result.data["created_allele_count"] == 0
        # Unchanged → no churn.
        links = session.scalars(select(GnomadAlleleLink).where(GnomadAlleleLink.allele_id == allele.id)).all()
        assert len(links) == 1
        assert links[0].valid_to is None

    async def test_link_gnomad_variants_successful_linking_pipeline(
        self,
        session,
        with_populated_domain_data,
        mock_worker_ctx,
        sample_link_gnomad_variants_run_pipeline,
        sample_link_gnomad_variants_pipeline,
        setup_sample_alleles_with_caid,
        athena_engine,
    ):
        """Test the end-to-end functionality of the link_gnomad_variants job with successful linking in a pipeline."""

        # Patch the athena engine to use the mock athena_engine fixture
        with patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine):
            result = await link_gnomad_variants(mock_worker_ctx, sample_link_gnomad_variants_run_pipeline.id)

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify that allele links were created
        assert len(session.scalars(select(GnomadAlleleLink).where(GnomadAlleleLink.current)).all()) > 0

        # Verify annotation status was rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run_pipeline)
        assert sample_link_gnomad_variants_run_pipeline.status == JobStatus.SUCCEEDED

        # Verify pipeline status updates
        session.refresh(sample_link_gnomad_variants_pipeline)
        assert sample_link_gnomad_variants_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_link_gnomad_variants_exceptions_handled_by_decorators(
        self,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        mock_worker_ctx,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
        athena_engine,
    ):
        """Test that exceptions during the linking process are handled by decorators."""

        # Patch the athena engine to use the mock athena_engine fixture
        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            result = await link_gnomad_variants(
                mock_worker_ctx,
                sample_link_gnomad_variants_run.id,
            )

        mock_send_slack_job_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.ERRORED
        assert isinstance(result.exception, Exception)

        # Verify job status updates
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.ERRORED


@pytest.mark.asyncio
@pytest.mark.integration
class TestLinkGnomadVariantsArqContext:
    """Tests for link_gnomad_variants job using the ARQ context fixture."""

    async def test_link_gnomad_variants_with_arq_context_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        athena_engine,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
    ):
        """Test that the link_gnomad_variants job works with the ARQ context fixture."""

        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            await arq_redis.enqueue_job("link_gnomad_variants", sample_link_gnomad_variants_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that allele links were created
        assert len(session.scalars(select(GnomadAlleleLink).where(GnomadAlleleLink.current)).all()) > 0

        # Verify annotation status was rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify that the job completed successfully
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.SUCCEEDED

    async def test_link_gnomad_variants_with_arq_context_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        athena_engine,
        sample_link_gnomad_variants_run_pipeline,
        sample_link_gnomad_variants_pipeline,
        setup_sample_alleles_with_caid,
    ):
        """Test that the link_gnomad_variants job works with the ARQ context fixture in a pipeline."""

        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
        ):
            await arq_redis.enqueue_job("link_gnomad_variants", sample_link_gnomad_variants_run_pipeline.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that allele links were created
        assert len(session.scalars(select(GnomadAlleleLink).where(GnomadAlleleLink.current)).all()) > 0

        # Verify annotation status was rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].status == "success"
        assert annotation_statuses[0].annotation_type == "gnomad_allele_frequency"

        # Verify that the job completed successfully
        session.refresh(sample_link_gnomad_variants_run_pipeline)
        assert sample_link_gnomad_variants_run_pipeline.status == JobStatus.SUCCEEDED

        # Verify pipeline status updates
        session.refresh(sample_link_gnomad_variants_pipeline)
        assert sample_link_gnomad_variants_pipeline.status == PipelineStatus.SUCCEEDED

    async def test_link_gnomad_variants_with_arq_context_exception_handling_independent(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        with_gnomad_linking_job,
        athena_engine,
        sample_link_gnomad_variants_run,
        setup_sample_alleles_with_caid,
    ):
        """Test that exceptions in the link_gnomad_variants job are handled with the ARQ context fixture."""

        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job("link_gnomad_variants", sample_link_gnomad_variants_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        # Verify that no allele links were created
        assert len(session.scalars(select(GnomadAlleleLink)).all()) == 0

        # Verify no annotations were rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job errored
        session.refresh(sample_link_gnomad_variants_run)
        assert sample_link_gnomad_variants_run.status == JobStatus.ERRORED

    async def test_link_gnomad_variants_with_arq_context_exception_handling_pipeline(
        self,
        arq_redis,
        arq_worker,
        session,
        with_populated_domain_data,
        athena_engine,
        sample_link_gnomad_variants_pipeline,
        sample_link_gnomad_variants_run_pipeline,
        setup_sample_alleles_with_caid,
    ):
        """Test that exceptions in the link_gnomad_variants job are handled with the ARQ context fixture."""

        with (
            patch("mavedb.worker.jobs.external_services.gnomad.athena.engine", athena_engine),
            patch(
                "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
                side_effect=Exception("Test exception"),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job("link_gnomad_variants", sample_link_gnomad_variants_run_pipeline.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        # Verify that no allele links were created
        assert len(session.scalars(select(GnomadAlleleLink)).all()) == 0

        # Verify no annotations were rendered
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job errored
        session.refresh(sample_link_gnomad_variants_run_pipeline)
        assert sample_link_gnomad_variants_run_pipeline.status == JobStatus.ERRORED

        # Verify that the pipeline failed
        session.refresh(sample_link_gnomad_variants_pipeline)
        assert sample_link_gnomad_variants_pipeline.status == PipelineStatus.FAILED
