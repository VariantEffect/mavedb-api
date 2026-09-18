# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from asyncio.unix_events import _UnixSelectorEventLoop
from copy import deepcopy
from unittest.mock import patch

from sqlalchemy import select

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.models.allele import Allele
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant
from mavedb.models.annotation_event import AnnotationEvent
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

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

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

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

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

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

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

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # 4 variants dedup to 1 allele → one allele-keyed event, failed (no CAR response).
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "failed"
            assert event.allele_id is not None

    async def test_submit_score_set_mappings_to_car_all_car_errors(
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

        # All 4 variants share 1 allele (same VRS digest). Build an error response for that 1 HGVS.
        alleles = session.scalars(select(Allele)).all()
        assert len(alleles) == 1
        registered_alleles_mock = [
            {
                "errorType": "InvalidHGVS",
                "hgvs": get_hgvs_from_post_mapped(alleles[0].post_mapped) or "",
                "message": "Invalid HGVS expression.",
                "description": "",
                "inputLine": get_hgvs_from_post_mapped(alleles[0].post_mapped) or "",
                "position": "0",
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
        assert result.status == JobStatus.FAILED

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # 1 allele, rejected by CAR → one failed event (reason=service_rejected).
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "failed"
            assert event.reason == "service_rejected"

    async def test_submit_score_set_mappings_to_car_event_per_allele(
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
        """Each allele — authoritative and derived — gets exactly one allele-keyed event; there is no
        per-variant fan-out. Both alleles are registered with a CAID."""
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # The sample's 4 variants dedup to one authoritative allele.
        authoritative_allele = session.scalars(select(Allele)).one()

        # Attach a second, derived (is_authoritative=False) allele to one variant's current mapping
        # record. It shares the authoritative allele's HGVS (different vrs_digest) so it is submitted
        # under the same line — the realistic shape is a different level, but identical HGVS is enough
        # to exercise the multi-allele-per-variant fan-out.
        a_link = session.scalars(
            select(MappingRecordAllele).where(MappingRecordAllele.allele_id == authoritative_allele.id)
        ).first()
        derived_allele = Allele(
            vrs_digest=f"{authoritative_allele.vrs_digest}-derived",
            level=authoritative_allele.level,
            post_mapped={**deepcopy(authoritative_allele.post_mapped), "id": "derived-allele-id"},
        )
        session.add(derived_allele)
        session.flush()
        session.add(
            MappingRecordAllele(
                mapping_record_id=a_link.mapping_record_id,
                allele_id=derived_allele.id,
                is_authoritative=False,
            )
        )
        session.flush()

        def fake_dispatch(hgvs_list):
            return [{"@id": f"CA{idx}", "type": "nucleotide", "genomicAlleles": []} for idx, _ in enumerate(hgvs_list)]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                side_effect=fake_dispatch,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED

        # One event per allele (2 alleles → 2 events), each keyed on its allele, not fanned per-variant.
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 2
        assert all(e.variant_id is None for e in events)
        assert {e.allele_id for e in events} == {authoritative_allele.id, derived_allele.id}
        assert all(e.disposition == "present" for e in events)

        # Both alleles registered: registration breadth covers the derived allele too.
        session.refresh(authoritative_allele)
        session.refresh(derived_allele)
        assert authoritative_allele.clingen_allele_id is not None
        assert derived_allele.clingen_allele_id is not None

    async def test_submit_score_set_mappings_to_car_response_count_mismatch(
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

        # All 4 variants share 1 allele → 1 submitted HGVS, but CAR returns 2 results. The count
        # violates the one-result-per-input contract, so positional alignment can't be trusted
        # and the whole batch must be rejected without writing any CAID.
        alleles = session.scalars(select(Allele)).all()
        assert len(alleles) == 1
        registered_alleles_mock = [
            {"@id": "CA111111", "type": "nucleotide", "genomicAlleles": []},
            {"@id": "CA222222", "type": "nucleotide", "genomicAlleles": []},
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

        # No CAID written — neither of the returned values is trusted.
        assert len(session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()) == 0
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "failed"
            assert event.reason == "api_error"

    async def test_submit_score_set_mappings_to_car_malformed_response(
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

        # CAR returns a response that is neither an error (no "errorType") nor a registration
        # (no "@id"). This must be surfaced as a rejection, not crash the loop with a KeyError.
        alleles = session.scalars(select(Allele)).all()
        assert len(alleles) == 1
        registered_alleles_mock = [{"type": "nucleotide", "genomicAlleles": []}]

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

        # No CAID assigned; the one allele's event is failed (reason=malformed_response).
        assert len(session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()) == 0
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "failed"
            assert event.reason == "malformed_response"

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

        # 4 variants share 1 allele; CAR returns 1 response for the 1 submitted HGVS
        alleles = session.scalars(select(Allele)).all()
        assert len(alleles) == 1
        registered_alleles_mock = [
            {
                "@id": "CA_DUPLICATE",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(alleles[0].post_mapped)}],
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

        # 1 allele received the CAID
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 1
        assert alleles[0].clingen_allele_id == "CA_DUPLICATE"

        # 1 allele → one present event.
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "present"
            assert event.allele_id is not None

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
        """All 4 variants share 1 allele; CAR returns 1 success → 1 registered allele, 0 failed."""
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

        alleles = session.scalars(select(Allele)).all()
        assert len(alleles) == 1

        registered_alleles_mock = [
            {
                "@id": f"CA{alleles[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(alleles[0].post_mapped)}],
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
        assert result.data["registered_allele_count"] == 1
        assert result.data["failed_allele_count"] == 0

        # 1 allele got a CAID
        alleles_with_caid = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles_with_caid) == 1
        assert alleles_with_caid[0].clingen_allele_id == f"CA{alleles[0].id}"

        # All 4 variant annotations succeeded
        present_events = session.scalars(
            select(AnnotationEvent).where(
                AnnotationEvent.annotation_type == "clingen_allele_id",
                AnnotationEvent.disposition == "present",
            )
        ).all()
        assert len(present_events) == 1

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

        # Patch get_hgvs_from_post_mapped to return None for all alleles
        with (
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch("mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped", return_value=None),
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # Verify annotation statuses were rendered as failed — 4 variants, all failed
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "not_applicable"
            assert event.reason == "no_hgvs"

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

        # All 4 variants share 1 allele (same VRS digest from construct_mock_mapping_output)
        alleles = session.scalars(select(Allele)).all()
        assert len(alleles) == 1

        registered_alleles_mock = [
            {
                "@id": f"CA{alleles[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(alleles[0].post_mapped)}],
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

        # 1 allele received the CAID
        alleles_with_caid = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles_with_caid) == 1
        assert alleles_with_caid[0].clingen_allele_id == f"CA{alleles[0].id}"

        # 4 per-variant annotations — all success
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "present"
            assert event.allele_id is not None

    async def test_submit_score_set_mappings_to_car_preexisting(
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
        """Already-registered allele is re-annotated as preexisting, not re-submitted."""
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        # Pre-set the CAID on the allele to simulate prior registration
        allele = session.scalars(select(Allele)).first()
        allele.clingen_allele_id = "CA_PRIOR"
        session.flush()

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
            ) as mock_dispatch,
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        # CAR should NOT be called since the allele is already registered
        mock_dispatch.assert_not_called()
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["already_registered_allele_count"] == 1
        assert result.data["submitted_allele_count"] == 0

        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "present"
            assert event.reason == "preexisting"
            assert event.event_metadata["clingen_allele_id"] == "CA_PRIOR"

    async def test_submit_score_set_mappings_to_car_force_reregister_same_caid(
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
        """Force re-registration that returns the same CAID is a success with registration_source=reconfirmed."""
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        allele = session.scalars(select(Allele)).first()
        allele.clingen_allele_id = "CA_CONFIRMED"
        session.flush()

        submit_score_set_mappings_to_car_sample_job_run.job_params = {
            **submit_score_set_mappings_to_car_sample_job_run.job_params,
            "force_reregister": True,
        }
        session.flush()

        registered_alleles_mock = [{"@id": "CA_CONFIRMED", "type": "nucleotide", "genomicAlleles": []}]

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

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["registered_allele_count"] == 1
        assert result.data["submitted_allele_count"] == 1

        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "present"
            assert event.reason == "reconfirmed"

    async def test_submit_score_set_mappings_to_car_force_reregister_caid_conflict(
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
        """Force re-registration returning a different CAID fails without overwriting the stored CAID."""
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )

        allele = session.scalars(select(Allele)).first()
        allele.clingen_allele_id = "CA_STORED"
        session.flush()

        submit_score_set_mappings_to_car_sample_job_run.job_params = {
            **submit_score_set_mappings_to_car_sample_job_run.job_params,
            "force_reregister": True,
        }
        session.flush()

        # CAR returns a DIFFERENT CAID — this is an invariant violation
        registered_alleles_mock = [{"@id": "CA_DIFFERENT", "type": "nucleotide", "genomicAlleles": []}]

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

        # Job fails because all CAID-returning submissions had a conflict
        assert result.status == JobStatus.FAILED

        # CAID must NOT have been overwritten
        session.refresh(allele)
        assert allele.clingen_allele_id == "CA_STORED"

        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "failed"
            assert event.reason == "caid_conflict"
            assert event.event_metadata["clingen_allele_id"] == "CA_STORED"
            assert event.event_metadata["conflicting_caid"] == "CA_DIFFERENT"

    async def _add_derived_alleles_with_distinct_hgvs(self, session, count):
        """Attach ``count`` extra alleles to the score set, each carrying a distinct HGVS so the CAR
        job produces ``count + 1`` distinct submission lines (one per HGVS). Returns every allele."""
        authoritative_allele = session.scalars(select(Allele)).one()
        a_link = session.scalars(
            select(MappingRecordAllele).where(MappingRecordAllele.allele_id == authoritative_allele.id)
        ).first()

        alleles = [authoritative_allele]
        for i in range(count):
            post_mapped = deepcopy(authoritative_allele.post_mapped)
            post_mapped["expressions"][0]["value"] = f"NC_000001.11:g.{1000 + i}A>T"
            derived = Allele(
                vrs_digest=f"{authoritative_allele.vrs_digest}-d{i}",
                level=authoritative_allele.level,
                post_mapped=post_mapped,
            )
            session.add(derived)
            session.flush()
            session.add(
                MappingRecordAllele(
                    mapping_record_id=a_link.mapping_record_id,
                    allele_id=derived.id,
                    is_authoritative=False,
                )
            )
            alleles.append(derived)

        session.flush()
        return alleles

    async def test_submit_score_set_mappings_to_car_batches_submissions(
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
        """A large allele set is dispatched in fixed-size chunks rather than one PUT: 4 distinct HGVS
        with a batch size of 2 yields two dispatch calls, and every allele is still registered."""
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )
        alleles = await self._add_derived_alleles_with_distinct_hgvs(session, count=3)

        dispatched_batches = []

        def fake_dispatch(hgvs_list):
            dispatched_batches.append(list(hgvs_list))
            return [{"@id": f"CA/{hgvs}", "type": "nucleotide", "genomicAlleles": []} for hgvs in hgvs_list]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                side_effect=fake_dispatch,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.DEFAULT_CAR_SUBMISSION_BATCH_SIZE", 2),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED

        # 4 distinct HGVS at batch size 2 → two dispatch calls, neither exceeding the batch size, and
        # together covering every submitted line exactly once.
        assert len(dispatched_batches) == 2
        assert all(len(batch) <= 2 for batch in dispatched_batches)
        assert sum(len(batch) for batch in dispatched_batches) == 4

        # Every allele across both batches is registered.
        for allele in alleles:
            session.refresh(allele)
            assert allele.clingen_allele_id is not None

        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 4
        assert all(event.disposition == "present" for event in events)

    async def test_submit_score_set_mappings_to_car_batch_failure_isolated(
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
        """A failed batch fails only its own alleles: the first chunk returns nothing (request failure)
        while the second registers normally. Per-batch reconciliation keeps the two independent."""
        await create_mappings_in_score_set(
            session,
            mock_s3_client,
            mock_worker_ctx,
            sample_score_dataframe,
            sample_count_dataframe,
            dummy_variant_creation_job_run,
            dummy_variant_mapping_job_run,
        )
        await self._add_derived_alleles_with_distinct_hgvs(session, count=3)

        call_count = {"n": 0}

        def fake_dispatch(hgvs_list):
            call_count["n"] += 1
            # First batch mimics a request failure (dispatch_submissions returns [] on error); the rest
            # register normally.
            if call_count["n"] == 1:
                return []
            return [{"@id": f"CA/{hgvs}", "type": "nucleotide", "genomicAlleles": []} for hgvs in hgvs_list]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                side_effect=fake_dispatch,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.DEFAULT_CAR_SUBMISSION_BATCH_SIZE", 2),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
        ):
            result = await submit_score_set_mappings_to_car(
                mock_worker_ctx,
                submit_score_set_mappings_to_car_sample_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], submit_score_set_mappings_to_car_sample_job_run.id),
            )

        # The good batch links alleles, so the job overall succeeds despite the failed batch.
        assert result.status == JobStatus.SUCCEEDED
        assert call_count["n"] == 2

        # Exactly the second batch's two alleles are registered; the first batch's two are not.
        registered = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(registered) == 2

        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 4
        assert sum(event.disposition == "present" for event in events) == 2
        failed = [event for event in events if event.disposition == "failed"]
        assert len(failed) == 2
        assert all(event.reason == "api_error" for event in failed)


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

        # All 4 variants share 1 allele; build 1 CAR response
        alleles = session.scalars(select(Allele)).all()
        registered_alleles_mock = [
            {
                "@id": f"CA{alleles[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(alleles[0].post_mapped)}],
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
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # 1 allele received the CAID
        alleles_with_caid = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles_with_caid) == 1
        assert alleles_with_caid[0].clingen_allele_id == f"CA{alleles[0].id}"

        # 4 per-variant annotations — all success
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "present"

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

        # All 4 variants share 1 allele; build 1 CAR response
        alleles = session.scalars(select(Allele)).all()
        registered_alleles_mock = [
            {
                "@id": f"CA{alleles[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(alleles[0].post_mapped)}],
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
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run_in_pipeline.id
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # 1 allele received the CAID
        alleles_with_caid = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles_with_caid) == 1
        assert alleles_with_caid[0].clingen_allele_id == f"CA{alleles[0].id}"

        # 4 per-variant annotations — all success
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "present"

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

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # Verify no annotation statuses were created
        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 0

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_failure") as mock_send_slack_job_failure,
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        mock_send_slack_job_failure.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # Verify no annotation statuses were created
        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 0

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

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # Verify no annotation statuses were created
        events = session.scalars(select(AnnotationEvent)).all()
        assert len(events) == 0

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_failure") as mock_send_slack_job_failure,
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        mock_send_slack_job_failure.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # Verify annotation statuses were rendered as failed — 4 variants, all failed
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1

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

        # Patch ClinGenAlleleRegistryService to return only errors with no linked alleles
        registered_alleles_mock = [
            {"errorType": "InvalidHGVS", "hgvs": "test"},
        ]

        with (
            patch(
                "mavedb.worker.jobs.external_services.clingen.ClinGenAlleleRegistryService.dispatch_submissions",
                return_value=registered_alleles_mock,
            ),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", "http://fake-endpoint"),
            patch("mavedb.worker.jobs.external_services.clingen.CLIN_GEN_SUBMISSION_ENABLED", True),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_failure") as mock_send_slack_job_failure,
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        mock_send_slack_job_failure.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # Verify annotation statuses were rendered as failed — 4 variants, all failed
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1

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
        """All 4 variants share 1 allele; CAR returns 1 success → 1 registered allele, 0 failed, job SUCCEEDED."""
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

        alleles = session.scalars(select(Allele)).all()
        assert len(alleles) == 1

        registered_alleles_mock = [
            {
                "@id": f"CA{alleles[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(alleles[0].post_mapped)}],
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
        assert result.data["registered_allele_count"] == 1
        assert result.data["failed_allele_count"] == 0

        # 1 allele got a CAID
        alleles_with_caid = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles_with_caid) == 1
        assert alleles_with_caid[0].clingen_allele_id == f"CA{alleles[0].id}"

        # All 4 variant annotations succeeded
        present_events = session.scalars(
            select(AnnotationEvent).where(
                AnnotationEvent.annotation_type == "clingen_allele_id",
                AnnotationEvent.disposition == "present",
            )
        ).all()
        assert len(present_events) == 1

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

        # All 4 variants share 1 allele. CAR returns an explicit error for that 1 HGVS.
        alleles = session.scalars(select(Allele)).all()
        assert len(alleles) == 1
        allele_hgvs = get_hgvs_from_post_mapped(alleles[0].post_mapped)
        registered_alleles_mock = [
            {
                "errorType": "InvalidHGVS",
                "hgvs": allele_hgvs,
                "message": "The HGVS string is invalid.",
                "description": "error",
                "inputLine": allele_hgvs,
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

        # The 1 shared allele was rejected by ClinGen → one failed event (reason service_rejected).
        rejected_events = session.scalars(
            select(AnnotationEvent).where(
                AnnotationEvent.annotation_type == "clingen_allele_id",
                AnnotationEvent.reason == "service_rejected",
            )
        ).all()
        assert len(rejected_events) == 1
        for event in rejected_events:
            assert event.disposition == "failed"
            assert event.event_metadata["submitted_hgvs"] == allele_hgvs
            assert event.event_metadata["car_error_type"] == "InvalidHGVS"
            assert event.event_metadata["car_error_message"] == "The HGVS string is invalid."

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            result = await submit_score_set_mappings_to_car(
                standalone_worker_context, submit_score_set_mappings_to_car_sample_job_run.id
            )

        mock_send_slack_job_error.assert_called_once()
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

        # All 4 variants share 1 allele; build 1 CAR response
        alleles = session.scalars(select(Allele)).all()
        registered_alleles_mock = [
            {
                "@id": f"CA{alleles[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(alleles[0].post_mapped)}],
            }
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

        # 1 allele received the CAID
        alleles_with_caid = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles_with_caid) == 1
        assert alleles_with_caid[0].clingen_allele_id == f"CA{alleles[0].id}"

        # 4 per-variant annotations — all success
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "present"

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

        # All 4 variants share 1 allele; build 1 CAR response
        alleles = session.scalars(select(Allele)).all()
        registered_alleles_mock = [
            {
                "@id": f"CA{alleles[0].id}",
                "type": "nucleotide",
                "genomicAlleles": [{"hgvs": get_hgvs_from_post_mapped(alleles[0].post_mapped)}],
            }
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

        # 1 allele received the CAID
        alleles_with_caid = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles_with_caid) == 1
        assert alleles_with_caid[0].clingen_allele_id == f"CA{alleles[0].id}"

        # 4 per-variant annotations — all success
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 1
        for event in events:
            assert event.disposition == "present"

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_car", submit_score_set_mappings_to_car_sample_job_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run)
        assert submit_score_set_mappings_to_car_sample_job_run.status == JobStatus.ERRORED
        assert submit_score_set_mappings_to_car_sample_job_run.error_message == "ClinGen service error"

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # Verify no annotation statuses were created
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 0

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_car", submit_score_set_mappings_to_car_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_car_sample_job_run_in_pipeline.status == JobStatus.ERRORED
        assert submit_score_set_mappings_to_car_sample_job_run_in_pipeline.error_message == "ClinGen service error"

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_car_sample_pipeline)
        assert submit_score_set_mappings_to_car_sample_pipeline.status == PipelineStatus.FAILED

        # Verify no alleles have CAIDs assigned
        alleles = session.scalars(select(Allele).where(Allele.clingen_allele_id.isnot(None))).all()
        assert len(alleles) == 0

        # Verify no annotation statuses were created
        events = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "clingen_allele_id")
        ).all()
        assert len(events) == 0


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
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.disposition == "present"

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
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.disposition == "present"

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        mock_send_slack_job_error.assert_called_once()
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
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.disposition == "failed"

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
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_failure") as mock_send_slack_job_failure,
        ):
            result = await submit_score_set_mappings_to_ldh(
                standalone_worker_context, submit_score_set_mappings_to_ldh_sample_job_run.id
            )

        mock_send_slack_job_failure.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        # Verify annotation statuses were created with failures
        annotation_statuses = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.disposition == "failed"

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
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        success_count = 0
        failure_count = 0
        for ann in annotation_statuses:
            if ann.disposition == "present":
                success_count += 1
            elif ann.disposition == "failed":
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
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.disposition == "present"

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
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.disposition == "present"

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
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 4
        for ann in annotation_statuses:
            assert ann.disposition == "present"

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_ldh", submit_score_set_mappings_to_ldh_sample_job_run.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job(
                "submit_score_set_mappings_to_ldh", submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.id
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        # Verify no annotation statuses were created
        annotation_statuses = session.scalars(
            select(AnnotationEvent).where(AnnotationEvent.annotation_type == "ldh_submission")
        ).all()
        assert len(annotation_statuses) == 0

        # Verify the job status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.status == JobStatus.ERRORED
        assert submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline.error_message == "LDH service error"

        # Verify the pipeline status is updated in the database
        session.refresh(submit_score_set_mappings_to_ldh_sample_pipeline)
        assert submit_score_set_mappings_to_ldh_sample_pipeline.status == PipelineStatus.FAILED
