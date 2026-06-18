# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from datetime import date
from unittest.mock import AsyncMock, patch

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.allele import Allele
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.external_services.clingen_cache import warm_clingen_cache
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


def _make_allele_with_caid(session, score_set_id: int, urn_suffix: str, caid: str | None, vrs_digest: str) -> Allele:
    """Create a Variant → MappingRecord → Allele chain and return the Allele.

    get_alleles_for_score_set joins: Allele ← MappingRecordAllele (current) ← MappingRecord (current) ← Variant.
    Leaving valid_to=NULL on MappingRecord and MappingRecordAllele makes them current.
    """
    variant = Variant(
        urn=f"urn:variant:{urn_suffix}",
        score_set_id=score_set_id,
        hgvs_nt="NM_000000.1:c.1A>G",
        hgvs_pro="NP_000000.1:p.Met1Val",
        data={},
    )
    session.add(variant)
    session.flush()

    allele = Allele(
        vrs_digest=vrs_digest,
        level="genomic",
        post_mapped={"type": "Allele", "location": {"sequenceReference": {"refgetAccession": vrs_digest}}},
        clingen_allele_id=caid,
    )
    session.add(allele)
    session.flush()

    mapping_record = MappingRecord(
        variant_id=variant.id,
        assay_level="genomic",
        mapping_api_version="1.0.0",
        mapped_date=date.today(),
    )
    session.add(mapping_record)
    session.flush()

    link = MappingRecordAllele(
        mapping_record_id=mapping_record.id,
        allele_id=allele.id,
        is_authoritative=True,
    )
    session.add(link)
    session.flush()

    return allele


@pytest.mark.unit
@pytest.mark.asyncio
class TestWarmClingenCacheUnit:
    """Tests for the warm_clingen_cache job function."""

    async def test_no_mapped_variants_succeeds(
        self,
        mock_worker_ctx,
        session,
        with_warm_clingen_cache_job,
        sample_warm_clingen_cache_job_run,
    ):
        """Job completes successfully when there are no mapped variants."""
        result = await warm_clingen_cache(
            mock_worker_ctx,
            sample_warm_clingen_cache_job_run.id,
            JobManager(session, mock_worker_ctx["redis"], sample_warm_clingen_cache_job_run.id),
        )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

    async def test_warms_cache_for_variants_with_caids(
        self,
        mock_worker_ctx,
        session,
        with_warm_clingen_cache_job,
        sample_warm_clingen_cache_job_run,
    ):
        """Job calls get_clingen_allele_data for each distinct allele ID."""
        score_set = session.get(ScoreSet, sample_warm_clingen_cache_job_run.job_params["score_set_id"])

        # Three variants, two sharing the same CAID — should only warm 2 distinct IDs.
        # Two separate allele rows are needed (different VRS digests) to get 2 distinct CAIDs.
        _make_allele_with_caid(session, score_set.id, "warm-test-0", "CA111111", "digest-warm-0")
        _make_allele_with_caid(session, score_set.id, "warm-test-1", "CA222222", "digest-warm-1")
        # Third variant points to same allele as first (same CAID CA111111 via a fresh allele row
        # with the same digest — but since get_alleles_for_score_set returns per-variant rows and
        # caid dedup happens in the warmer, we can share the digest).
        _make_allele_with_caid(session, score_set.id, "warm-test-2", "CA111111", "digest-warm-2")
        session.commit()

        mock_get_allele_data = AsyncMock(return_value={"some": "data"})

        with patch(
            "mavedb.worker.jobs.external_services.clingen_cache.get_clingen_allele_data",
            mock_get_allele_data,
        ):
            result = await warm_clingen_cache(
                mock_worker_ctx,
                sample_warm_clingen_cache_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_warm_clingen_cache_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        # Should be called exactly 2 times (CA111111 and CA222222, deduplicated)
        assert mock_get_allele_data.call_count == 2
        called_ids = {call.args[0] for call in mock_get_allele_data.call_args_list}
        assert called_ids == {"CA111111", "CA222222"}

    async def test_skips_null_and_multi_variant_caids(
        self,
        mock_worker_ctx,
        session,
        with_warm_clingen_cache_job,
        sample_warm_clingen_cache_job_run,
    ):
        """Job ignores variants with null or multi-variant (comma-separated) ClinGen IDs."""
        score_set = session.get(ScoreSet, sample_warm_clingen_cache_job_run.job_params["score_set_id"])

        caids = ["CA333333", None, "CA-MULTI-001,CA-MULTI-002"]
        for i, caid in enumerate(caids):
            _make_allele_with_caid(session, score_set.id, f"warm-filter-{i}", caid, f"digest-filter-{i}")
        session.commit()

        mock_get_allele_data = AsyncMock(return_value={"some": "data"})

        with patch(
            "mavedb.worker.jobs.external_services.clingen_cache.get_clingen_allele_data",
            mock_get_allele_data,
        ):
            result = await warm_clingen_cache(
                mock_worker_ctx,
                sample_warm_clingen_cache_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_warm_clingen_cache_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        # Only CA333333 should be warmed; null and multi-variant IDs are excluded
        assert mock_get_allele_data.call_count == 1
        mock_get_allele_data.assert_called_once_with("CA333333")

    async def test_continues_on_individual_fetch_failure(
        self,
        mock_worker_ctx,
        session,
        with_warm_clingen_cache_job,
        sample_warm_clingen_cache_job_run,
    ):
        """Job continues warming remaining alleles when one fetch fails."""
        score_set = session.get(ScoreSet, sample_warm_clingen_cache_job_run.job_params["score_set_id"])

        for i, caid in enumerate(["CA444444", "CA555555"]):
            _make_allele_with_caid(session, score_set.id, f"warm-fail-{i}", caid, f"digest-fail-{i}")
        session.commit()

        # First call raises, second succeeds
        mock_get_allele_data = AsyncMock(
            side_effect=[Exception("ClinGen API timeout"), {"some": "data"}],
        )

        with patch(
            "mavedb.worker.jobs.external_services.clingen_cache.get_clingen_allele_data",
            mock_get_allele_data,
        ):
            result = await warm_clingen_cache(
                mock_worker_ctx,
                sample_warm_clingen_cache_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_warm_clingen_cache_job_run.id),
            )

        # Job should still succeed — individual failures are non-fatal
        assert result.status == JobStatus.SUCCEEDED
        assert mock_get_allele_data.call_count == 2

    async def test_only_warms_current_mapped_variants(
        self,
        mock_worker_ctx,
        session,
        with_warm_clingen_cache_job,
        sample_warm_clingen_cache_job_run,
    ):
        """Job only fetches allele IDs from current (not superseded) mapping records."""
        score_set = session.get(ScoreSet, sample_warm_clingen_cache_job_run.job_params["score_set_id"])

        variant = Variant(
            urn="urn:variant:warm-current-test",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.30A>G",
            hgvs_pro="NP_000000.1:p.Met30Val",
            data={},
        )
        session.add(variant)
        session.flush()

        # Superseded allele (MappingRecord with valid_to set — not current)
        old_allele = Allele(
            vrs_digest="digest-old-mv",
            level="genomic",
            post_mapped={"type": "Allele"},
            clingen_allele_id="CA666666",
        )
        session.add(old_allele)
        session.flush()

        from datetime import datetime, timezone

        old_mapping_record = MappingRecord(
            variant_id=variant.id,
            assay_level="genomic",
            mapping_api_version="0.9.0",
            mapped_date=date(2023, 1, 1),
        )
        # Close this record so it is non-current (valid_to set)
        old_mapping_record.valid_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
        session.add(old_mapping_record)
        session.flush()

        old_link = MappingRecordAllele(
            mapping_record_id=old_mapping_record.id,
            allele_id=old_allele.id,
            is_authoritative=True,
        )
        old_link.valid_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
        session.add(old_link)
        session.flush()

        # Current allele (MappingRecord with valid_to=NULL — current)
        current_allele = Allele(
            vrs_digest="digest-current-mv",
            level="genomic",
            post_mapped={"type": "Allele"},
            clingen_allele_id="CA777777",
        )
        session.add(current_allele)
        session.flush()

        current_mapping_record = MappingRecord(
            variant_id=variant.id,
            assay_level="genomic",
            mapping_api_version="1.0.0",
            mapped_date=date.today(),
        )
        session.add(current_mapping_record)
        session.flush()

        current_link = MappingRecordAllele(
            mapping_record_id=current_mapping_record.id,
            allele_id=current_allele.id,
            is_authoritative=True,
        )
        session.add(current_link)
        session.commit()

        mock_get_allele_data = AsyncMock(return_value={"some": "data"})

        with patch(
            "mavedb.worker.jobs.external_services.clingen_cache.get_clingen_allele_data",
            mock_get_allele_data,
        ):
            result = await warm_clingen_cache(
                mock_worker_ctx,
                sample_warm_clingen_cache_job_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_warm_clingen_cache_job_run.id),
            )

        assert result.status == JobStatus.SUCCEEDED
        mock_get_allele_data.assert_called_once_with("CA777777")
