# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from unittest.mock import AsyncMock, patch

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.external_services.clingen_cache import warm_clingen_cache
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


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

        # Create two variants with the same CAID — should only warm once (distinct)
        for i, caid in enumerate(["CA111111", "CA222222", "CA111111"]):
            variant = Variant(
                urn=f"urn:variant:warm-test-{i}",
                score_set_id=score_set.id,
                hgvs_nt=f"NM_000000.1:c.{i + 1}A>G",
                hgvs_pro=f"NP_000000.1:p.Met{i + 1}Val",
                data={},
            )
            session.add(variant)
            session.commit()
            mapped_variant = MappedVariant(
                variant_id=variant.id,
                clingen_allele_id=caid,
                current=True,
                mapped_date="2024-01-01T00:00:00Z",
                mapping_api_version="1.0.0",
            )
            session.add(mapped_variant)
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
            variant = Variant(
                urn=f"urn:variant:warm-filter-{i}",
                score_set_id=score_set.id,
                hgvs_nt=f"NM_000000.1:c.{i + 10}A>G",
                hgvs_pro=f"NP_000000.1:p.Met{i + 10}Val",
                data={},
            )
            session.add(variant)
            session.commit()
            mapped_variant = MappedVariant(
                variant_id=variant.id,
                clingen_allele_id=caid,
                current=True,
                mapped_date="2024-01-01T00:00:00Z",
                mapping_api_version="1.0.0",
            )
            session.add(mapped_variant)
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
            variant = Variant(
                urn=f"urn:variant:warm-fail-{i}",
                score_set_id=score_set.id,
                hgvs_nt=f"NM_000000.1:c.{i + 20}A>G",
                hgvs_pro=f"NP_000000.1:p.Met{i + 20}Val",
                data={},
            )
            session.add(variant)
            session.commit()
            mapped_variant = MappedVariant(
                variant_id=variant.id,
                clingen_allele_id=caid,
                current=True,
                mapped_date="2024-01-01T00:00:00Z",
                mapping_api_version="1.0.0",
            )
            session.add(mapped_variant)
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
        """Job only fetches allele IDs from current (not superseded) mapped variants."""
        score_set = session.get(ScoreSet, sample_warm_clingen_cache_job_run.job_params["score_set_id"])

        variant = Variant(
            urn="urn:variant:warm-current-test",
            score_set_id=score_set.id,
            hgvs_nt="NM_000000.1:c.30A>G",
            hgvs_pro="NP_000000.1:p.Met30Val",
            data={},
        )
        session.add(variant)
        session.commit()

        # Non-current mapped variant should be ignored
        old_mv = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA666666",
            current=False,
            mapped_date="2023-01-01T00:00:00Z",
            mapping_api_version="0.9.0",
        )
        # Current mapped variant should be included
        current_mv = MappedVariant(
            variant_id=variant.id,
            clingen_allele_id="CA777777",
            current=True,
            mapped_date="2024-01-01T00:00:00Z",
            mapping_api_version="1.0.0",
        )
        session.add_all([old_mv, current_mv])
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
