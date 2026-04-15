"""Tests for HGVS nomenclature job submission."""

import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session

from mavedb.lib.exceptions import HGVSProcessingError
from mavedb.lib.hgvs import populate_mapped_hgvs_for_variants
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.worker.jobs.external_services.hgvs import submit_hgvs_mapping_jobs_for_score_set
from mavedb.worker.lib.managers.job_manager import JobManager


@pytest.fixture
def mock_job_manager(db: Session):
    """Create a mock JobManager for testing."""
    manager = MagicMock(spec=JobManager)
    manager.db = db
    manager.get_job = MagicMock()
    manager.logging_context = MagicMock(return_value={})
    manager.save_to_context = MagicMock()
    manager.update_progress = MagicMock()
    return manager


@pytest.fixture
def score_set_with_variants(db: Session):
    """Create a score set with variants."""
    score_set = ScoreSet(urn="urn:mavedb:00000001", title="Test Score Set")
    db.add(score_set)
    db.flush()

    variant1 = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000001")
    variant2 = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000002")
    db.add_all([variant1, variant2])
    db.flush()

    mapped_variant1 = MappedVariant(variant_id=variant1.id, current=True)
    mapped_variant2 = MappedVariant(variant_id=variant2.id, current=True)
    db.add_all([mapped_variant1, mapped_variant2])
    db.commit()

    return score_set


class TestSubmitHgvsMappingJobsForScoreSet:
    """Tests for submit_hgvs_mapping_jobs_for_score_set function."""

    @pytest.mark.asyncio
    async def test_successful_hgvs_population(self, mock_job_manager, score_set_with_variants):
        """Test successful HGVS population for a score set."""
        mock_job = MagicMock()
        mock_job.job_params = {
            "score_set_id": score_set_with_variants.id,
            "correlation_id": "test-correlation-123",
        }
        mock_job.metadata_ = {}
        mock_job_manager.get_job.return_value = mock_job

        with patch("mavedb.worker.jobs.external_services.hgvs.populate_mapped_hgvs_for_variants") as mock_populate:
            mock_populate.return_value = True

            result = await submit_hgvs_mapping_jobs_for_score_set({}, 1, mock_job_manager)

            assert result["status"] == "ok"
            assert "variants_processed" in result["data"]
            assert result["exception"] is None

    @pytest.mark.asyncio
    async def test_no_mapped_variants(self, mock_job_manager, db: Session):
        """Test handling when no mapped variants are found."""
        score_set = ScoreSet(urn="urn:mavedb:00000002", title="Empty Score Set")
        db.add(score_set)
        db.commit()

        mock_job = MagicMock()
        mock_job.job_params = {
            "score_set_id": score_set.id,
            "correlation_id": "test-correlation-123",
        }
        mock_job.metadata_ = {}
        mock_job_manager.get_job.return_value = mock_job

        result = await submit_hgvs_mapping_jobs_for_score_set({}, 1, mock_job_manager)

        assert result["status"] == "ok"
        assert result["data"] == {}

    @pytest.mark.asyncio
    async def test_hgvs_processing_error_handling(self, mock_job_manager, score_set_with_variants):
        """Test proper error handling during HGVS processing."""
        mock_job = MagicMock()
        mock_job.job_params = {
            "score_set_id": score_set_with_variants.id,
            "correlation_id": "test-correlation-123",
        }
        mock_job.metadata_ = {}
        mock_job_manager.get_job.return_value = mock_job

        with patch("mavedb.worker.jobs.external_services.hgvs.populate_mapped_hgvs_for_variants") as mock_populate:
            mock_populate.side_effect = HGVSProcessingError("API error")

            result = await submit_hgvs_mapping_jobs_for_score_set({}, 1, mock_job_manager)

            assert result["status"] == "failed"
            assert result["exception"] is not None


class TestHgvsLibraryFunctions:
    """Tests for HGVS library functions."""

    @pytest.mark.asyncio
    async def test_populate_mapped_hgvs_for_variants_success(self, db: Session):
        """Test successful HGVS population for variants."""
        score_set = ScoreSet(urn="urn:mavedb:00000001", title="Test Score Set")
        db.add(score_set)
        db.flush()

        variant = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000001")
        db.add(variant)
        db.flush()

        mapped_variant = MappedVariant(variant_id=variant.id, current=True)
        db.add(mapped_variant)
        db.commit()

        with patch("mavedb.worker.lib.hgvs.get_target_info") as mock_target_info:
            mock_target_info.return_value = (True, "NM_000001.1")

            with patch("mavedb.worker.lib.hgvs.get_hgvs_from_variant") as mock_get_hgvs:
                mock_get_hgvs.return_value = {
                    "expressions": [
                        {"value": "NM_000001.1:c.100A>G"},
                        {"value": "NP_000001.1:p.Met1Val"},
                        {"value": "NC_000001.14:g.1000A>G"},
                    ]
                }

                result = populate_mapped_hgvs_for_variants(db, score_set, [mapped_variant])

                assert result is True
                assert mapped_variant.post_mapped is not None

    @pytest.mark.asyncio
    async def test_populate_mapped_hgvs_for_variants_failure(self, db: Session):
        """Test handling of HGVS population failure."""
        score_set = ScoreSet(urn="urn:mavedb:00000002", title="Test Score Set")
        db.add(score_set)
        db.flush()

        variant = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000002")
        db.add(variant)
        db.flush()

        mapped_variant = MappedVariant(variant_id=variant.id, current=True)
        db.add(mapped_variant)
        db.commit()

        with patch("mavedb.worker.lib.hgvs.get_target_info") as mock_target_info:
            mock_target_info.return_value = (True, "NM_000001.1")

            with patch("mavedb.worker.lib.hgvs.get_hgvs_from_variant") as mock_get_hgvs:
                mock_get_hgvs.return_value = None

                result = populate_mapped_hgvs_for_variants(db, score_set, [mapped_variant])

                assert result is False
