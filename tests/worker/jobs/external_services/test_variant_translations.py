"""Tests for variant translation job submission."""

import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session

from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.worker.jobs.external_services.variant_translations import (
    submit_variant_translation_jobs_for_score_set,
)
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.lib.exceptions import VariantTranslationProcessingError


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
def score_set_with_clingen_ids(db: Session):
    """Create a score set with ClinGen allele IDs."""
    score_set = ScoreSet(urn="urn:mavedb:00000001", title="Test Score Set")
    db.add(score_set)
    db.flush()

    variant1 = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000001")
    variant2 = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000002")
    db.add_all([variant1, variant2])
    db.flush()

    mapped_variant1 = MappedVariant(
        variant_id=variant1.id,
        current=True,
        clingen_allele_id="CA123456",
    )
    mapped_variant2 = MappedVariant(
        variant_id=variant2.id,
        current=True,
        clingen_allele_id="PA123456",
    )
    db.add_all([mapped_variant1, mapped_variant2])
    db.commit()

    return score_set


class TestSubmitVariantTranslationJobsForScoreSet:
    """Tests for submit_variant_translation_jobs_for_score_set function."""

    @pytest.mark.asyncio
    async def test_successful_variant_translation(self, mock_job_manager, score_set_with_clingen_ids):
        """Test successful variant translation for a score set."""
        mock_job = MagicMock()
        mock_job.job_params = {
            "score_set_id": score_set_with_clingen_ids.id,
            "correlation_id": "test-correlation-123",
        }
        mock_job.metadata_ = {}
        mock_job_manager.get_job.return_value = mock_job

        with patch(
            "mavedb.worker.jobs.external_services.variant_translations.populate_variant_translations_for_score_set"
        ) as mock_populate:
            mock_populate.return_value = 2

            result = await submit_variant_translation_jobs_for_score_set({}, 1, mock_job_manager)

            assert result["status"] == "ok"
            assert "allele_ids_processed" in result["data"]
            assert result["exception"] is None

    @pytest.mark.asyncio
    async def test_no_clingen_allele_ids(self, mock_job_manager, db: Session):
        """Test handling when no ClinGen allele IDs are found."""
        score_set = ScoreSet(urn="urn:mavedb:00000002", title="Score Set Without ClinGen IDs")
        db.add(score_set)
        db.flush()

        variant = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000001")
        db.add(variant)
        db.flush()

        mapped_variant = MappedVariant(variant_id=variant.id, current=True)
        db.add(mapped_variant)
        db.commit()

        mock_job = MagicMock()
        mock_job.job_params = {
            "score_set_id": score_set.id,
            "correlation_id": "test-correlation-123",
        }
        mock_job.metadata_ = {}
        mock_job_manager.get_job.return_value = mock_job

        result = await submit_variant_translation_jobs_for_score_set({}, 1, mock_job_manager)

        assert result["status"] == "ok"
        assert result["data"] == {}

    @pytest.mark.asyncio
    async def test_multi_variant_allele_id_expansion(self, mock_job_manager, db: Session):
        """Test that multi-variant (comma-separated) allele IDs are properly expanded."""
        score_set = ScoreSet(urn="urn:mavedb:00000003", title="Multi-variant Score Set")
        db.add(score_set)
        db.flush()

        variant = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000001")
        db.add(variant)
        db.flush()

        # Multi-variant ClinGen allele ID
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            current=True,
            clingen_allele_id="CA123456,CA789012",
        )
        db.add(mapped_variant)
        db.commit()

        mock_job = MagicMock()
        mock_job.job_params = {
            "score_set_id": score_set.id,
            "correlation_id": "test-correlation-123",
        }
        mock_job.metadata_ = {}
        mock_job_manager.get_job.return_value = mock_job

        with patch(
            "mavedb.worker.jobs.external_services.variant_translations.populate_variant_translations_for_score_set"
        ) as mock_populate:
            mock_populate.return_value = 2

            result = await submit_variant_translation_jobs_for_score_set({}, 1, mock_job_manager)

            assert result["status"] == "ok"
            # Should process 2 unique allele IDs
            assert mock_populate.call_count == 2

    @pytest.mark.asyncio
    async def test_variant_translation_error_handling(self, mock_job_manager, score_set_with_clingen_ids):
        """Test proper error handling during variant translation processing."""
        mock_job = MagicMock()
        mock_job.job_params = {
            "score_set_id": score_set_with_clingen_ids.id,
            "correlation_id": "test-correlation-123",
        }
        mock_job.metadata_ = {}
        mock_job_manager.get_job.return_value = mock_job

        with patch(
            "mavedb.worker.jobs.external_services.variant_translations.populate_variant_translations_for_score_set"
        ) as mock_populate:
            mock_populate.side_effect = VariantTranslationProcessingError("API error")

            result = await submit_variant_translation_jobs_for_score_set({}, 1, mock_job_manager)

            # Should return ok with errors counted
            assert result["status"] == "ok"


class TestVariantTranslationLibraryFunctions:
    """Tests for variant translation library functions."""

    @pytest.mark.asyncio
    async def test_populate_variant_translations_ca_to_pa(self, db: Session):
        """Test translation from CA to PA allele IDs."""
        with patch("mavedb.worker.lib.variant_translations.get_canonical_pa_ids") as mock_get_pa:
            mock_get_pa.return_value = ["PA123456"]

            with patch("mavedb.worker.lib.variant_translations.get_matching_registered_ca_ids") as mock_get_ca:
                mock_get_ca.return_value = ["CA789012"]

                from mavedb.worker.lib.variant_translations import (
                    populate_variant_translations_for_score_set,
                )

                result = await populate_variant_translations_for_score_set(db, "CA123456")

                assert result > 0
                mock_get_pa.assert_called_once_with("CA123456")

    @pytest.mark.asyncio
    async def test_populate_variant_translations_pa_to_ca(self, db: Session):
        """Test translation from PA to CA allele IDs."""
        with patch("mavedb.worker.lib.variant_translations.get_matching_registered_ca_ids") as mock_get_ca:
            mock_get_ca.return_value = ["CA789012", "CA345678"]

            from mavedb.worker.lib.variant_translations import (
                populate_variant_translations_for_score_set,
            )

            result = await populate_variant_translations_for_score_set(db, "PA123456")

            assert result > 0
            mock_get_ca.assert_called_once_with("PA123456")

    @pytest.mark.asyncio
    async def test_populate_variant_translations_no_results(self, db: Session):
        """Test handling when no translations are found."""
        with patch("mavedb.worker.lib.variant_translations.get_canonical_pa_ids") as mock_get_pa:
            mock_get_pa.return_value = []

            from mavedb.worker.lib.variant_translations import (
                populate_variant_translations_for_score_set,
            )

            result = await populate_variant_translations_for_score_set(db, "CA123456")

            assert result == 0

    @pytest.mark.asyncio
    async def test_populate_variant_translations_api_error(self, db: Session):
        """Test proper error handling for API failures."""
        import requests

        with patch("mavedb.worker.lib.variant_translations.get_canonical_pa_ids") as mock_get_pa:
            mock_get_pa.side_effect = requests.exceptions.RequestException("Connection error")

            from mavedb.worker.lib.variant_translations import (
                populate_variant_translations_for_score_set,
            )

            with pytest.raises(VariantTranslationProcessingError):
                await populate_variant_translations_for_score_set(db, "CA123456")
