"""Tests for VEP functional consequence job submission."""

import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session

from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.worker.jobs.external_services.vep import submit_vep_jobs_for_score_set
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
def score_set_with_mapped_variants(db: Session):
    """Create a score set with mapped variants."""
    score_set = ScoreSet(urn="urn:mavedb:00000001", title="Test Score Set")
    db.add(score_set)
    db.flush()

    target_gene = MagicMock()
    target_gene.id = 1
    score_set.target_genes = [target_gene]

    variant1 = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000001")
    variant2 = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000002")
    db.add_all([variant1, variant2])
    db.flush()

    mapped_variant1 = MappedVariant(
        variant_id=variant1.id,
        current=True,
        post_mapped={"expressions": [{"value": "NM_000001.1:c.100A>G"}]},
    )
    mapped_variant2 = MappedVariant(
        variant_id=variant2.id,
        current=True,
        post_mapped={"expressions": [{"value": "NM_000001.1:c.200C>T"}]},
    )
    db.add_all([mapped_variant1, mapped_variant2])
    db.commit()

    return score_set


class TestSubmitVepJobsForScoreSet:
    """Tests for submit_vep_jobs_for_score_set function."""

    @pytest.mark.asyncio
    async def test_successful_vep_processing(self, mock_job_manager, score_set_with_mapped_variants):
        """Test successful VEP processing for a score set."""
        mock_job = MagicMock()
        mock_job.job_params = {
            "score_set_id": score_set_with_mapped_variants.id,
            "correlation_id": "test-correlation-123",
        }
        mock_job.metadata_ = {}
        mock_job_manager.get_job.return_value = mock_job

        with patch(
            "mavedb.worker.jobs.external_services.vep.populate_variant_translations_for_score_set"
        ) as mock_get_consequences:
            mock_get_consequences.return_value = {
                "NM_000001.1:c.100A>G": "missense_variant",
                "NM_000001.1:c.200C>T": "synonymous_variant",
            }

            result = await submit_vep_jobs_for_score_set({}, 1, mock_job_manager)

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

        result = await submit_vep_jobs_for_score_set({}, 1, mock_job_manager)

        assert result["status"] == "ok"
        assert result["data"] == {}

    @pytest.mark.asyncio
    async def test_missing_hgvs_string(self, mock_job_manager, db: Session):
        """Test handling of variants with missing HGVS strings."""
        score_set = ScoreSet(urn="urn:mavedb:00000003", title="Missing HGVS Score Set")
        db.add(score_set)
        db.flush()

        variant = Variant(score_set_id=score_set.id, urn="urn:mavedb:variant:00000003")
        db.add(variant)
        db.flush()

        # Mapped variant with missing HGVS
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            current=True,
            post_mapped={"expressions": []},
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

        result = await submit_vep_jobs_for_score_set({}, 1, mock_job_manager)

        assert result["status"] == "ok"
        mock_job_manager.update_progress.assert_called()

    @pytest.mark.asyncio
    async def test_batch_processing(self, mock_job_manager, db: Session):
        """Test that batches of 200 variants are processed correctly."""
        score_set = ScoreSet(urn="urn:mavedb:00000004", title="Large Score Set")
        db.add(score_set)
        db.flush()

        # Create 250 variants to test batching
        variants = [Variant(score_set_id=score_set.id, urn=f"urn:mavedb:variant:0000000{i}") for i in range(250)]
        db.add_all(variants)
        db.flush()

        mapped_variants = [
            MappedVariant(
                variant_id=variants[i].id,
                current=True,
                post_mapped={"expressions": [{"value": f"NM_000001.1:c.{i}A>G"}]},
            )
            for i in range(250)
        ]
        db.add_all(mapped_variants)
        db.commit()

        mock_job = MagicMock()
        mock_job.job_params = {
            "score_set_id": score_set.id,
            "correlation_id": "test-correlation-123",
        }
        mock_job.metadata_ = {}
        mock_job_manager.get_job.return_value = mock_job

        with patch("mavedb.worker.jobs.external_services.vep.get_functional_consequence") as mock_get_consequences:
            mock_get_consequences.return_value = {f"NM_000001.1:c.{i}A>G": "missense_variant" for i in range(250)}

            result = await submit_vep_jobs_for_score_set({}, 1, mock_job_manager)

            assert result["status"] == "ok"
            # Should be called twice (200 + 50)
            assert mock_get_consequences.call_count == 2


class TestVepLibraryFunctions:
    """Tests for VEP library functions."""

    def test_get_functional_consequence_success(self):
        """Test successful functional consequence retrieval."""
        with patch("mavedb.worker.lib.vep.requests.post") as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = [
                {
                    "input": "NM_000001.1:c.100A>G",
                    "most_severe_consequence": "missense_variant",
                },
                {
                    "input": "NM_000001.1:c.200C>T",
                    "most_severe_consequence": "synonymous_variant",
                },
            ]
            mock_post.return_value = mock_response

            from mavedb.worker.lib.vep import get_functional_consequence

            result = get_functional_consequence(["NM_000001.1:c.100A>G", "NM_000001.1:c.200C>T"])

            assert result["NM_000001.1:c.100A>G"] == "missense_variant"
            assert result["NM_000001.1:c.200C>T"] == "synonymous_variant"

    def test_get_functional_consequence_with_fallback(self):
        """Test functional consequence with Variant Recoder fallback."""
        with patch("mavedb.worker.lib.vep.requests.post") as mock_post:
            # First call returns partial results
            first_response = MagicMock()
            first_response.status_code = 200
            first_response.json.return_value = [
                {
                    "input": "NM_000001.1:c.100A>G",
                    "most_severe_consequence": "missense_variant",
                }
            ]

            # Variant Recoder call
            recoder_response = MagicMock()
            recoder_response.status_code = 200
            recoder_response.json.return_value = [
                {
                    "input": "NM_000001.1:c.200C>T",
                    "NC_000001.14:g.1000A>G": {
                        "hgvsg": ["NC_000001.14:g.1000A>G"],
                    },
                }
            ]

            # VEP call for genomic
            vep_response = MagicMock()
            vep_response.status_code = 200
            vep_response.json.return_value = [
                {
                    "input": "NC_000001.14:g.1000A>G",
                    "most_severe_consequence": "synonymous_variant",
                }
            ]

            mock_post.side_effect = [first_response, recoder_response, vep_response]

            from mavedb.worker.lib.vep import get_functional_consequence

            result = get_functional_consequence(["NM_000001.1:c.100A>G", "NM_000001.1:c.200C>T"])

            assert result["NM_000001.1:c.100A>G"] == "missense_variant"
            assert result["NM_000001.1:c.200C>T"] == "synonymous_variant"

    def test_run_variant_recoder_success(self):
        """Test successful Variant Recoder execution."""
        with patch("mavedb.worker.lib.vep.requests.post") as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = [
                {
                    "input": "NM_000001.1:c.100A>G",
                    "NC_000001.14:g.1000A>G": {
                        "hgvsg": ["NC_000001.14:g.1000A>G"],
                    },
                }
            ]
            mock_post.return_value = mock_response

            from mavedb.worker.lib.vep import run_variant_recoder

            result = run_variant_recoder(["NM_000001.1:c.100A>G"])

            assert "NM_000001.1:c.100A>G" in result
            assert "NC_000001.14:g.1000A>G" in result["NM_000001.1:c.100A>G"]
