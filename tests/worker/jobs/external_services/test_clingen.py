# ruff: noqa: E402

from unittest.mock import MagicMock, call, patch
from uuid import uuid4

import pytest

from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.job_run import JobRun
from mavedb.worker.lib.managers.job_manager import JobManager

arq = pytest.importorskip("arq")

from sqlalchemy.exc import NoResultFound

from mavedb.lib.clingen.services import (
    ClinGenAlleleRegistryService,
)
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.worker.jobs import (
    submit_score_set_mappings_to_car,
)
from tests.helpers.constants import (
    TEST_CLINGEN_ALLELE_OBJECT,
    TEST_MINIMAL_SEQ_SCORESET,
)
from tests.helpers.util.setup.worker import (
    setup_records_files_and_variants_with_mapping,
)

############################################################################################################################################
# ClinGen CAR Submission
############################################################################################################################################


@pytest.mark.asyncio
@pytest.mark.unit
class TestSubmitScoreSetMappingsToCARUnit:
    """Tests for the submit_score_set_mappings_to_car function."""

    @pytest.mark.parametrize("missing_param", ["score_set_id", "correlation_id"])
    async def test_submit_score_set_mappings_to_car_required_params(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
        missing_param,
    ):
        """Test that submitting a non-existent score set raises an exception."""

        mock_job_run.job_params = {"score_set_id": 99, "correlation_id": uuid4().hex}

        del mock_job_run.job_params[missing_param]

        with pytest.raises(ValueError):
            await submit_score_set_mappings_to_car(mock_worker_ctx, 99, job_manager=mock_job_manager)

    async def test_submit_score_set_mappings_to_car_raises_when_no_score_set(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
    ):
        """Test that submitting a non-existent score set raises an exception."""

        mock_job_run.job_params = {"score_set_id": 99, "correlation_id": uuid4().hex}

        with (
            pytest.raises(NoResultFound),
            patch.object(mock_job_manager.db, "scalars", side_effect=NoResultFound()),
            patch.object(mock_job_manager, "update_progress", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.validate_job_params", return_value=None),
        ):
            await submit_score_set_mappings_to_car(mock_worker_ctx, 99, job_manager=mock_job_manager)

    async def test_submit_score_set_mappings_to_car_no_mapped_variants(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
    ):
        """Test that submitting a score set with no mapped variants completes successfully."""

        mock_job_run.job_params = {"score_set_id": 1, "correlation_id": uuid4().hex}

        with (
            patch.object(
                mock_job_manager.db,
                "scalars",
                return_value=MagicMock(one=MagicMock(spec=ScoreSetDbModel, urn="urn:1", num_variants=0)),
            ),
            patch.object(
                mock_job_manager.db,
                "execute",
                return_value=MagicMock(all=lambda: []),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.validate_job_params", return_value=None),
            patch.object(mock_job_manager, "update_progress", return_value=None),
        ):
            result = await submit_score_set_mappings_to_car(mock_worker_ctx, 1, job_manager=mock_job_manager)

        assert result["status"] == "ok"

    async def test_submit_score_set_mappings_to_car_no_variants_updates_progress(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
    ):
        """Test that submitting a score set with no variants updates progress to 100%."""

        mock_job_run.job_params = {"score_set_id": 1, "correlation_id": uuid4().hex}

        with (
            patch.object(
                mock_job_manager.db,
                "scalars",
                return_value=MagicMock(one=MagicMock(spec=ScoreSetDbModel, urn="urn:1", num_variants=0)),
            ),
            patch.object(
                mock_job_manager.db,
                "execute",
                return_value=MagicMock(all=lambda: []),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.validate_job_params", return_value=None),
            patch.object(mock_job_manager, "update_progress", return_value=None) as mock_update_progress,
        ):
            await submit_score_set_mappings_to_car(mock_worker_ctx, 1, job_manager=mock_job_manager)

        expected_calls = [
            call(0, 100, "Starting CAR mapped resource submission."),
            call(100, 100, "No mapped variants to submit to CAR. Skipped submission."),
        ]
        mock_update_progress.assert_has_calls(expected_calls)

    async def test_submit_score_set_mappings_to_car_no_submission_endpoint(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
    ):
        """Test that submitting a score set with no CAR submission endpoint configured raises an exception."""

        mock_job_run.job_params = {"score_set_id": 1, "correlation_id": uuid4().hex}

        with (
            patch.object(
                mock_job_manager.db,
                "scalars",
                return_value=MagicMock(one=MagicMock(spec=ScoreSetDbModel, urn="urn:1", num_variants=1)),
            ),
            patch.object(
                mock_job_manager.db,
                "execute",
                return_value=MagicMock(all=lambda: [(999, {}), (1000, {})]),
            ),
            patch("mavedb.worker.jobs.external_services.clingen.validate_job_params", return_value=None),
            patch.object(mock_job_manager, "update_progress", return_value=None),
            patch("mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT", None),
            pytest.raises(ValueError),
        ):
            await submit_score_set_mappings_to_car(mock_worker_ctx, 1, job_manager=mock_job_manager)

    async def test_submit_score_set_mappings_to_car_no_variants_associated(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
    ):
        """Test that submitting a score set with no variants associated completes successfully."""

        mock_job_run.job_params = {"score_set_id": 1, "correlation_id": uuid4().hex}

        mocked_score_set = MagicMock(spec=ScoreSetDbModel, urn="urn:1", num_variants=2)
        mocked_mapped_variant_with_hgvs = MagicMock(spec=MappedVariant, id=1000, clingen_allele_id=None)

        with (
            # db.scalars is called twice in this function: once to get the score set (one), once to get the mapped variants (all)
            patch.object(
                mock_job_manager.db,
                "scalars",
                return_value=MagicMock(
                    one=mocked_score_set,
                    all=lambda: [mocked_mapped_variant_with_hgvs],
                ),
            ),
            # db.execute is called to get the mapped variant IDs and post mapped data
            patch.object(mock_job_manager.db, "execute", return_value=MagicMock(all=lambda: [(999, {}), (1000, {})])),
            # get_hgvs_from_post_mapped is called twice, once for each mapped variant. mock that both
            # calls return valid HGVS strings.
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped",
                side_effect=["c.122G>C", "c.123A>T"],
            ),
            # validate_job_params is called to validate job parameters
            patch("mavedb.worker.jobs.external_services.clingen.validate_job_params", return_value=None),
            # update_progress is called multiple times to update job progress
            patch.object(mock_job_manager, "update_progress", return_value=None),
            # CAR_SUBMISSION_ENDPOINT is patched to a test URL
            patch(
                "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
                "https://reg.test.genome.network/pytest",
            ),
            # Mock the dispatch_submissions method to return a test ClinGen allele object, which we should associate with the variant
            patch.object(ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[]),
            # Mock the get_allele_registry_associations function to return a mapping from HGVS to CAID
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_allele_registry_associations",
                return_value={},
            ),
            patch.object(mock_job_manager.db, "add", return_value=None) as mock_db_add,
        ):
            result = await submit_score_set_mappings_to_car(mock_worker_ctx, 1, job_manager=mock_job_manager)

        # Assert no CAID was not added to the variant
        mock_db_add.assert_not_called()
        assert mocked_mapped_variant_with_hgvs.clingen_allele_id is None
        assert result["status"] == "ok"

    async def test_submit_score_set_mappings_to_car_no_variants_found_in_db(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
    ):
        """Test that submitting a score set with no mapped variants found in the db completes successfully."""

        mock_job_run.job_params = {"score_set_id": 1, "correlation_id": uuid4().hex}

        mocked_score_set = MagicMock(spec=ScoreSetDbModel, urn="urn:1", num_variants=2)
        mocked_mapped_variant_with_hgvs = MagicMock(spec=MappedVariant, id=1000, clingen_allele_id=None)

        with (
            # db.scalars is called twice in this function: once to get the score set (one), twice to get the mapped variants (all)
            patch.object(
                mock_job_manager.db,
                "scalars",
                return_value=MagicMock(
                    one=mocked_score_set,
                    all=lambda: [],
                ),
            ),
            # db.execute is called to get the mapped variant IDs and post mapped data
            patch.object(mock_job_manager.db, "execute", return_value=MagicMock(all=lambda: [(999, {}), (1000, {})])),
            # get_hgvs_from_post_mapped is called twice, once for each mapped variant. mock that both
            # calls return valid HGVS strings.
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped",
                side_effect=["c.122G>C", "c.123A>T"],
            ),
            # validate_job_params is called to validate job parameters
            patch("mavedb.worker.jobs.external_services.clingen.validate_job_params", return_value=None),
            # update_progress is called multiple times to update job progress
            patch.object(mock_job_manager, "update_progress", return_value=None),
            # CAR_SUBMISSION_ENDPOINT is patched to a test URL
            patch(
                "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
                "https://reg.test.genome.network/pytest",
            ),
            # Mock the dispatch_submissions method to return a test ClinGen allele object, which we should associate with the variant
            patch.object(
                ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[TEST_CLINGEN_ALLELE_OBJECT]
            ),
            # Mock the get_allele_registry_associations function to return a mapping from HGVS to CAID
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_allele_registry_associations",
                return_value={"c.122G>C": "CAID:0000000", "c.123A>T": "CAID:0000001"},
            ),
            patch.object(mock_job_manager.db, "add", return_value=None) as mock_db_add,
        ):
            result = await submit_score_set_mappings_to_car(mock_worker_ctx, 1, job_manager=mock_job_manager)

        # Assert no CAID was not added to the variant
        mock_db_add.assert_not_called()
        assert mocked_mapped_variant_with_hgvs.clingen_allele_id is None
        assert result["status"] == "ok"

    async def test_submit_score_set_mappings_to_car_skips_submission_for_variants_without_hgvs_string(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
    ):
        """Test that submitting a score set with mapped variants completes successfully but skips variants without an HGVS string."""

        mock_job_run.job_params = {"score_set_id": 1, "correlation_id": uuid4().hex}

        mocked_score_set = MagicMock(spec=ScoreSetDbModel, urn="urn:1", num_variants=2)
        mocked_mapped_variant_with_hgvs = MagicMock(spec=MappedVariant, id=1000)

        with (
            # db.scalars is called twice in this function: once to get the score set (one), once to get the mapped variants (all)
            patch.object(
                mock_job_manager.db,
                "scalars",
                return_value=MagicMock(
                    one=mocked_score_set,
                    all=lambda: [mocked_mapped_variant_with_hgvs],
                ),
            ),
            # db.execute is called to get the mapped variant IDs and post mapped data
            patch.object(mock_job_manager.db, "execute", return_value=MagicMock(all=lambda: [(999, {}), (1000, {})])),
            # get_hgvs_from_post_mapped is called twice, once for each mapped variant. mock that the first
            # call returns None (no HGVS), the second returns a valid HGVS string.
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped",
                side_effect=[None, "c.123A>T"],
            ),
            # validate_job_params is called to validate job parameters
            patch("mavedb.worker.jobs.external_services.clingen.validate_job_params", return_value=None),
            # update_progress is called multiple times to update job progress
            patch.object(mock_job_manager, "update_progress", return_value=None),
            # CAR_SUBMISSION_ENDPOINT is patched to a test URL
            patch(
                "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
                "https://reg.test.genome.network/pytest",
            ),
            # Mock the dispatch_submissions method to return a test ClinGen allele object, which we should associate with the variant
            patch.object(
                ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[TEST_CLINGEN_ALLELE_OBJECT]
            ),
            # Mock the get_allele_registry_associations function to return a mapping from HGVS to CAID
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_allele_registry_associations",
                return_value={"c.123A>T": "CAID:0000001"},
            ),
            patch.object(mock_job_manager.db, "add", return_value=None) as mock_db_add,
        ):
            result = await submit_score_set_mappings_to_car(mock_worker_ctx, 1, job_manager=mock_job_manager)

        # Assert the variant without an HGVS string was skipped, and the other variant was updated with the CAID
        mock_db_add.assert_has_calls([call(mocked_mapped_variant_with_hgvs)])
        assert mocked_mapped_variant_with_hgvs.clingen_allele_id == "CAID:0000001"
        assert result["status"] == "ok"

    async def test_submit_score_set_mappings_to_car_success(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
    ):
        """Test that submitting a score set with mapped variants completes successfully."""

        mock_job_run.job_params = {"score_set_id": 1, "correlation_id": uuid4().hex}

        mocked_score_set = MagicMock(spec=ScoreSetDbModel, urn="urn:1", num_variants=2)
        mocked_mapped_variant_with_hgvs_999 = MagicMock(spec=MappedVariant, id=999)
        mocked_mapped_variant_with_hgvs_1000 = MagicMock(spec=MappedVariant, id=1000)

        with (
            # db.scalars is called three times in this function: once to get the score set (one), twice to get the mapped variants (all)
            patch.object(
                mock_job_manager.db,
                "scalars",
                return_value=MagicMock(
                    one=mocked_score_set,
                    all=MagicMock(
                        side_effect=[[mocked_mapped_variant_with_hgvs_999], [mocked_mapped_variant_with_hgvs_1000]]
                    ),
                ),
            ),
            # db.execute is called to get the mapped variant IDs and post mapped data
            patch.object(mock_job_manager.db, "execute", return_value=MagicMock(all=lambda: [(999, {}), (1000, {})])),
            # get_hgvs_from_post_mapped is called twice, once for each mapped variant. mock that both
            # calls return valid HGVS strings.
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped",
                side_effect=["c.122G>C", "c.123A>T"],
            ),
            # validate_job_params is called to validate job parameters
            patch("mavedb.worker.jobs.external_services.clingen.validate_job_params", return_value=None),
            # update_progress is called multiple times to update job progress
            patch.object(mock_job_manager, "update_progress", return_value=None),
            # CAR_SUBMISSION_ENDPOINT is patched to a test URL
            patch(
                "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
                "https://reg.test.genome.network/pytest",
            ),
            # Mock the dispatch_submissions method to return a test ClinGen allele object, which we should associate with the variant
            patch.object(
                ClinGenAlleleRegistryService,
                "dispatch_submissions",
                return_value=[TEST_CLINGEN_ALLELE_OBJECT, TEST_CLINGEN_ALLELE_OBJECT],
            ),
            # Mock the get_allele_registry_associations function to return a mapping from HGVS to CAID
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_allele_registry_associations",
                return_value={"c.122G>C": "CAID:0000000", "c.123A>T": "CAID:0000001"},
            ),
            patch.object(mock_job_manager.db, "add", return_value=None) as mock_db_add,
        ):
            result = await submit_score_set_mappings_to_car(mock_worker_ctx, 1, job_manager=mock_job_manager)

        # Assert the variant without an HGVS string was skipped, and the other variant was updated with the CAID
        mock_db_add.assert_has_calls(
            [call(mocked_mapped_variant_with_hgvs_999), call(mocked_mapped_variant_with_hgvs_1000)]
        )
        assert mocked_mapped_variant_with_hgvs_999.clingen_allele_id == "CAID:0000000"
        assert mocked_mapped_variant_with_hgvs_1000.clingen_allele_id == "CAID:0000001"
        assert result["status"] == "ok"

    async def test_submit_score_set_mappings_to_car_updates_progress(
        self,
        mock_job_manager,
        mock_job_run,
        mock_worker_ctx,
    ):
        """Test that submitting a score set with mapped variants updates progress correctly."""

        mock_job_run.job_params = {"score_set_id": 1, "correlation_id": uuid4().hex}

        mocked_score_set = MagicMock(spec=ScoreSetDbModel, urn="urn:1", num_variants=2)
        mocked_mapped_variant_with_hgvs_999 = MagicMock(spec=MappedVariant, id=999)
        mocked_mapped_variant_with_hgvs_1000 = MagicMock(spec=MappedVariant, id=1000)

        with (
            # db.scalars is called three times in this function: once to get the score set (one), twice to get the mapped variants (all)
            patch.object(
                mock_job_manager.db,
                "scalars",
                return_value=MagicMock(
                    one=mocked_score_set,
                    all=MagicMock(
                        side_effect=[[mocked_mapped_variant_with_hgvs_999], [mocked_mapped_variant_with_hgvs_1000]]
                    ),
                ),
            ),
            # db.execute is called to get the mapped variant IDs and post mapped data
            patch.object(mock_job_manager.db, "execute", return_value=MagicMock(all=lambda: [(999, {}), (1000, {})])),
            # get_hgvs_from_post_mapped is called twice, once for each mapped variant. mock that both
            # calls return valid HGVS strings.
            patch(
                "mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped",
                side_effect=["c.122G>C", "c.123A>T"],
            ),
            # validate_job_params is called to validate job parameters
            patch("mavedb.worker.jobs.external_services.clingen.validate_job_params", return_value=None),
            # update_progress is called multiple times to update job progress
            patch.object(mock_job_manager, "update_progress", return_value=None) as mock_update_progress,
            # CAR_SUBMISSION_ENDPOINT is patched to a test URL
            patch(
                "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
                "https://reg.test.genome.network/pytest",
            ),
            # Mock the dispatch_submissions method to return a test ClinGen allele object, which we should associate with the variant
            patch.object(
                ClinGenAlleleRegistryService,
                "dispatch_submissions",
                return_value=[TEST_CLINGEN_ALLELE_OBJECT],
            ),
        ):
            result = await submit_score_set_mappings_to_car(mock_worker_ctx, 1, job_manager=mock_job_manager)

        # Assert the variant without an HGVS string was skipped, and the other variant was updated with the CAID
        mock_update_progress.assert_has_calls(
            [
                call(0, 100, "Starting CAR mapped resource submission."),
                call(10, 100, "Preparing 2 mapped variants for CAR submission."),
                call(15, 100, "Submitting mapped variants to CAR."),
                call(50, 100, "Processing registered alleles from CAR."),
                call(100, 100, "Completed CAR mapped resource submission."),
            ]
        )
        assert result["status"] == "ok"


@pytest.mark.asyncio
@pytest.mark.integration
class TestSubmitScoreSetMappingsToCARIntegration:
    """Integration tests for the submit_score_set_mappings_to_car function."""

    @pytest.fixture()
    def setup_car_submission_job_run(self, session):
        """Add a submit_score_set_mappings_to_car job run to the DB before each test."""
        job_run = JobRun(
            job_type="external_service",
            job_function="submit_score_set_mappings_to_car",
            status=JobStatus.PENDING,
            job_params={"correlation_id": "test-corr-id"},
        )
        session.add(job_run)
        session.commit()
        return job_run

    async def test_submit_score_set_mappings_to_car_no_submission_endpoint(
        self,
        standalone_worker_context,
        session,
        with_populated_test_data,
        setup_car_submission_job_run,
        async_client,
        data_files,
        arq_redis,
    ):
        """Test that submitting a score set with no CAR submission endpoint configured raises an exception."""
        score_set = await setup_records_files_and_variants_with_mapping(
            session,
            async_client,
            data_files,
            TEST_MINIMAL_SEQ_SCORESET,
            standalone_worker_context,
        )

        with patch(
            "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
            None,
        ):
            with pytest.raises(ValueError):
                await submit_score_set_mappings_to_car(
                    standalone_worker_context,
                    score_set.id,
                    JobManager(
                        session,
                        arq_redis,
                        setup_car_submission_job_run.id,
                    ),
                )
