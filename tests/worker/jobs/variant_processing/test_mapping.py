from asyncio.unix_events import _UnixSelectorEventLoop
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy.exc import NoResultFound

from mavedb.lib.exceptions import (
    NonexistentMappingReferenceError,
    NonexistentMappingResultsError,
    NonexistentMappingScoresError,
)
from mavedb.lib.mapping import EXCLUDED_PREMAPPED_ANNOTATION_KEYS
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.worker.jobs.variant_processing.mapping import map_variants_for_score_set
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.constants import TEST_CODING_LAYER, TEST_GENOMIC_LAYER, TEST_PROTEIN_LAYER
from tests.helpers.util.setup.worker import construct_mock_mapping_output, create_variants_in_score_set

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.unit
@pytest.mark.asyncio
class TestMapVariantsForScoreSetUnit:
    """Unit tests for map_variants_for_score_set job."""

    async def dummy_mapping_output(self, output_data={}):
        return output_data

    async def test_map_variants_for_score_set_no_mapping_results(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when no mapping results are found."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=self.dummy_mapping_output({})),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(NonexistentMappingResultsError),
        ):
            await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        mock_update_progress.assert_any_call(100, 100, "Variant mapping failed due to missing results.")

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert (
            "Mapping results were not returned from VRS mapping service"
            in sample_score_set.mapping_errors["error_message"]
        )

    async def test_map_variants_for_score_set_no_mapped_scores(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when no scores are mapped."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=self.dummy_mapping_output(
                    {"mapped_scores": [], "error_message": "No variants were mapped for this score set"}
                ),
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(NonexistentMappingScoresError),
        ):
            await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        mock_update_progress.assert_any_call(100, 100, "Variant mapping failed; no variants were mapped.")

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert "No variants were mapped for this score set" in sample_score_set.mapping_errors["error_message"]

    async def test_map_variants_for_score_set_no_reference_data(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when no reference data is available."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=self.dummy_mapping_output(
                    {"mapped_scores": [MagicMock()], "error_message": "Reference metadata missing from mapping results"}
                ),
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(NonexistentMappingReferenceError),
        ):
            await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        mock_update_progress.assert_any_call(100, 100, "Variant mapping failed due to missing reference metadata.")

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert "Reference metadata missing from mapping results" in sample_score_set.mapping_errors["error_message"]

    async def test_map_variants_for_score_set_nonexistent_target_gene(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when the target gene does not exist."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=self.dummy_mapping_output(
                    {
                        "mapped_scores": [MagicMock()],
                        "reference_sequences": {"some_key": "some_value"},
                    }
                ),
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(ValueError),
        ):
            await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        mock_update_progress.assert_any_call(100, 100, "Variant mapping failed due to an unexpected error.")

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

    async def test_map_variants_for_score_set_returns_variants_not_in_score_set(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when variants not in score set are returned."""
        # Add a non-existent variant to the mapped output to ensure at least one invalid mapping
        mapping_output = await construct_mock_mapping_output(
            session=session, score_set=sample_score_set, with_layers={"g", "c", "p"}
        )
        mapping_output["mapped_scores"].append({"variant_id": "not_in_score_set", "some_other_data": "value"})

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=self.dummy_mapping_output(mapping_output),
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(NoResultFound),
        ):
            await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        mock_update_progress.assert_any_call(100, 100, "Variant mapping failed due to an unexpected error.")

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

    async def test_map_variants_for_score_set_success_missing_gene_info(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test successful mapping variants with missing gene info."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=False,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Create a variant in the score set to be mapped
        variant = Variant(
            score_set_id=sample_score_set.id, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={}
        )
        session.add(variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert result["status"] == "ok"
        assert result["data"] == {}
        assert result["exception_details"] is None

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify the gene info is missing from the target gene reference sequence
        for target in sample_score_set.target_genes:
            assert target.mapped_hgnc_name is None

        # Verify that a mapped variant was created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 1

    @pytest.mark.parametrize(
        "with_layers",
        [
            {"g"},
            {"c"},
            {"p"},
            {"g", "c"},
            {"g", "p"},
            {"c", "p"},
            {"g", "c", "p"},
        ],
    )
    async def test_map_variants_for_score_set_success_layer_permutations(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        with_layers,
    ):
        """Test successful mapping variants with annotation layer permutations."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers=with_layers,
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Create a variant in the score set to be mapped
        variant = Variant(
            score_set_id=sample_score_set.id, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={}
        )
        session.add(variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert result["status"] == "ok"
        assert result["data"] == {}
        assert result["exception_details"] is None

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify the annotation layers presence/absence
        for target in sample_score_set.target_genes:
            if "g" in with_layers:
                assert target.pre_mapped_metadata["genomic"] is not None
                assert target.post_mapped_metadata["genomic"] is not None
                pre_mapped_comparator = TEST_GENOMIC_LAYER["computed_reference_sequence"].copy()
                for key in EXCLUDED_PREMAPPED_ANNOTATION_KEYS:
                    pre_mapped_comparator.pop(key, None)

                assert target.pre_mapped_metadata["genomic"] == pre_mapped_comparator
                assert target.post_mapped_metadata["genomic"] == TEST_GENOMIC_LAYER["mapped_reference_sequence"]
            else:
                assert target.post_mapped_metadata.get("genomic") is None

            if "c" in with_layers:
                assert target.pre_mapped_metadata["cdna"] is not None
                assert target.post_mapped_metadata["cdna"] is not None
                pre_mapped_comparator = TEST_CODING_LAYER["computed_reference_sequence"].copy()
                for key in EXCLUDED_PREMAPPED_ANNOTATION_KEYS:
                    pre_mapped_comparator.pop(key, None)

                assert target.pre_mapped_metadata["cdna"] == pre_mapped_comparator
                assert target.post_mapped_metadata["cdna"] == TEST_CODING_LAYER["mapped_reference_sequence"]
            else:
                assert target.post_mapped_metadata.get("cdna") is None

            if "p" in with_layers:
                assert target.pre_mapped_metadata["protein"] is not None
                assert target.post_mapped_metadata["protein"] is not None
                pre_mapped_comparator = TEST_PROTEIN_LAYER["computed_reference_sequence"].copy()
                for key in EXCLUDED_PREMAPPED_ANNOTATION_KEYS:
                    pre_mapped_comparator.pop(key, None)

                assert target.pre_mapped_metadata["protein"] == pre_mapped_comparator
                assert target.post_mapped_metadata["protein"] == TEST_PROTEIN_LAYER["mapped_reference_sequence"]
            else:
                assert target.post_mapped_metadata.get("protein") is None

        # Verify that a mapped variant was created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 1

    async def test_map_variants_for_score_set_success_no_successful_mapping(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test successful mapping variants with no successful mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=False,  # Missing post-mapped
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Create a variant in the score set to be mapped
        variant = Variant(
            score_set_id=sample_score_set.id, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={}
        )
        session.add(variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert result["status"] == "error"
        assert result["data"] == {}
        assert result["exception_details"] is None

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors["error_message"] == "All variants failed to map."

        # Verify that one mapped variant was created. Although no successful mapping, an entry is still created.
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 1

        # Verify that the mapped variant has no post-mapped data
        mapped_variant = mapped_variants[0]
        assert mapped_variant.post_mapped == {}

    async def test_map_variants_for_score_set_incomplete_mapping(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test successful mapping variants with incomplete mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=False,  # Only some variants mapped
            )

        # Create two variants in the score set to be mapped
        variant1 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
            urn="variant:1",
        )
        variant2 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.2G>T",
            hgvs_pro="NP_000000.1:p.Val2Leu",
            data={},
            urn="variant:2",
        )
        session.add_all([variant1, variant2])
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert result["status"] == "ok"
        assert result["data"] == {}
        assert result["exception_details"] is None

        assert sample_score_set.mapping_state == MappingState.incomplete
        assert sample_score_set.mapping_errors is None

        # Although only one variant was successfully mapped, verify that an entity was created
        # for each variant in the score set
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 2

        # Verify that only one variant has post-mapped data
        mapped_variant_with_post_data = (
            session.query(MappedVariant).filter(MappedVariant.post_mapped != {}).one_or_none()
        )
        assert mapped_variant_with_post_data is not None

        mapped_variant_without_post_data = (
            session.query(MappedVariant).filter(MappedVariant.post_mapped == {}).one_or_none()
        )
        assert mapped_variant_without_post_data is not None

    async def test_map_variants_for_score_set_complete_mapping(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test successful mapping variants with complete mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,  # All variants mapped
            )

        # Create two variants in the score set to be mapped
        variant1 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
            urn="variant:1",
        )
        variant2 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.2G>T",
            hgvs_pro="NP_000000.1:p.Val2Leu",
            data={},
            urn="variant:2",
        )
        session.add_all([variant1, variant2])
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert result["status"] == "ok"
        assert result["data"] == {}
        assert result["exception_details"] is None

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that mapped variants were created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 2

        # Verify that both variants have post-mapped data. I'm comfortable assuming the
        # data is correct given our layer permutation tests above.
        for urn in ["variant:1", "variant:2"]:
            mapped_variant = session.query(MappedVariant).filter(MappedVariant.variant.has(urn=urn)).one_or_none()
            assert mapped_variant is not None
            assert mapped_variant.post_mapped != {}

    async def test_map_variants_for_score_set_updates_existing_mapped_variants(
        self,
        with_independent_processing_runs,
        session,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants updates existing mapped variants."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Create a variant and associated mapped data in the score set to be updated
        variant = Variant(
            score_set_id=sample_score_set.id, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={}
        )
        session.add(variant)
        session.commit()
        mapped_variant = MappedVariant(
            variant_id=variant.id,
            current=True,
            mapped_date="2023-01-01T00:00:00Z",
            mapping_api_version="v1.0.0",
        )
        session.add(mapped_variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert result["status"] == "ok"
        assert result["data"] == {}
        assert result["exception_details"] is None

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify the existing mapped variant was marked as non-current
        non_current_mapped_variant = (
            session.query(MappedVariant)
            .filter(MappedVariant.id == mapped_variant.id, MappedVariant.current.is_(False))
            .one_or_none()
        )
        assert non_current_mapped_variant is not None

        # Verify a new mapped variant entry was created
        new_mapped_variant = (
            session.query(MappedVariant)
            .filter(MappedVariant.variant_id == variant.id, MappedVariant.current.is_(True))
            .one_or_none()
        )
        assert new_mapped_variant is not None

        # Verify that the new mapped variant has updated mapping data
        assert new_mapped_variant.mapped_date != "2023-01-01T00:00:00Z"
        assert new_mapped_variant.mapping_api_version != "v1.0.0"

    async def test_map_variants_for_score_set_progress_updates(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants reports progress updates."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Create a variant in the score set to be mapped
        variant = Variant(
            score_set_id=sample_score_set.id, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={}
        )
        session.add(variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert result["status"] == "ok"
        assert result["data"] == {}
        assert result["exception_details"] is None

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify progress updates were reported
        mock_update_progress.assert_has_calls(
            [
                call(0, 100, "Starting variant mapping job."),
                call(10, 100, "Score set prepared for variant mapping."),
                call(30, 100, "Mapping variants using VRS mapping service."),
                call(80, 100, "Processing mapped variants."),
                call(90, 100, "Saving mapped variants."),
                call(100, 100, "Finished processing mapped variants."),
            ]
        )


@pytest.mark.integration
@pytest.mark.asyncio
class TestMapVariantsForScoreSetIntegration:
    """Integration tests for map_variants_for_score_set job."""

    async def test_map_variants_for_score_set_independent_job(
        self,
        session,
        with_independent_processing_runs,
        mock_s3_client,
        mock_worker_ctx,
        sample_independent_variant_creation_run,
        sample_independent_variant_mapping_run,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
    ):
        """Test mapping variants for an independent processing run."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Mock mapping output
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            # Now, map variants for the score set
            result = await map_variants_for_score_set(mock_worker_ctx, sample_independent_variant_mapping_run.id)

        assert result["status"] == "ok"
        assert result["data"] == {}
        assert result["exception_details"] is None

        # Verify that mapped variants were created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 4

        # Verify score set mapping state
        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that target gene info was updated
        for target in sample_score_set.target_genes:
            assert target.mapped_hgnc_name is not None
            assert target.post_mapped_metadata is not None

        # Verify that each variant has a corresponding mapped variant
        variants = (
            session.query(Variant)
            .join(MappedVariant, MappedVariant.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, MappedVariant.current.is_(True))
            .all()
        )
        assert len(variants) == 4

        # Verify that the job status was updated
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

    async def test_map_variants_for_score_set_pipeline_context(
        self,
        session,
        with_variant_creation_pipeline_runs,
        with_variant_mapping_pipeline_runs,
        mock_s3_client,
        mock_worker_ctx,
        sample_pipeline_variant_creation_run,
        sample_pipeline_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
    ):
        """Test mapping variants for a pipeline processing run."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_pipeline_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Mock mapping output
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            # Now, map variants for the score set
            result = await map_variants_for_score_set(mock_worker_ctx, sample_pipeline_variant_mapping_run.id)

        assert result["status"] == "ok"
        assert result["data"] == {}
        assert result["exception_details"] is None

        # Verify that mapped variants were created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 4

        # Verify score set mapping state
        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that target gene info was updated
        for target in sample_score_set.target_genes:
            assert target.mapped_hgnc_name is not None
            assert target.post_mapped_metadata is not None

        # Verify that each variant has a corresponding mapped variant
        variants = (
            session.query(Variant)
            .join(MappedVariant, MappedVariant.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, MappedVariant.current.is_(True))
            .all()
        )
        assert len(variants) == 4

        # Verify that the job status was updated
        processing_run = (
            session.query(sample_pipeline_variant_mapping_run.__class__)
            .filter(sample_pipeline_variant_mapping_run.__class__.id == sample_pipeline_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

        # Verify that the pipeline run status was updated. We expect RUNNING here because
        # the mapping job is not the only job in our dummy pipeline.
        pipeline_run = (
            session.query(sample_pipeline_variant_mapping_run.pipeline.__class__)
            .filter(
                sample_pipeline_variant_mapping_run.pipeline.__class__.id
                == sample_pipeline_variant_mapping_run.pipeline.id
            )
            .one()
        )
        assert pipeline_run.status == PipelineStatus.RUNNING

    async def test_map_variants_for_score_set_empty_mapping_results(
        self,
        session,
        mock_s3_client,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_independent_variant_creation_run,
    ):
        """Test mapping variants when no mapping results are returned."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return {}

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=dummy_mapping_job()),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        assert result["status"] == "failed"
        assert result["exception_details"]["type"] == "NonexistentMappingResultsError"
        assert result["data"] == {}

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert (
            "Mapping results were not returned from VRS mapping service"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify that no mapped variants were created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

    async def test_map_variants_for_score_set_no_mapped_scores(
        self,
        session,
        mock_s3_client,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_independent_variant_creation_run,
    ):
        """Test mapping variants when no variants are mapped."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=False,
                with_reference_metadata=True,
                with_mapped_scores=False,  # No mapped scores
                with_all_variants=True,
            )

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        assert result["status"] == "failed"
        assert result["exception_details"]["type"] == "NonexistentMappingScoresError"
        assert result["data"] == {}

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        # Error message originates from our mock mapping construction function
        assert "test error: no mapped scores" in sample_score_set.mapping_errors["error_message"]

        # Verify that no mapped variants were created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

    async def test_map_variants_for_score_set_no_reference_data(
        self,
        session,
        mock_s3_client,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_independent_variant_creation_run,
    ):
        """Test mapping variants when no reference data is provided."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=False,  # No reference metadata
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        assert result["status"] == "failed"
        assert result["exception_details"]["type"] == "NonexistentMappingReferenceError"
        assert result["data"] == {}

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert "Reference metadata missing from mapping results" in sample_score_set.mapping_errors["error_message"]

        # Verify that no mapped variants were created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

    async def test_map_variants_for_score_set_updates_current_mapped_variants(
        self,
        session,
        mock_s3_client,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_independent_variant_creation_run,
    ):
        """Test mapping variants updates current mapped variants even if no changes occur."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        # Associate mapped variants with all variants just created in the score set
        variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        for variant in variants:
            mapped_variant = MappedVariant(
                variant_id=variant.id,
                current=True,
                mapped_date="2023-01-01T00:00:00Z",
                mapping_api_version="v1.0.0",
            )
            session.add(mapped_variant)
        session.commit()

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        assert result["status"] == "ok"
        assert result["data"] == {}
        assert result["exception_details"] is None

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that mapped variants were marked as non-current and new entries created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == len(variants) * 2  # Each variant has two mapped entries now
        for variant in variants:
            non_current_mapped_variant = (
                session.query(MappedVariant)
                .filter(MappedVariant.variant_id == variant.id, MappedVariant.current.is_(False))
                .one_or_none()
            )
            assert non_current_mapped_variant is not None

            new_mapped_variant = (
                session.query(MappedVariant)
                .filter(MappedVariant.variant_id == variant.id, MappedVariant.current.is_(True))
                .one_or_none()
            )
            assert new_mapped_variant is not None

            # Verify that the new mapped variant has updated mapping data
            assert new_mapped_variant.mapped_date != "2023-01-01T00:00:00Z"
            assert new_mapped_variant.mapping_api_version != "v1.0.0"

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

    async def test_map_variants_for_score_set_no_variants(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when no variants exist in the score set."""

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        assert result["status"] == "failed"
        assert result["data"] == {}
        assert result["exception_details"] is not None
        assert result["exception_details"]["type"] == "NonexistentMappingScoresError"

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert "test error: no mapped scores" in sample_score_set.mapping_errors["error_message"]

        # Verify that no mapped variants were created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

    async def test_map_variants_for_score_set_exception_in_mapping(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when an exception occurs during mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            raise ValueError("test exception during mapping")

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        assert result["status"] == "failed"
        assert result["data"] == {}
        assert result["exception_details"]["type"] == "ValueError"
        # exception messages are persisted in internal properties
        assert "test exception during mapping" in result["exception_details"]["message"]

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        # but replaced with generic error message for external visibility
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify that no mapped variants were created
        mapped_variants = session.query(MappedVariant).all()
        assert len(mapped_variants) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED


@pytest.mark.integration
@pytest.mark.asyncio
class TestMapVariantsForScoreSetArqContext:
    """Integration tests for map_variants_for_score_set job using ARQ worker context."""

    async def test_create_variants_for_score_set_with_arq_context_independent_ctx(
        self,
        session,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
        sample_independent_variant_mapping_run,
    ):
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            standalone_worker_context,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=standalone_worker_context["db"],
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            await arq_redis.enqueue_job("map_variants_for_score_set", sample_independent_variant_mapping_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that mapped variants were created
        mapped_variants = standalone_worker_context["db"].query(MappedVariant).all()
        assert len(mapped_variants) == 4

        # Verify score set mapping state
        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that each variant has a corresponding mapped variant
        variants = (
            standalone_worker_context["db"]
            .query(Variant)
            .join(MappedVariant, MappedVariant.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, MappedVariant.current.is_(True))
            .all()
        )
        assert len(variants) == 4

        # Verify that the job status was updated
        processing_run = (
            standalone_worker_context["db"]
            .query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

    async def test_map_variants_for_score_set_with_arq_context_pipeline_ctx(
        self,
        session,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        with_variant_creation_pipeline_runs,
        with_variant_mapping_pipeline_runs,
        with_populated_domain_data,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_pipeline_variant_creation_run,
        sample_pipeline_variant_mapping_run,
    ):
        """Test mapping variants for a pipeline processing run using ARQ context."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            standalone_worker_context,
            sample_pipeline_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=standalone_worker_context["db"],
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Mock mapping output
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            # Now, map variants for the score set
            await arq_redis.enqueue_job("map_variants_for_score_set", sample_pipeline_variant_mapping_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that mapped variants were created
        mapped_variants = standalone_worker_context["db"].query(MappedVariant).all()
        assert len(mapped_variants) == 4

        # Verify score set mapping state
        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that each variant has a corresponding mapped variant
        variants = (
            standalone_worker_context["db"]
            .query(Variant)
            .join(MappedVariant, MappedVariant.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, MappedVariant.current.is_(True))
            .all()
        )
        assert len(variants) == 4

        # Verify that the job status was updated
        processing_run = (
            standalone_worker_context["db"]
            .query(sample_pipeline_variant_mapping_run.__class__)
            .filter(sample_pipeline_variant_mapping_run.__class__.id == sample_pipeline_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

        # Verify that the pipeline run status was updated. We expect RUNNING here because
        # the mapping job is not the only job in our dummy pipeline.
        pipeline_run = (
            standalone_worker_context["db"]
            .query(sample_pipeline_variant_mapping_run.pipeline.__class__)
            .filter(
                sample_pipeline_variant_mapping_run.pipeline.__class__.id
                == sample_pipeline_variant_mapping_run.pipeline.id
            )
            .one()
        )
        assert pipeline_run.status == PipelineStatus.RUNNING

    async def test_map_variants_for_score_set_with_arq_context_generic_exception_handling(
        self,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        with_independent_processing_runs,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants with ARQ context when an exception occurs during mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            raise ValueError("test exception during mapping")

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            await arq_redis.enqueue_job("map_variants_for_score_set", sample_independent_variant_mapping_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        # but replaced with generic error message for external visibility
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify that no mapped variants were created
        mapped_variants = standalone_worker_context["db"].query(MappedVariant).all()
        assert len(mapped_variants) == 0

        # Verify that the job status was updated.
        processing_run = (
            standalone_worker_context["db"]
            .query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

    async def test_map_variants_for_score_set_with_arq_context_generic_exception_in_pipeline_ctx(
        self,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        with_variant_mapping_pipeline_runs,
        sample_pipeline_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants with ARQ context in pipeline when an exception occurs during mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            raise ValueError("test exception during mapping")

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            await arq_redis.enqueue_job("map_variants_for_score_set", sample_pipeline_variant_mapping_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        # but replaced with generic error message for external visibility
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify that no mapped variants were created
        mapped_variants = standalone_worker_context["db"].query(MappedVariant).all()
        assert len(mapped_variants) == 0

        # Verify that the job status was updated.
        processing_run = (
            standalone_worker_context["db"]
            .query(sample_pipeline_variant_mapping_run.__class__)
            .filter(sample_pipeline_variant_mapping_run.__class__.id == sample_pipeline_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

        # Verify that the pipeline run status was updated to FAILED.
        pipeline_run = (
            standalone_worker_context["db"]
            .query(sample_pipeline_variant_mapping_run.pipeline.__class__)
            .filter(
                sample_pipeline_variant_mapping_run.pipeline.__class__.id
                == sample_pipeline_variant_mapping_run.pipeline.id
            )
            .one()
        )
        assert pipeline_run.status == PipelineStatus.FAILED

        # Verify that other jobs in the pipeline were skipped
        for job_run in pipeline_run.job_runs:
            if job_run.id != sample_pipeline_variant_mapping_run.id:
                assert job_run.status == JobStatus.SKIPPED
