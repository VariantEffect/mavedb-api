import math
from unittest.mock import ANY, MagicMock, call, patch

import pytest

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.models.variant import Variant
from mavedb.worker.jobs.variant_processing.creation import create_variants_for_score_set
from mavedb.worker.lib.managers.job_manager import JobManager


@pytest.mark.unit
@pytest.mark.asyncio
class TestCreateVariantsForScoreSetUnit:
    """Unit tests for create_variants_for_score_set job."""

    async def test_create_variants_for_score_set_raises_key_error_on_missing_hdp_from_ctx(
        self,
        mock_job_manager,
    ):
        ctx = {}  # Missing 'hdp' key

        with pytest.raises(KeyError) as exc_info:
            await create_variants_for_score_set(ctx=ctx, job_id=999, job_manager=mock_job_manager)

        assert str(exc_info.value) == "'hdp'"

    async def test_create_variants_for_score_set_calls_s3_client_with_correct_parameters(
        self,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None) as mock_download_fileobj,
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                return_value=(
                    sample_score_dataframe,
                    sample_count_dataframe,
                    create_variants_sample_params["score_columns_metadata"],
                    create_variants_sample_params["count_columns_metadata"],
                ),
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.create_variants_data",
                return_value=[MagicMock(spec=Variant)],
            ),
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants", return_value=None),
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        # Use ANY for dynamically created Fileobj parameters.
        mock_download_fileobj.assert_has_calls(
            [
                call(Bucket="score-set-csv-uploads-dev", Key="sample_scores.csv", Fileobj=ANY),
                call(Bucket="score-set-csv-uploads-dev", Key="sample_counts.csv", Fileobj=ANY),
            ]
        )

    async def test_create_variants_for_score_set_s3_file_not_found(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(
                mock_s3_client,
                "download_fileobj",
                side_effect=Exception("The specified key does not exist."),
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(Exception) as exc_info,
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        mock_update_progress.assert_any_call(100, 100, "Variant creation job failed due to an internal error.")
        assert str(exc_info.value) == "The specified key does not exist."
        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.failed
        assert sample_score_set.mapping_state == MappingState.not_attempted

    async def test_create_variants_for_score_set_counts_file_can_be_optional(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        # Remove counts_file_key to test optional behavior
        create_variants_sample_params_without_counts = create_variants_sample_params.copy()
        create_variants_sample_params_without_counts["counts_file_key"] = None
        create_variants_sample_params_without_counts["count_columns_metadata"] = None
        sample_independent_variant_creation_run.job_params = create_variants_sample_params_without_counts
        session.add(sample_independent_variant_creation_run)
        session.commit()

        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample score dataframe only
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                return_value=(
                    sample_score_dataframe,
                    None,
                    create_variants_sample_params_without_counts["score_columns_metadata"],
                    None,
                ),
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.create_variants_data",
                return_value=[MagicMock(spec=Variant)],
            ),
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants", return_value=None),
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

    async def test_create_variants_for_score_set_raises_when_no_targets_exist(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        # Remove all TargetGene entries to simulate no targets existing
        sample_score_set.target_genes = []
        session.commit()

        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(ValueError) as exc_info,
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        mock_update_progress.assert_any_call(100, 100, "Score set has no targets; cannot create variants.")
        assert str(exc_info.value) == "Can't create variants when score set has no targets."

    async def test_create_variants_for_score_set_calls_validate_standardize_dataframe_with_correct_parameters(
        self,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                return_value=(
                    sample_score_dataframe,
                    sample_count_dataframe,
                    create_variants_sample_params["score_columns_metadata"],
                    create_variants_sample_params["count_columns_metadata"],
                ),
            ) as mock_validate,
            patch(
                "mavedb.worker.jobs.variant_processing.creation.create_variants_data",
                return_value=[MagicMock(spec=Variant)],
            ),
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants", return_value=None),
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        mock_validate.assert_called_once_with(
            scores_df=sample_score_dataframe,
            counts_df=sample_count_dataframe,
            score_columns_metadata=create_variants_sample_params["score_columns_metadata"],
            count_columns_metadata=create_variants_sample_params["count_columns_metadata"],
            targets=sample_score_set.target_genes,
            hdp=mock_worker_ctx["hdp"],
        )

    async def test_create_variants_for_score_set_calls_create_variants_data_with_correct_parameters(
        self,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                return_value=(
                    sample_score_dataframe,
                    sample_count_dataframe,
                    create_variants_sample_params["score_columns_metadata"],
                    create_variants_sample_params["count_columns_metadata"],
                ),
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.create_variants_data",
                return_value=[MagicMock(spec=Variant)],
            ) as mock_create_variants_data,
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants", return_value=None),
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        mock_create_variants_data.assert_called_once_with(sample_score_dataframe, sample_count_dataframe, None)

    async def test_create_variants_for_score_set_calls_create_variants_with_correct_parameters(
        self,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        mock_variant = MagicMock(spec=Variant)
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                return_value=(
                    sample_score_dataframe,
                    sample_count_dataframe,
                    create_variants_sample_params["score_columns_metadata"],
                    create_variants_sample_params["count_columns_metadata"],
                ),
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.create_variants_data",
                return_value=[mock_variant],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.create_variants",
                return_value=None,
            ) as mock_create_variants,
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        mock_create_variants.assert_called_once_with(mock_worker_ctx["db"], sample_score_set, [mock_variant])

    async def test_create_variants_for_score_set_handles_empty_variant_data(
        self,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                return_value=(
                    sample_score_dataframe,
                    sample_count_dataframe,
                    create_variants_sample_params["score_columns_metadata"],
                    create_variants_sample_params["count_columns_metadata"],
                ),
            ),
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants_data", return_value=[]),
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants", return_value=None),
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )
        # If no exceptions are raised, the test passes for handling empty variant data.

    async def test_create_variants_for_score_set_removes_existing_variants_before_creation(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        # Add existing variants to the score set to test removal
        sample_score_set.num_variants = 1
        variant = Variant(data={}, score_set_id=sample_score_set.id)
        session.add(variant)
        session.commit()

        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                return_value=(
                    sample_score_dataframe,
                    sample_count_dataframe,
                    create_variants_sample_params["score_columns_metadata"],
                    create_variants_sample_params["count_columns_metadata"],
                ),
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.create_variants_data",
                return_value=[MagicMock(spec=Variant)],
            ),
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants", return_value=None),
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        # Verify that existing variants have been removed
        remaining_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(remaining_variants) == 0
        session.refresh(sample_score_set)
        assert sample_score_set.num_variants == 0  # Updated after creation

    async def test_create_variants_for_score_set_updates_processing_state(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                return_value=(
                    sample_score_dataframe,
                    sample_count_dataframe,
                    create_variants_sample_params["score_columns_metadata"],
                    create_variants_sample_params["count_columns_metadata"],
                ),
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.create_variants_data",
                return_value=[MagicMock(spec=Variant)],
            ),
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants", return_value=None),
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.success
        assert sample_score_set.mapping_state == MappingState.queued
        assert sample_score_set.processing_errors is None

    async def test_create_variants_for_score_set_updates_progress(
        self,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                return_value=(
                    sample_score_dataframe,
                    sample_count_dataframe,
                    create_variants_sample_params["score_columns_metadata"],
                    create_variants_sample_params["count_columns_metadata"],
                ),
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.create_variants_data",
                return_value=[MagicMock(spec=Variant)],
            ),
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants", return_value=None),
            patch.object(JobManager, "update_progress") as mock_update_progress,
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        mock_update_progress.assert_has_calls(
            [
                call(0, 100, "Starting variant creation job."),
                call(10, 100, "Validated score set metadata and beginning data validation."),
                call(80, 100, "Data validation complete; creating variants in database."),
                call(100, 100, "Completed variant creation job."),
            ]
        )

    async def test_create_variants_for_score_set_retains_existing_variants_when_exception_occurs(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        # Add existing variants to the score set to test retention on failure
        sample_score_set.num_variants = 1
        variant = Variant(data={}, score_set_id=sample_score_set.id)
        session.add(variant)
        session.commit()

        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                side_effect=Exception("Test exception during data validation"),
            ),
            pytest.raises(Exception) as exc_info,
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        assert str(exc_info.value) == "Test exception during data validation"

        # Verify that existing variants are still present
        remaining_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(remaining_variants) == 1
        session.refresh(sample_score_set)
        assert sample_score_set.num_variants == 1  # Should remain unchanged

    async def test_create_variants_for_score_set_handles_exception_and_updates_state(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                side_effect=Exception("Test exception during data validation"),
            ),
            patch.object(JobManager, "update_progress") as mock_update_progress,
            pytest.raises(Exception) as exc_info,
        ):
            await create_variants_for_score_set(
                ctx=mock_worker_ctx,
                job_id=sample_independent_variant_creation_run.id,
                job_manager=JobManager(
                    mock_worker_ctx["db"], mock_worker_ctx["redis"], sample_independent_variant_creation_run.id
                ),
            )

        assert str(exc_info.value) == "Test exception during data validation"

        # Verify that the score set's processing state is updated to failed
        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.failed
        assert sample_score_set.mapping_state == MappingState.not_attempted
        assert "Test exception during data validation" in sample_score_set.processing_errors["exception"]
        mock_update_progress.assert_any_call(100, 100, "Variant creation job failed due to an internal error.")


@pytest.mark.integration
@pytest.mark.asyncio
class TestCreateVariantsForScoreSetIntegration:
    """Integration tests for create_variants_for_score_set job."""

    ## Common success workflows

    async def test_create_variants_for_score_set_independent_job(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            # Assume the S3 client works as expected.
            #
            # Moto is omitted here for brevity since this
            # function doesn't have S3 side effects. We assume the file is already in S3 for this test,
            # and any cases where the file is not present will be handled by the job manager and tested
            # in unit tests.
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes.
            #
            # A side effect of not mocking S3 more thoroughly
            # is that our S3 download has no return value and just side effects data into a file-like object,
            # so we mock pd.read_csv directly to avoid it trying to read from an empty file.
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        # Verify that variants have been created in the database
        created_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(created_variants) == sample_score_dataframe.shape[0]
        session.refresh(sample_score_set)
        assert sample_score_set.num_variants == len(created_variants)
        assert sample_score_set.processing_state == ProcessingState.success
        assert sample_score_set.mapping_state == MappingState.queued

        # Verify that the created variants have expected data
        for variant in created_variants:
            assert variant.data  # Ensure data is not empty
            assert "score_data" in variant.data  # Ensure score_data is present
            expected_score = sample_score_dataframe.loc[
                sample_score_dataframe["hgvs_nt"] == variant.hgvs_nt, "score"
            ].values[0]
            actual_score = variant.data["score_data"]["score"]
            if actual_score is None and (isinstance(expected_score, float) and math.isnan(expected_score)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_score == expected_score  # Ensure score matches
            assert "count_data" in variant.data  # Ensure count_data is present
            expected_count = sample_count_dataframe.loc[
                sample_count_dataframe["hgvs_nt"] == variant.hgvs_nt, "c_0"
            ].values[0]
            actual_count = variant.data["count_data"]["c_0"]
            if actual_count is None and (isinstance(expected_count, float) and math.isnan(expected_count)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_count == expected_count  # Ensure count matches

        # Verify that no extra variants were created
        all_variants = session.query(Variant).all()
        assert len(all_variants) == len(created_variants)

        # Verify that job state is as expected
        job_run = (
            session.query(sample_independent_variant_creation_run.__class__)
            .filter(sample_independent_variant_creation_run.__class__.id == sample_independent_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.SUCCEEDED

    async def test_create_variants_for_score_set_pipeline_job(
        self,
        session,
        with_variant_creation_pipeline_runs,
        sample_variant_creation_pipeline,
        sample_pipeline_variant_creation_run,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes.
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_pipeline_variant_creation_run.id)

        # Verify that variants have been created in the database
        created_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(created_variants) == sample_score_dataframe.shape[0]
        session.refresh(sample_score_set)
        assert sample_score_set.num_variants == len(created_variants)
        assert sample_score_set.processing_state == ProcessingState.success
        assert sample_score_set.mapping_state == MappingState.queued

        # Verify that the created variants have expected data
        for variant in created_variants:
            assert variant.data  # Ensure data is not empty
            assert "score_data" in variant.data  # Ensure score_data is present
            expected_score = sample_score_dataframe.loc[
                sample_score_dataframe["hgvs_nt"] == variant.hgvs_nt, "score"
            ].values[0]
            actual_score = variant.data["score_data"]["score"]
            if actual_score is None and (isinstance(expected_score, float) and math.isnan(expected_score)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_score == expected_score  # Ensure score matches
            assert "count_data" in variant.data  # Ensure count_data is present
            expected_count = sample_count_dataframe.loc[
                sample_count_dataframe["hgvs_nt"] == variant.hgvs_nt, "c_0"
            ].values[0]
            actual_count = variant.data["count_data"]["c_0"]
            if actual_count is None and (isinstance(expected_count, float) and math.isnan(expected_count)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_count == expected_count  # Ensure count matches

        # Verify that no extra variants were created
        all_variants = session.query(Variant).all()
        assert len(all_variants) == len(created_variants)

        # Verify that pipeline job state is as expected
        job_run = (
            session.query(sample_pipeline_variant_creation_run.__class__)
            .filter(sample_pipeline_variant_creation_run.__class__.id == sample_pipeline_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.SUCCEEDED

        # Verify that pipeline status is updated. Pipeline will remain RUNNING
        # as our default test pipeline includes the mapping job as well.
        session.refresh(sample_variant_creation_pipeline)
        assert sample_variant_creation_pipeline.status == PipelineStatus.RUNNING

    ## Common edge cases

    async def test_create_variants_for_score_set_replaces_variants(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        # First run to create initial variants
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        initial_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(initial_variants) == sample_score_dataframe.shape[0]

        # Modify dataframes to simulate updated data
        updated_score_dataframe = sample_score_dataframe.copy()
        updated_score_dataframe["score"] += 10  # Increment scores by 10

        updated_count_dataframe = sample_count_dataframe.copy()
        updated_count_dataframe["c_0"] += 5  # Increment counts by 5

        # Mock a second run with updated dataframes
        sample_independent_variant_creation_run.status = JobStatus.PENDING
        session.commit()

        # Second run to replace existing variants
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[updated_score_dataframe, updated_count_dataframe],
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        replaced_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(replaced_variants) == sample_score_dataframe.shape[0]

        # Verify that the variants have been replaced with updated data
        for variant in replaced_variants:
            assert variant.data  # Ensure data is not empty
            assert "score_data" in variant.data  # Ensure score_data is present
            expected_score = updated_score_dataframe.loc[
                updated_score_dataframe["hgvs_nt"] == variant.hgvs_nt, "score"
            ].values[0]
            actual_score = variant.data["score_data"]["score"]
            if actual_score is None and (isinstance(expected_score, float) and math.isnan(expected_score)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_score == expected_score  # Ensure score matches
            assert "count_data" in variant.data  # Ensure count_data is present
            expected_count = updated_count_dataframe.loc[
                updated_count_dataframe["hgvs_nt"] == variant.hgvs_nt, "c_0"
            ].values[0]
            actual_count = variant.data["count_data"]["c_0"]
            if actual_count is None and (isinstance(expected_count, float) and math.isnan(expected_count)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_count == expected_count  # Ensure count matches

        # Verify that no extra variants were created
        all_variants = session.query(Variant).all()
        assert len(all_variants) == len(replaced_variants)

        # Verify that job state is as expected
        job_run = (
            session.query(sample_independent_variant_creation_run.__class__)
            .filter(sample_independent_variant_creation_run.__class__.id == sample_independent_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.SUCCEEDED

    async def test_create_variants_for_score_set_handles_missing_counts_file(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        sample_independent_variant_creation_run.job_params["counts_file_key"] = None
        sample_independent_variant_creation_run.job_params["count_columns_metadata"] = {}
        session.commit()

        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return only the score dataframe
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe],
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        # Verify that variants have been created in the database
        created_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(created_variants) == sample_score_dataframe.shape[0]
        session.refresh(sample_score_set)
        assert sample_score_set.num_variants == len(created_variants)
        assert sample_score_set.processing_state == ProcessingState.success
        assert sample_score_set.mapping_state == MappingState.queued

        # Verify that the created variants have expected data
        for variant in created_variants:
            assert variant.data  # Ensure data is not empty
            assert "score_data" in variant.data  # Ensure score_data is present
            expected_score = sample_score_dataframe.loc[
                sample_score_dataframe["hgvs_nt"] == variant.hgvs_nt, "score"
            ].values[0]
            actual_score = variant.data["score_data"]["score"]
            if actual_score is None and (isinstance(expected_score, float) and math.isnan(expected_score)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_score == expected_score  # Ensure score matches
            assert "count_data" in variant.data  # Ensure count_data is present but...
            assert variant.data["count_data"] == {}  # ...ensure count_data is empty since no counts file was provided

        # Verify that no extra variants were created
        all_variants = session.query(Variant).all()
        assert len(all_variants) == len(created_variants)

        # Verify that job state is as expected
        job_run = (
            session.query(sample_independent_variant_creation_run.__class__)
            .filter(sample_independent_variant_creation_run.__class__.id == sample_independent_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.SUCCEEDED

    ## Common failure workflows

    async def test_create_variants_for_score_set_validation_error_during_creation(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        sample_score_dataframe.loc[0, "hgvs_nt"] = "c.G>X"  # Introduce invalid value to trigger validation error

        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        # Verify that the score set's processing state is updated to failed
        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.failed
        assert sample_score_set.mapping_state == MappingState.not_attempted
        assert "encountered 1 invalid variant strings" in sample_score_set.processing_errors["exception"]
        assert len(sample_score_set.processing_errors["detail"]) > 0

        # Verify that no variants were created
        created_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(created_variants) == 0

        # Verify that job state is as expected
        job_run = (
            session.query(sample_independent_variant_creation_run.__class__)
            .filter(sample_independent_variant_creation_run.__class__.id == sample_independent_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.FAILED

    async def test_create_variants_for_score_set_generic_exception_handling_during_creation(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                side_effect=Exception("Generic exception during data validation"),
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        # Verify that the score set's processing state is updated to failed
        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.failed
        assert sample_score_set.mapping_state == MappingState.not_attempted
        assert "Generic exception during data validation" in sample_score_set.processing_errors["exception"]

        # Verify that job state is as expected
        job_run = (
            session.query(sample_independent_variant_creation_run.__class__)
            .filter(sample_independent_variant_creation_run.__class__.id == sample_independent_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.FAILED

    async def test_create_variants_for_score_set_generic_exception_handling_during_replacement(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        # First run to create initial variants
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        initial_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(initial_variants) == sample_score_dataframe.shape[0]

        # Mock a second run to replace existing variants
        sample_independent_variant_creation_run.status = JobStatus.PENDING
        session.commit()

        # Second run to replace existing variants but trigger a generic exception
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                side_effect=Exception("Generic exception during data validation"),
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        # Verify that the score set's processing state is updated to failed
        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.failed
        assert sample_score_set.mapping_state == MappingState.not_attempted
        assert "Generic exception during data validation" in sample_score_set.processing_errors["exception"]

        # Verify that initial variants are still present
        remaining_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(remaining_variants) == len(initial_variants)

        # Verify that job state is as expected
        job_run = (
            session.query(sample_independent_variant_creation_run.__class__)
            .filter(sample_independent_variant_creation_run.__class__.id == sample_independent_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.FAILED

    ## Pipeline failure workflow

    async def test_create_variants_for_score_set_pipeline_job_generic_exception_handling(
        self,
        session,
        with_variant_creation_pipeline_runs,
        sample_variant_creation_pipeline,
        sample_pipeline_variant_creation_run,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                side_effect=Exception("Generic exception during data validation"),
            ),
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_pipeline_variant_creation_run.id)

        # Verify that the score set's processing state is updated to failed
        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.failed
        assert sample_score_set.mapping_state == MappingState.not_attempted
        assert "Generic exception during data validation" in sample_score_set.processing_errors["exception"]

        # Verify that job state is as expected
        job_run = (
            session.query(sample_pipeline_variant_creation_run.__class__)
            .filter(sample_pipeline_variant_creation_run.__class__.id == sample_pipeline_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.FAILED

        # Verify that pipeline status is updated.
        session.refresh(sample_variant_creation_pipeline)
        assert sample_variant_creation_pipeline.status == PipelineStatus.FAILED

        # Verify other pipeline runs are marked as failed
        other_runs = (
            session.query(Pipeline)
            .filter(
                JobRun.pipeline_id == sample_variant_creation_pipeline.id,
                Pipeline.id != sample_pipeline_variant_creation_run.id,
            )
            .all()
        )
        for run in other_runs:
            assert run.status == PipelineStatus.CANCELLED


@pytest.mark.asyncio
@pytest.mark.integration
class TestCreateVariantsForScoreSetArqContext:
    """Integration tests for create_variants_for_score_set job using ARQ worker context."""

    async def test_create_variants_for_score_set_with_arq_context_independent_ctx(
        self,
        session,
        arq_redis,
        arq_worker,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes.
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
        ):
            await arq_redis.enqueue_job("create_variants_for_score_set", sample_independent_variant_creation_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that variants have been created in the database
        created_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(created_variants) == sample_score_dataframe.shape[0]
        session.refresh(sample_score_set)
        assert sample_score_set.num_variants == len(created_variants)
        assert sample_score_set.processing_state == ProcessingState.success
        assert sample_score_set.mapping_state == MappingState.queued

        # Verify that the created variants have expected data
        for variant in created_variants:
            assert variant.data  # Ensure data is not empty
            assert "score_data" in variant.data  # Ensure score_data is present
            expected_score = sample_score_dataframe.loc[
                sample_score_dataframe["hgvs_nt"] == variant.hgvs_nt, "score"
            ].values[0]
            actual_score = variant.data["score_data"]["score"]
            if actual_score is None and (isinstance(expected_score, float) and math.isnan(expected_score)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_score == expected_score  # Ensure score matches
            assert "count_data" in variant.data  # Ensure count_data is present
            expected_count = sample_count_dataframe.loc[
                sample_count_dataframe["hgvs_nt"] == variant.hgvs_nt, "c_0"
            ].values[0]
            actual_count = variant.data["count_data"]["c_0"]
            if actual_count is None and (isinstance(expected_count, float) and math.isnan(expected_count)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_count == expected_count  # Ensure count matches

        # Verify that no extra variants were created
        all_variants = session.query(Variant).all()
        assert len(all_variants) == len(created_variants)

        # Verify that job state is as expected
        job_run = (
            session.query(sample_independent_variant_creation_run.__class__)
            .filter(sample_independent_variant_creation_run.__class__.id == sample_independent_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.SUCCEEDED

    async def test_create_variants_for_score_set_with_arq_context_pipeline_ctx(
        self,
        session,
        arq_redis,
        arq_worker,
        with_variant_creation_pipeline_runs,
        sample_variant_creation_pipeline,
        sample_pipeline_variant_creation_run,
        with_populated_domain_data,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes.
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
        ):
            await arq_redis.enqueue_job(
                "create_variants_for_score_set",
                sample_pipeline_variant_creation_run.id,
                _job_id=sample_pipeline_variant_creation_run.urn,
            )
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that variants have been created in the database
        created_variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        assert len(created_variants) == sample_score_dataframe.shape[0]
        session.refresh(sample_score_set)
        assert sample_score_set.num_variants == len(created_variants)
        assert sample_score_set.processing_state == ProcessingState.success
        assert sample_score_set.mapping_state == MappingState.queued

        # Verify that the created variants have expected data
        for variant in created_variants:
            assert variant.data  # Ensure data is not empty
            assert "score_data" in variant.data  # Ensure score_data is present
            expected_score = sample_score_dataframe.loc[
                sample_score_dataframe["hgvs_nt"] == variant.hgvs_nt, "score"
            ].values[0]
            actual_score = variant.data["score_data"]["score"]
            if actual_score is None and (isinstance(expected_score, float) and math.isnan(expected_score)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_score == expected_score  # Ensure score matches
            assert "count_data" in variant.data  # Ensure count_data is present
            expected_count = sample_count_dataframe.loc[
                sample_count_dataframe["hgvs_nt"] == variant.hgvs_nt, "c_0"
            ].values[0]
            actual_count = variant.data["count_data"]["c_0"]
            if actual_count is None and (isinstance(expected_count, float) and math.isnan(expected_count)):
                pass  # None in variant, NaN in DataFrame: OK
            else:
                assert actual_count == expected_count  # Ensure count matches

        # Verify that no extra variants were created
        all_variants = session.query(Variant).all()
        assert len(all_variants) == len(created_variants)

        # Verify that pipeline job state is as expected
        job_run = (
            session.query(sample_pipeline_variant_creation_run.__class__)
            .filter(sample_pipeline_variant_creation_run.__class__.id == sample_pipeline_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.SUCCEEDED

        # Verify that pipeline status is updated. Pipeline will remain RUNNING
        # as our default test pipeline includes the mapping job as well.
        session.refresh(sample_variant_creation_pipeline)
        assert sample_variant_creation_pipeline.status == PipelineStatus.RUNNING

    async def test_create_variants_for_score_set_with_arq_context_generic_exception_handling_independent_ctx(
        self,
        session,
        arq_redis,
        arq_worker,
        with_variant_creation_pipeline_runs,
        sample_variant_creation_pipeline,
        sample_independent_variant_creation_run,
        with_populated_domain_data,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                side_effect=Exception("Generic exception during data validation"),
            ),
        ):
            await arq_redis.enqueue_job("create_variants_for_score_set", sample_independent_variant_creation_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that the score set's processing state is updated to failed
        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.failed
        assert sample_score_set.mapping_state == MappingState.not_attempted
        assert "Generic exception during data validation" in sample_score_set.processing_errors["exception"]

        # Verify that job state is as expected
        job_run = (
            session.query(sample_independent_variant_creation_run.__class__)
            .filter(sample_independent_variant_creation_run.__class__.id == sample_independent_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.FAILED

    async def test_create_variants_for_score_set_with_arq_context_generic_exception_handling_pipeline_ctx(
        self,
        session,
        arq_redis,
        arq_worker,
        with_variant_creation_pipeline_runs,
        sample_variant_creation_pipeline,
        sample_pipeline_variant_creation_run,
        with_populated_domain_data,
        mock_s3_client,
        create_variants_sample_params,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
    ):
        with (
            patch.object(mock_s3_client, "download_fileobj", return_value=None),
            # Mock pd.read_csv to return sample dataframes
            patch(
                "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
                side_effect=[sample_score_dataframe, sample_count_dataframe],
            ),
            patch(
                "mavedb.worker.jobs.variant_processing.creation.validate_and_standardize_dataframe_pair",
                side_effect=Exception("Generic exception during data validation"),
            ),
        ):
            await arq_redis.enqueue_job("create_variants_for_score_set", sample_pipeline_variant_creation_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that the score set's processing state is updated to failed
        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.failed
        assert sample_score_set.mapping_state == MappingState.not_attempted
        assert "Generic exception during data validation" in sample_score_set.processing_errors["exception"]

        # Verify that job state is as expected
        job_run = (
            session.query(sample_pipeline_variant_creation_run.__class__)
            .filter(sample_pipeline_variant_creation_run.__class__.id == sample_pipeline_variant_creation_run.id)
            .one()
        )
        assert job_run.progress_current == 100
        assert job_run.status == JobStatus.FAILED

        # Verify that pipeline status is updated.
        session.refresh(sample_variant_creation_pipeline)
        assert sample_variant_creation_pipeline.status == PipelineStatus.FAILED

        # Verify other pipeline runs are marked as cancelled
        other_runs = (
            session.query(Pipeline)
            .filter(
                JobRun.pipeline_id == sample_variant_creation_pipeline.id,
                Pipeline.id != sample_pipeline_variant_creation_run.id,
            )
            .all()
        )
        for run in other_runs:
            assert run.status == PipelineStatus.CANCELLED
