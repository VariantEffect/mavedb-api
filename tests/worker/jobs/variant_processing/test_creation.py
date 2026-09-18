# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

import math
from datetime import date, timedelta
from unittest.mock import ANY, MagicMock, call, patch

from mavedb.lib.mondo import get_generic_disease_term
from mavedb.models.calibration_control import CalibrationControl
from mavedb.models.enums.calibration_control_status import CalibrationControlStatus
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.variant import Variant
from mavedb.worker.jobs.variant_processing.creation import create_variants_for_score_set
from mavedb.worker.lib.managers.job_manager import JobManager

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.usefixtures("patch_db_session_ctxmgr")
class TestCreateVariantsForScoreSetUnit:
    """Unit tests for create_variants_for_score_set job."""

    async def test_create_variants_for_score_set_raises_key_error_on_missing_hdp_from_ctx(
        self,
        mock_worker_ctx,
        mock_job_manager,
    ):
        ctx = mock_worker_ctx.copy()
        del ctx["hdp"]

        with pytest.raises(KeyError) as exc_info:
            await create_variants_for_score_set(ctx, 999, mock_job_manager)

        assert str(exc_info.value) == "'hdp'"

    async def test_create_variants_for_score_set_calls_s3_client_with_correct_parameters(
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
                mock_worker_ctx,
                sample_independent_variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_creation_run.id),
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
            pytest.raises(Exception, match="The specified key does not exist."),
        ):
            await create_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_creation_run.id),
            )

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
                mock_worker_ctx,
                sample_independent_variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_creation_run.id),
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
            pytest.raises(ValueError, match="Can't create variants when score set has no targets."),
        ):
            await create_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_creation_run.id),
            )

    async def test_create_variants_for_score_set_handles_empty_variant_data(
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
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants_data", return_value=[]),
            patch("mavedb.worker.jobs.variant_processing.creation.create_variants", return_value=None),
        ):
            await create_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_creation_run.id),
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
                mock_worker_ctx,
                sample_independent_variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_creation_run.id),
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
                mock_worker_ctx,
                sample_independent_variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_creation_run.id),
            )

        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.success
        assert sample_score_set.mapping_state == MappingState.queued
        assert sample_score_set.processing_errors is None

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
            pytest.raises(Exception, match="Test exception during data validation"),
        ):
            await create_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_creation_run.id),
            )

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
            pytest.raises(Exception, match="Test exception during data validation"),
        ):
            await create_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_creation_run.id),
            )

        # Verify that the score set's processing state is updated to failed
        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.failed
        assert sample_score_set.mapping_state == MappingState.not_attempted
        assert "Test exception during data validation" in sample_score_set.processing_errors["exception"]


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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_failure") as mock_send_slack_job_failure,
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        mock_send_slack_job_failure.assert_called_once()
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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        mock_send_slack_job_error.assert_called_once()
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
        assert job_run.status == JobStatus.ERRORED

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_independent_variant_creation_run.id)

        mock_send_slack_job_error.assert_called_once()
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
        assert job_run.status == JobStatus.ERRORED

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await create_variants_for_score_set(mock_worker_ctx, sample_pipeline_variant_creation_run.id)

        mock_send_slack_job_error.assert_called_once()
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
        assert job_run.status == JobStatus.ERRORED

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
            assert run.status == JobStatus.SKIPPED


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
            await arq_redis.enqueue_job("create_variants_for_score_set", sample_pipeline_variant_creation_run.id)
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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job("create_variants_for_score_set", sample_independent_variant_creation_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
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
        assert job_run.status == JobStatus.ERRORED

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
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job("create_variants_for_score_set", sample_pipeline_variant_creation_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
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
        assert job_run.status == JobStatus.ERRORED

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
            assert run.status == JobStatus.SKIPPED


def _make_calibration(session, score_set, user, controls_not_phi=None):
    """Persist a bare calibration on a score set; controls and bin membership need a parent."""
    calibration = ScoreCalibration(
        title="Relink test calibration",
        score_set_id=score_set.id,
        disease_term=get_generic_disease_term(session),
        controls_not_phi=controls_not_phi,
        created_by=user,
        modified_by=user,
    )
    session.add(calibration)
    session.commit()
    session.refresh(calibration)
    return calibration


def _variants_by_hgvs_nt(session, score_set):
    return {variant.hgvs_nt: variant for variant in session.query(Variant).filter_by(score_set_id=score_set.id).all()}


def _without_variant(scores_df, counts_df, hgvs_nt):
    """Return the score/count pair with one variant removed, simulating a corrected re-upload."""
    return (
        scores_df[scores_df["hgvs_nt"] != hgvs_nt].copy(),
        counts_df[counts_df["hgvs_nt"] != hgvs_nt].copy(),
    )


async def _run_creation_job(mock_worker_ctx, mock_s3_client, job_run, scores_df, counts_df):
    with (
        patch.object(mock_s3_client, "download_fileobj", return_value=None),
        patch(
            "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
            side_effect=[scores_df, counts_df],
        ),
    ):
        await create_variants_for_score_set(mock_worker_ctx, job_run.id)


@pytest.mark.integration
@pytest.mark.asyncio
class TestCreateVariantsForScoreSetCalibrationRelinking:
    """Re-upload behavior for score sets whose calibrations reference variants.

    Calibration controls and functional classification bin membership hold plain foreign keys into
    ``variants``, which a re-upload deletes and recreates wholesale. These tests pin both halves of
    the fix: the delete no longer trips those constraints, and surviving references are re-resolved
    against the new rows by HGVS identity rather than by the positional variant URN.
    """

    async def test_reupload_with_calibration_controls_does_not_violate_foreign_key(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_user,
        sample_independent_variant_creation_run,
    ):
        await _run_creation_job(
            mock_worker_ctx,
            mock_s3_client,
            sample_independent_variant_creation_run,
            sample_score_dataframe,
            sample_count_dataframe,
        )

        calibration = _make_calibration(session, sample_score_set, sample_user)
        session.add(
            CalibrationControl(
                calibration_id=calibration.id,
                variant_id=_variants_by_hgvs_nt(session, sample_score_set)["c.1A>T"].id,
                clinical_status=CalibrationControlStatus.pathogenic,
                created_by=sample_user,
                modified_by=sample_user,
            )
        )
        sample_independent_variant_creation_run.status = JobStatus.PENDING
        session.commit()

        # Regression: this second upload raised ForeignKeyViolation on calibration_controls_variant_id_fkey.
        await _run_creation_job(
            mock_worker_ctx,
            mock_s3_client,
            sample_independent_variant_creation_run,
            sample_score_dataframe,
            sample_count_dataframe,
        )

        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.success
        assert session.query(CalibrationControl).filter_by(calibration_id=calibration.id).count() == 1

    async def test_controls_relink_to_new_variants_when_every_identity_persists(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_user,
        sample_extra_user,
        sample_independent_variant_creation_run,
    ):
        await _run_creation_job(
            mock_worker_ctx,
            mock_s3_client,
            sample_independent_variant_creation_run,
            sample_score_dataframe,
            sample_count_dataframe,
        )

        original_variant_ids = {
            hgvs: variant.id for hgvs, variant in _variants_by_hgvs_nt(session, sample_score_set).items()
        }
        calibration = _make_calibration(session, sample_score_set, sample_user, controls_not_phi=True)

        # An older submission date proves the relinked row keeps its own provenance rather than
        # inheriting the re-upload's.
        submitted_on = date.today() - timedelta(days=30)
        for hgvs_nt, status in (
            ("c.1A>T", CalibrationControlStatus.pathogenic),
            ("c.2T>A", CalibrationControlStatus.benign),
        ):
            session.add(
                CalibrationControl(
                    calibration_id=calibration.id,
                    variant_id=original_variant_ids[hgvs_nt],
                    clinical_status=status,
                    created_by=sample_extra_user,
                    modified_by=sample_extra_user,
                    creation_date=submitted_on,
                    modification_date=submitted_on,
                )
            )

        updated_scores = sample_score_dataframe.copy()
        updated_scores["score"] += 10

        sample_independent_variant_creation_run.status = JobStatus.PENDING
        session.commit()

        await _run_creation_job(
            mock_worker_ctx,
            mock_s3_client,
            sample_independent_variant_creation_run,
            updated_scores,
            sample_count_dataframe.copy(),
        )

        new_variants = _variants_by_hgvs_nt(session, sample_score_set)
        assert set(new_variants) == set(original_variant_ids)
        # Every variant row really was replaced, so a stale link would be visible below.
        assert all(new_variants[hgvs].id != variant_id for hgvs, variant_id in original_variant_ids.items())

        controls = session.query(CalibrationControl).filter_by(calibration_id=calibration.id).all()
        assert len(controls) == 2
        assert {control.variant_id for control in controls} == {
            new_variants["c.1A>T"].id,
            new_variants["c.2T>A"].id,
        }
        assert {control.clinical_status for control in controls} == {
            CalibrationControlStatus.pathogenic,
            CalibrationControlStatus.benign,
        }
        assert all(control.created_by_id == sample_extra_user.id for control in controls)
        assert all(control.creation_date == submitted_on for control in controls)
        assert all(control.modified_by_id == sample_user.id for control in controls)

        # Nothing was dropped, so the submitter's PHI affirmation still describes this control set.
        session.refresh(calibration)
        assert calibration.controls_not_phi is True

    async def test_controls_absent_from_new_upload_are_dropped_and_reset_phi_affirmation(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_user,
        sample_independent_variant_creation_run,
    ):
        await _run_creation_job(
            mock_worker_ctx,
            mock_s3_client,
            sample_independent_variant_creation_run,
            sample_score_dataframe,
            sample_count_dataframe,
        )

        original_variants = _variants_by_hgvs_nt(session, sample_score_set)
        calibration = _make_calibration(session, sample_score_set, sample_user, controls_not_phi=True)
        for hgvs_nt in ("c.1A>T", "c.4C>G"):
            session.add(
                CalibrationControl(
                    calibration_id=calibration.id,
                    variant_id=original_variants[hgvs_nt].id,
                    clinical_status=CalibrationControlStatus.pathogenic,
                    created_by=sample_user,
                    modified_by=sample_user,
                )
            )

        sample_independent_variant_creation_run.status = JobStatus.PENDING
        session.commit()

        trimmed_scores, trimmed_counts = _without_variant(sample_score_dataframe, sample_count_dataframe, "c.4C>G")

        recorded_context: dict = {}
        original_save_to_context = JobManager.save_to_context

        def _capture_context(self, ctx):
            recorded_context.update(ctx)
            return original_save_to_context(self, ctx)

        with patch.object(JobManager, "save_to_context", _capture_context):
            await _run_creation_job(
                mock_worker_ctx,
                mock_s3_client,
                sample_independent_variant_creation_run,
                trimmed_scores,
                trimmed_counts,
            )

        new_variants = _variants_by_hgvs_nt(session, sample_score_set)
        assert "c.4C>G" not in new_variants

        controls = session.query(CalibrationControl).filter_by(calibration_id=calibration.id).all()
        assert len(controls) == 1
        assert controls[0].variant_id == new_variants["c.1A>T"].id

        # A control set that lost a member is no longer the one the submitter affirmed as PHI-free.
        session.refresh(calibration)
        assert calibration.controls_not_phi is None

        assert recorded_context["calibration_controls_relinked"] == 1
        assert recorded_context["calibration_controls_dropped"] == 1
        assert recorded_context["calibrations_pending_phi_reaffirmation"] == [calibration.id]

    async def test_range_based_classification_membership_is_rebinned_against_new_scores(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_user,
        sample_independent_variant_creation_run,
    ):
        await _run_creation_job(
            mock_worker_ctx,
            mock_s3_client,
            sample_independent_variant_creation_run,
            sample_score_dataframe,
            sample_count_dataframe,
        )

        # Sample scores: c.1A>T 0.3, c.2T>A 0.0, c.3G>C -1.65, c.4C>G unscored.
        original_variants = _variants_by_hgvs_nt(session, sample_score_set)
        calibration = _make_calibration(session, sample_score_set, sample_user)
        normal = ScoreCalibrationFunctionalClassification(
            calibration_id=calibration.id,
            label="Normal function",
            range=[0.0, None],
            inclusive_lower_bound=True,
        )
        normal.variants = [original_variants["c.1A>T"], original_variants["c.2T>A"]]
        abnormal = ScoreCalibrationFunctionalClassification(
            calibration_id=calibration.id,
            label="Abnormal function",
            range=[None, 0.0],
            inclusive_upper_bound=False,
        )
        abnormal.variants = [original_variants["c.3G>C"]]
        session.add_all([normal, abnormal])

        original_variant_ids = {variant.id for variant in original_variants.values()}

        sample_independent_variant_creation_run.status = JobStatus.PENDING
        session.commit()
        normal_id, abnormal_id = normal.id, abnormal.id

        # Shifting every score up by two moves c.3G>C across the threshold, so recorded membership and
        # recomputed membership disagree and the test can tell which one was applied.
        updated_scores = sample_score_dataframe.copy()
        updated_scores["score"] += 2

        recorded_context: dict = {}
        original_save_to_context = JobManager.save_to_context

        def _capture_context(self, ctx):
            recorded_context.update(ctx)
            return original_save_to_context(self, ctx)

        with patch.object(JobManager, "save_to_context", _capture_context):
            await _run_creation_job(
                mock_worker_ctx,
                mock_s3_client,
                sample_independent_variant_creation_run,
                updated_scores,
                sample_count_dataframe.copy(),
            )

        session.expire_all()
        normal = session.get(ScoreCalibrationFunctionalClassification, normal_id)
        abnormal = session.get(ScoreCalibrationFunctionalClassification, abnormal_id)

        # Membership follows the new scores rather than what was recorded: c.3G>C (-1.65 -> 0.35) moves
        # into the normal bin, emptying the abnormal one. c.4C>G stays out of both, having no score.
        assert {variant.hgvs_nt for variant in normal.variants} == {"c.1A>T", "c.2T>A", "c.3G>C"}
        assert normal.variant_count == 3
        assert abnormal.variants == []
        assert abnormal.variant_count == 0

        # Re-binned membership points at the new variant rows, not the deleted ones.
        new_variants = _variants_by_hgvs_nt(session, sample_score_set)
        assert {variant.id for variant in normal.variants} <= {variant.id for variant in new_variants.values()}
        assert {variant.id for variant in normal.variants}.isdisjoint(original_variant_ids)

        assert recorded_context["calibration_classifications_rebinned"] == 2
        assert recorded_context["calibration_classification_members_rebinned"] == 3
        # Range bins are recomputed, never relinked, so nothing is reported as carried across.
        assert recorded_context["calibration_classification_members_relinked"] == 0

    async def test_class_based_classification_membership_relinks_by_hgvs_identity(
        self,
        session,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_worker_ctx,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_user,
        sample_independent_variant_creation_run,
    ):
        await _run_creation_job(
            mock_worker_ctx,
            mock_s3_client,
            sample_independent_variant_creation_run,
            sample_score_dataframe,
            sample_count_dataframe,
        )

        original_variants = _variants_by_hgvs_nt(session, sample_score_set)
        calibration = _make_calibration(session, sample_score_set, sample_user)

        # A class-based bin comes from an uploaded classes file that a score upload does not carry, so
        # its membership can only survive by identity.
        classification = ScoreCalibrationFunctionalClassification(
            calibration_id=calibration.id,
            label="Loss of function",
            class_="loss_of_function",
        )
        classification.variants = [original_variants["c.1A>T"], original_variants["c.4C>G"]]
        session.add(classification)

        sample_independent_variant_creation_run.status = JobStatus.PENDING
        session.commit()
        classification_id = classification.id

        trimmed_scores, trimmed_counts = _without_variant(sample_score_dataframe, sample_count_dataframe, "c.4C>G")

        recorded_context: dict = {}
        original_save_to_context = JobManager.save_to_context

        def _capture_context(self, ctx):
            recorded_context.update(ctx)
            return original_save_to_context(self, ctx)

        with patch.object(JobManager, "save_to_context", _capture_context):
            await _run_creation_job(
                mock_worker_ctx,
                mock_s3_client,
                sample_independent_variant_creation_run,
                trimmed_scores,
                trimmed_counts,
            )

        new_variants = _variants_by_hgvs_nt(session, sample_score_set)
        assert "c.4C>G" not in new_variants

        session.expire_all()
        classification = session.get(ScoreCalibrationFunctionalClassification, classification_id)

        # The surviving member is relinked to its new row; the one absent from the upload is dropped.
        assert [variant.id for variant in classification.variants] == [new_variants["c.1A>T"].id]
        assert classification.variant_count == 1

        assert recorded_context["calibration_classification_members_relinked"] == 1
        assert recorded_context["calibration_classification_members_dropped"] == 1
        assert recorded_context["calibration_classifications_rebinned"] == 0

    async def test_reupload_without_calibrations_is_unaffected(
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
        await _run_creation_job(
            mock_worker_ctx,
            mock_s3_client,
            sample_independent_variant_creation_run,
            sample_score_dataframe,
            sample_count_dataframe,
        )

        sample_independent_variant_creation_run.status = JobStatus.PENDING
        session.commit()

        recorded_context: dict = {}
        original_save_to_context = JobManager.save_to_context

        def _capture_context(self, ctx):
            recorded_context.update(ctx)
            return original_save_to_context(self, ctx)

        with patch.object(JobManager, "save_to_context", _capture_context):
            await _run_creation_job(
                mock_worker_ctx,
                mock_s3_client,
                sample_independent_variant_creation_run,
                sample_score_dataframe,
                sample_count_dataframe,
            )

        session.refresh(sample_score_set)
        assert sample_score_set.processing_state == ProcessingState.success
        assert sample_score_set.num_variants == sample_score_dataframe.shape[0]
        # The relink path stays out of the way entirely when there is nothing to relink.
        assert "calibration_controls_relinked" not in recorded_context
