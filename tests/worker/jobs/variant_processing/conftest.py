from unittest import mock

import pytest
from mypy_boto3_s3 import S3Client

from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline


@pytest.fixture
def create_variants_sample_params(with_populated_domain_data, sample_score_set, sample_user):
    """Provide sample parameters for create_variants_for_score_set job."""

    return {
        "scores_file_key": "sample_scores.csv",
        "counts_file_key": "sample_counts.csv",
        "correlation_id": "sample-correlation-id",
        "updater_id": sample_user.id,
        "score_set_id": sample_score_set.id,
        "score_columns_metadata": {"s_0": {"description": "metadataS", "details": "detailsS"}},
        "count_columns_metadata": {"c_0": {"description": "metadataC", "details": "detailsC"}},
    }


@pytest.fixture
def map_variants_sample_params(with_populated_domain_data, sample_score_set, sample_user):
    """Provide sample parameters for map_variants_for_score_set job."""

    return {
        "score_set_id": sample_score_set.id,
        "correlation_id": "sample-mapping-correlation-id",
        "updater_id": sample_user.id,
    }


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for tests that interact with S3."""

    with mock.patch("mavedb.worker.jobs.variant_processing.creation.s3_client") as mock_s3_client_func:
        mock_s3 = mock.MagicMock(spec=S3Client)
        mock_s3_client_func.return_value = mock_s3
        yield mock_s3


@pytest.fixture
def sample_independent_variant_creation_run(create_variants_sample_params):
    """Create a JobRun instance for variant creation job."""

    return JobRun(
        urn="test:create_variants_for_score_set",
        job_type="create_variants_for_score_set",
        job_function="create_variants_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params=create_variants_sample_params,
    )


@pytest.fixture
def sample_independent_variant_mapping_run(map_variants_sample_params):
    """Create a JobRun instance for variant mapping job."""

    return JobRun(
        urn="test:map_variants_for_score_set",
        job_type="map_variants_for_score_set",
        job_function="map_variants_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params=map_variants_sample_params,
    )


@pytest.fixture
def dummy_pipeline_step():
    """Create a dummy pipeline step function for testing."""

    return JobRun(
        urn="test:dummy_pipeline_step",
        job_type="dummy_pipeline_step",
        job_function="dummy_arq_function",
        max_retries=3,
        retry_count=0,
    )


@pytest.fixture
def sample_pipeline_variant_creation_run(
    session,
    with_variant_creation_pipeline,
    sample_variant_creation_pipeline,
    sample_independent_variant_creation_run,
):
    """Create a JobRun instance for variant creation job."""

    sample_independent_variant_creation_run.pipeline_id = sample_variant_creation_pipeline.id
    session.add(sample_independent_variant_creation_run)
    session.commit()
    return sample_independent_variant_creation_run


@pytest.fixture
def sample_pipeline_variant_mapping_run(
    session,
    with_variant_mapping_pipeline,
    sample_independent_variant_mapping_run,
    sample_variant_mapping_pipeline,
):
    """Create a JobRun instance for variant mapping job."""

    sample_independent_variant_mapping_run.pipeline_id = sample_variant_mapping_pipeline.id
    session.add(sample_independent_variant_mapping_run)
    session.commit()
    return sample_independent_variant_mapping_run


@pytest.fixture
def sample_variant_creation_pipeline():
    """Create a Pipeline instance."""

    return Pipeline(
        name="variant_creation_pipeline",
        description="Pipeline for creating variants",
    )


@pytest.fixture
def sample_variant_mapping_pipeline():
    """Create a Pipeline instance."""

    return Pipeline(
        name="variant_mapping_pipeline",
        description="Pipeline for mapping variants",
    )


@pytest.fixture
def with_independent_processing_runs(
    session,
    sample_independent_variant_creation_run,
    sample_independent_variant_mapping_run,
):
    """Fixture to ensure independent variant processing runs exist in the database."""

    session.add(sample_independent_variant_creation_run)
    session.add(sample_independent_variant_mapping_run)
    session.commit()


@pytest.fixture
def with_variant_creation_pipeline(session, sample_variant_creation_pipeline):
    """Fixture to ensure variant creation pipeline and its runs exist in the database."""
    session.add(sample_variant_creation_pipeline)
    session.commit()


@pytest.fixture
def with_variant_creation_pipeline_runs(
    session,
    with_variant_creation_pipeline,
    sample_variant_creation_pipeline,
    sample_pipeline_variant_creation_run,
    dummy_pipeline_step,
):
    """Fixture to ensure pipeline variant processing runs exist in the database."""
    session.add(sample_pipeline_variant_creation_run)
    dummy_pipeline_step.pipeline_id = sample_variant_creation_pipeline.id
    session.add(dummy_pipeline_step)
    session.commit()


@pytest.fixture
def with_variant_mapping_pipeline(session, sample_variant_mapping_pipeline):
    """Fixture to ensure variant mapping pipeline and its runs exist in the database."""
    session.add(sample_variant_mapping_pipeline)
    session.commit()


@pytest.fixture
def with_variant_mapping_pipeline_runs(
    session,
    with_variant_mapping_pipeline,
    sample_variant_mapping_pipeline,
    sample_pipeline_variant_mapping_run,
    dummy_pipeline_step,
):
    """Fixture to ensure pipeline variant processing runs exist in the database."""
    session.add(sample_pipeline_variant_mapping_run)
    dummy_pipeline_step.pipeline_id = sample_variant_mapping_pipeline.id
    session.add(dummy_pipeline_step)
    session.commit()
