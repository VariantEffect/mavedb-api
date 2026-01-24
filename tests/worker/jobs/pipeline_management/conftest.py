import pytest

from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline


@pytest.fixture
def sample_dummy_pipeline():
    """Create a sample Pipeline instance for testing."""

    return Pipeline(
        name="Dummy Pipeline",
        description="A dummy pipeline for testing purposes",
    )


@pytest.fixture
def with_dummy_pipeline(session, sample_dummy_pipeline):
    """Fixture to ensure dummy pipeline exists in the database."""
    session.add(sample_dummy_pipeline)
    session.commit()


@pytest.fixture
def sample_dummy_pipeline_start(session, with_dummy_pipeline, sample_dummy_pipeline):
    """Create a sample JobRun instance for starting the dummy pipeline."""
    start_job_run = JobRun(
        pipeline_id=sample_dummy_pipeline.id,
        job_type="start_pipeline",
        job_function="start_pipeline",
    )
    session.add(start_job_run)
    session.commit()

    return start_job_run


@pytest.fixture
def with_dummy_pipeline_start(session, with_dummy_pipeline, sample_dummy_pipeline_start):
    """Fixture to ensure a start pipeline job run for the dummy pipeline exists in the database."""
    session.add(sample_dummy_pipeline_start)
    session.commit()


@pytest.fixture
def sample_dummy_pipeline_step(session, sample_dummy_pipeline):
    """Create a sample PipelineStep instance for the dummy pipeline."""
    step = JobRun(
        pipeline_id=sample_dummy_pipeline.id,
        job_type="dummy_step",
        job_function="dummy_arq_function",
    )
    session.add(step)
    session.commit()
    return step


@pytest.fixture
def with_full_dummy_pipeline(session, with_dummy_pipeline_start, sample_dummy_pipeline, sample_dummy_pipeline_step):
    """Fixture to ensure dummy pipeline steps exist in the database."""
    session.add(sample_dummy_pipeline_step)
    session.commit()
