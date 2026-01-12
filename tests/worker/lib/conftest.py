# ruff: noqa: E402

"""
Test configuration and fixtures for worker lib tests.
"""

import pytest

pytest.importorskip("arq")  # Skip tests if arq is not installed

from datetime import datetime
from unittest.mock import Mock, patch

from arq import ArqRedis
from sqlalchemy.orm import Session

from mavedb.models.enums.job_pipeline import DependencyType, JobStatus, PipelineStatus
from mavedb.models.job_dependency import JobDependency
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.worker.lib.managers.job_manager import JobManager


@pytest.fixture
def sample_job_run():
    """Create a sample JobRun instance for testing."""
    return JobRun(
        id=1,
        urn="test:job:1",
        job_type="test_job",
        job_function="test_function",
        status=JobStatus.PENDING,
        pipeline_id=1,
        progress_current=0,
        progress_total=100,
        progress_message="Ready to start",
        created_at=datetime.now(),
    )


@pytest.fixture
def sample_dependent_job_run():
    """Create a sample dependent JobRun instance for testing."""
    return JobRun(
        id=2,
        urn="test:job:2",
        job_type="dependent_job",
        job_function="dependent_function",
        status=JobStatus.PENDING,
        pipeline_id=1,
        progress_current=0,
        progress_total=100,
        progress_message="Waiting for dependency",
        created_at=datetime.now(),
    )


@pytest.fixture
def sample_independent_job_run():
    """Create a sample independent JobRun instance for testing."""
    return JobRun(
        id=3,
        urn="test:job:3",
        job_type="independent_job",
        job_function="independent_function",
        status=JobStatus.PENDING,
        pipeline_id=None,
        progress_current=0,
        progress_total=100,
        progress_message="Ready to start",
        created_at=datetime.now(),
    )


@pytest.fixture
def sample_pipeline():
    """Create a sample Pipeline instance for testing."""
    return Pipeline(
        id=1,
        urn="test:pipeline:1",
        name="Test Pipeline",
        description="A test pipeline",
        status=PipelineStatus.CREATED,
        correlation_id="test_correlation_123",
        created_at=datetime.now(),
    )


@pytest.fixture
def sample_job_dependency():
    """Create a sample JobDependency instance for testing."""
    return JobDependency(
        id=2,  # dependent job
        depends_on_job_id=1,  # depends on job 1
        dependency_type=DependencyType.SUCCESS_REQUIRED,
        created_at=datetime.now(),
    )


@pytest.fixture
def setup_worker_db(
    session,
    sample_job_run,
    sample_pipeline,
    sample_job_dependency,
    sample_dependent_job_run,
    sample_independent_job_run,
):
    """Set up the database with sample data for worker tests."""
    session.add(sample_pipeline)
    session.add(sample_job_run)
    session.add(sample_dependent_job_run)
    session.add(sample_independent_job_run)
    session.add(sample_job_dependency)
    session.commit()


@pytest.fixture
def job_manager_with_mocks(session, sample_job_run, sample_pipeline):
    """Create a JobManager instance with mocked dependencies."""
    # Add test data to session
    session.add(sample_job_run)
    session.add(sample_pipeline)
    session.commit()

    # Create JobManager instance
    manager = JobManager(session, sample_job_run.id)
    return manager


@pytest.fixture
def async_context():
    """Create a mock async context similar to ARQ worker context."""
    return {
        "db": None,  # Will be set by specific tests
        "redis": None,  # Will be set by specific tests
        "job_id": 1,
        "state": {},
    }


@pytest.fixture
def mock_job_run():
    """Create a mock JobRun instance. By default,
    properties are identical to a default new JobRun entered into the db
    with sensible defaults for non-nullable but unset fields.
    """
    return Mock(
        spec=JobRun,
        id=123,
        urn="test:job:123",
        job_type="test_job",
        job_function="test_function",
        status=JobStatus.PENDING,
        pipeline_id=None,
        priority=0,
        max_retries=3,
        retry_count=0,
        retry_delay_seconds=None,
        scheduled_at=datetime.now(),
        started_at=None,
        finished_at=None,
        created_at=datetime.now(),
        error_message=None,
        error_traceback=None,
        failure_category=None,
        worker_id=None,
        worker_host=None,
        progress_current=None,
        progress_total=None,
        progress_message=None,
        correlation_id=None,
        metadata_={},
        mavedb_version=None,
    )


@pytest.fixture
def mock_job_manager(mock_job_run):
    """Create a JobManager with mocked database and Redis dependencies."""
    mock_db = Mock(spec=Session)
    mock_redis = Mock(spec=ArqRedis)

    # Don't call the real constructor since it tries to load the job from DB
    manager = object.__new__(JobManager)
    manager.db = mock_db
    manager.redis = mock_redis
    manager.job_id = mock_job_run.id

    with patch.object(manager, "get_job", return_value=mock_job_run):
        yield manager
