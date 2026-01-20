from unittest.mock import Mock, patch

import pytest
from arq import ArqRedis
from sqlalchemy.orm import Session

from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.pipeline_manager import PipelineManager


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


@pytest.fixture
def mock_pipeline_manager(mock_job_manager, mock_pipeline):
    """Create a PipelineManager with mocked database, Redis dependencies, and job manager."""
    mock_db = Mock(spec=Session)
    mock_redis = Mock(spec=ArqRedis)

    # Don't call the real constructor since it tries to validate the pipeline
    manager = object.__new__(PipelineManager)
    manager.db = mock_db
    manager.redis = mock_redis
    manager.pipeline_id = 123

    with (
        patch("mavedb.worker.lib.managers.pipeline_manager.JobManager") as mock_job_manager_class,
        patch.object(manager, "get_pipeline", return_value=mock_pipeline),
    ):
        mock_job_manager_class.return_value = mock_job_manager
        yield manager


@pytest.fixture
def mock_worker_ctx():
    """Create a mock worker context dictionary for testing."""
    mock_db = Mock(spec=Session)
    mock_redis = Mock(spec=ArqRedis)

    return {
        "db": mock_db,
        "redis": mock_redis,
        "hdp": Mock(),  # Mock HDP data provider
    }
