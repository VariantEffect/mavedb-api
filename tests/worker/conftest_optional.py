from concurrent.futures import ProcessPoolExecutor
from unittest.mock import Mock, patch

import pytest
from arq import ArqRedis
from cdot.hgvs.dataproviders import RESTDataProvider
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
    mock_redis = Mock(spec=ArqRedis)
    mock_hdp = Mock(spec=RESTDataProvider)
    mock_pool = Mock(spec=ProcessPoolExecutor)

    # Don't mock the session itself to allow real DB interactions in tests
    # It's generally more pain than it's worth to mock out SQLAlchemy sessions,
    # although it can sometimes be useful when raising specific exceptions.
    return {
        "redis": mock_redis,
        "hdp": mock_hdp,
        "pool": mock_pool,
    }
