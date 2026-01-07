from datetime import datetime
from pathlib import Path
from shutil import copytree
from unittest.mock import Mock

import pytest

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.job_run import JobRun
from mavedb.models.license import License
from mavedb.models.pipeline import Pipeline
from mavedb.models.taxonomy import Taxonomy
from mavedb.models.user import User
from tests.helpers.constants import (
    EXTRA_USER,
    TEST_INACTIVE_LICENSE,
    TEST_LICENSE,
    TEST_MAVEDB_ATHENA_ROW,
    TEST_SAVED_TAXONOMY,
    TEST_USER,
)


@pytest.fixture
def setup_worker_db(session):
    db = session
    db.add(User(**TEST_USER))
    db.add(User(**EXTRA_USER))
    db.add(Taxonomy(**TEST_SAVED_TAXONOMY))
    db.add(License(**TEST_LICENSE))
    db.add(License(**TEST_INACTIVE_LICENSE))
    db.commit()


@pytest.fixture
def with_populated_job_data(
    session,
    sample_job_run,
    sample_pipeline,
    sample_empty_pipeline,
    sample_job_dependency,
    sample_dependent_job_run,
    sample_independent_job_run,
):
    """Set up the database with sample data for worker tests."""
    session.add(sample_pipeline)
    session.add(sample_empty_pipeline)
    session.add(sample_job_run)
    session.add(sample_dependent_job_run)
    session.add(sample_independent_job_run)
    session.add(sample_job_dependency)
    session.commit()


@pytest.fixture
def mock_pipeline():
    """Create a mock Pipeline instance. By default,
    properties are identical to a default new Pipeline entered into the db
    with sensible defaults for non-nullable but unset fields.
    """
    return Mock(
        spec=Pipeline,
        id=1,
        urn="test:pipeline:1",
        name="Test Pipeline",
        description="A test pipeline",
        status=PipelineStatus.CREATED,
        correlation_id="test_correlation_123",
        metadata_={},
        created_at=datetime.now(),
        started_at=None,
        finished_at=None,
        created_by_user_id=None,
        mavedb_version=None,
    )


@pytest.fixture
def mock_job_run(mock_pipeline):
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
        pipeline_id=mock_pipeline.id,
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
        progress_current=None,
        progress_total=None,
        progress_message=None,
        correlation_id=None,
        metadata_={},
        mavedb_version=None,
    )


@pytest.fixture
def data_files(tmp_path):
    copytree(Path(__file__).absolute().parent / "data", tmp_path / "data")
    return tmp_path / "data"


@pytest.fixture
def mocked_gnomad_variant_row():
    gnomad_variant = Mock()

    for key, value in TEST_MAVEDB_ATHENA_ROW.items():
        setattr(gnomad_variant, key, value)

    return gnomad_variant
