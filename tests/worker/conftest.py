"""
Test configuration and fixtures for worker lib tests.
"""

from datetime import datetime
from pathlib import Path
from shutil import copytree
from unittest.mock import Mock

import pandas as pd
import pytest

from mavedb.models.enums.job_pipeline import DependencyType, JobStatus, PipelineStatus
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.job_dependency import JobDependency
from mavedb.models.job_run import JobRun
from mavedb.models.license import License
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.user import User
from tests.helpers.constants import EXTRA_USER, TEST_LICENSE, TEST_USER

# Attempt to import optional top level fixtures. If the modules they depend on are not installed,
# we won't have access to our full fixture suite and only a limited subset of tests can be run.
try:
    from .conftest_optional import *  # noqa: F401, F403

except ModuleNotFoundError:
    pass


@pytest.fixture
def sample_job_run(sample_pipeline):
    """Create a sample JobRun instance for testing."""
    return JobRun(
        id=1,
        urn="test:job:1",
        job_type="test_job",
        job_function="test_function",
        status=JobStatus.PENDING,
        pipeline_id=sample_pipeline.id,
        progress_current=0,
        progress_total=100,
        progress_message="Ready to start",
        created_at=datetime.now(),
    )


@pytest.fixture
def sample_dependent_job_run(sample_pipeline):
    """Create a sample dependent JobRun instance for testing."""
    return JobRun(
        id=2,
        urn="test:job:2",
        job_type="dependent_job",
        job_function="dependent_function",
        status=JobStatus.PENDING,
        pipeline_id=sample_pipeline.id,
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
def sample_empty_pipeline():
    """Create a sample Pipeline instance with no jobs for testing."""
    return Pipeline(
        id=999,
        urn="test:pipeline:999",
        name="Empty Pipeline",
        description="A pipeline with no jobs",
        status=PipelineStatus.CREATED,
        correlation_id="empty_correlation_456",
        created_at=datetime.now(),
    )


@pytest.fixture
def sample_job_dependency(sample_dependent_job_run, sample_job_run):
    """Create a sample JobDependency instance for testing."""
    return JobDependency(
        id=sample_dependent_job_run.id,  # dependent job
        depends_on_job_id=sample_job_run.id,  # depends on job 1
        dependency_type=DependencyType.SUCCESS_REQUIRED,
        created_at=datetime.now(),
    )


@pytest.fixture
def sample_user():
    """Create a sample User instance for testing."""
    return User(**TEST_USER)


@pytest.fixture
def sample_extra_user():
    """Create an extra sample User instance for testing."""
    return User(**EXTRA_USER)


@pytest.fixture
def sample_license():
    """Create a sample License instance for testing."""
    return License(**TEST_LICENSE)


@pytest.fixture
def sample_experiment_set(sample_user):
    """Create a sample ExperimentSet instance for testing."""
    return ExperimentSet(
        extra_metadata={},
        created_by=sample_user,
    )


@pytest.fixture
def sample_experiment(sample_experiment_set, sample_user):
    """Create a sample Experiment instance for testing."""
    return Experiment(
        title="Sample Experiment",
        short_description="A sample experiment for testing purposes",
        abstract_text="This is an abstract for the sample experiment.",
        method_text="This is a method description for the sample experiment.",
        extra_metadata={},
        experiment_set=sample_experiment_set,
        created_by=sample_user,
    )


@pytest.fixture
def sample_score_set(sample_experiment, sample_user, sample_license):
    """Create a sample ScoreSet instance for testing."""
    return ScoreSet(
        title="Sample Score Set",
        short_description="A sample score set for testing purposes",
        abstract_text="This is an abstract for the sample score set.",
        method_text="This is a method description for the sample score set.",
        extra_metadata={},
        experiment=sample_experiment,
        created_by=sample_user,
        license=sample_license,
        target_genes=[
            TargetGene(
                name="Sample Gene",
                category="protein_coding",
                target_sequence=TargetSequence(label="testsequence", sequence_type="dna", sequence="ATGCAT"),
            )
        ],
    )


@pytest.fixture
def with_populated_domain_data(
    session,
    sample_user,
    sample_extra_user,
    sample_experiment_set,
    sample_experiment,
    sample_score_set,
    sample_license,
):
    db = session
    db.add(sample_user)
    db.add(sample_extra_user)
    db.add(sample_experiment_set)
    db.add(sample_experiment)
    db.add(sample_score_set)
    db.add(sample_license)
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
def sample_score_dataframe(data_files):
    return pd.read_csv(data_files / "scores.csv")


@pytest.fixture
def sample_count_dataframe(data_files):
    return pd.read_csv(data_files / "counts.csv")
