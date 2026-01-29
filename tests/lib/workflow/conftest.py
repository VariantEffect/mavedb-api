from unittest.mock import patch

import pytest

from mavedb.models.enums.job_pipeline import DependencyType
from mavedb.models.user import User
from tests.helpers.constants import TEST_USER

try:
    from .conftest_optional import *  # noqa: F403, F401
except ImportError:
    pass


@pytest.fixture
def sample_job_definition():
    """Provides a sample job definition for testing."""
    return {
        "key": "sample_job",
        "type": "data_processing",
        "function": "process_data",
        "params": {"param1": "value1", "param2": "value2", "required_param": None},
        "dependencies": [],
    }


@pytest.fixture
def sample_independent_pipeline_definition(sample_job_definition):
    """Provides a sample pipeline definition for testing."""
    return {
        "name": "sample_pipeline",
        "description": "A sample pipeline for testing purposes.",
        "job_definitions": [sample_job_definition],
    }


@pytest.fixture
def sample_dependent_pipeline_definition():
    """Provides a sample pipeline definition with job dependencies for testing."""
    job_def_1 = {
        "key": "job_1",
        "type": "data_processing",
        "function": "process_data_1",
        "params": {"paramA": None},
        "dependencies": [],
    }
    job_def_2 = {
        "key": "job_2",
        "type": "data_processing",
        "function": "process_data_2",
        "params": {"paramB": None},
        "dependencies": [("job_1", DependencyType.SUCCESS_REQUIRED)],
    }
    return {
        "name": "dependent_pipeline",
        "description": "A sample pipeline with job dependencies for testing.",
        "job_definitions": [job_def_1, job_def_2],
    }


@pytest.fixture
def with_test_pipeline_definition_ctx(sample_dependent_pipeline_definition, sample_independent_pipeline_definition):
    """Fixture to temporarily add a test pipeline definition."""
    test_pipeline_definitions = {
        sample_dependent_pipeline_definition["name"]: sample_dependent_pipeline_definition,
        sample_independent_pipeline_definition["name"]: sample_independent_pipeline_definition,
    }

    with patch("mavedb.lib.workflow.pipeline_factory.PIPELINE_DEFINITIONS", test_pipeline_definitions):
        yield


@pytest.fixture
def test_user(session):
    """Fixture to create and provide a test user in the database."""
    db = session
    user = User(**TEST_USER)
    db.add(user)
    db.commit()
    yield user
