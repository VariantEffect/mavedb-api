import pytest

from mavedb.lib.workflow.job_factory import JobFactory
from mavedb.lib.workflow.pipeline_factory import PipelineFactory


@pytest.fixture
def job_factory(session):
    """Fixture to provide a mocked JobFactory instance."""
    yield JobFactory(session)


@pytest.fixture
def pipeline_factory(session):
    """Fixture to provide a mocked PipelineFactory instance."""
    yield PipelineFactory(session)
