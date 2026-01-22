from unittest.mock import Mock

import pytest

from mavedb.models.job_run import JobRun
from mavedb.worker.jobs.utils.setup import validate_job_params


@pytest.mark.unit
def test_validate_job_params_success():
    job = Mock(spec=JobRun, job_params={"foo": 1, "bar": 2})

    # Should not raise
    validate_job_params(["foo", "bar"], job)


@pytest.mark.unit
def test_validate_job_params_missing_param():
    job = Mock(spec=JobRun, job_params={"foo": 1})

    with pytest.raises(ValueError, match="Missing required job param: bar"):
        validate_job_params(["foo", "bar"], job)


@pytest.mark.unit
def test_validate_job_params_no_params():
    job = Mock(spec=JobRun, job_params=None)

    with pytest.raises(ValueError, match="Job has no job_params defined."):
        validate_job_params(["foo"], job)
