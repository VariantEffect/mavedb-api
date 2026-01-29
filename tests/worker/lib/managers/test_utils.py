# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from mavedb.models.enums.job_pipeline import DependencyType, JobStatus
from mavedb.worker.lib.managers.constants import COMPLETED_JOB_STATUSES
from mavedb.worker.lib.managers.utils import (
    construct_bulk_cancellation_result,
    job_dependency_is_met,
    job_should_be_skipped_due_to_unfulfillable_dependency,
)


@pytest.mark.unit
class TestConstructBulkCancellationResultUnit:
    def test_construct_bulk_cancellation_result(self):
        reason = "Test cancellation reason"
        result = construct_bulk_cancellation_result(reason)

        assert result["status"] == "cancelled"
        assert result["data"]["reason"] == reason
        assert "timestamp" in result["data"]
        assert result["exception"] is None


@pytest.mark.unit
class TestJobDependencyIsMetUnit:
    @pytest.mark.parametrize(
        "dependency_type, dependent_job_status, expected",
        [
            (None, "any_status", True),
            # success required dependencies-- should only be met if dependent job succeeded
            (DependencyType.SUCCESS_REQUIRED, JobStatus.SUCCEEDED, True),
            *[
                (DependencyType.SUCCESS_REQUIRED, dependent_job_status, False)
                for dependent_job_status in JobStatus._member_map_.values()
                if dependent_job_status != JobStatus.SUCCEEDED
            ],
            # completion required dependencies-- should be met if dependent job is in any terminal state
            *[
                (
                    DependencyType.COMPLETION_REQUIRED,
                    dependent_job_status,
                    dependent_job_status in COMPLETED_JOB_STATUSES,
                )
                for dependent_job_status in JobStatus._member_map_.values()
            ],
        ],
    )
    def test_job_dependency_is_met(self, dependency_type, dependent_job_status, expected):
        result = job_dependency_is_met(dependency_type, dependent_job_status)
        assert result == expected


@pytest.mark.unit
class TestJobShouldBeSkippedDueToUnfulfillableDependencyUnit:
    @pytest.mark.parametrize(
        "dependency_type, dependent_job_status, expected",
        [
            # No dependency-- should not be skipped
            (None, "any_status", False),
            # success required dependencies-- should be skipped if dependent job in terminal non-success state
            (DependencyType.SUCCESS_REQUIRED, JobStatus.SUCCEEDED, False),
            *[
                (
                    DependencyType.SUCCESS_REQUIRED,
                    dependent_job_status,
                    dependent_job_status in (JobStatus.FAILED, JobStatus.SKIPPED, JobStatus.CANCELLED),
                )
                for dependent_job_status in JobStatus._member_map_.values()
            ],
            # completion required dependencies-- should be skipped if dependent job is not in a terminal state
            *[
                (
                    DependencyType.COMPLETION_REQUIRED,
                    dependent_job_status,
                    dependent_job_status in (JobStatus.CANCELLED, JobStatus.SKIPPED),
                )
                for dependent_job_status in JobStatus._member_map_.values()
            ],
        ],
    )
    def test_job_should_be_skipped_due_to_unfulfillable_dependency(
        self, dependency_type, dependent_job_status, expected
    ):
        result = job_should_be_skipped_due_to_unfulfillable_dependency(dependency_type, dependent_job_status)

        if expected:
            assert result[0] is True
            assert isinstance(result[1], str)
        else:
            assert result == (False, None)
