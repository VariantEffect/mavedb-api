"""Tests for JobExecutionOutcome dataclass and factory methods."""

import pytest

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus


@pytest.mark.unit
class TestJobExecutionOutcomeSucceeded:
    def test_default(self):
        result = JobExecutionOutcome.succeeded()
        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {}
        assert result.error is None
        assert result.exception is None

    def test_with_data(self):
        result = JobExecutionOutcome.succeeded(data={"variant_count": 42})
        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"variant_count": 42}
        assert result.error is None
        assert result.exception is None

    def test_none_data_defaults_to_empty_dict(self):
        result = JobExecutionOutcome.succeeded(data=None)
        assert result.data == {}


@pytest.mark.unit
class TestJobExecutionOutcomeFailed:
    def test_with_reason(self):
        result = JobExecutionOutcome.failed(reason="bad input")
        assert result.status == JobStatus.FAILED
        assert result.error == "bad input"
        assert result.exception is None
        assert result.data == {}

    def test_with_reason_and_data(self):
        result = JobExecutionOutcome.failed(reason="bad input", data={"partial": 5})
        assert result.status == JobStatus.FAILED
        assert result.error == "bad input"
        assert result.data == {"partial": 5}
        assert result.exception is None

    def test_empty_reason_is_valid(self):
        result = JobExecutionOutcome.failed(reason="")
        assert result.error == ""

    def test_none_data_defaults_to_empty_dict(self):
        result = JobExecutionOutcome.failed(reason="x", data=None)
        assert result.data == {}

    def test_with_failure_category(self):
        result = JobExecutionOutcome.failed(reason="HGVS parse error", failure_category=FailureCategory.DATA_ERROR)
        assert result.failure_category == FailureCategory.DATA_ERROR

    def test_without_failure_category_defaults_to_none(self):
        result = JobExecutionOutcome.failed(reason="bad input")
        assert result.failure_category is None


@pytest.mark.unit
class TestJobExecutionOutcomeErrored:
    def test_with_exception(self):
        exc = RuntimeError("boom")
        result = JobExecutionOutcome.errored(exception=exc)
        assert result.status == JobStatus.ERRORED
        assert result.error == "boom"
        assert result.exception is exc
        assert result.data == {}

    def test_with_exception_and_data(self):
        exc = ValueError("invalid")
        result = JobExecutionOutcome.errored(exception=exc, data={"processed": 50})
        assert result.status == JobStatus.ERRORED
        assert result.error == "invalid"
        assert result.data == {"processed": 50}
        assert result.exception is exc

    def test_empty_exception_message(self):
        exc = ValueError("")
        result = JobExecutionOutcome.errored(exception=exc)
        assert result.error == ""

    def test_none_data_defaults_to_empty_dict(self):
        exc = RuntimeError("x")
        result = JobExecutionOutcome.errored(exception=exc, data=None)
        assert result.data == {}

    def test_with_failure_category(self):
        exc = ConnectionError("timeout")
        result = JobExecutionOutcome.errored(exception=exc, failure_category=FailureCategory.NETWORK_ERROR)
        assert result.failure_category == FailureCategory.NETWORK_ERROR

    def test_without_failure_category_defaults_to_none(self):
        exc = RuntimeError("boom")
        result = JobExecutionOutcome.errored(exception=exc)
        assert result.failure_category is None


@pytest.mark.unit
class TestJobExecutionOutcomeSkipped:
    def test_default(self):
        result = JobExecutionOutcome.skipped()
        assert result.status == JobStatus.SKIPPED
        assert result.data == {}
        assert result.error is None
        assert result.exception is None

    def test_with_data(self):
        result = JobExecutionOutcome.skipped(data={"reason": "disabled"})
        assert result.data == {"reason": "disabled"}

    def test_none_data_defaults_to_empty_dict(self):
        result = JobExecutionOutcome.skipped(data=None)
        assert result.data == {}


@pytest.mark.unit
class TestJobExecutionOutcomeDirectConstruction:
    """Direct construction bypassing factories is at-your-own-risk but should not raise."""

    def test_semantically_invalid_combination_is_allowed(self):
        result = JobExecutionOutcome(
            status=JobStatus.SUCCEEDED,
            data={},
            error="oops",
            exception=RuntimeError("x"),
        )
        assert result.status == JobStatus.SUCCEEDED
        assert result.error == "oops"
        assert result.exception is not None


@pytest.mark.unit
class TestJobExecutionOutcomeToDict:
    def test_succeeded(self):
        result = JobExecutionOutcome.succeeded(data={"k": 1})
        d = result.to_dict()
        assert d == {"status": "succeeded", "data": {"k": 1}, "error": None, "failure_category": None}

    def test_failed(self):
        result = JobExecutionOutcome.failed(reason="bad", data={"partial": 3})
        d = result.to_dict()
        assert d == {"status": "failed", "data": {"partial": 3}, "error": "bad", "failure_category": None}

    def test_failed_with_failure_category(self):
        result = JobExecutionOutcome.failed(reason="bad", failure_category=FailureCategory.DATA_ERROR)
        d = result.to_dict()
        assert d["failure_category"] == "data_error"

    def test_errored_excludes_exception(self):
        exc = RuntimeError("crash")
        result = JobExecutionOutcome.errored(exception=exc)
        d = result.to_dict()
        assert d == {"status": "errored", "data": {}, "error": "crash", "failure_category": None}
        assert "exception" not in d

    def test_skipped(self):
        result = JobExecutionOutcome.skipped()
        d = result.to_dict()
        assert d == {"status": "skipped", "data": {}, "error": None, "failure_category": None}
