# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from arq.worker import get_kwargs

from mavedb.worker.lib.managers.utils import FAST_LANE_JOB_FUNCTIONS, FAST_LANE_QUEUE_NAME
from mavedb.worker.settings import ArqFastLaneWorkerSettings, ArqWorkerSettings


@pytest.mark.unit
class TestArqFastLaneWorkerSettingsUnit:
    """ARQ builds a Worker from the settings class ``__dict__``, so assert on what it actually reads."""

    def test_consumes_fast_lane_queue(self):
        assert get_kwargs(ArqFastLaneWorkerSettings)["queue_name"] == FAST_LANE_QUEUE_NAME

    def test_registers_exactly_the_fast_lane_functions(self):
        functions = get_kwargs(ArqFastLaneWorkerSettings)["functions"]
        assert {f.__name__ for f in functions} == FAST_LANE_JOB_FUNCTIONS

    def test_runs_no_cron_jobs(self):
        assert "cron_jobs" not in get_kwargs(ArqFastLaneWorkerSettings)

    def test_main_worker_still_registers_fast_lane_functions(self):
        """Fast-lane jobs already on the default queue at deploy time must still be runnable."""
        kwargs = get_kwargs(ArqWorkerSettings)
        assert "queue_name" not in kwargs
        assert FAST_LANE_JOB_FUNCTIONS <= {f.__name__ for f in kwargs["functions"]}
