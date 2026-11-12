# ruff: noqa: E402

import asyncio
import functools
import os
import pickle

import pytest

pytest.importorskip("arq")

from arq.worker import get_kwargs

from mavedb.lib.clingen.services import ClinGenLdhService
from mavedb.lib.logging.context import (
    logging_context_scope,
    run_in_executor_with_logging_context,
    save_to_logging_context,
)
from mavedb.lib.mapping.client import VRSMap
from mavedb.worker.lib.managers.utils import FAST_LANE_JOB_FUNCTIONS, FAST_LANE_QUEUE_NAME
from mavedb.worker.settings import ArqFastLaneWorkerSettings, ArqWorkerSettings
from mavedb.worker.settings.constants import MAX_JOBS
from mavedb.worker.settings.lifecycle import process_pool


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


def record_pid_in_child(value: str) -> int:
    """Module level, so the spawned pool process can import it."""
    save_to_logging_context({"child_fact": value})
    return os.getpid()


@pytest.mark.unit
class TestProcessPoolUnit:
    def test_process_pool_default_has_one_process_per_job_slot(self):
        with process_pool() as pool:
            assert pool._max_workers == MAX_JOBS

    def test_process_pool_default_starts_processes_with_spawn_and_recycles_them(self):
        with process_pool() as pool:
            assert pool._max_tasks_per_child == 1
            assert pool._mp_context.get_start_method() == "spawn"

    def test_process_pool_several_tasks_each_run_in_a_new_process(self):
        with process_pool() as pool:
            pids = {pool.submit(os.getpid).result() for _ in range(3)}

        assert len(pids) == 3
        assert os.getpid() not in pids

    @pytest.mark.parametrize(
        "blocking",
        [
            functools.partial(VRSMap("http://mapper").map_score_set, "urn:mavedb:00000001-a-1"),
            functools.partial(ClinGenLdhService(url="http://ldh").dispatch_submissions, [], 10),
        ],
        ids=["mapping", "ldh-submission"],
    )
    def test_process_pool_job_work_pickles_for_spawned_processes(self, blocking):
        restored = pickle.loads(pickle.dumps(blocking))

        assert restored.func.__name__ == blocking.func.__name__
        assert restored.args == blocking.args


@pytest.mark.integration
def test_process_pool_carries_logging_context_through_a_spawned_process():
    async def run():
        with logging_context_scope({"parent_fact": "p"}) as ctx:
            with process_pool() as pool:
                pid = await run_in_executor_with_logging_context(pool, functools.partial(record_pid_in_child, "c"))
            return pid, ctx

    pid, ctx = asyncio.run(run())

    assert pid != os.getpid()
    assert ctx == {"parent_fact": "p", "child_fact": "c"}
