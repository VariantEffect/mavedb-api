# Job Registry and Configuration

The registry (`jobs/registry.py`) is the central manifest of all worker jobs. ARQ uses it to discover available functions, cron schedules, and job metadata.

## Registry Components

### `BACKGROUND_FUNCTIONS`

A flat list of all async job functions that ARQ can execute. Every job — whether pipeline, standalone, or cron — must be listed here.

```python
BACKGROUND_FUNCTIONS: List[Callable] = [
    # Variant processing jobs
    create_variants_for_score_set,
    map_variants_for_score_set,
    # External service jobs
    submit_score_set_mappings_to_car,
    submit_score_set_mappings_to_ldh,
    refresh_clinvar_controls,
    # ... etc
    # Pipeline management jobs
    start_pipeline,
    # System maintenance jobs
    cleanup_stalled_jobs,
]
```

ARQ resolves functions by name — the `job_function` field on `JobRun` must match `func.__name__` for the function listed here.

### `BACKGROUND_CRONJOBS`

Cron-scheduled jobs with ARQ's `cron()` utility:

```python
BACKGROUND_CRONJOBS: List[CronJob] = [
    cron(
        refresh_materialized_views,
        name="refresh_all_materialized_views",
        hour=20, minute=0,
        keep_result=timedelta(minutes=2).total_seconds(),
    ),
    cron(
        cleanup_stalled_jobs,
        name="cleanup_stalled_jobs_cron",
        minute={15, 45},  # Every 30 minutes
        keep_result=timedelta(minutes=25).total_seconds(),
    ),
]
```

### `STANDALONE_JOB_DEFINITIONS`

Metadata for jobs that can be invoked independently via operational scripts (`run_job.py`). Maps function references to `JobDefinition` dicts:

```python
STANDALONE_JOB_DEFINITIONS: dict[Callable, JobDefinition] = {
    create_variants_for_score_set: {
        "dependencies": [],
        "params": {"score_set_id": None, "updater_id": None, ...},
        "function": "create_variants_for_score_set",
        "key": "create_variants_for_score_set",
        "type": JobType.VARIANT_CREATION,
    },
    # ...
}
```

These are used by `src/mavedb/scripts/run_job.py` to create a `JobRun` with the correct params structure for running a single job outside of a pipeline.

### `PIPELINE_DEFINITIONS`

Located in `src/mavedb/lib/workflow/definitions.py` (not in the registry file). Defines multi-step pipeline workflows. See [Pipeline Management](pipeline_management.md#defining-a-new-pipeline) for details.

## Adding a Pipeline Job

Follow these steps to add a new job to an existing pipeline:

### 1. Create the job function

Create a new file or add to an existing file in the appropriate `jobs/<category>/` directory:

```python
# src/mavedb/worker/jobs/external_services/my_new_service.py

import logging
from sqlalchemy import select

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)


@with_pipeline_management
async def submit_to_new_service(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Submit mapped variants to NewService for annotation."""
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    score_set = job_manager.db.scalars(
        select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])
    ).one()
    correlation_id = job.job_params["correlation_id"]

    job_manager.save_to_context({
        "application": "mavedb-worker",
        "function": "submit_to_new_service",
        "resource": score_set.urn,
        "correlation_id": correlation_id,
    })
    job_manager.update_progress(0, 100, "Starting NewService submission.")
    logger.info("Started NewService submission", extra=job_manager.logging_context())

    # ... business logic ...

    job_manager.update_progress(100, 100, "NewService submission complete.")
    return JobExecutionOutcome.succeeded(data={"variants_submitted": count})
```

### 2. Export from the category's `__init__.py`

```python
# src/mavedb/worker/jobs/external_services/__init__.py
from mavedb.worker.jobs.external_services.my_new_service import submit_to_new_service
```

### 3. Register in `registry.py`

Add the function to `BACKGROUND_FUNCTIONS`:

```python
from mavedb.worker.jobs.external_services import submit_to_new_service

BACKGROUND_FUNCTIONS: List[Callable] = [
    # ... existing entries ...
    submit_to_new_service,
]
```

### 4. Add to pipeline definition

In `src/mavedb/lib/workflow/definitions.py`, add a `JobDefinition` to the appropriate pipeline:

```python
{
    "key": "submit_to_new_service",
    "function": "submit_to_new_service",
    "type": JobType.MAPPED_VARIANT_ANNOTATION,
    "params": {
        "correlation_id": None,
        "score_set_id": None,
    },
    "dependencies": [("map_variants_for_score_set", DependencyType.SUCCESS_REQUIRED)],
},
```

### 5. Write tests

Create `tests/worker/jobs/external_services/test_my_new_service.py` following the patterns in existing test files (e.g., `test_clingen.py`).

## Adding a Standalone/Cron Job

### 1. Create the job function

```python
# src/mavedb/worker/jobs/system/my_maintenance.py

import logging
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.worker.lib.decorators.job_guarantee import with_guaranteed_job_run_record
from mavedb.worker.lib.decorators.job_management import with_job_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)


@with_guaranteed_job_run_record("system_maintenance")
@with_job_management
async def my_maintenance_job(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Periodic maintenance task."""
    db = job_manager.db

    # ... maintenance logic ...

    return JobExecutionOutcome.succeeded(data={"records_cleaned": count})
```

### 2. Export and register

Same as steps 2-3 for pipeline jobs.

### 3. Add cron schedule (if applicable)

```python
BACKGROUND_CRONJOBS: List[CronJob] = [
    # ... existing entries ...
    cron(
        my_maintenance_job,
        name="my_maintenance_job_cron",
        hour=4, minute=0,  # Run daily at 4:00 AM
        keep_result=timedelta(minutes=5).total_seconds(),
    ),
]
```

### 4. Add to `STANDALONE_JOB_DEFINITIONS` (if needed)

Only if the job should be invocable via `run_job.py` for manual execution:

```python
STANDALONE_JOB_DEFINITIONS: dict[Callable, JobDefinition] = {
    # ... existing entries ...
    my_maintenance_job: {
        "dependencies": [],
        "params": {},
        "function": "my_maintenance_job",
        "key": "my_maintenance_job",
        "type": JobType.SYSTEM_MAINTENANCE,
    },
}
```

## Worker Settings

The `ArqWorkerSettings` class (in `settings/worker.py`) brings everything together for ARQ:

```python
class ArqWorkerSettings:
    on_startup = startup          # Create ProcessPoolExecutor
    on_shutdown = shutdown
    on_job_start = on_job_start   # Initialize hdp (HGVS data provider), state dict
    on_job_end = on_job_end
    after_job_end = log_job       # Canonical job logging
    redis_settings = RedisWorkerSettings
    functions = BACKGROUND_FUNCTIONS
    cron_jobs = BACKGROUND_CRONJOBS
    job_timeout = 5 * 60 * 60    # 5 hours
```

The lifecycle hooks (in `settings/lifecycle.py`) manage the ARQ context dict (`ctx`):
- `startup`: Creates `ProcessPoolExecutor` for CPU-intensive tasks
- `on_job_start`: Initializes `hdp` (CDOT REST data provider) and `state` dict
- `standalone_ctx()`: Creates an equivalent context for running jobs outside ARQ (used by scripts)

## See Also

- [Job System Overview](jobs_overview.md) — How everything fits together
- [Pipeline Management](pipeline_management.md) — Pipeline definitions and coordination
- [Best Practices](best_practices.md) — Patterns for writing job code
