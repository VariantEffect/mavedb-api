---
description: 'MaveDB worker patterns — jobs, decorators, managers, pipelines'
applyTo: 'src/mavedb/worker/**/*.py'
---

# Worker Conventions for MaveDB

*For comprehensive documentation with walkthroughs and examples, see `src/mavedb/worker/README.md` and linked docs.*

## Architecture

The worker is a two-layer system:

- **Infrastructure layer** (`lib/decorators/`, `lib/managers/`): Handles job lifecycle, state persistence, error recovery, pipeline coordination. Developers rarely modify this.
- **Business layer** (`jobs/`): Implements domain logic (variant creation, mapping, external service calls). This is where most new code goes.

Decorators bridge the two layers. Job functions focus purely on business logic and return a `JobExecutionOutcome`. Decorators handle lifecycle state, commits, error recovery, and pipeline coordination automatically.

## Job Function Contract

Every job function follows this signature:

```python
@with_pipeline_management
async def my_job(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
```

- `ctx`: ARQ context dict containing `db`, `redis`, `hdp` (HGVS data provider), `pool`, `state`
- `job_id`: `JobRun.id` from the database (passed by the caller / ARQ)
- `job_manager`: Injected by the decorator — **NOT passed by the caller**
- Return: Always a `JobExecutionOutcome` via its factory methods

**Callers enqueue jobs with only the job function name and `job_id`.** The decorator injects `job_manager` as a keyword argument before the function executes.

## Decorator Rules

| Decorator | Use For | Stacking |
|-----------|---------|----------|
| `@with_pipeline_management` | Jobs that belong (or may belong) to a pipeline | Use alone — it wraps `@with_job_management` internally |
| `@with_job_management` | Standalone jobs without pipeline coordination | Use alone or under `@with_guaranteed_job_run_record` |
| `@with_guaranteed_job_run_record(type)` | Cron/standalone jobs that need a `JobRun` record auto-created | Must be outermost; stack with `@with_job_management` only |

**Most jobs use `@with_pipeline_management`** because it works for both pipeline and non-pipeline jobs. If the job has no pipeline association, the decorator simply skips coordination.

`@with_guaranteed_job_run_record` is **NOT compatible** with `@with_pipeline_management`. It is only for standalone/cron jobs.

### Decorator internals

- All decorators become no-ops when `MAVEDB_TEST_MODE=1` (checked via `is_test_mode()`). This allows tests to call job functions directly with a pre-built `JobManager`.
- `ensure_session_ctx()` creates a task-local DB session via `ContextVar`, preventing concurrent ARQ jobs from sharing or closing each other's sessions.
- `with_pipeline_management` wraps `with_job_management` internally by calling `with_job_management(func)` inside `_execute_managed_pipeline`. Do not stack them manually.

## JobManager API (What Job Code Uses)

From within a job function, use `job_manager` for:

```python
# Access the job's DB record and parameters
job = job_manager.get_job()                      # Returns JobRun ORM object
params = job.job_params                           # Dict of job parameters (JSONB)

# Access the database session
score_set = job_manager.db.scalars(select(ScoreSet).where(...)).one()

# Progress tracking (each call commits as a checkpoint by default)
job_manager.update_progress(current, total, message)
job_manager.update_progress(50, 100, "Halfway done", commit=False)  # Skip checkpoint

# Logging context
job_manager.save_to_context({"score_set_id": 123, "correlation_id": "abc"})
logger.info("Processing", extra=job_manager.logging_context())
```

**Do not call** `start_job()`, `succeed_job()`, `fail_job()`, `error_job()`, or `complete_job()` from job code. The decorator handles these based on the `JobExecutionOutcome` you return.

## Session & Commit Discipline

- **Decorators handle commits** for job lifecycle state transitions (start, complete, fail, retry)
- **`update_progress()` commits by default** as a checkpoint — this commits ALL pending session changes, so call it only at safe transaction boundaries. Pass `commit=False` to skip.
- **Job code should NOT call `db.commit()`** — use `db.flush()` if you need generated IDs before the decorator commits
- **PipelineManager commits before its async Redis enqueue loop** to release PostgreSQL row locks and prevent deadlocks (psycopg2 is synchronous, so a blocked UPDATE would freeze asyncio)

## Return Values (JobExecutionOutcome)

Always return using factory methods:

```python
return JobExecutionOutcome.succeeded(data={"variants_created": count})
return JobExecutionOutcome.failed(reason="No mapped variants found", data={...})
return JobExecutionOutcome.skipped(data={"reason": "Feature disabled"})
# For unhandled exceptions: let them propagate — the decorator catches and creates .errored()
```

**Do not return `.errored()` from job code.** Let unhandled exceptions propagate; the decorator catches them, marks the job as ERRORED, sends Slack alerts, and handles retry logic.

## Parameter Access Pattern

Job parameters live in `JobRun.job_params` (JSONB column), not in function arguments:

```python
job = job_manager.get_job()

_job_required_params = ["score_set_id", "correlation_id", "updater_id"]
validate_job_params(_job_required_params, job)

score_set_id = job.job_params["score_set_id"]
correlation_id = job.job_params["correlation_id"]
```

Always call `validate_job_params()` (from `worker.jobs.utils.setup`) before accessing params.

Parameters with `None` values in pipeline definitions are filled at runtime from `pipeline_params` passed by the router/script when creating the pipeline.

## Error Handling

- **Business failures** (validation errors, missing data): Return `JobExecutionOutcome.failed(reason=...)`
- **Unhandled exceptions**: Let them propagate. The decorator catches them, marks the job as ERRORED, sends a Slack alert, and evaluates retry eligibility.
- **External service disabled/unavailable**: Return `JobExecutionOutcome.skipped()` if a config check shows the service is disabled. Let connection errors propagate for retry handling.
- **Retry eligibility**: Determined by `should_retry()` which checks `retry_count < max_retries` and `failure_category in RETRYABLE_FAILURE_CATEGORIES`.

## Pipeline Lifecycle (Brief)

1. Router calls `PipelineFactory.create_pipeline()` → creates `Pipeline`, `JobRun`, and `JobDependency` records
2. Router enqueues the `start_pipeline` entrypoint job in ARQ
3. `start_pipeline` runs → its `@with_pipeline_management` decorator starts the pipeline and calls `coordinate_pipeline()`
4. `coordinate_pipeline()` finds PENDING jobs whose dependencies are met → marks them QUEUED → enqueues in ARQ
5. Each job runs → after completion, its decorator calls `coordinate_pipeline()` again
6. Cycle repeats until all jobs complete or the pipeline fails/is cancelled

Pipeline definitions live in `src/mavedb/lib/workflow/definitions.py`. The `PipelineFactory` (in `src/mavedb/lib/workflow/pipeline_factory.py`) reads these definitions and creates the DB records.

*For full details, see `src/mavedb/worker/pipeline_management.md`.*

## Adding a New Pipeline Job

1. Create the job function in `src/mavedb/worker/jobs/<category>/<name>.py`
2. Decorate with `@with_pipeline_management`
3. Follow the signature: `async def job_name(ctx, job_id, job_manager) -> JobExecutionOutcome`
4. Export from the category's `__init__.py`
5. Register in `src/mavedb/worker/jobs/registry.py` → add to `BACKGROUND_FUNCTIONS`
6. Add a `JobDefinition` entry to the relevant pipeline in `src/mavedb/lib/workflow/definitions.py`

## Adding a Standalone/Cron Job

1. Create the job function in `src/mavedb/worker/jobs/<category>/<name>.py`
2. Stack `@with_guaranteed_job_run_record("job_type")` (outer) + `@with_job_management` (inner)
3. Export from the category's `__init__.py`
4. Register in `src/mavedb/worker/jobs/registry.py` → add to `BACKGROUND_FUNCTIONS`
5. For cron: also add to `BACKGROUND_CRONJOBS` with schedule
6. Optionally add to `STANDALONE_JOB_DEFINITIONS` if the job needs to be invoked via operational scripts

## Testing

- Decorators are no-ops in test mode (`MAVEDB_TEST_MODE=1`). Tests call job functions directly, passing a real `JobManager` instance.
- Assert on `JobExecutionOutcome.status` and `.data` for every job test.
- Assert on DB state changes (query for created/updated/deleted records).
- Let `update_progress()` run unpatched — its commit behavior is production behavior that should be tested.
- Mock only at system boundaries (external APIs, S3, Slack). Do not mock internal helpers.
- Use `TransactionSpy` in manager/decorator tests only, not in job-level tests.

*For full testing conventions, see `.github/instructions/testing.instructions.md`.*

## Key Reference Files

| File | Purpose |
|------|---------|
| `jobs/registry.py` | All registered job functions, cron definitions, standalone definitions |
| `jobs/variant_processing/creation.py` | Reference pipeline job implementation |
| `jobs/system/cleanup.py` | Reference standalone cron job implementation |
| `lib/decorators/pipeline_management.py` | Pipeline decorator (coordinates after job completion) |
| `lib/decorators/job_management.py` | Job lifecycle decorator (start/complete/error handling) |
| `lib/decorators/utils.py` | Session management (`ensure_session_ctx`), test mode (`is_test_mode`) |
| `lib/managers/job_manager.py` | Job state management (used by decorators and job code) |
| `lib/managers/pipeline_manager.py` | Pipeline coordination, dependency resolution, job enqueueing |
| `lib/managers/constants.py` | Status groupings (`TERMINAL_JOB_STATUSES`, `STARTABLE_JOB_STATUSES`, etc.) |
| `lib/managers/exceptions.py` | Exception hierarchy (`JobStateError`, `PipelineCoordinationError`, etc.) |
| `settings/worker.py` | `ArqWorkerSettings` class (ARQ worker configuration) |
| `settings/lifecycle.py` | Worker startup/shutdown hooks, `standalone_ctx()` |
| `src/mavedb/lib/workflow/definitions.py` | Pipeline and job definitions (`PIPELINE_DEFINITIONS`) |
| `src/mavedb/lib/workflow/pipeline_factory.py` | Creates Pipeline + JobRun + JobDependency records |
| `src/mavedb/lib/types/workflow.py` | `JobExecutionOutcome`, `JobDefinition`, `PipelineDefinition` types |
| `src/mavedb/models/job_run.py` | `JobRun` ORM model |
| `src/mavedb/models/pipeline.py` | `Pipeline` ORM model |
| `src/mavedb/models/enums/job_pipeline.py` | `JobStatus`, `PipelineStatus`, `DependencyType`, `FailureCategory` enums |
