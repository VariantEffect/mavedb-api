# Job Decorators

Decorators are the bridge between the infrastructure layer and business layer. They wrap job functions to provide lifecycle management, error handling, state persistence, and pipeline coordination — so job functions can focus purely on business logic.

## Available Decorators

### `@with_pipeline_management` — The Default Choice

**Use for**: Any job that belongs to (or may belong to) a pipeline. This is the most commonly used decorator.

```python
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management

@with_pipeline_management
async def my_job(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    # Business logic here
    return JobExecutionOutcome.succeeded(data={...})
```

**What it does** (in order):
1. Creates a task-local DB session via `ensure_session_ctx()`
2. Checks test mode — if `MAVEDB_TEST_MODE=1`, skips all decorator logic and calls the function directly
3. Loads the job's `pipeline_id` from the `JobRun` record
4. If the pipeline exists and is in `CREATED` state, starts it (status → `RUNNING`) without coordinating yet
5. Wraps the function with `@with_job_management` (see below) and executes it
6. After the job completes (success or failure): calls `PipelineManager.coordinate_pipeline()`
7. On unhandled exceptions: rolls back, attempts final coordination, sends Slack alert, swallows exception so ARQ finishes cleanly

**If the job has no pipeline** (pipeline_id is null): the decorator skips all pipeline coordination and only applies job management. This makes it safe to use on jobs that might or might not be part of a pipeline.

### `@with_job_management` — Job Lifecycle Only

**Use for**: Standalone jobs that will never be part of a pipeline. Usually stacked under `@with_guaranteed_job_run_record`.

```python
from mavedb.worker.lib.decorators.job_management import with_job_management

@with_job_management
async def my_standalone_job(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    # Business logic here
    return JobExecutionOutcome.succeeded(data={...})
```

**What it does** (in order):
1. Creates a task-local DB session via `ensure_session_ctx()`
2. Checks test mode — if `MAVEDB_TEST_MODE=1`, calls the function directly
3. Extracts `db`, `redis`, and `job_id` from context/args
4. Creates a `JobManager` instance
5. Checks if the job is already in a terminal state (race condition protection — e.g., a sibling job cancelled this one before ARQ picked it up). If so, returns `SKIPPED`.
6. Marks job as `RUNNING` and commits
7. Injects `job_manager` into kwargs and calls the function
8. Based on the returned `JobExecutionOutcome.status`:
   - `SUCCEEDED` → `job_manager.succeed_job()` + commit
   - `FAILED` → `job_manager.fail_job()` + Slack alert + commit
   - `ERRORED` → `job_manager.error_job()` + Slack alert + commit
   - `SKIPPED` → `job_manager.skip_job()` + commit
9. If job didn't succeed: checks `should_retry()` and prepares retry if eligible
10. On unhandled exceptions: rolls back, marks job as `ERRORED`, checks retry, sends Slack alert, swallows exception

### `@with_guaranteed_job_run_record(job_type)` — Auto-Create JobRun

**Use for**: Cron jobs or standalone jobs where no `JobRun` record exists before execution (because no `PipelineFactory` or script pre-created one).

```python
from mavedb.worker.lib.decorators.job_guarantee import with_guaranteed_job_run_record
from mavedb.worker.lib.decorators.job_management import with_job_management

@with_guaranteed_job_run_record("cron_job")
@with_job_management
async def cleanup_stalled_jobs(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    # Business logic here
    return JobExecutionOutcome.succeeded(data={...})
```

**What it does**:
1. Checks test mode — if `MAVEDB_TEST_MODE=1`, calls the function directly
2. If `job_id` is already present (pre-created by a script): validates it exists and passes through
3. Otherwise: creates a new `JobRun` record with the given `job_type` and the function name, commits, and inserts the `job_id` into the function's args
4. Calls the wrapped function (which should be `@with_job_management`)

## Stacking Rules

| Pattern | When |
|---------|------|
| `@with_pipeline_management` alone | Pipeline jobs (most common) |
| `@with_guaranteed_job_run_record` + `@with_job_management` | Standalone/cron jobs needing auto-created JobRun |
| `@with_job_management` alone | Standalone jobs with pre-created JobRun |

**Never** stack `@with_guaranteed_job_run_record` with `@with_pipeline_management`. Pipeline jobs get their `JobRun` records created by `PipelineFactory`, not by the guarantee decorator.

**Never** stack `@with_job_management` on top of `@with_pipeline_management`. The pipeline decorator wraps job management internally.

## Session Management Internals

The `ensure_session_ctx()` context manager (in `lib/decorators/utils.py`) solves a critical concurrency problem:

**Problem**: ARQ runs multiple jobs concurrently as asyncio tasks. If all tasks share the same `ctx["db"]` session, one task closing or rolling back the session can corrupt another task's database operations.

**Solution**: A `ContextVar` named `_task_db_session` provides task-local storage:

```python
@contextmanager
def ensure_session_ctx(ctx):
    existing = _task_db_session.get()
    if existing is not None:
        # Re-entrant: update ctx["db"] to this task's session
        ctx["db"] = existing
        yield existing
    else:
        # First entry: create a new session for this task
        with db_session() as session:
            _task_db_session.set(session)
            ctx["db"] = session
            try:
                yield session
            finally:
                _task_db_session.set(None)
```

This means:
- Each concurrent ARQ job gets its own database session
- Nested decorators (`with_pipeline_management` → `with_job_management`) share the same session via the ContextVar
- The session is cleaned up when the outermost decorator exits

## Test Mode Bypass

All decorators check `is_test_mode()` (which reads `MAVEDB_TEST_MODE` env var) and become **no-ops** when it's set to `"1"`. This is critical for testing because:

1. Decorators are applied at **import time** — they can't be easily mocked or patched
2. Tests need to control the `JobManager` instance (e.g., use a test DB session) rather than having the decorator create one
3. Tests need deterministic behavior without Redis, task-local sessions, or automatic commits

In tests, job functions are called directly:

```python
# Test code
job_manager = JobManager(session, mock_redis, sample_job_run.id)
result = await create_variants_for_score_set(
    mock_worker_ctx,
    sample_job_run.id,
    job_manager,  # Passed directly, not injected by decorator
)
assert result.status == JobStatus.SUCCEEDED
```

The `MAVEDB_TEST_MODE=1` environment variable is set in the test `conftest.py`. The `patch_db_session_ctxmgr` fixture further patches session management for integration tests.

## Error Handling Flow

When a job raises an unhandled exception, the decorator chain handles it:

```
Job function raises Exception
  │
  ├─► @with_job_management catches it
  │     ├─ Rolls back DB session
  │     ├─ Creates JobExecutionOutcome.errored(exception=e)
  │     ├─ Calls job_manager.error_job(result)
  │     ├─ Commits error state
  │     ├─ Checks should_retry()
  │     │   ├─ If retryable: prepare_retry() → commit → return result (don't re-raise)
  │     │   └─ If not: just return result
  │     ├─ Sends Slack alert
  │     └─ Returns result (swallows exception)
  │
  ├─► @with_pipeline_management receives the result
  │     ├─ Calls PipelineManager.coordinate_pipeline()
  │     │   ├─ transition_pipeline_status() → likely FAILED or still RUNNING (if retry pending)
  │     │   ├─ If FAILED: cancel_remaining_jobs()
  │     │   └─ If RUNNING: enqueue_ready_jobs() (may pick up retried job)
  │     └─ Commits coordination changes
  │
  └─► ARQ receives a clean return value (no exception propagation)
```

Exceptions are **swallowed** after alerting. This prevents ARQ from marking the job with its own error handling, since we manage job state ourselves via `JobManager`.

## See Also

- [Job Managers](job_managers.md) — What `JobManager` and `PipelineManager` do
- [Pipeline Management](pipeline_management.md) — How coordination works
- [Best Practices](best_practices.md) — Return value patterns, when to let exceptions propagate
