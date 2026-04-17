# Job Managers

Managers handle state transitions and coordination. There are two managers, each with a distinct role:

- **`JobManager`** — Manages individual job lifecycle (start, progress, complete, retry). Used by both decorators and job code.
- **`PipelineManager`** — Coordinates pipeline execution (dependency resolution, job enqueueing, status transitions). Used primarily by decorators.

Both inherit from `BaseManager`, which provides a common `db` (SQLAlchemy session) and `redis` (ARQ client) interface.

## JobManager — Individual Job Lifecycle

### Who uses it

| Context | How it's used |
|---------|---------------|
| **Job code** | Call `update_progress()`, `save_to_context()`, `logging_context()`, access `db` and `get_job()` |
| **`@with_job_management` decorator** | Call `start_job()`, `succeed_job()`, `fail_job()`, `error_job()`, `should_retry()`, `prepare_retry()` |
| **`PipelineManager`** | Call `prepare_queue()`, `skip_job()`, `cancel_job()`, `reset_job()` |

### Methods job code should use

```python
# Get the JobRun ORM object (to read job_params, status, etc.)
job = job_manager.get_job()

# Access the database session
score_set = job_manager.db.scalars(select(ScoreSet).where(...)).one()

# Update progress (commits by default as a checkpoint)
job_manager.update_progress(current=50, total=100, message="Processing variants")
job_manager.update_progress(75, 100, "Annotating", commit=False)  # Skip checkpoint

# Update just the status message (commits by default)
job_manager.update_status_message("Connecting to ClinGen API...")

# Add context for structured logging
job_manager.save_to_context({
    "score_set_id": score_set.id,
    "correlation_id": correlation_id,
    "function": "create_variants_for_score_set",
})
logger.info("Started processing", extra=job_manager.logging_context())
```

### Methods decorators/infrastructure use (not job code)

| Method | What it does |
|--------|-------------|
| `start_job()` | Transitions QUEUED/PENDING → RUNNING, sets started_at timestamp |
| `complete_job(status, result)` | Transitions to terminal status, sets finished_at, records result |
| `succeed_job(result)` | Shortcut for `complete_job(SUCCEEDED, result)` |
| `fail_job(result)` | Shortcut for `complete_job(FAILED, result)` |
| `error_job(result)` | Shortcut for `complete_job(ERRORED, result)` |
| `cancel_job(result)` | Shortcut for `complete_job(CANCELLED, result)` |
| `skip_job(result)` | Shortcut for `complete_job(SKIPPED, result)` |
| `should_retry()` | Checks retry_count < max_retries AND failure_category is retryable |
| `prepare_retry(reason)` | Resets job to PENDING, increments retry_count, records retry history |
| `prepare_queue()` | Transitions PENDING → QUEUED before ARQ enqueueing |
| `reset_job()` | Resets all fields to initial state (for pipeline restart) |
| `get_job_status()` | Returns current `JobStatus` |
| `is_cancelled()` | Checks if job has been cancelled |

### Commit discipline

**JobManager methods do not commit.** They mutate the `JobRun` ORM object in memory. The **caller** (decorator or pipeline manager) is responsible for committing.

**Exception**: `update_progress(commit=True)` (the default) commits immediately as a checkpoint. This is by design — it provides real-time progress visibility and creates safe transaction boundaries during long-running jobs.

When `update_progress()` commits, it commits **all** pending session changes, not just the progress update. Call it only at safe transaction boundaries (e.g., after processing a batch of independent records).

### Exception hierarchy

```
ManagerError
├── JobManagerError
│   ├── JobStateError          # Cannot persist state changes (critical)
│   ├── JobTransitionError     # Invalid state transition (e.g., start already-running job)
│   └── DatabaseConnectionError  # Cannot fetch job from DB
└── PipelineManagerError
    ├── PipelineStateError         # Cannot persist pipeline state (critical)
    ├── PipelineTransitionError    # Invalid pipeline state transition
    └── PipelineCoordinationError  # Coordination failed (enqueueing, cancelling)
```

All exceptions are defined in `lib/managers/exceptions.py`.

## PipelineManager — Pipeline Coordination

### Who uses it

| Context | How it's used |
|---------|---------------|
| **`@with_pipeline_management` decorator** | Calls `coordinate_pipeline()` after each job completes |
| **`start_pipeline` job** | Calls `coordinate_pipeline()` explicitly for initial coordination |
| **`cleanup_stalled_jobs`** | Uses it to check dependencies before re-enqueueing stalled pipeline jobs |
| **Scripts** | Manual pipeline operations (pause, cancel, restart) |

### Key methods

| Method | What it does |
|--------|-------------|
| `start_pipeline()` | Sets CREATED → RUNNING, optionally coordinates |
| `coordinate_pipeline()` | Main coordination loop: updates status, enqueues ready jobs or cancels remaining |
| `transition_pipeline_status()` | Analyzes job status distribution, determines pipeline status |
| `enqueue_ready_jobs()` | Finds PENDING jobs with met dependencies, marks QUEUED, enqueues in ARQ |
| `cancel_remaining_jobs(reason)` | Skips PENDING jobs, cancels QUEUED/RUNNING jobs |
| `cancel_pipeline(reason)` | Sets pipeline CANCELLED, coordinates cleanup |
| `pause_pipeline(reason)` | Sets PAUSED, stops new job enqueueing |
| `unpause_pipeline(reason)` | Sets RUNNING, resumes coordination |
| `restart_pipeline()` | Resets all jobs and pipeline, starts fresh |
| `can_enqueue_job(job)` | Checks if all dependencies for a job are met |
| `should_skip_job_due_to_dependencies(job)` | Checks if a job has unfulfillable dependencies |
| `get_pipeline_progress()` | Returns progress statistics dict |
| `get_job_counts_by_status()` | Returns dict of `JobStatus → count` |

### Commit discipline

PipelineManager methods generally **flush** (not commit) for status changes. The notable exception:

**`enqueue_ready_jobs()` commits before the async Redis enqueue loop.** This is critical to prevent deadlocks:
- `flush()` holds PostgreSQL row-level locks
- The `await` in the enqueue loop yields control to the event loop
- A downstream job started by ARQ could attempt a synchronous UPDATE on the locked row
- Since psycopg2 is synchronous, that UPDATE would block the event loop entirely

By committing before the loop, we release the locks and prevent this deadlock scenario.

## Status Grouping Constants

The `lib/managers/constants.py` module defines commonly-used status groupings:

```python
STARTABLE_JOB_STATUSES = [QUEUED, PENDING]
TERMINAL_JOB_STATUSES = [SUCCEEDED, FAILED, ERRORED, CANCELLED, SKIPPED]
COMPLETED_JOB_STATUSES = [SUCCEEDED, FAILED, ERRORED]
ACTIVE_JOB_STATUSES = [PENDING, QUEUED, RUNNING]
RETRYABLE_JOB_STATUSES = [FAILED, ERRORED, CANCELLED, SKIPPED]
CANCELLED_JOB_STATUSES = [CANCELLED, SKIPPED, FAILED, ERRORED]

TERMINAL_PIPELINE_STATUSES = [SUCCEEDED, FAILED, PARTIAL, CANCELLED]
RUNNING_PIPELINE_STATUSES = [RUNNING]
CANCELLED_PIPELINE_STATUSES = [CANCELLED, FAILED]

RETRYABLE_FAILURE_CATEGORIES = (NETWORK_ERROR, TIMEOUT, SERVICE_UNAVAILABLE)
```

These are used throughout the managers and decorators for state validation and transition logic. Always use these constants rather than hardcoding status checks.

## See Also

- [Job Decorators](job_decorators.md) — How decorators call manager methods
- [Pipeline Management](pipeline_management.md) — Detailed coordination logic
- [Best Practices](best_practices.md) — How to use JobManager from job code
