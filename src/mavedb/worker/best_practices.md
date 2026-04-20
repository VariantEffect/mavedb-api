# Best Practices & Patterns

Concrete patterns to follow when writing job code. Every example comes from or is modeled on the existing codebase.

## Job Function Structure

Every job function follows this template:

```python
@with_pipeline_management
async def my_job(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    # 1. Get the job record and validate params
    job = job_manager.get_job()
    validate_job_params(["score_set_id", "correlation_id"], job)

    # 2. Extract params and set up logging context
    score_set_id = job.job_params["score_set_id"]
    correlation_id = job.job_params["correlation_id"]

    job_manager.save_to_context({
        "application": "mavedb-worker",
        "function": "my_job",
        "resource": score_set_id,
        "correlation_id": correlation_id,
    })

    # 3. Initialize progress
    job_manager.update_progress(0, 100, "Starting my job.")
    logger.info("Starting my job", extra=job_manager.logging_context())

    # 4. Load domain objects and do work
    score_set = job_manager.db.scalars(
        select(ScoreSet).where(ScoreSet.id == score_set_id)
    ).one()

    # ... business logic ...

    # 5. Final progress update and return
    job_manager.update_progress(100, 100, "My job complete.")
    return JobExecutionOutcome.succeeded(data={"items_processed": count})
```

## Parameter Validation

Always validate required parameters at the top of the job function, before accessing them:

```python
job = job_manager.get_job()
_job_required_params = ["score_set_id", "correlation_id", "updater_id"]
validate_job_params(_job_required_params, job)
```

`validate_job_params()` (from `jobs/utils/setup.py`) raises a `KeyError` if any required param is missing from `job.job_params`. This turns into an ERRORED status via the decorator.

**Do not access `job.job_params[key]` without validation first** — a missing key would raise an uncontrolled `KeyError` without a helpful message.

## Return Values

Use `JobExecutionOutcome` factory methods to communicate results:

### Succeeded — job completed normally
```python
return JobExecutionOutcome.succeeded(data={"variants_created": count})
```

### Failed — a business-logic failure (not a bug)
```python
# Missing data, validation failure, precondition not met
if not mapped_variants:
    return JobExecutionOutcome.failed(
        reason="No mapped variants found for score set",
        data={"score_set_id": score_set_id}
    )
```

The decorator marks the job as FAILED. Depending on the pipeline's dependency configuration, downstream jobs may still run (if using `SUCCESS_OR_FAILURE_REQUIRED`) or be cancelled.

### Skipped — job intentionally not executed
```python
# Feature is disabled, already completed, nothing to do
if not settings.LDH_ENABLED:
    return JobExecutionOutcome.skipped(data={"reason": "LDH submissions disabled"})
```

The decorator marks the job as SKIPPED. In pipelines, SKIPPED counts as a completed state for dependency resolution — downstream jobs whose dependency on this job is `SUCCESS_REQUIRED` will NOT be blocked.

### Errored — never return this from job code
Unhandled exceptions are caught by the decorator and automatically create an `.errored()` outcome. Do not return `JobExecutionOutcome.errored()` from job functions.

## Progress Tracking

`update_progress()` commits the session as a checkpoint. This is intentional — it persists progress even if the job fails later.

### Simple progress (known total)
```python
job_manager.update_progress(0, total_records, "Starting variant creation")

for i, record in enumerate(records):
    process_record(record)
    job_manager.update_progress(i + 1, total_records, f"Processed {i + 1}/{total_records} records")
```

### Incremental progress (using convenience methods)
```python
job_manager.set_progress_total(total_records, "Starting variant creation")

for record in records:
    process_record(record)
    job_manager.increment_progress()
```

### Stage-based progress (multiple phases)
```python
job_manager.update_progress(0, 100, "Loading score set data.")
# ... loading phase ...
job_manager.update_progress(25, 100, "Validating variants.")
# ... validation phase ...
job_manager.update_progress(50, 100, "Writing to database.")
# ... write phase ...
job_manager.update_progress(100, 100, "Variant creation complete.")
```

## Logging Context

Always set up logging context early in the job function:

```python
job_manager.save_to_context({
    "application": "mavedb-worker",
    "function": "my_job_name",
    "resource": score_set.urn,
    "correlation_id": correlation_id,
})
```

Then use `job_manager.logging_context()` with every log call:

```python
logger.info("Processing variants", extra=job_manager.logging_context())
logger.warning("Missing expected data", extra=job_manager.logging_context())
```

This provides structured, correlated logs across the full request lifecycle (API request → pipeline creation → multiple job executions).

## External Service Integration Pattern

Jobs that submit to external services follow a consistent pattern:

```python
@with_pipeline_management
async def submit_to_external_service(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    job = job_manager.get_job()
    validate_job_params(["score_set_id", "correlation_id"], job)

    # 1. Check if the service is enabled
    if not settings.SERVICE_ENABLED:
        return JobExecutionOutcome.skipped(data={"reason": "Service submissions disabled"})

    # 2. Load required data
    score_set = job_manager.db.scalars(
        select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])
    ).one()

    # 3. Check preconditions
    if not score_set.mapped_variants:
        return JobExecutionOutcome.failed(reason="No mapped variants to submit")

    # 4. Submit to the service (let exceptions propagate for service errors)
    result = await external_client.submit(score_set)

    # 5. Return outcome
    return JobExecutionOutcome.succeeded(data={"submission_id": result.id})
```

Key points:
- Return `skipped()` if the service is disabled — don't raise an exception
- Return `failed()` if preconditions aren't met — this is a business failure, not a bug
- Let connection errors and timeouts propagate as exceptions — the decorator handles them (ERRORED status, Slack alert, retry logic)

## Database Access

### Use `job_manager.db` for the session
```python
db = job_manager.db  # This is the task-local SQLAlchemy Session

score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()
```

### Do NOT commit from job code
The decorator handles commits for lifecycle transitions. The sole exception is `update_progress()`, which commits as a checkpoint.

If you need database IDs (e.g., after creating records), use `db.flush()`:
```python
new_record = MyModel(name="example")
db.add(new_record)
db.flush()  # new_record.id is now available, but not committed
```

### Bulk operations
For performance-critical operations (e.g., variant creation), use bulk inserts:
```python
db.execute(insert(Variant), variant_dicts)
db.flush()
```

## Score Set Processing State Management

Jobs that process score sets update the score set's `processing_state` and `mapping_state` fields via dedicated methods in `JobManager`:

```python
# Managed by the infrastructure — don't set these directly from job code.
# The decorator/manager handles score set state transitions based on
# the job type and outcome.
```

**Exception**: Some jobs currently manage score set state directly. This is legacy behavior being refactored. New jobs should rely on the infrastructure-layer state management where possible.

## Idempotency Contract

**All job functions must be safe to retry from scratch.** The worker infrastructure retries jobs that fail with transient errors (network timeouts, DB disconnects) and recovers stalled jobs via the cleanup cron. A retried job re-executes the entire function — there is no checkpointing or partial-resume mechanism.

This means a job that partially completes, crashes, and gets retried must not produce duplicate side effects. In practice:

- **Database writes** are generally safe — if the crash happens before commit, the transaction rolls back and retry starts clean.
- **External API submissions** (CAR, LDH, UniProt, ClinGen) must tolerate duplicate calls. Currently our external targets handle this gracefully (idempotent endpoints or deduplication on their side), but this is an implicit assumption, not an enforced guarantee.
- **Cache writes** are inherently idempotent.

When writing a new job that calls an external service, verify that the target handles duplicate submissions. If it doesn't, guard against re-submission by checking for prior results before calling:

```python
# Check if we already submitted successfully in a prior attempt
existing = db.scalars(
    select(Submission).where(
        Submission.score_set_id == score_set_id,
        Submission.status == "accepted",
    )
).first()

if existing:
    return JobExecutionOutcome.succeeded(data={"submission_id": existing.external_id})

# No prior submission — proceed
result = await external_client.submit(score_set)
```

## Common Pitfalls

### Don't call lifecycle methods from job code
```python
# WRONG — the decorator handles these
job_manager.start_job()
job_manager.succeed_job(outcome)

# RIGHT — just return the outcome
return JobExecutionOutcome.succeeded()
```

### Don't construct JobExecutionOutcome directly
```python
# WRONG
return JobExecutionOutcome(status="succeeded", data={})

# RIGHT
return JobExecutionOutcome.succeeded(data={})
```

### Don't catch exceptions just to re-raise or log
```python
# WRONG — the decorator already handles this
try:
    result = await external_service.call()
except Exception as e:
    logger.error(f"Failed: {e}")
    raise

# RIGHT — let it propagate
result = await external_service.call()
```

The decorator catches unhandled exceptions, logs them with full context, sends Slack alerts, and marks the job as ERRORED.

### Don't forget to export new job functions
New job functions must be:
1. Exported from their category's `__init__.py`
2. Added to `BACKGROUND_FUNCTIONS` in `registry.py`
3. Added to a pipeline definition in `definitions.py` (if a pipeline job)

Missing any of these will cause the job to either not be discoverable by ARQ or not be included in a pipeline.

### Don't pass `job_manager` when enqueueing
```python
# WRONG — ARQ can't serialize a JobManager
await redis.enqueue_job("my_job", job_id, job_manager=manager)

# RIGHT — decorator injects job_manager
await redis.enqueue_job("my_job", job_id)
```

## Testing Patterns

### Test mode bypasses decorators
When `MAVEDB_TEST_MODE=1` (set by the test fixtures), all decorators become no-ops. Tests call job functions directly, passing a pre-built `JobManager`:

```python
manager = JobManager(session, mock_worker_ctx["redis"], sample_job_run.id)
result = await create_variants_for_score_set(mock_worker_ctx, sample_job_run.id, manager)
assert result.status == "succeeded"
```

### Mock only at system boundaries
- Mock external services (ClinGen, DCD Mapping, etc.)
- Mock Redis/ARQ enqueue calls
- Mock Slack notifications
- **Do NOT mock** `update_progress`, `validate_job_params`, or other internal helpers

### Use fixtures for job setup
The test `conftest.py` provides fixtures for creating `JobRun` records with the right params structure. Use these rather than constructing records manually.

For complete testing guidelines, see `.github/instructions/testing.instructions.md`.

## See Also

- [Job Registry](job_registry.md) — Step-by-step guides for adding new jobs
- [Job Decorators](job_decorators.md) — How the decorator layer works
- [Job Managers](job_managers.md) — Manager APIs and commit discipline
