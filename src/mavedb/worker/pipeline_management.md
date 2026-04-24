# Pipeline Management

Pipelines orchestrate multi-step workflows where jobs have dependency relationships. The system handles job ordering, status propagation, failure cascading, retries, pausing, and cancellation.

## Pipeline Lifecycle

```
                        ┌──────────┐
    PipelineFactory ──► │ CREATED  │
                        └────┬─────┘
                             │ start_pipeline job runs
                        ┌────▼─────┐
               ┌───────►│ RUNNING  │◄───────┐
               │        └──┬───┬───┘        │
               │           │   │            │
          unpause          │   │        coordinate_pipeline()
               │           │   │        enqueues ready jobs
               │           │   │            │
        ┌──────┴──┐        │   │            │
        │ PAUSED  │◄───────┘   │            │
        └─────────┘  pause     │
                               │
              ┌────────────────┼────────────────┐
              │                │                │
        ┌─────▼─────┐  ┌──────▼──────┐  ┌──────▼──────┐
        │ SUCCEEDED  │  │   FAILED    │  │  PARTIAL    │
        │ (all ok)   │  │ (any error) │  │ (mixed)     │
        └────────────┘  └─────────────┘  └─────────────┘

                        ┌─────────────┐
        cancel_pipeline │ CANCELLED   │
          ──────────────►             │
                        └─────────────┘
```

## Defining a New Pipeline

Pipelines are declared in `src/mavedb/lib/workflow/definitions.py` as entries in `PIPELINE_DEFINITIONS`:

```python
PIPELINE_DEFINITIONS: dict[str, PipelineDefinition] = {
    "my_new_pipeline": {
        "description": "Human-readable description of what this pipeline does",
        "job_definitions": [
            {
                "key": "first_job",                          # Unique key within pipeline
                "function": "first_job_function_name",       # Must match registered function name
                "type": JobType.VARIANT_CREATION,            # Job category enum
                "params": {
                    "score_set_id": None,                    # None = filled at runtime from pipeline_params
                    "correlation_id": None,
                },
                "dependencies": [],                          # No dependencies = runs first
            },
            {
                "key": "second_job",
                "function": "second_job_function_name",
                "type": JobType.VARIANT_MAPPING,
                "params": {
                    "score_set_id": None,
                    "correlation_id": None,
                },
                "dependencies": [
                    ("first_job", DependencyType.SUCCESS_REQUIRED),  # Runs only after first_job succeeds
                ],
            },
            {
                "key": "optional_annotation",
                "function": "annotate_function_name",
                "type": JobType.MAPPED_VARIANT_ANNOTATION,
                "params": {
                    "score_set_id": None,
                    "correlation_id": None,
                },
                "dependencies": [
                    ("second_job", DependencyType.COMPLETION_REQUIRED),  # Runs even if second_job fails
                ],
            },
        ],
    },
}
```

### Key rules for pipeline definitions

- **`key`** must be unique within the pipeline. By convention, use the function name. For repeated functions (e.g., `refresh_clinvar_controls` for different date ranges), add a suffix: `refresh_clinvar_controls_202501`.
- **`function`** must match a registered function name in `BACKGROUND_FUNCTIONS`.
- **`params`** values of `None` are populated at runtime from `pipeline_params`. Values with actual data (e.g., `"year": 2025`) are used as-is.
- **`dependencies`** reference other jobs by their `key`. Use `SUCCESS_REQUIRED` when the dependent job cannot proceed without the prerequisite's output. Use `COMPLETION_REQUIRED` when the dependent job should run regardless of whether the prerequisite succeeded or failed.

## How Pipelines Are Created and Triggered

### From a Router Endpoint

```python
# In src/mavedb/routers/score_sets.py
pipeline_factory = PipelineFactory(session=db)
pipeline, pipeline_entrypoint = pipeline_factory.create_pipeline(
    pipeline_name="validate_map_annotate_score_set",
    creating_user=user_data.user,
    pipeline_params={
        "correlation_id": correlation_id_for_context(),
        "score_set_id": item.id,
        "updater_id": user_data.user.id,
        "scores_file_key": scores_file_key,
        "counts_file_key": counts_file_key,
        "score_columns_metadata": {...},
        "count_columns_metadata": {...},
    },
)

# Enqueue only the start_pipeline entrypoint — coordination handles the rest
job = await worker.enqueue_job(
    pipeline_entrypoint.job_function,
    pipeline_entrypoint.id,
    _job_id=arq_job_id(pipeline_entrypoint.urn),
)
```

### What PipelineFactory.create_pipeline() Does

1. Looks up `PIPELINE_DEFINITIONS[pipeline_name]`
2. Creates a `Pipeline` record (status=CREATED)
3. Creates a `start_pipeline` `JobRun` as the pipeline entrypoint
4. For each `JobDefinition` in the pipeline: creates a `JobRun` with params merged from `pipeline_params`
5. For each dependency: creates a `JobDependency` record
6. Commits everything and returns `(pipeline, start_pipeline_job_run)`

### From a Script

```python
# In src/mavedb/scripts/run_pipeline.py
pipeline_factory = PipelineFactory(session=db)
pipeline, entrypoint = pipeline_factory.create_pipeline(
    pipeline_name="validate_map_annotate_score_set",
    creating_user=user,
    pipeline_params={...},
)
```

## Coordination Loop

The `PipelineManager.coordinate_pipeline()` method is the heart of pipeline orchestration. It runs after every job completes (called by the `@with_pipeline_management` decorator):

```python
async def coordinate_pipeline(self):
    # 1. Evaluate pipeline status from job states
    new_status = self.transition_pipeline_status()
    self.db.flush()

    # 2. If pipeline failed/cancelled → cancel remaining jobs
    if new_status in CANCELLED_PIPELINE_STATUSES:
        self.cancel_remaining_jobs(reason="Pipeline failed or cancelled")

    # 3. If pipeline still running → find and enqueue ready jobs
    if new_status in RUNNING_PIPELINE_STATUSES:
        await self.enqueue_ready_jobs()

        # 4. Re-evaluate status (some jobs may have been skipped due to unfulfillable deps)
        self.transition_pipeline_status()
        self.db.flush()
```

### How `transition_pipeline_status()` Determines Status

The method counts jobs by status and applies these rules in order:

| Condition | New Pipeline Status |
|-----------|-------------------|
| Any job `FAILED` or `ERRORED` | `FAILED` |
| Any job `RUNNING` or `QUEUED` | `RUNNING` |
| Any job `PENDING` | No change (waiting for coordination) |
| All jobs `SUCCEEDED` | `SUCCEEDED` |
| Mix of `SUCCEEDED` + `SKIPPED`/`CANCELLED` | `PARTIAL` |
| All remaining jobs `CANCELLED` | `CANCELLED` |

### How `enqueue_ready_jobs()` Works

For each PENDING job in the pipeline:
1. Check if all dependencies are met (via `can_enqueue_job()`)
2. If met: mark as QUEUED via `JobManager.prepare_queue()`
3. If dependencies are unfulfillable (e.g., hard dependency on a failed job): mark as SKIPPED
4. **Commit** all status changes before the async enqueue loop (prevents PostgreSQL deadlocks)
5. Enqueue each QUEUED job in ARQ

### Dependency Resolution

A dependency is **met** when:
- `SUCCESS_REQUIRED`: prerequisite job status is `SUCCEEDED`
- `COMPLETION_REQUIRED`: prerequisite job is in any completed state (`SUCCEEDED`, `FAILED`, `ERRORED`)

A dependency is **unfulfillable** when:
- `SUCCESS_REQUIRED`: prerequisite job is in a terminal non-success state (`FAILED`, `ERRORED`, `SKIPPED`, `CANCELLED`)

When a dependency is unfulfillable, the dependent job is proactively **skipped** rather than left pending forever.

## Pipeline Operations

### Pause / Unpause

```python
await pipeline_manager.pause_pipeline(reason="Maintenance window")
# Running jobs complete, but no new jobs are enqueued
# ...later...
await pipeline_manager.unpause_pipeline(reason="Maintenance complete")
# Resumes coordination, enqueues ready jobs
```

### Cancel

```python
await pipeline_manager.cancel_pipeline(reason="User requested")
# Sets pipeline to CANCELLED, skips PENDING jobs, cancels QUEUED/RUNNING jobs
```

### Restart

```python
await pipeline_manager.restart_pipeline()
# Resets ALL jobs to PENDING, resets pipeline to CREATED, then starts fresh
```

## Failure and Retry Behavior

When a job fails:

1. The `@with_job_management` decorator marks it as `FAILED` or `ERRORED`
2. It checks `should_retry()`: retry count < max and failure category is retryable
3. If retryable: `prepare_retry()` resets job to `PENDING` with incremented `retry_count`
4. The `@with_pipeline_management` decorator calls `coordinate_pipeline()`
5. Coordination finds the retried job (now PENDING) and re-enqueues it if dependencies are met
6. If not retryable: job stays `FAILED`, coordination marks pipeline as `FAILED`, cancels remaining jobs

### Stalled Job Recovery

The `cleanup_stalled_jobs` cron job (runs every 30 minutes) catches jobs stuck in intermediate states:

| State | Timeout | Action |
|-------|---------|--------|
| `QUEUED` | 10 minutes | Fail → retry if eligible |
| `RUNNING` | 60 minutes | Fail → retry if eligible |
| `PENDING` (in pipeline) | 30 minutes | Fail → retry if eligible |

## See Also

- [Job System Overview](jobs_overview.md) — End-to-end flow diagrams
- [Job Decorators](job_decorators.md) — How decorators trigger coordination
- [Job Managers](job_managers.md) — JobManager and PipelineManager APIs
- [Job Registry](job_registry.md) — How to register pipeline definitions
