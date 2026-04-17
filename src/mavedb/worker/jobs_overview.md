# Job System Overview

## Core Concepts

| Concept | What It Is | Where It Lives |
|---------|-----------|----------------|
| **Job** | An async function that performs a unit of work (e.g., create variants, submit to ClinGen) | `jobs/<category>/<name>.py` |
| **Pipeline** | A collection of jobs with dependency ordering, executed as a workflow | `Pipeline` model + `PIPELINE_DEFINITIONS` |
| **JobRun** | A database record tracking a single job execution: status, params, progress, errors, retries | `models/job_run.py` |
| **JobDependency** | A record expressing that one job depends on another (with a dependency type) | `models/job_dependency.py` |
| **JobManager** | Manages individual job state transitions (start, progress, complete, retry) | `lib/managers/job_manager.py` |
| **PipelineManager** | Coordinates pipeline execution: dependency resolution, job enqueueing, status transitions | `lib/managers/pipeline_manager.py` |
| **Decorator** | Wraps job functions to add lifecycle management, error handling, and pipeline coordination | `lib/decorators/` |
| **JobExecutionOutcome** | Dataclass returned by every job function to indicate success, failure, skip, or error | `lib/types/workflow.py` |

## Two Execution Flows

### Flow 1: Pipeline Jobs (Most Common)

This is how variant processing works end-to-end:

```
Router (score_sets.py)
  │
  ├─ 1. PipelineFactory.create_pipeline("validate_map_annotate_score_set", ...)
  │     └─ Creates in DB:
  │         • Pipeline record (status=CREATED)
  │         • start_pipeline JobRun (entrypoint)
  │         • create_variants_for_score_set JobRun
  │         • map_variants_for_score_set JobRun (depends on create_variants)
  │         • submit_to_car JobRun (depends on map_variants)
  │         • link_gnomad JobRun (depends on submit_to_car)
  │         • ... more annotation jobs with dependencies
  │         • JobDependency records linking them
  │
  ├─ 2. worker.enqueue_job("start_pipeline", entrypoint.id)
  │     └─ Enqueues the start_pipeline job in ARQ/Redis
  │
  └─ 3. Returns HTTP response immediately (fire-and-forget)

ARQ Worker picks up start_pipeline
  │
  ├─ 4. @with_pipeline_management decorator:
  │     ├─ Creates task-local DB session (ensure_session_ctx)
  │     ├─ Starts pipeline (status → RUNNING)
  │     ├─ Wraps function with @with_job_management
  │     │   ├─ Marks start_pipeline job as RUNNING
  │     │   ├─ Runs start_pipeline function body
  │     │   │   └─ Calls PipelineManager.coordinate_pipeline()
  │     │   └─ Marks start_pipeline job as SUCCEEDED
  │     └─ After job completion, calls coordinate_pipeline() again
  │         ├─ Finds create_variants (PENDING, no dependencies) → QUEUED → enqueue in ARQ
  │         └─ Other jobs still have unmet dependencies → stay PENDING
  │
  ├─ 5. ARQ picks up create_variants_for_score_set
  │     ├─ @with_pipeline_management runs job, marks SUCCEEDED
  │     └─ coordinate_pipeline() finds map_variants (dependency met) → enqueue
  │
  ├─ 6. ARQ picks up map_variants_for_score_set
  │     ├─ @with_pipeline_management runs job, marks SUCCEEDED
  │     └─ coordinate_pipeline() finds submit_to_car, submit_uniprot, etc. → enqueue
  │
  ├─ 7... Continues until all jobs complete
  │
  └─ 8. Final coordinate_pipeline() → all jobs SUCCEEDED → pipeline status → SUCCEEDED
```

### Flow 2: Standalone/Cron Jobs

Used for system maintenance tasks that don't belong to a pipeline:

```
ARQ Cron Scheduler (or manual enqueue)
  │
  ├─ 1. @with_guaranteed_job_run_record("cron_job")
  │     └─ Creates a JobRun record in DB (since no PipelineFactory did it)
  │
  ├─ 2. @with_job_management
  │     ├─ Marks job RUNNING
  │     ├─ Injects JobManager into function kwargs
  │     ├─ Runs the job function
  │     └─ Marks job SUCCEEDED/FAILED/ERRORED based on return value
  │
  └─ 3. No pipeline coordination (job has no pipeline_id)
```

Example: `cleanup_stalled_jobs` runs every 30 minutes via ARQ cron to find and handle stuck jobs.

## Key Models

### JobRun (`models/job_run.py`)

The central record for every job execution:

| Field | Purpose |
|-------|---------|
| `id` | Primary key, passed as `job_id` to job functions |
| `urn` | Human-readable identifier (e.g., `mavedb:job_run:abc123`), used as ARQ `_job_id` |
| `job_type` | Category string (e.g., `"variant_creation"`, `"cron_job"`) |
| `job_function` | Function name (e.g., `"create_variants_for_score_set"`) |
| `job_params` | JSONB dict of runtime parameters (score_set_id, correlation_id, etc.) |
| `status` | Current `JobStatus` enum value |
| `pipeline_id` | FK to `Pipeline` (null for standalone jobs) |
| `max_retries` | Maximum retry attempts (default: 3) |
| `retry_count` | Current retry attempt count |
| `progress_current/total/message` | Progress tracking fields |
| `error_message/error_traceback` | Error details on failure |
| `failure_category` | `FailureCategory` enum for retry classification |
| `metadata_` | JSONB for retry history, result snapshots, etc. |
| `correlation_id` | End-to-end request tracing ID |

### Pipeline (`models/pipeline.py`)

Groups related jobs into a workflow:

| Field | Purpose |
|-------|---------|
| `id` | Primary key |
| `name` | Pipeline definition name (e.g., `"validate_map_annotate_score_set"`) |
| `status` | Current `PipelineStatus` enum value |
| `correlation_id` | Shared tracing ID for all jobs in pipeline |
| `job_runs` | Relationship to all `JobRun` records in this pipeline |

### JobDependency (`models/job_dependency.py`)

Expresses execution ordering between jobs:

| Field | Purpose |
|-------|---------|
| `id` | FK to the dependent job (the one that waits) |
| `depends_on_job_id` | FK to the prerequisite job |
| `dependency_type` | `SUCCESS_REQUIRED` or `COMPLETION_REQUIRED` |

## Status Enums

### JobStatus

```
PENDING ──► QUEUED ──► RUNNING ──► SUCCEEDED
                          │
                          ├──► FAILED    (business logic failure)
                          ├──► ERRORED   (unhandled exception)
                          ├──► CANCELLED (pipeline cancelled remaining jobs)
                          └──► SKIPPED   (dependency unfulfillable or feature disabled)

FAILED/ERRORED ──► PENDING (via prepare_retry, if retryable)
```

### PipelineStatus

```
CREATED ──► RUNNING ──► SUCCEEDED  (all jobs succeeded)
                  │
                  ├──► FAILED      (any job failed/errored)
                  ├──► PARTIAL     (mix of succeeded + skipped/cancelled, no failures)
                  ├──► CANCELLED   (manually cancelled)
                  └──► PAUSED ──► RUNNING (via unpause)
```

### DependencyType

| Type | Meaning |
|------|---------|
| `SUCCESS_REQUIRED` | Dependent job runs only if prerequisite **succeeded** |
| `COMPLETION_REQUIRED` | Dependent job runs if prerequisite reached any **completed** state (succeeded, failed, or errored) |

### FailureCategory

Classifies why a job failed, used to determine retry eligibility:

- **Retryable**: `NETWORK_ERROR`, `TIMEOUT`, `SERVICE_UNAVAILABLE`
- **Non-retryable**: `VALIDATION_ERROR`, `DATA_ERROR`, `SYSTEM_ERROR`, etc.

See `models/enums/job_pipeline.py` for the full list.

## How Job Parameters Flow

Parameters originate from the router/script and flow through the pipeline to individual jobs:

```
Router (score_sets.py)
  │
  │  pipeline_params = {
  │      "score_set_id": 42,
  │      "correlation_id": "abc-123",
  │      "updater_id": 7,
  │      "scores_file_key": "42/7/1234-scores.csv",
  │      ...
  │  }
  │
  ├─► PipelineFactory.create_pipeline(pipeline_params=pipeline_params)
  │     │
  │     ├─► Reads PIPELINE_DEFINITIONS["validate_map_annotate_score_set"]
  │     │   Each job_definition has a "params" dict with None placeholders:
  │     │     {"score_set_id": None, "correlation_id": None, ...}
  │     │
  │     ├─► JobFactory.create_job_run() merges pipeline_params into each job's params:
  │     │     JobRun.job_params = {"score_set_id": 42, "correlation_id": "abc-123", ...}
  │     │
  │     └─► Each JobRun record now has its own copy of the params it needs
  │
  └─► In the job function:
        job = job_manager.get_job()
        score_set_id = job.job_params["score_set_id"]  # → 42
```

## See Also

- [Job Decorators](job_decorators.md) — How lifecycle management works internally
- [Job Managers](job_managers.md) — Manager class APIs and commit discipline
- [Pipeline Management](pipeline_management.md) — Pipeline lifecycle and coordination details
- [Job Registry](job_registry.md) — How to register jobs and step-by-step guides
- [Best Practices](best_practices.md) — Coding patterns and conventions for job code
