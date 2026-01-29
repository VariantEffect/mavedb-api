# Job System Overview

The ARQ worker job system in MaveDB provides a robust, scalable, and auditable framework for background processing, data management, and integration with external services. It is designed to support both simple jobs and complex pipelines with dependency management, error handling, and progress tracking.

## Key Concepts

- **Job**: A discrete unit of work, typically implemented as an async function, executed by the ARQ worker.
- **Pipeline**: A sequence of jobs with defined dependencies, managed as a single workflow.
- **JobRun**: A database record tracking the execution state, progress, and results of a job.
- **JobManager**: A class responsible for managing the lifecycle and state transitions of a single job.
- **PipelineManager**: A class responsible for coordinating pipelines, managing dependencies, and updating pipeline status.
- **Decorators**: Utilities that add lifecycle management, error handling, and audit guarantees to job functions.

## Directory Structure

- `jobs/` — Entrypoints and registry for all ARQ worker jobs.
- `jobs/data_management/`, `jobs/external_services/`, `jobs/variant_processing/`, etc. — Job implementations grouped by domain.
- `lib/decorators/` — Decorators for job and pipeline management.
- `lib/managers/` — JobManager, PipelineManager, and related utilities.

## Job Lifecycle

1. **Job Registration**: All available jobs are registered in `jobs/registry.py` for ARQ configuration.
2. **Job Execution**: Jobs are executed by the ARQ worker, with decorators ensuring audit, error handling, and state management.
3. **State Tracking**: Each job run is tracked in the database via a `JobRun` record.
4. **Pipeline Coordination**: For jobs that are part of a pipeline, the `PipelineManager` coordinates dependencies and status.

## When to Add a Job
- When you need background processing, integration with external APIs, or scheduled/cron tasks.
- When you want robust error handling, progress tracking, and auditability for long-running or critical operations.

See the following sections for details on decorators, managers, and best practices.
