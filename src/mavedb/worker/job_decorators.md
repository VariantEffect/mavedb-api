# Job Decorators

Job decorators provide lifecycle management, error handling, and audit guarantees for ARQ worker jobs. They are essential for ensuring that jobs are tracked, failures are handled robustly, and pipelines are coordinated correctly.

## Key Decorators

### `with_guaranteed_job_run_record(job_type)`
- Ensures a `JobRun` record is created and persisted before job execution begins.
- Should be applied before any job management decorators.
- Not supported for pipeline jobs.
- Example:
  ```python
  @with_guaranteed_job_run_record("cron_job")
  @with_job_management
  async def my_cron_job(ctx, ...):
      ...
  ```

### `with_job_management`
- Adds automatic job lifecycle management to ARQ worker functions.
- Tracks job start/completion, injects a `JobManager` for progress and state updates, and handles errors robustly.
- Supports both sync and async functions.
- Example:
  ```python
  @with_job_management
  async def my_job(ctx, job_manager: JobManager):
      job_manager.update_progress(10, message="Starting work")
      ...
  ```

### `with_pipeline_management`
- Adds pipeline lifecycle management to jobs that are part of a pipeline.
- Coordinates the pipeline after the job completes (success or failure).
- Built on top of `with_job_management`.
- Example:
  ```python
  @with_pipeline_management
  async def my_pipeline_job(ctx, ...):
      ...
  ```

## Stacking Order
- If using both `with_guaranteed_job_run_record` and `with_job_management`, always apply `with_guaranteed_job_run_record` first.
- For pipeline jobs, use only `with_pipeline_management` (which includes job management).

## See Also
- [Job Managers](job_managers.md)
- [Pipeline Management](pipeline_management.md)
