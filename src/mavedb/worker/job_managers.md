# Job Managers

Job managers are responsible for the lifecycle, state transitions, and progress tracking of jobs and pipelines. They provide atomic operations, robust error handling, and ensure data consistency.

## JobManager
- Manages the lifecycle of a single job (start, progress, success, failure, retry, cancel).
- Ensures atomic state transitions and safe rollback on failure.
- Does not commit database changes (only flushes); the caller is responsible for commits.
- Handles progress tracking, retry logic, and session cleanup.
- Example usage:
  ```python
  manager = JobManager(db, redis, job_id=123)
  manager.start_job()
  manager.update_progress(25, message="Starting validation")
  manager.succeed_job(result={"count": 100})
  ```

## PipelineManager
- Coordinates pipeline execution, manages job dependencies, and updates pipeline status.
- Handles pausing, unpausing, and cancellation of pipelines.
- Uses the same exception hierarchy as JobManager for consistency.
- Example usage:
  ```python
  pipeline_manager = PipelineManager(db, redis, pipeline_id=456)
  await pipeline_manager.coordinate_pipeline()
  new_status = pipeline_manager.transition_pipeline_status()
  cancelled_count = pipeline_manager.cancel_remaining_jobs(reason="Dependency failed")
  ```

## Exception Handling
- Both managers use custom exceptions for database errors, state errors, and coordination errors.
- Always handle exceptions at the job or pipeline boundary to ensure robust recovery and logging.

## See Also
- [Job Decorators](job_decorators.md)
- [Pipeline Management](pipeline_management.md)
