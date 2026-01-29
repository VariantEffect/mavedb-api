# Best Practices & Patterns

## General Principles
- Use decorators to ensure all jobs are tracked, auditable, and robust to errors.
- Keep job functions focused and stateless; use the database and JobManager for state.
- Prefer async functions for jobs to maximize concurrency.
- Use the appropriate manager (JobManager or PipelineManager) for state transitions and coordination.
- Write unit tests for job logic and integration tests for job orchestration.

## Error Handling
- Always handle exceptions at the job or pipeline boundary. Legacy score set and mapping jobs track status at the 
item level, but this will be remedied in a future update.
- Use custom exception types for clarity and recovery strategies.
- Log all errors with sufficient context for debugging and audit.

## Job Design
- Use `with_guaranteed_job_run_record` for standalone jobs that require audit.
- Use `with_pipeline_management` for jobs that are part of a pipeline.
- Avoid side effects outside the job context; use dependency injection for testability.

## Testing
- Mock external services in unit tests.
- Use integration tests to verify job and pipeline orchestration.
- Test error paths and recovery logic.

## Documentation
- Document each job's purpose, parameters, and expected side effects.
- Update the registry and README when adding new jobs.

## References
- See the other markdown files in this directory for detailed usage and examples.
