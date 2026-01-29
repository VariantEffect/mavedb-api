# Pipeline Management

Pipeline management in the ARQ worker system allows for the orchestration of complex workflows composed of multiple dependent jobs. Pipelines are coordinated using the `PipelineManager` and the `with_pipeline_management` decorator.

## Key Concepts
- **Pipeline**: A collection of jobs with defined dependencies and a shared execution context.
- **PipelineManager**: Handles pipeline status, job dependencies, pausing/unpausing, and cancellation.
- **with_pipeline_management**: Decorator that ensures pipeline coordination after job completion.

## Usage Patterns
- Use pipelines for workflows that require multiple jobs to run in sequence or with dependencies.
- Each job in a pipeline should be decorated with `with_pipeline_management`.
- Pipelines are defined and started outside the decorator; the decorator only coordinates after job completion.

## Example
```python
@with_pipeline_management
async def validate_and_map_variants(ctx, ...):
    ...
```

## Features
- Automatic pipeline status updates
- Dependency management and job coordination
- Robust error handling and logging

## See Also
- [Job Managers](job_managers.md)
- [Job Decorators](job_decorators.md)
