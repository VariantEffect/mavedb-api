# Job Registry and Configuration

All ARQ worker jobs must be registered for execution and scheduling. The registry provides a centralized list of available jobs and cron jobs for ARQ configuration.

## Job Registry
- Located in `jobs/registry.py`.
- Lists all job functions in `BACKGROUND_FUNCTIONS` for ARQ worker discovery.
- Defines scheduled (cron) jobs in `BACKGROUND_CRONJOBS` using ARQ's `cron` utility.

## Example
```python
from mavedb.worker.jobs.data_management import refresh_materialized_views
from mavedb.worker.jobs.external_services import submit_score_set_mappings_to_car

BACKGROUND_FUNCTIONS = [
    refresh_materialized_views,
    submit_score_set_mappings_to_car,
    ...
]

BACKGROUND_CRONJOBS = [
    cron(
        refresh_materialized_views,
        name="refresh_all_materialized_views",
        hour=20,
        minute=0,
        keep_result=timedelta(minutes=2).total_seconds(),
    ),
]
```

## Adding a New Job
1. Implement the job function in the appropriate submodule.
2. Add the function to `BACKGROUND_FUNCTIONS` in `registry.py`.
3. (Optional) Add a cron job to `BACKGROUND_CRONJOBS` if scheduling is needed.

## See Also
- [Job System Overview](jobs_overview.md)
- [Best Practices](best_practices.md)
