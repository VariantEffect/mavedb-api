---
description: 'MaveDB API patterns — routers, authentication, authorization, endpoints'
applyTo: 'src/mavedb/routers/**/*.py'
---

# API Patterns for MaveDB

## Router Structure

All routers use:
- `ROUTER_BASE_PREFIX = "/api/v1"` from `src/mavedb/routers/__init__.py`
- `LoggedRoute` as the custom `route_class` for canonical request/response logging
- Kebab-case URL paths: `/score-sets`, `/experiment-sets`

```python
router = APIRouter(
    prefix="/api/v1/score-sets",
    tags=["score-sets"],
    route_class=LoggedRoute,
    responses=shared_responses,
)
```

## Authentication

Three tiers of auth dependency injection:

| Dependency | Returns | Use When |
|-----------|---------|----------|
| `get_current_user` | `Optional[UserData]` | Public endpoints that behave differently for authenticated users |
| `require_current_user` | `UserData` | Endpoints requiring login |
| `require_current_user_with_email` | `UserData` | Endpoints requiring verified email (write operations) |

Auth supports two mechanisms:
- **ORCID JWT tokens** — primary auth for web users
- **API keys** — for programmatic access

```python
@router.get("/{urn}")
def get_score_set(
    urn: str,
    db: Session = Depends(get_db),
    user: Optional[UserData] = Depends(get_current_user),
):
    ...
```

## Authorization

Permission checks use the `assert_permission()` helper with an `Action` enum:

```python
from mavedb.lib.authorization import assert_permission, Action

assert_permission(user, item, Action.READ)    # View
assert_permission(user, item, Action.UPDATE)  # Modify
assert_permission(user, item, Action.DELETE)  # Delete
assert_permission(user, item, Action.ADD_ROLE)  # Manage contributors
```

Key authorization behaviors:
- **Private resources return 404** (not 403) to prevent information leakage about existence
- Permission logic dispatches by resource type (ExperimentSet, Experiment, ScoreSet, etc.)
- Admins bypass most permission checks

## Endpoint Patterns

### Standard CRUD
```python
@router.get("/", response_model=list[ScoreSetShortModel])
def list_score_sets(db: Session = Depends(get_db)): ...

@router.get("/{urn}", response_model=ScoreSetFullModel)
def get_score_set(urn: str, db: Session = Depends(get_db)): ...

@router.post("/", response_model=ScoreSetSavedModel, status_code=201)
def create_score_set(body: ScoreSetCreateModel, db: Session = Depends(get_db)): ...

@router.put("/{urn}", response_model=ScoreSetSavedModel)
def update_score_set(urn: str, body: ScoreSetUpdateModel, db: Session = Depends(get_db)): ...

@router.delete("/{urn}", status_code=204)
def delete_score_set(urn: str, db: Session = Depends(get_db)): ...
```

### Background Job Enqueueing
For operations that trigger async processing:
```python
@router.post("/{urn}:publish")
async def publish_score_set(
    urn: str,
    db: Session = Depends(get_db),
    user: UserData = Depends(require_current_user_with_email),
    worker: ArqRedis = Depends(get_worker),
):
    # ... validation and DB updates ...
    await enqueue_pipeline_for_score_set(
        db=db,
        redis=worker,
        pipeline_name="publish_score_set",
        score_set=item,
        user=user_data.user,
    )
```

### Error Responses
Shared error response definitions are used across routers:
```python
responses=shared_responses  # Defines 4xx/5xx response schemas
```

## Worker Integration

### Pipeline System

Most write operations trigger a multi-step pipeline via the worker:

```python
from mavedb.lib.workflow.kickoff import enqueue_pipeline_for_score_set

# In a router endpoint:
pipeline, job = await enqueue_pipeline_for_score_set(
    db=db,
    redis=worker,
    pipeline_name="validate_map_annotate_score_set",
    score_set=item,
    user=user_data.user,
    extra_params={"scores_file_key": scores_file_key, ...},
)
```

`enqueue_pipeline_for_score_set` is the single kickoff seam shared by routers and scripts. It fills `score_set_id`, `updater_id` and `correlation_id`, uses `PipelineFactory` to create a `Pipeline` with its `JobRun` and `JobDependency` records, then enqueues the pipeline's `start_pipeline` entrypoint in ARQ. A failed enqueue discards the just-created pipeline records. The worker coordinates the rest — each job runs after its dependencies complete.

### Job Function Signature

All job functions follow this signature (the decorator injects `job_manager`):

```python
@with_pipeline_management
async def create_variants_for_score_set(
    ctx: dict, job_id: int, job_manager: JobManager
) -> JobExecutionOutcome:
    job = job_manager.get_job()
    validate_job_params(["score_set_id", "correlation_id", "updater_id"], job)
    # ... business logic using job_manager.db ...
    return JobExecutionOutcome.succeeded(data={"variants_created": count})
```

Callers pass only `ctx` and `job_id` when enqueueing. The decorator creates the `JobManager` from the `job_id`.

### Correlation IDs

Correlation IDs flow from the API request through the pipeline to each job:

```python
# In the router — save request context; the kickoff seam reads the correlation ID from it
from mavedb.lib.logging.context import save_to_logging_context

save_to_logging_context({"score_set_urn": urn})
await enqueue_pipeline_for_score_set(db=db, redis=worker, pipeline_name="validate_map_annotate_score_set", ...)
```

Each job retrieves the correlation ID from its `job_params` and uses `job_manager.save_to_context()` for structured logging.

For detailed worker conventions, see `.github/instructions/worker.instructions.md` and `src/mavedb/worker/README.md`.
