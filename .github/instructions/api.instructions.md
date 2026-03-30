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
    await worker.enqueue_job(
        "create_variants_for_score_set",
        score_set.id,
        correlation_id,
    )
```

### Error Responses
Shared error response definitions are used across routers:
```python
responses=shared_responses  # Defines 4xx/5xx response schemas
```

## Worker Integration

### Job Pipeline
Many operations chain through multiple worker jobs:
1. `create_variants_for_score_set` — Parse uploaded CSV, create variant records
2. `map_variants_for_score_set` — Map variants via DCD Mapping / VRS
3. `submit_score_set_mappings_to_*` — Submit to ClinGen services

### Job Patterns
```python
async def create_variants_for_score_set(ctx: dict, score_set_id: int, correlation_id: str):
    logging_context = setup_job_state(ctx, correlation_id)
    db = ctx["db"]

    try:
        # ... processing ...
        pass
    except Exception as e:
        send_slack_error(e, logging_context)
        raise
```

### Backoff and Retry
Use `enqueue_job_with_backoff()` for jobs that may need retries (e.g., external service calls).

## Correlation IDs
Every request gets a correlation ID via starlette-context middleware. Pass it to worker jobs for end-to-end request tracing:
```python
from mavedb.lib.logging.context import save_to_logging_context
correlation_id = save_to_logging_context({"score_set_urn": urn})
```
