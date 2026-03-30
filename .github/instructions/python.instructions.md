---
description: 'MaveDB Python coding conventions — SQLAlchemy, Pydantic, FastAPI patterns'
applyTo: '**/*.py'
---

# Python Conventions for MaveDB

## Style

- Follow PEP 8 with 4-space indentation
- Use type hints on all function signatures
- Use `from __future__ import annotations` where needed for forward references
- Prefer `list[str]` over `List[str]` (Python 3.10+ style) in new code, but match surrounding code style

## SQLAlchemy 2.0 Patterns

Use 2.0-style query syntax throughout:

```python
# Correct — 2.0 style
item = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one()
items = db.scalars(select(ScoreSet).where(ScoreSet.private.is_(False))).all()

# Avoid — legacy 1.x style
item = db.query(ScoreSet).filter_by(urn=urn).one()
```

Use `session.add()` / `session.flush()` / `session.commit()` appropriately:
- Routers: `db.add()` then `db.commit()` for write operations
- Worker jobs: manage sessions via `setup_db_session()` or `SessionLocal()`

## Pydantic v2 Patterns

### Base Model
All view models inherit from a custom `BaseModel` (`src/mavedb/view_models/base/base.py`) that:
- Converts empty strings to `None` via `@model_validator(mode="before")`
- Uses `alias_generator = humps.camelize` for camelCase JSON serialization
- Sets `populate_by_name = True` to accept both Python and camelCase names

### Model Hierarchy
Follow the established inheritance pattern:
```
Base (shared fields)
├── Create (POST request body)
├── Update (PUT/PATCH request body)
└── Saved (GET response)
    ├── Short (list endpoints, minimal fields)
    ├── Full (detail endpoints, all fields)
    ├── Admin (admin-only fields)
    └── PublicDump (data export)
```

### record_type Pattern
Every saved view model must include a `record_type` literal for frontend type discrimination:
```python
class ScoreSetSavedModel(BaseModel):
    record_type: Literal["score_set"] = "score_set"
```

### Validators
- `@field_validator("field_name", mode="before")` — single field transformations
- `@model_validator(mode="before")` — pre-parse transformations (e.g. ORM → dict conversions)
- `@model_validator(mode="after")` — cross-field validation

### Synthetic Fields (ORM → View Model)
When SQLAlchemy model attributes don't map 1:1 to view model fields, use `@model_validator(mode="before")`:
```python
@model_validator(mode="before")
@classmethod
def set_computed_field(cls, data):
    if hasattr(data, "some_relationship"):
        data.__setattr__("computed_count", len(data.some_relationship))
    return data
```

### all_fields_optional_model Decorator
Used to create PATCH-style update models from existing models:
```python
@all_fields_optional_model
class ScoreSetUpdateModel(ScoreSetCreateModel):
    ...
```

## Enum Conventions

- Enum values use `snake_case`: `ProcessingState.success`, `MappingState.incomplete`
- Database enums use `native_enum=False` with `String` column type
- Define in `src/mavedb/lib/` or alongside the model that uses them

## Async Patterns

- Router endpoints: `async def` when they await worker/external calls, regular `def` for synchronous DB operations
- Worker jobs: `async def` (ARQ requirement)
- Database operations are synchronous (SQLAlchemy sync sessions)

## Logging

Use structured logging with starlette-context correlation IDs:
```python
import logging
from mavedb.lib.logging.context import logging_context, save_to_logging_context

logger = logging.getLogger(__name__)
logger.info("Processing score set", extra=logging_context())
```

## Scripts

Operational scripts use Click with a `@with_database_session` decorator:
- Always default to `dry_run=True`
- Located in `src/mavedb/scripts/`
- Run via `poetry run python -m mavedb.scripts.<name>`
