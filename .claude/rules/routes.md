---
paths:
  - "src/mavedb/routers/**"
  - "src/mavedb/lib/authentication.py"
  - "src/mavedb/lib/authorization.py"
---

# Routes

FastAPI runs `def` handlers and dependencies in a thread pool and `async def` ones on the event loop, so a synchronous database call inside `async def` stalls every request on that process (#881).

- Routes, dependencies and helpers are `def`. A sync HTTP call in a `def` route blocks only its own thread.
- Use `async def` only where the code can't work without an await: reading a multipart form, or a route that waits only on an HTTP call and never touches the database. Keep database work out of it by running it in the thread pool, and say in a comment why the route is `async`.
- `tests/routers/test_async_routes.py` fails when an `async` endpoint or dependency takes a session from `get_db`. Allowlist a route there only if it reads the form and runs the rest in the thread pool.
- Start worker pipelines only through `lib/workflow/kickoff.py`: `enqueue_pipeline_for_score_set` from async code, or `enqueue_pipeline_for_score_set_from_thread` from code running in the thread pool.
- Route tests go in `tests/routers/`.
