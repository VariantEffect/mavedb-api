---
paths:
  - "src/mavedb/routers/**"
  - "src/mavedb/lib/authentication.py"
  - "src/mavedb/lib/authorization.py"
---

# Routes

FastAPI runs `def` handlers and dependencies in a thread pool and `async def` ones on the event loop, so a synchronous database call inside `async def` stalls every request on that process (#881).

- New routes, dependencies and helpers are `def`.
- Use `async def` only for real awaits (ARQ enqueue, async HTTP), and run the synchronous database work in the thread pool.
- Start worker pipelines only through `enqueue_pipeline_for_score_set` (`lib/workflow/kickoff.py`).
- Route tests go in `tests/routers/`.
