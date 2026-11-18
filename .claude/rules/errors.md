---
paths:
  - "src/mavedb/routers/**"
  - "src/mavedb/lib/**"
  - "src/mavedb/db/**"
---

# Errors

- An exception the api answers with anything but a plain 500 is mapped in its domain's table in `lib/exception_handlers/`: its status, its body and whether it alerts Slack. One handler serves every table. A new domain gets its own module, merged into `ERROR_RESPONSES` in `handlers.py`.
- Raise a domain exception and map it, rather than catching it in a route to re-raise an `HTTPException` with a literal message. Many routes still do the latter; don't copy them.
- Define exceptions in the domain that raises them (`lib/exceptions.py`, `lib/permissions/exceptions.py`, `db/errors.py`), with no HTTP status or response text. The worker raises the same exceptions.
- `tests/lib/test_exception_handlers.py` fails when an exception in those modules is neither mapped nor listed in `UNMAPPED`, or when a handler is registered outside the tables.
- Slack alerts carry the exception type, the code location and the correlation id, never the exception's message. The logs keep the message.
