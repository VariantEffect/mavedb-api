---
paths:
  - "src/mavedb/models/**"
  - "src/mavedb/lib/**"
  - "src/mavedb/routers/**"
  - "src/mavedb/worker/**"
  - "src/mavedb/scripts/**"
---

# Database

- Write new queries in SQLAlchemy 2.0 `select()` style. Older code still uses `db.query()`; don't copy it.
- ValidTime tables never return only live rows by default. Filter with `Model.current` or `Model.live_at(...)`.
- Replace ValidTime rows with `supersede_with` or `supersede_live_where`, never a separate retire-then-insert, so the handoff has no gap.
