---
paths:
  - "src/mavedb/routers/**"
  - "src/mavedb/lib/**"
---

# Permissions

- Import from `mavedb.lib.permissions`. Check the entity a route names with `assert_permission(user, entity, Action.X)`.
- When a read fans out from that entity to related ones (score set → calibrations, allele → score sets), get a `Principal` from the `get_principal` dependency and filter with its `Viewer` for that entity type. Calibrations are stricter than their score set: reading a score set doesn't grant reading its calibrations.
- `Principal` and `Viewer` are request-scoped. Never use one as a default argument.
- Read enforcement is moving into Postgres row-level security (#808). Until it lands, these application checks are the only enforcement.
