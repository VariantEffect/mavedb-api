---
paths:
  - "src/mavedb/db/rls.py"
  - "src/mavedb/db/bootstrap.py"
  - "src/mavedb/models/**"
  - "alembic/versions/**"
  - "src/mavedb/lib/permissions/**"
---

# Row-level security and grants

Database permissions fail closed (#808). Any database object that nobody made an explicit access decision on should end up with no access, not silent access. A design that can fail open is wrong and undesirable for this project, in all cases.

- Every table, view, materialized view and function in the schema has an entry in `src/mavedb/db/rls.py`. A new one gets no grants until it has one, and `tests/db/test_bootstrap.py` fails CI while any entry is missing.
- Picking a table's entry is the decision about its row-level security. `GUARDED` means a policy (#809) decides which rows the api sees. `CARVE_OUT` means every caller sees every row, and needs a comment beside it saying why that's safe. `NONE` means the api never reads it. Ask the user if you're unsure which to choose, as this decision has important security implications.
- A view reads its tables as its owner, who skips their policies. A `GUARDED` view must be `WITH (security_invoker = true)`, or the api isn't granted it. A materialized view can never be `GUARDED`: its rows were read as the owner at refresh, and Postgres has no RLS on it. So a materialized view is `CARVE_OUT`, with a reason why every row is safe for everyone, or `NONE`.
- Grants come only from `apply_grants` reading `rls.py`. Never `GRANT` or `REVOKE` in a migration or by hand. The next upgrade revokes it anyway, and a squashed baseline is dumped without privileges, so it would be lost at the next squash too.
- Prefer allow-lists to deny-lists. Prefer a check that raises to code that trusts existing state or quietly repairs it. The bootstrap refuses roles with privileges it didn't give them.
- Name database functions `mavedb_*`, so it's clear in the database that a function comes from MaveDB's own code and not from Postgres or an extension.
- A `SECURITY DEFINER` function runs with the owner's privileges. List it with one executor and a reason, and pin its `search_path`.
