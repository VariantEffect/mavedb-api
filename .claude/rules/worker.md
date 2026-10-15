---
paths:
  - "src/mavedb/worker/**"
  - "tests/worker/**"
---

# Worker

Read `src/mavedb/worker/README.md` first; `best_practices.md`, `job_decorators.md`, `job_managers.md` and `pipeline_management.md` beside it cover job structure, decorators and pipelines.

## Testing jobs

- Assert on `JobExecutionOutcome.status` and `.data` in every job test, plus database state for the objects the job changes. For external-service jobs, assert the boundary calls (ClinGen CAR/LDH, UniProt, gnomAD/Athena, ClinVar, S3).
- Let `update_progress()` run unpatched. It commits as a checkpoint in production, and tests should exercise that.
- Use `TransactionSpy` (`tests/helpers/transaction_spy.py`) only in manager and decorator tests, where commit and rollback are the contract. In job tests, check database state instead.
- `mock_database_flush_failure` and `mock_database_rollback_failure` are for infrastructure error-recovery paths that are hard to trigger otherwise.
