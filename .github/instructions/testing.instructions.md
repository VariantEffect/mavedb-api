---
description: 'Testing philosophy and conventions for the MaveDB API'
applyTo: 'tests/**/*.py'
---

# Testing Conventions

## Outcome-Based Testing

Test what code does (return values, DB state, external boundary calls), not how it does it (internal method calls, message strings, call sequences). Tests should survive internal refactoring without changes.

**Assert on:**
- Return values and response objects
- DB state changes (query for created/updated/deleted records)
- External boundary calls (see Mocking Boundaries below)

**Do not assert on:**
- Internal function invocations (e.g., that a helper was called with specific args)
- Call counts or call sequences on internal methods
- Log or progress message strings

## Mocking Boundaries

Only mock at system boundaries — the edges where your code talks to something external:
- External services (APIs, third-party clients)
- Infrastructure (Redis/ARQ, Slack, email)
- Network I/O (`run_in_executor`, HTTP clients)
- File I/O (S3, local filesystem in tests)

Do NOT mock internal helpers, validators, or data transforms. Test through them.

## Unit vs Integration Test Responsibilities

**Unit tests:** Edge cases, error paths, invalid inputs, boundary conditions. Use mocked external services.

**Integration tests:** Happy paths, end-to-end workflows, DB state verification. Use real DB with test fixtures.

## Assertion Best Practices

- Use `session.refresh()` before asserting on modified ORM objects
- Add custom assertion messages to complex assertions where the failure message wouldn't immediately clarify what went wrong
- Include negative assertions where appropriate (verify unwanted records don't exist)
- Don't add messages to trivially clear assertions like `assert len(variants) == 0`

## Test Naming

Use the pattern: `test_<function_name>_<condition>_<expected_outcome>`

Examples:
- `test_submit_to_car_when_disabled_skips_submission`
- `test_create_score_set_returns_422_when_missing_target`

Apply to tests being modified; don't rename all tests at once.

## Parametrization

Use `@pytest.mark.parametrize` with descriptive `ids` when the same logic is tested across multiple states. Prefer parametrization over copy-pasting near-identical tests.

## Fixtures

- Keep fixtures minimal and composable
- Define fixtures in the most specific `conftest.py` where they're needed
- Don't duplicate fixtures across test classes — lift shared ones to the nearest common conftest
- Use factory fixtures when tests need variants of the same object

---

# Worker-Specific Conventions

The following conventions apply specifically to `tests/worker/`.

## Job Test Assertions

- Assert on `JobExecutionOutcome.status` and `.data` for every job test
- Assert on DB state changes for the domain objects the job modifies
- For external service jobs: assert boundary calls (ClinGen CAR/LDH, UniProt, gnomAD/Athena, S3, ClinVar)

## Let `update_progress` Run Unpatched

`update_progress()` calls `session.commit()` as a checkpoint. This is production behavior and should execute in tests. Letting it run means tests verify that checkpoint commits don't break state or interfere with final outcomes. Don't patch it, don't mock it, don't assert on its messages.

## TransactionSpy Usage

**USE in manager/decorator tests** (e.g., `test_job_manager.py`, `test_pipeline_manager.py`): The commit/rollback boundary IS the contract here. If someone removes a commit, data silently won't persist in production. DB state checks alone can't catch this because the test session may auto-commit on teardown.

**USE `mock_database_flush_failure` / `mock_database_rollback_failure`**: These simulate DB errors that are genuinely hard to reproduce otherwise. Valuable for testing error recovery paths in infrastructure code.

**DO NOT USE in job-level tests** (e.g., `test_clingen.py`, `test_cleanup.py`, `test_creation.py`): The job's contract is "variants were created" or "stalled jobs were retried," not "session.commit() was called." Use DB state queries instead.
