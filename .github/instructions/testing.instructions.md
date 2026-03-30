---
description: 'MaveDB testing conventions — fixtures, mocking, test data patterns'
applyTo: 'tests/**/*.py'
---

# Testing Conventions for MaveDB

## Test Infrastructure

### Database
- **pytest-postgresql** provides ephemeral PostgreSQL instances per test session
- Database schema is created from SQLAlchemy models via `Base.metadata.create_all()`
- Each test gets a clean transaction that rolls back after completion
- Core fixtures live in `tests/conftest.py`

### Network Isolation
- **pytest-socket** blocks real network calls in tests
- External services (HGVS, SeqRepo, DCD Mapping, ClinGen) must be mocked

## Fixtures

### Two-Tier conftest
- `tests/conftest.py` — Core fixtures: database session, auth overrides, user contexts, API client
- `tests/<module>/conftest.py` — Module-specific fixtures for that test directory

### Auth Fixtures
Four pre-configured user contexts:
- **Default user** — standard authenticated user (test ORCID)
- **Anonymous user** — unauthenticated
- **Extra user** — second authenticated user (for permission tests)
- **Admin user** — user with admin role

### DependencyOverrider
Switch auth context mid-test using the `DependencyOverrider` context manager:
```python
with DependencyOverrider(app, {get_current_user: lambda: admin_user}):
    response = client.get("/api/v1/score-sets/private-urn")
    assert response.status_code == 200
```

## Test Data Constants

All test constants live in `tests/helpers/constants.py` with naming conventions:

| Prefix | Purpose | Example |
|--------|---------|---------|
| `VALID_*` | Valid input values | `VALID_ACCESSION`, `VALID_GENE_NAME` |
| `TEST_*` | Complete test objects (dicts) | `TEST_SCORE_SET`, `TEST_EXPERIMENT` |
| `TEST_MINIMAL_*` | Minimal valid objects | `TEST_MINIMAL_SCORE_SET` |
| `SAVED_*` | Expected shapes after save | `SAVED_SCORE_SET` |
| `*_RESPONSE` | Expected API response shapes | `SCORE_SET_RESPONSE` |

## Test Naming

Use descriptive names that reflect the operation and expected outcome:
```python
def test_cannot_publish_score_set_without_variants(): ...
def test_admin_can_view_private_score_set(): ...
def test_create_experiment_with_invalid_urn_returns_422(): ...
```

## Mocking External Services

Always mock external bioinformatics services:
```python
from unittest.mock import patch

@patch("mavedb.data_providers.services.cdot_rest")
@patch("mavedb.worker.jobs.map_variants_for_score_set")
def test_publish_enqueues_mapping(mock_map, mock_cdot, client, db):
    ...
```

Common mock targets:
- `mavedb.data_providers.services.cdot_rest`
- `mavedb.worker.jobs.*` (individual job functions)
- `mavedb.lib.authentication.get_current_user`
- HGVS/SeqRepo data providers

## Helper Factories

Use factory functions in test helpers to create test objects:
```python
from tests.helpers.constants import TEST_SCORE_SET

def create_score_set(client, payload=TEST_SCORE_SET):
    response = client.post("/api/v1/score-sets/", json=payload)
    assert response.status_code == 201
    return response.json()
```

## Testing Patterns

### Permission Testing
Test both allowed and denied access for each role:
```python
def test_owner_can_update_draft(client, db):
    ...

def test_non_owner_cannot_update_draft(client, db):
    with DependencyOverrider(app, {get_current_user: lambda: other_user}):
        response = client.put(f"/api/v1/score-sets/{urn}", json=update_data)
        assert response.status_code == 404  # 404, not 403
```

### Worker Job Testing
Test job logic directly, not through the API:
```python
async def test_create_variants_processes_csv(db, score_set):
    ctx = {"db": db}
    await create_variants_for_score_set(ctx, score_set.id, "test-correlation-id")
    assert score_set.num_variants > 0
```

### Schema Validation
Verify that response shapes match view models:
```python
def test_score_set_response_has_record_type(client):
    response = client.get(f"/api/v1/score-sets/{urn}")
    assert response.json()["recordType"] == "score_set"
```
