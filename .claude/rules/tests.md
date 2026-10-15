---
paths:
  - "tests/**"
---

# Tests

- Assert on outcomes: return values, responses, database state, and calls at system boundaries. Don't assert on internal calls, call counts or log and progress message strings; tests should survive a refactor.
- Unit tests cover edge cases and error paths with external services mocked. Integration tests cover happy paths end to end against the real test database.
- `session.refresh()` before asserting on a modified ORM object. Include negative assertions where they matter (the record that must not exist).
- Name tests `test_<function>_<condition>_<expected_outcome>` when you write or modify them; don't rename untouched tests.
- Use `@pytest.mark.parametrize` with `ids` instead of near-identical copies.
- Put a fixture in the most specific `conftest.py` that needs it, and lift it to the nearest common one when it's shared.
