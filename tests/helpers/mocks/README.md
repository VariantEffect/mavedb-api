# Test Helpers

This directory contains shared utilities for creating mock objects and test fixtures across the MaveDB test suite.

## Structure

### Core Mock Infrastructure

- **`mock_utilities.py`** - Base utilities for creating mock objects that work with both direct attribute access and Pydantic validation
  - `MockVariantCollection` - Mock collection class for controlling `variant in collection` behavior
  - `MockObjectWithPydanticFunctionality` - Dict subclass with snake_case/camelCase attribute access
  - `create_sealed_mock()` - Helper for creating basic MagicMock objects

### General Factories

- **`factories.py`** - All mock factories for MaveDB objects - provides reusable "lego brick" mocks that can be composed for various testing scenarios
  - **User and identity helpers:** `create_mock_user()`, `create_mock_license()`
  - **Publication helpers:** `create_mock_publication()`
  - **Core MaveDB objects:** `create_mock_score_set()`, `create_mock_mapped_variant()`, `create_mock_score_set_variant()`
  - **Classification helpers:** `create_mock_functional_classification()`, `create_mock_acmg_classification()`
  - **Calibration helpers:** `create_mock_score_calibration()`, `create_mock_pathogenicity_range()`, `create_mock_score_calibration_with_ranges()`
  - **Composite score set helpers:** `create_mock_functional_calibration_score_set()`, `create_mock_pathogenicity_calibration_score_set()`
  - **Mapped variant helpers:** `create_mock_mapped_variant_with_functional_calibration_score_set()`, `create_mock_mapped_variant_with_pathogenicity_calibration_score_set()`
  - **Resource helpers:** `create_mock_resource_with_dates()`

## Usage Guidelines

### For General MaveDB Objects
```python
from tests.helpers.mocks.factories import create_mock_user, create_mock_score_set

user = create_mock_user()
score_set = create_mock_score_set(created_by=user)
```

### For Annotation-Specific Scenarios
```python
from tests.helpers.mocks.factories import (
    create_mock_functional_classification,
    create_mock_score_calibration,
    create_mock_mapped_variant_with_functional_calibration_score_set
)

classification = create_mock_functional_classification()
calibration = create_mock_score_calibration(functional_classifications=[classification])
mapped_variant = create_mock_mapped_variant_with_functional_calibration_score_set()
```

### For Custom Mock Behavior
```python
from tests.helpers.mocks.mock_utilities import (
    MockObjectWithPydanticFunctionality,
    MockVariantCollection
)

# Mock that works with both Pydantic validation and attribute access
mock_data = MockObjectWithPydanticFunctionality({
    "camelCaseKey": "value"
})
assert mock_data["camelCaseKey"] == "value"  # Dict access
assert mock_data.camel_case_key == "value"   # Attribute access (snake_case)

# Mock collection for controlling variant membership tests
variants = MockVariantCollection(contains_result=True)
assert any_variant in variants  # Always returns True
```

## Design Principles

1. **Shared Infrastructure** - Common utilities in `mock_utilities.py` for all domains
2. **Maximum Reusability** - All domain objects in `factories.py` as composable building blocks
3. **Pydantic Compatibility** - All mocks work with Pydantic validation via `MockObjectWithPydanticFunctionality`
4. **Attribute Access** - Support both dictionary access and snake_case attribute access
5. **Modular Composition** - Build complex objects from simpler components
6. **Flexibility** - Factory functions accept parameters to customize mock behavior

## Migration from Old Structure

The previous monolithic `annotation_helpers.py` file has been refactored into this simplified structure:
- Base infrastructure → `mock_utilities.py` 
- All mock factories → `factories.py`

All existing imports have been updated to use the new structure while maintaining the same API.