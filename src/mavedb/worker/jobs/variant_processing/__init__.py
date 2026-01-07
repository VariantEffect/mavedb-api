"""Variant processing job functions.

This module exports jobs responsible for variant creation and mapping:
- Variant creation from uploaded score/count data
- VRS mapping to standardized genomic coordinates
- Queue management for mapping workflows
"""

from .creation import create_variants_for_score_set
from .mapping import (
    map_variants_for_score_set,
    variant_mapper_manager,
)

__all__ = [
    "create_variants_for_score_set",
    "map_variants_for_score_set",
    "variant_mapper_manager",
]
