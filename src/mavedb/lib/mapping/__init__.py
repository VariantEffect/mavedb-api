"""Public API for the mapping library.

Re-exports the symbols that callers historically imported from the flat
``mavedb.lib.mapping`` module so existing import sites keep working.
"""

from mavedb.lib.mapping.client import VRSMap
from mavedb.lib.mapping.constants import EXCLUDED_PREMAPPED_ANNOTATION_KEYS
from mavedb.lib.mapping.metadata import extract_ids_from_post_mapped_metadata
from mavedb.lib.mapping.schema import (
    GeneInfo,
    ScoreSetMappingResults,
    TargetAnnotation,
    TargetMapping,
)

__all__ = [
    "EXCLUDED_PREMAPPED_ANNOTATION_KEYS",
    "GeneInfo",
    "ScoreSetMappingResults",
    "TargetAnnotation",
    "TargetMapping",
    "VRSMap",
    "extract_ids_from_post_mapped_metadata",
]
