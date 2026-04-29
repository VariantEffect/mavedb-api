"""Wire-format payload schemas for the dcd-mapping ``ScoresetMapping`` API.

These :class:`TypedDict` definitions document the JSON shape returned by the
external service. They are intentionally ``total=False`` -- the upstream service
omits absent fields rather than emitting nulls for every key, so callers must
treat membership as optional.
"""

from datetime import datetime
from typing import Any, Optional, TypedDict


class GeneInfo(TypedDict, total=False):
    hgnc_symbol: Optional[str]
    selection_method: Optional[str]


class TargetAnnotation(TypedDict, total=False):
    """Per-target reference metadata returned alongside ``mapped_scores``.

    ``layers`` is keyed by the dcd-mapping single-character annotation layer
    code (``p`` / ``c`` / ``g``); each layer is an open dict containing keys
    such as ``computed_reference_sequence`` and ``mapped_reference_sequence``.
    """

    gene_info: Optional[GeneInfo]
    layers: dict[str, dict[str, Any]]


class TargetMapping(TypedDict, total=False):
    """Per-(target, alignment_level) QC record from the dcd-mapping API."""

    target_gene_identifier: str
    alignment_level: str
    preferred: bool
    reference_assembly: Optional[str]
    reference_accession: Optional[str]
    reference_sequence_id: Optional[str]
    alignment_score: Optional[float]
    next_best_alignment_score: Optional[float]
    alignment_length: Optional[int]
    alignment_string: Optional[str]
    mismatch_count: Optional[int]
    gap_count: Optional[int]
    percent_identity: Optional[float]
    total_variants: Optional[int]
    variants_failed: Optional[int]
    variants_failed_pre_layer_selection: Optional[int]
    variants_with_alignment_warnings: Optional[int]
    variants_with_mapping_warnings: Optional[int]
    variants_mapped_cleanly: Optional[int]
    tool_name: str
    tool_version: str
    tool_parameters: Optional[dict[str, Any]]
    alignment_metadata: Optional[dict[str, Any]]
    vrs_version: Optional[str]


class ScoreSetMappingResults(TypedDict, total=False):
    """Top-level response payload from ``POST /api/v1/map/{score_set_urn}``."""

    metadata: Any
    mapped_date: Optional[datetime]
    reference_sequences: Optional[dict[str, TargetAnnotation]]
    mapped_scores: Optional[list[dict[str, Any]]]
    target_mappings: Optional[list[TargetMapping]]
    error_message: Optional[str]
