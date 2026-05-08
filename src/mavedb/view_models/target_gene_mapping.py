"""View models for ``TargetGeneMapping`` -- per-(target gene, alignment level)
mapping QC and provenance produced by the dcd-mapping QC API.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

from mavedb.models.enums.annotation_layer import AnnotationLayer
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class TargetGeneMappingBase(BaseModel):
    alignment_level: AnnotationLayer
    preferred: bool = False

    reference_assembly: Optional[str] = None
    reference_accession: Optional[str] = None
    reference_sequence_id: Optional[str] = None

    alignment_score: Optional[float] = None
    next_best_alignment_score: Optional[float] = None
    alignment_length: Optional[int] = None
    alignment_string: Optional[str] = None
    mismatch_count: Optional[int] = None
    gap_count: Optional[int] = None
    percent_identity: Optional[float] = None

    total_variants: Optional[int] = None
    variants_failed: Optional[int] = None
    variants_with_alignment_warnings: Optional[int] = None
    variants_mapped_cleanly: Optional[int] = None

    tool_name: str
    tool_version: str
    tool_parameters: Optional[dict[str, Any]] = None
    alignment_metadata: Optional[dict[str, Any]] = None

    vrs_version: Optional[str] = None
    mapped_date: Optional[date] = None


class SavedTargetGeneMapping(TargetGeneMappingBase):
    id: int
    creation_date: date
    modification_date: date

    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


class TargetGeneMapping(SavedTargetGeneMapping):
    pass
