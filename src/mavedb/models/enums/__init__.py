"""
Enums used by MaveDB models.
"""

from .contribution_role import ContributionRole
from .job_pipeline import AnnotationStatus, DependencyType, FailureCategory, JobStatus, PipelineStatus
from .mapping_state import MappingState
from .processing_state import ProcessingState
from .score_calibration_relation import ScoreCalibrationRelation
from .target_category import TargetCategory
from .user_role import UserRole

__all__ = [
    "ContributionRole",
    "JobStatus",
    "PipelineStatus",
    "DependencyType",
    "FailureCategory",
    "AnnotationStatus",
    "MappingState",
    "ProcessingState",
    "ScoreCalibrationRelation",
    "TargetCategory",
    "UserRole",
]
