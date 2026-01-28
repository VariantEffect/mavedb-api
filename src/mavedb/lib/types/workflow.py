from typing import Any, TypedDict

from mavedb.models.enums.job_pipeline import DependencyType


class JobDefinition(TypedDict):
    key: str
    type: str
    function: str
    params: dict[str, Any]
    dependencies: list[tuple[str, DependencyType]]


class PipelineDefinition(TypedDict):
    description: str
    job_definitions: list[JobDefinition]
