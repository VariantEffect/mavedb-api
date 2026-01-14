from typing import TypedDict


class ClassificationDict(TypedDict):
    indexed_by: str
    classifications: dict[str, set[str]]
