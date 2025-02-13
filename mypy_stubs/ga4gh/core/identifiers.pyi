from enum import Enum

class PrevVrsVersion(str, Enum):
    V1_3: str
    @classmethod
    def validate(cls, version) -> None: ...
