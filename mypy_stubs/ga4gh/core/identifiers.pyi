from enum import Enum

class PrevVrsVersion(str, Enum):
    V1_3 = "1.3"

    @classmethod
    def validate(cls, version) -> None: ...
