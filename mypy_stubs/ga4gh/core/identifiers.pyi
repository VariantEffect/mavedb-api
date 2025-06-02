from enum import Enum
from re import Pattern

GA4GH_IR_REGEXP: Pattern[str]

class PrevVrsVersion(str, Enum):
    V1_3 = "1.3"

    @classmethod
    def validate(cls, version) -> None: ...
