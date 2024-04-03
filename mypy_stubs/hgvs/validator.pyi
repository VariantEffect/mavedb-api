from typing import Any, Optional
import hgvs


class Validator:
    def __init__(self, hdp: Any, strict: bool = hgvs.global_config.validator.strict): ...
    def validate(self, var, strict: Optional[bool] = None) -> bool: ...
