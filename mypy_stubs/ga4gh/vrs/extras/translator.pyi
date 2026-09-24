from typing import Any

from ga4gh.vrs.dataproxy import _DataProxy

class _Translator:
    default_assembly_name: str
    data_proxy: _DataProxy
    identify: bool
    def __init__(
        self,
        data_proxy: _DataProxy,
        default_assembly_name: str = ...,
        identify: bool = ...,
    ) -> None: ...
    # Returns a VRS variation (Allele/CisPhasedBlock/...); typed loosely so callers
    # can annotate the concrete subtype they expect without a redundant cast.
    def translate_from(self, var: str, fmt: str | None = ..., **kwargs: Any) -> Any: ...

class AlleleTranslator(_Translator): ...
