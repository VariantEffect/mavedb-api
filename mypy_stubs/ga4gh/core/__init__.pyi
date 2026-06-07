from . import models as models
from .identifiers import PrevVrsVersion as PrevVrsVersion

def ga4gh_identify(
    vro: object,
    in_place: str = ...,
    as_version: PrevVrsVersion | None = ...,
) -> str | None: ...

__all__ = ["PrevVrsVersion", "ga4gh_identify", "models"]
