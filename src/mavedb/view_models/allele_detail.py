"""Response view models for the allele-detail view (``GET /alleles/{digest|CAID}``).

Pydantic serialization boundary over the :mod:`lib.allele_detail` transit dataclasses; the
allele-grain counterpart of ``view_models.variant_detail``.
"""

from typing import Any, Optional

from mavedb.view_models.allele_annotation import AlleleAnnotations
from mavedb.view_models.allele_identity import AlleleIdentity
from mavedb.view_models.base.base import BaseModel


class AlleleDetail(BaseModel):
    """The allele-detail envelope (``GET /alleles/{digest|CAID}``).

    ``alleles`` is the full cross-layer equivalence class, keyed by VRS digest; ``isFocus`` marks
    the queried allele. ``annotations`` shares those same keys. Measurement-agnostic: no score,
    classification, or re-anchored Cat-VRS (those belong to ``GET /variants/{urn}``).
    """

    digest: str
    level: Optional[str] = None
    hgvs: Optional[str] = None
    clingen_allele_id: Optional[str] = None
    vrs: Optional[dict[str, Any]] = None

    alleles: dict[str, AlleleIdentity] = {}
    annotations: dict[str, AlleleAnnotations] = {}

    class Config:
        from_attributes = True
