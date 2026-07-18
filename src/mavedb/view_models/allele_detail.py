"""Response view models for the allele-detail view (``GET /alleles/{digest|CAID}``).

The pydantic serialization boundary over the :mod:`lib.allele_detail` transit dataclasses. The
allele-grain sibling of ``view_models.variant_detail``. The ``alleles`` map reuses the shared
:class:`AlleleIdentity` (``view_models.allele_identity``) and the digest-keyed
:class:`AlleleAnnotations` block (``view_models.allele_annotation``) — both shared with the
variant view.
"""

from typing import Any, Optional

from mavedb.view_models.allele_annotation import AlleleAnnotations
from mavedb.view_models.allele_identity import AlleleIdentity
from mavedb.view_models.base.base import BaseModel


class AlleleDetail(BaseModel):
    """The allele-detail envelope (``GET /alleles/{digest|CAID}``).

    Flat anchor-identity fields (``digest`` / ``level`` / ``hgvs`` / ``clingenAlleleId`` and the spec-pure
    GA4GH ``vrs`` variation) plus the MaveDB layer riding alongside. This layer, keyed by VRS digest,
    contains the ``alleles`` map: the full cross-layer equivalence class. Each entry is an ``AlleleIdentity``,
    labelled relative to the focus (``isFocus`` marks the queried allele / the CAID's representations) and
    the digest-keyed ``annotations`` map. The two maps share keys. Measurement-agnostic: no score,
    classification, or version standing, and no re-anchored Cat-VRS (those belong to ``GET /variants/{urn}``).
    Absent fields are omitted.
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
