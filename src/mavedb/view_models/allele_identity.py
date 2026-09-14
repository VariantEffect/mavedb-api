"""Response view model for the shared allele molecular-identity shape.

Pydantic boundary over :class:`lib.allele_identity.AlleleIdentity`. Shared by the ``alleles`` map
on both ``GET /variants/{urn}`` and ``GET /alleles/{digest}``. See the lib docstring for the
focus-relative semantics of each axis.
"""

from typing import Optional

from mavedb.lib.allele_identity import AlleleDerivation
from mavedb.view_models.base.base import BaseModel


class AlleleIdentity(BaseModel):
    """One allele in a view's ``alleles`` map, keyed by VRS digest and labelled relative to the
    view's focus allele. ``isFocus`` marks the anchor; ``relation`` and ``derivation`` describe
    every other member's relationship to it and are absent on the focus itself.
    """

    level: Optional[str] = None
    hgvs: Optional[str] = None
    clingen_allele_id: Optional[str] = None
    is_focus: bool
    relation: Optional[str] = None
    derivation: Optional[AlleleDerivation] = None
    # VRS digest of this allele's c<->g projection counterpart, when one exists.
    projection_of: Optional[str] = None

    class Config:
        from_attributes = True
