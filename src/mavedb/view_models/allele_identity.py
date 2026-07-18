"""Response view model for the shared allele molecular-identity shape.

The pydantic boundary over :class:`lib.allele_identity.AlleleIdentity` (``from_attributes`` coerces it;
aliases camelize per the shared base config). Shared by both serving views — the ``alleles`` map on
``GET /variants/{urn}`` and on ``GET /alleles/{digest}``. See the lib docstring for the focus-relative
semantics of each axis.
"""

from typing import Optional

from mavedb.view_models.base.base import BaseModel


class AlleleIdentity(BaseModel):
    """One allele in a view's ``alleles`` map, keyed by VRS digest and labelled relative to the view's
    focus allele. ``isFocus`` marks the anchor (measured allele / queried allele); ``relation`` and
    ``derivation`` describe every other member's structural + provenance relationship to it, and are
    absent on the focus itself."""

    level: Optional[str] = None
    hgvs: Optional[str] = None
    clingen_allele_id: Optional[str] = None
    is_focus: bool
    relation: Optional[str] = None
    derivation: Optional[str] = None
    projection_of: Optional[str] = None

    class Config:
        from_attributes = True
