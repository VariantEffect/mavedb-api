from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional, TypedDict

from mavedb.models.enums.calibration_control_status import CalibrationControlStatus


class ClassificationDict(TypedDict):
    indexed_by: str
    classifications: dict[str, set[str]]


VariantIdentity = tuple[Optional[str], Optional[str], Optional[str]]
"""A variant's ``(hgvs_nt, hgvs_pro, hgvs_splice)`` tuple.

Variant URNs are positional within a score set (``{score_set.urn}#{n}``, renumbered from one on every
upload), so ``#5`` can name a different biological variant after a re-upload. The HGVS tuple is the
identity that survives one, and so is what calibration variant references are re-resolved against.
"""


@dataclass(frozen=True)
class CalibrationControlSnapshot:
    """One calibration control, recorded by variant identity instead of variant id.

    Audit provenance is carried alongside the identity so a relinked control keeps the authorship and
    date of the original submission rather than appearing to have been entered by whoever re-uploaded
    the scores.
    """

    identity: VariantIdentity
    clinical_status: CalibrationControlStatus
    created_by_id: int
    creation_date: date


@dataclass
class CalibrationVariantLinkSnapshot:
    """The variant references held by one calibration that a re-upload cannot reconstruct.

    Only irreproducible references are captured. Range-based bin membership is omitted on purpose: it
    is a function of the variants' scores, so it is recomputed from the new upload rather than
    remembered (see :func:`~mavedb.lib.score_calibrations.restore_calibration_variant_links`).
    """

    calibration_id: int
    controls: list[CalibrationControlSnapshot] = field(default_factory=list)

    # Class-based bin membership keyed by functional classification id. Classification rows survive a
    # re-upload, so their ids remain valid addresses when the membership is restored.
    classification_members: dict[int, list[VariantIdentity]] = field(default_factory=dict)


@dataclass
class CalibrationVariantRelinkReport:
    """Outcome of re-establishing a score set's calibration variant references after a re-upload.

    Relinked and dropped counts describe references carried across by identity (controls and
    class-based bin membership). Re-binned counts describe range-based membership recomputed from the
    new scores, where "dropped" has no meaning — the bin simply holds whatever now falls in it.
    """

    controls_relinked: int = 0
    controls_dropped: int = 0
    classification_members_relinked: int = 0
    classification_members_dropped: int = 0
    classifications_rebinned: int = 0
    classification_members_rebinned: int = 0

    # Calibrations whose ``controls_not_phi`` affirmation was cleared because a control was dropped.
    calibrations_pending_phi_reaffirmation: list[int] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable summary suitable for a job's logging context."""
        return {
            "calibration_controls_relinked": self.controls_relinked,
            "calibration_controls_dropped": self.controls_dropped,
            "calibration_classification_members_relinked": self.classification_members_relinked,
            "calibration_classification_members_dropped": self.classification_members_dropped,
            "calibration_classifications_rebinned": self.classifications_rebinned,
            "calibration_classification_members_rebinned": self.classification_members_rebinned,
            "calibrations_pending_phi_reaffirmation": list(self.calibrations_pending_phi_reaffirmation),
        }
