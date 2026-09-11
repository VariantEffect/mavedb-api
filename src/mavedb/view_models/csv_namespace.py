from typing import Optional

from mavedb.lib.csv.namespaces import CsvNamespaceGroup
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.score_set import ShorterScoreSet


class AvailableCsvNamespace(BaseModel):
    """One CSV column namespace a record has data for, ready to be offered as a choice.

    Labels are served rather than derived client-side: only the server knows a calibration's title or a
    ClinVar release date.
    """

    record_type: str = None  # type: ignore

    namespace: str
    """The value to pass back in the ``namespaces`` query parameter."""

    label: str
    """Human-readable name for a picker."""

    group: CsvNamespaceGroup
    """Which section of a picker this belongs in."""

    score_set: Optional[ShorterScoreSet] = None
    """The score set a calibration namespace belongs to; None for namespaces that apply to any.

    A picker should group calibrations by this when a response spans more than one score set.
    """

    selected_by_default: bool = True
    """Whether a picker should open with this group checked.

    False for research-use-only calibrations and for calibrations with no ranges: both are offered, but
    neither should be swept into a download unasked.
    """

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True
