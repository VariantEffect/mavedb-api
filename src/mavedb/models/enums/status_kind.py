from enum import Enum

from mavedb.models.enums.disposition import Disposition


class StatusKind(str, Enum):
    """Which of a key's two live slots an ``annotation_status`` row fills.

    - ``outcome`` — what the latest successful check found (present, absent or not applicable)
    - ``failure`` — the latest check failed; live from when the failing started until a check succeeds

    A failure never replaces an outcome, so a key can have both live at once: a known result whose
    re-check is failing.
    """

    OUTCOME = "outcome"
    FAILURE = "failure"

    @classmethod
    def for_disposition(cls, disposition: Disposition) -> "StatusKind":
        """The slot a row with ``disposition`` fills."""
        return cls.FAILURE if disposition is Disposition.FAILED else cls.OUTCOME
