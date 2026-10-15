from enum import Enum


class Disposition(str, Enum):
    """The stable, consumer-facing status axis of a variant event.

    Defined by *what the consumer may conclude* — not by the domain-specific
    operation that produced it (that lives in ``reason``).

    - ``present`` — we hold the result / the step succeeded
    - ``absent`` — the source or biology has nothing — an informative negative
    - ``not_applicable`` — we could not ask — a pipeline/structural gap, not a statement about the source
    - ``failed`` — errored, might retry; failure_category carries transient vs permanent
    """

    PRESENT = "present"
    ABSENT = "absent"
    NOT_APPLICABLE = "not_applicable"
    FAILED = "failed"
