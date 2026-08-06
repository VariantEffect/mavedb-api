"""The vocabulary of CSV column namespaces: names, labels, validation.

Most namespaces are fixed names. Two families are parameterized, since their columns depend on which
record the caller wants: ``clinvar.YYYY_MM`` and ``calibration.<urn>``. A parameterized namespace carries
its parameter into the column header, keeping values traceable without a separate provenance column.
"""

import re
from enum import StrEnum
from typing import Annotated, Optional

from pydantic import AfterValidator, WithJsonSchema

from mavedb.lib.validation.urn_re import MAVEDB_CALIBRATION_URN_PATTERN


class CsvNamespaceGroup(StrEnum):
    """Presentational grouping, so a client can section a namespace picker."""

    DATA = "data"
    ANNOTATION = "annotation"
    CALIBRATION = "calibration"
    PROVENANCE = "provenance"


class CsvNamespace(StrEnum):
    """Namespaces whose column sets are fixed and take no parameter.

    The parameterized families cannot be members, so requests are validated against this enum *and* those
    patterns. See ``is_valid_csv_namespace``.
    """

    SCORES = "scores"
    """The one score column every score set is required to define."""

    SCORES_CUSTOM = "scores_custom"
    """The remaining score columns the investigator uploaded.

    A request token only: its columns are emitted under the ``scores`` prefix, since they are score
    columns. Splitting selection from emission is what let this replace the ``include_custom_columns``
    flag without changing a published header.
    """

    COUNTS = "counts"

    # Value frozen as "mavedb", and its columns keep their post_mapped_* names: both are published, and
    # the score-set histogram parses mavedb.post_mapped_hgvs_c by name.
    REFERENCE_HGVS = "mavedb"

    VEP = "vep"
    GNOMAD = "gnomad"
    CLINGEN = "clingen"
    SCORE_SET = "score_set"
    RELATIONSHIP = "relationship"


STATIC_CSV_NAMESPACES: tuple[str, ...] = tuple(ns.value for ns in CsvNamespace)
"""The static namespace values, in declaration order, for iteration and documentation."""

STATIC_CSV_NAMESPACE_LABELS: dict[str, tuple[str, CsvNamespaceGroup]] = {
    CsvNamespace.SCORES: ("Score", CsvNamespaceGroup.DATA),
    CsvNamespace.SCORES_CUSTOM: ("Investigator-provided score columns", CsvNamespaceGroup.DATA),
    CsvNamespace.COUNTS: ("Counts", CsvNamespaceGroup.DATA),
    CsvNamespace.CLINGEN: ("ClinGen allele ID", CsvNamespaceGroup.ANNOTATION),
    CsvNamespace.REFERENCE_HGVS: ("Reference-frame HGVS", CsvNamespaceGroup.ANNOTATION),
    CsvNamespace.VEP: ("VEP consequence", CsvNamespaceGroup.ANNOTATION),
    CsvNamespace.GNOMAD: ("gnomAD allele frequency", CsvNamespaceGroup.ANNOTATION),
    CsvNamespace.SCORE_SET: ("Score set and target gene", CsvNamespaceGroup.PROVENANCE),
    CsvNamespace.RELATIONSHIP: ("Relationship to the requested variant", CsvNamespaceGroup.PROVENANCE),
}
"""Label and group for each static namespace, so a client need not maintain its own mapping.

The parameterized families are labeled from their parameter instead; see ``clinvar_namespace_label``.
"""


_CLINVAR_MONTH_NAMES = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


CLINVAR_NS_PATTERN = re.compile(r"^clinvar\.(\d+)_(0[1-9]|1[0-2])$")
"""Pattern for ClinVar namespaces of the form ``"clinvar.YEAR_MONTH"``, e.g. ``clinvar.2024_01``."""


CLINVAR_DB_NAME = "ClinVar"
"""The ``clinical_controls.db_name`` a ``clinvar.*`` namespace selects on."""


def parse_clinvar_namespace(ns: str) -> Optional[str]:
    """Parse a ClinVar namespace into the ``db_version`` stored in ``clinical_controls``.

    Namespaces are of the form ``"clinvar.YEAR_MONTH"`` (e.g. ``"clinvar.2024_01"`` for January 2024).
    The corresponding ``db_version`` is ``"MONTH_YEAR"`` (e.g. ``"01_2024"``).

    Returns ``None`` if *ns* does not match the expected pattern.
    """
    m = CLINVAR_NS_PATTERN.match(ns)
    if not m:
        return None
    year, month = m.group(1), m.group(2)
    return f"{month}_{year}"


def parse_clinvar_db_version(db_version: str) -> Optional[tuple[int, int]]:
    """Parse a ClinVar ``"MM_YYYY"`` db_version into ``(year, month)``.

    Returns ``None`` when *db_version* is not in the expected form. The tuple orders chronologically, so
    it doubles as a sort key for picking the most recent release.
    """
    try:
        month, year = db_version.split("_")
        return (int(year), int(month))
    except (ValueError, AttributeError):
        return None


_UNDATED_CLINVAR_SORT_KEY = (-1, -1)
"""Sorts before every real release, so a namespace we cannot date never wins a "newest" comparison."""


def clinvar_namespace_sort_key(ns: str) -> tuple[int, int]:
    """Chronological sort key for a ClinVar release namespace.

    Use this for every "which release is newest" decision rather than comparing namespace strings: the
    year group is unpadded, so ``"clinvar.999_12" > "clinvar.2025_01"`` lexically. Non-release namespaces
    sort before every real one.
    """
    match = CLINVAR_NS_PATTERN.match(ns)
    if not match:
        return _UNDATED_CLINVAR_SORT_KEY
    return (int(match.group(1)), int(match.group(2)))


CALIBRATION_NS_PATTERN = re.compile(rf"^calibration\.({MAVEDB_CALIBRATION_URN_PATTERN})$")
"""Pattern for calibration namespaces of the form ``"calibration.<calibration urn>"``."""


def parse_calibration_namespace(ns: str) -> Optional[str]:
    """Parse a calibration namespace into the calibration URN it names.

    Returns ``None`` if *ns* does not match the expected pattern.
    """
    match = CALIBRATION_NS_PATTERN.match(ns)
    if not match:
        return None
    return match.group(1)


def calibration_namespace_for_urn(urn: str) -> str:
    """Build the namespace naming a calibration URN."""
    return f"calibration.{urn}"


def clinvar_namespace_for_db_version(db_version: str) -> Optional[str]:
    """Build the namespace naming a ClinVar release from its ``"MM_YYYY"`` db_version.

    Returns ``None`` when *db_version* is not in the expected form.
    """
    parsed = parse_clinvar_db_version(db_version)
    if parsed is None:
        return None
    year, month = parsed
    return f"clinvar.{year}_{month:02d}"


def clinvar_namespace_label(ns: str) -> Optional[str]:
    """Human-readable label for a ClinVar release namespace, e.g. ``"ClinVar significance (November 2024)"``.

    Returns ``None`` when *ns* is not a ClinVar namespace.
    """
    match = CLINVAR_NS_PATTERN.match(ns)
    if not match:
        return None
    year, month = int(match.group(1)), int(match.group(2))
    return f"ClinVar significance ({_CLINVAR_MONTH_NAMES[month - 1]} {year})"


_STATIC_CSV_NAMESPACE_VALUES = frozenset(STATIC_CSV_NAMESPACES)
"""Membership set. ``"scores" in CsvNamespace`` raises TypeError on Python 3.11, so test against this."""


def is_valid_csv_namespace(ns: str) -> bool:
    """Whether *ns* is a namespace any CSV endpoint will accept."""
    return (
        ns in _STATIC_CSV_NAMESPACE_VALUES
        or CLINVAR_NS_PATTERN.match(ns) is not None
        or CALIBRATION_NS_PATTERN.match(ns) is not None
    )


CSV_NAMESPACE_ERROR_MESSAGE = (
    "must be one of "
    + ", ".join(f'"{ns}"' for ns in STATIC_CSV_NAMESPACES)
    + ', a ClinVar release namespace of the form "clinvar.YEAR_MONTH" (e.g. "clinvar.2024_01"),'
    ' or a calibration namespace of the form "calibration.<calibration urn>"'
)


def _validated_csv_namespace(ns: str) -> str:
    """Pydantic validator backing ``CsvNamespaceStr``."""
    if not is_valid_csv_namespace(ns):
        raise ValueError(CSV_NAMESPACE_ERROR_MESSAGE)
    return ns


CsvNamespaceStr = Annotated[
    str,
    AfterValidator(_validated_csv_namespace),
    # Hand-declared because no Python type expresses "closed enum OR two open patterns". FastAPI then
    # rejects bad values itself, so endpoints need no vocabulary check.
    #
    # Caveat: this does not reach clients as a *type*. openapi-typescript narrows an enum mixed with
    # patterns to plain `string`, so generated clients see `string[]` and cannot check a namespace name at
    # compile time. Only own-component schemas (CsvNamespaceGroup) survive as a union.
    WithJsonSchema(
        {
            "type": "string",
            "anyOf": [
                {"enum": list(STATIC_CSV_NAMESPACES)},
                {"pattern": CLINVAR_NS_PATTERN.pattern},
                {"pattern": CALIBRATION_NS_PATTERN.pattern},
            ],
        }
    ),
]
"""The type for a ``namespaces`` query-parameter element on any CSV endpoint.

Use ``Optional[List[CsvNamespaceStr]]`` and FastAPI handles validation and documentation.
"""


CSV_NAMESPACES_PARAM_DESCRIPTION = (
    "One or more groups of columns to include. Naming any group replaces the default set rather than "
    "adding to it, so list every group you want. Fixed groups: "
    + ", ".join(f'"{ns}"' for ns in STATIC_CSV_NAMESPACES)
    + '. Versioned groups: "clinvar.YEAR_MONTH" (e.g. "clinvar.2024_01") for one ClinVar release, and '
    '"calibration.<calibration urn>" for one score calibration\'s functional and ACMG interpretation. '
    "Several ClinVar and calibration namespaces may be requested at once; each carries its release or "
    "URN in the column header. To discover which namespaces are available for a record, query the "
    "`csv-namespaces` endpoint."
)
"""Shared OpenAPI description for the ``namespaces`` query parameter on every CSV endpoint."""
