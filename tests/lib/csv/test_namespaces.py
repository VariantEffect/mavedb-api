import pytest

from pydantic import TypeAdapter, ValidationError

from mavedb.lib.csv.namespaces import (
    CSV_NAMESPACE_ERROR_MESSAGE,
    STATIC_CSV_NAMESPACES,
    CsvNamespace,
    CsvNamespaceStr,
    calibration_namespace_for_urn,
    clinvar_namespace_for_db_version,
    clinvar_namespace_sort_key,
    is_valid_csv_namespace,
    parse_calibration_namespace,
    parse_clinvar_db_version,
    parse_clinvar_namespace,
)
from tests.helpers.constants import VALID_CALIBRATION_URN

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# ClinVar namespaces
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ns, expected",
    [
        ("clinvar.2024_01", "01_2024"),
        ("clinvar.2015_12", "12_2015"),
        ("clinvar.2026_06", "06_2026"),
        ("clinvar.2024_00", None),
        ("clinvar.2024_13", None),
        ("scores", None),
        ("clinvar", None),
        ("clinvar.2024_01.extra", None),
        ("", None),
    ],
)
def test_parse_clinvar_namespace(ns, expected):
    assert parse_clinvar_namespace(ns) == expected


@pytest.mark.parametrize(
    "db_version, expected",
    [
        ("01_2024", (2024, 1)),
        ("12_2015", (2015, 12)),
        ("2024", None),
        ("aa_bbbb", None),
        ("", None),
    ],
)
def test_parse_clinvar_db_version(db_version, expected):
    assert parse_clinvar_db_version(db_version) == expected


@pytest.mark.parametrize(
    "db_version, expected",
    [
        ("01_2024", "clinvar.2024_01"),
        ("11_2024", "clinvar.2024_11"),
        ("2_2025", "clinvar.2025_02"),
        ("nonsense", None),
    ],
)
def test_clinvar_namespace_for_db_version(db_version, expected):
    assert clinvar_namespace_for_db_version(db_version) == expected


def test_clinvar_namespace_round_trips_through_db_version():
    assert clinvar_namespace_for_db_version(parse_clinvar_namespace("clinvar.2024_01")) == "clinvar.2024_01"


@pytest.mark.parametrize(
    "ns, expected",
    [
        ("clinvar.2024_01", (2024, 1)),
        ("clinvar.2025_10", (2025, 10)),
        # Not a release namespace at all: must sort below every real one rather than raise.
        ("scores", (-1, -1)),
        ("calibration.urn:mavedb:calibration-abc", (-1, -1)),
        ("clinvar.2024_13", (-1, -1)),
    ],
)
def test_clinvar_namespace_sort_key(ns, expected):
    assert clinvar_namespace_sort_key(ns) == expected


def test_clinvar_namespaces_sort_chronologically():
    namespaces = ["clinvar.2024_11", "clinvar.2025_02", "clinvar.2024_02", "clinvar.2025_10"]

    assert max(namespaces, key=clinvar_namespace_sort_key) == "clinvar.2025_10"
    assert sorted(namespaces, key=clinvar_namespace_sort_key) == [
        "clinvar.2024_02",
        "clinvar.2024_11",
        "clinvar.2025_02",
        "clinvar.2025_10",
    ]


def test_sort_key_beats_string_ordering_on_uneven_year_widths():
    """The year group in CLINVAR_NS_PATTERN is unpadded, so string ordering is not chronological.

    This is the decay this key exists to prevent: as plain strings "clinvar.999_12" sorts above
    "clinvar.2025_01", which would make a malformed release look like the newest one and hand it the
    picker's default selection.
    """
    namespaces = ["clinvar.2025_01", "clinvar.999_12"]

    assert max(namespaces) == "clinvar.999_12"
    assert max(namespaces, key=clinvar_namespace_sort_key) == "clinvar.2025_01"


def test_undatable_namespace_never_sorts_newest():
    assert max(["clinvar.2024_01", "scores"], key=clinvar_namespace_sort_key) == "clinvar.2024_01"


# ---------------------------------------------------------------------------
# Calibration namespaces
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ns, expected",
    [
        (f"calibration.{VALID_CALIBRATION_URN}", VALID_CALIBRATION_URN),
        (
            "calibration.urn:mavedb:calibration-00000000-0000-0000-0000-000000000000",
            "urn:mavedb:calibration-00000000-0000-0000-0000-000000000000",
        ),
        ("calibration.urn:mavedb:00000001-a-1", None),
        ("calibration.not-a-urn", None),
        ("calibration.", None),
        ("calibration", None),
        ("scores", None),
        ("", None),
    ],
)
def test_parse_calibration_namespace(ns, expected):
    assert parse_calibration_namespace(ns) == expected


def test_calibration_namespace_round_trips():
    namespace = calibration_namespace_for_urn(VALID_CALIBRATION_URN)

    assert namespace == f"calibration.{VALID_CALIBRATION_URN}"
    assert parse_calibration_namespace(namespace) == VALID_CALIBRATION_URN


@pytest.mark.parametrize(
    "urn",
    [
        "urn:mavedb:collection-79471b5b-2dbd-4a96-833c-c33023862437",
        "urn:mavedb:00000001-a-1",
        "urn:mavedb:calibration-short",
    ],
)
def test_non_calibration_urns_do_not_form_valid_namespaces(urn):
    assert parse_calibration_namespace(f"calibration.{urn}") is None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ns", STATIC_CSV_NAMESPACES)
def test_static_namespaces_are_valid(ns):
    assert is_valid_csv_namespace(ns)


@pytest.mark.parametrize(
    "ns",
    [
        "clinvar.2024_01",
        f"calibration.{VALID_CALIBRATION_URN}",
    ],
)
def test_parameterized_namespaces_are_valid(ns):
    assert is_valid_csv_namespace(ns)


@pytest.mark.parametrize(
    "ns",
    [
        "bogus",
        "clinvar",
        "clinvar.2024_13",
        "calibration",
        "calibration.nope",
        "SCORES",
        "",
    ],
)
def test_invalid_namespaces_are_rejected(ns):
    assert not is_valid_csv_namespace(ns)


def test_static_namespaces_are_the_enum_values():
    """The tuple and the enum must not drift; the tuple feeds the published JSON schema."""
    assert STATIC_CSV_NAMESPACES == tuple(ns.value for ns in CsvNamespace)


def test_enum_members_are_usable_as_plain_strings():
    """`plan_csv_columns` keys its column dict by these, so they must behave as their values."""
    assert CsvNamespace.SCORE_SET == "score_set"
    assert f"{CsvNamespace.SCORE_SET}" == "score_set"
    assert {"score_set": 1}[CsvNamespace.SCORE_SET] == 1


# ---------------------------------------------------------------------------
# CsvNamespaceStr — the validated query-parameter type
# ---------------------------------------------------------------------------


_ADAPTER = TypeAdapter(CsvNamespaceStr)


@pytest.mark.parametrize(
    "ns",
    list(STATIC_CSV_NAMESPACES) + ["clinvar.2024_01", f"calibration.{VALID_CALIBRATION_URN}"],
)
def test_validated_type_accepts_valid_namespaces(ns):
    assert _ADAPTER.validate_python(ns) == ns


@pytest.mark.parametrize("ns", ["bogus", "clinvar", "clinvar.2024_13", "calibration", "calibration.nope", ""])
def test_validated_type_rejects_invalid_namespaces(ns):
    with pytest.raises(ValidationError) as exc_info:
        _ADAPTER.validate_python(ns)

    assert CSV_NAMESPACE_ERROR_MESSAGE in str(exc_info.value)


def test_error_message_names_the_whole_vocabulary():
    for ns in STATIC_CSV_NAMESPACES:
        assert f'"{ns}"' in CSV_NAMESPACE_ERROR_MESSAGE
    assert "clinvar.YEAR_MONTH" in CSV_NAMESPACE_ERROR_MESSAGE
    assert "calibration.<calibration urn>" in CSV_NAMESPACE_ERROR_MESSAGE
