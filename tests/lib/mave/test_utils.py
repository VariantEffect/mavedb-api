import pytest

from mavedb.lib.mave.utils import is_csv_output_null


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expected",
    [
        (None, True),
        ("", True),
        ("  ", True),
        ("NA", True),
        ("na", True),
        ("None", True),
        ("none", True),
        ("NaN", True),
        ("nan", True),
        ("null", True),
        ("NULL", True),
        ("nil", True),
        ("N/A", True),
        ("undefined", True),
        ("1.5", False),
        ("0", False),
        ("hello", False),
        ("p.Met1Val", False),
    ],
)
def test_is_csv_output_null(value, expected):
    assert bool(is_csv_output_null(value)) is expected
