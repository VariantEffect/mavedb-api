import pytest

from mavedb.models.variant import Variant


@pytest.mark.unit
@pytest.mark.parametrize(
    "urn, expected",
    [("urn:mavedb:00000053-a-1#648022", 648022), ("tmp:0c8d1a2e-5f3b-4c6d-9e7f-1a2b3c4d5e6f#1", 1), (None, None)],
)
def test_variant_number_is_the_integer_after_the_hash(urn, expected):
    assert Variant(urn=urn).variant_number == expected
