import re
import pytest

from mavedb.lib.validation.urn_re import (
    MAVEDB_TMP_URN_RE,
    MAVEDB_EXPERIMENT_SET_URN_RE,
    MAVEDB_EXPERIMENT_URN_RE,
    MAVEDB_SCORE_SET_URN_RE,
    MAVEDB_VARIANT_URN_RE,
    MAVEDB_ANY_URN_RE,
)


VALID_TMP_URNS = ("tmp:2ba10de7-18e5-4f5c-adf6-e08a2f71277b",)
VALID_EXPERIMENT_SET_URNS = (
    "urn:mavedb:00000011",
    "urn:mavedb:12345678",
)
VALID_EXPERIMENT_URNS = (
    "urn:mavedb:00000001-a",
    "urn:mavedb:88888888-zx",
    "urn:mavedb:00000111-0",
)
VALID_SCORE_SET_URNS = (
    "urn:mavedb:00000008-b-2",
    "urn:mavedb:11112222-x-99",
    "urn:mavedb:00000091-0-22",
)
VALID_VARIANT_URNS = (
    "urn:mavedb:00000123-c-1#1234",
    "urn:mavedb:00004444-0-4#44",
)
INVALID_URNS = (
    "tmp:mavedb:00000022",
    "urn:mavedb:1234567",
    "urn:mavedb:123456789",
    "urn:mavedb:00000001-a-01",
    "urn:mavedb:00000011-1",
    "urn:mavedb:00000001-a-0",
    "urn:mavedb:00000013-a-2#0",
    "urn:mavedb:00000103-d-1#0001",
)

VALID_COMBINATIONS = (
    [(MAVEDB_TMP_URN_RE, u) for u in VALID_TMP_URNS]
    + [(MAVEDB_ANY_URN_RE, u) for u in VALID_TMP_URNS]
    + [(MAVEDB_EXPERIMENT_SET_URN_RE, u) for u in VALID_EXPERIMENT_SET_URNS]
    + [(MAVEDB_ANY_URN_RE, u) for u in VALID_EXPERIMENT_SET_URNS]
    + [(MAVEDB_EXPERIMENT_URN_RE, u) for u in VALID_EXPERIMENT_URNS]
    + [(MAVEDB_ANY_URN_RE, u) for u in VALID_EXPERIMENT_URNS]
    + [(MAVEDB_SCORE_SET_URN_RE, u) for u in VALID_SCORE_SET_URNS]
    + [(MAVEDB_ANY_URN_RE, u) for u in VALID_SCORE_SET_URNS]
    + [(MAVEDB_VARIANT_URN_RE, u) for u in VALID_VARIANT_URNS]
    + [(MAVEDB_ANY_URN_RE, u) for u in VALID_VARIANT_URNS]
)

INVALID_COMBINATIONS = (
    [(MAVEDB_EXPERIMENT_SET_URN_RE, u) for u in VALID_TMP_URNS]
    + [(MAVEDB_EXPERIMENT_URN_RE, u) for u in VALID_TMP_URNS]
    + [(MAVEDB_SCORE_SET_URN_RE, u) for u in VALID_TMP_URNS]
    + [(MAVEDB_VARIANT_URN_RE, u) for u in VALID_TMP_URNS]
    + [(MAVEDB_TMP_URN_RE, u) for u in VALID_EXPERIMENT_SET_URNS]
    + [(MAVEDB_EXPERIMENT_URN_RE, u) for u in VALID_EXPERIMENT_SET_URNS]
    + [(MAVEDB_SCORE_SET_URN_RE, u) for u in VALID_EXPERIMENT_SET_URNS]
    + [(MAVEDB_VARIANT_URN_RE, u) for u in VALID_EXPERIMENT_SET_URNS]
    + [(MAVEDB_TMP_URN_RE, u) for u in VALID_EXPERIMENT_URNS]
    + [(MAVEDB_EXPERIMENT_SET_URN_RE, u) for u in VALID_EXPERIMENT_URNS]
    + [(MAVEDB_SCORE_SET_URN_RE, u) for u in VALID_EXPERIMENT_URNS]
    + [(MAVEDB_VARIANT_URN_RE, u) for u in VALID_EXPERIMENT_URNS]
    + [(MAVEDB_TMP_URN_RE, u) for u in VALID_SCORE_SET_URNS]
    + [(MAVEDB_EXPERIMENT_SET_URN_RE, u) for u in VALID_SCORE_SET_URNS]
    + [(MAVEDB_EXPERIMENT_URN_RE, u) for u in VALID_SCORE_SET_URNS]
    + [(MAVEDB_VARIANT_URN_RE, u) for u in VALID_SCORE_SET_URNS]
    + [(MAVEDB_TMP_URN_RE, u) for u in VALID_VARIANT_URNS]
    + [(MAVEDB_EXPERIMENT_SET_URN_RE, u) for u in VALID_VARIANT_URNS]
    + [(MAVEDB_EXPERIMENT_URN_RE, u) for u in VALID_VARIANT_URNS]
    + [(MAVEDB_SCORE_SET_URN_RE, u) for u in VALID_VARIANT_URNS]
)


@pytest.mark.parametrize("test_pattern,test_input", VALID_COMBINATIONS)
def test_valid_urns(test_pattern, test_input):
    assert isinstance(test_pattern.fullmatch(test_input), re.Match)


@pytest.mark.parametrize("test_pattern,test_input", INVALID_COMBINATIONS)
def test_invalid_urns(test_pattern, test_input):
    assert test_pattern.fullmatch(test_input) is None
