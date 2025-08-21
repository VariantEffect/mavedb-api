import pytest  # noqa: F401

from mavedb.lib.annotation.condition import generic_disease_condition, generic_disease_condition_iri
from mavedb.lib.annotation.constants import GENERIC_DISEASE_MEDGEN_CODE, MEDGEN_SYSTEM


def test_generic_disease_condition_iri():
    iri = generic_disease_condition_iri()
    expected_root = f"http://identifiers.org/medgen/{GENERIC_DISEASE_MEDGEN_CODE}"

    assert iri.root == expected_root


def test_generic_disease_condition():
    condition = generic_disease_condition()

    assert condition.root.conceptType == "Disease"
    coding = condition.root.primaryCoding

    assert coding.code.root == GENERIC_DISEASE_MEDGEN_CODE
    assert coding.system == MEDGEN_SYSTEM
    assert len(coding.iris) == 1
    assert f"http://identifiers.org/medgen/{GENERIC_DISEASE_MEDGEN_CODE}" in [ciri.root for ciri in coding.iris]
